"""Main entry point for maico-enocean-mqtt bridge.

Orchestrates EnOcean serial, MAICO MSC protocol, MQTT client, and Web-UI.
Uses direct MSC 27 20 commands. All devices are treated equally — master/slave
relationships are detected automatically from 27 00 sync traffic.

Device state model lives in devices.py (DeviceTable, DeviceStatus); sleep/boost
timers in timers.py (TimerManager).
"""

import asyncio
import logging
import os
import signal
import sys
import time

from . import __version__

from .config import AppConfig, DeviceConfig, load_config
from .devices import (
    AVAILABILITY_CHECK_INTERVAL,
    DEVICE_OFFLINE_TIMEOUT,
    SYNC_ROLE_TIMEOUT,
    ConnectionStatus,
    DeviceStatus,
    DeviceTable,
    OPPOSITE_DIR as _OPPOSITE_DIR,
    SYNC_STATUS_MAP as _SYNC_STATUS_MAP,
    is_valid_device_id as _is_valid_device_id,
)
from .enocean_serial import EnOceanSerial, RORG_MSC
from .events import EventLog
from .maico_protocol import (
    MscType,
    VentilationMode,
    VentilationState,
    AirflowDirection,
    build_set_level,
    build_status_report,
    build_teach_in_response,
    id_to_str,
    parse_msc_telegram,
    str_to_id,
)
from .mqtt_client import MqttClient
from .timers import TimerManager

logger = logging.getLogger(__name__)

# Localised phrasing for the UI event log. The system language drives the bridge,
# Home Assistant and the web UI alike, so composing the message at record time is
# consistent with whatever the user sees in the /logs page.
_EVENT_TEXT = {
    "de": {
        "level": "{dev} → Stufe {level} ({mode})",
        "level_off": "{dev} → Aus",
        "mode": "{dev} → {mode}",
        "rls_change": "RLS → Stufe {level} ({mode}), alle Geräte",
        "available": "{dev} erreichbar",
        "unavailable": "{dev} nicht erreichbar",
        "teach_resp": "Teach-in-Antwort von {id}",
        "device_found": "Gerät {id} im Anlernmodus erkannt",
        "rls_paired": "RLS-Fernbedienung {id} gepairt",
        "mqtt_up": "MQTT-Verbindung wiederhergestellt",
        "mqtt_down": "MQTT-Verbindung verloren",
        "restart": "🔄 Bridge gestartet (v{version})",
        "modes": {
            "off": "Aus", "heat_exchanger": "Wärmetauscher", "summer": "Sommer",
            "sleep_heat": "Schlaf (WT)", "sleep_summer": "Schlaf (Sommer)", "boost": "Stoßlüftung",
        },
    },
    "en": {
        "level": "{dev} → level {level} ({mode})",
        "level_off": "{dev} → off",
        "mode": "{dev} → {mode}",
        "rls_change": "RLS → level {level} ({mode}), all devices",
        "available": "{dev} available",
        "unavailable": "{dev} unavailable",
        "teach_resp": "Teach-in response from {id}",
        "device_found": "Device {id} detected in learn mode",
        "rls_paired": "RLS remote {id} paired",
        "mqtt_up": "MQTT connection restored",
        "mqtt_down": "MQTT connection lost",
        "restart": "🔄 Bridge started (v{version})",
        "modes": {
            "off": "Off", "heat_exchanger": "Heat exchanger", "summer": "Summer",
            "sleep_heat": "Sleep (HX)", "sleep_summer": "Sleep (summer)", "boost": "Boost",
        },
    },
}


class MaicoMqttBridge:
    """Bridge between MAICO PP 45 RC units and Home Assistant via MQTT."""

    def __init__(self, config: AppConfig):
        self.config = config
        self.serial = EnOceanSerial(config.enocean.device)
        self.mqtt = MqttClient(config, self)
        self.devices = DeviceTable()
        self.timers = TimerManager(
            on_expire=self._restore_mode,
            on_publish=lambda name, minutes: self.mqtt.publish_timer(name, minutes),
        )
        self._running = False
        self._poll_task: asyncio.Task | None = None
        self._web_app = None
        self._last_rls_state: VentilationState | None = None
        self._rls_teach_in_active: bool = False
        self._rls_teach_in_result: str | None = None
        self._polling_paused: bool = False
        self._discovery_enabled: bool = False  # only register unknown senders during explicit pairing
        self._poll_skip_until: float = 0.0  # skip polls until this timestamp (after RLS sync)
        self._availability: dict[str, bool] = {}  # last-published availability per device
        self._availability_task: asyncio.Task | None = None
        # Event log surfaced on the /logs page, persisted next to the config so
        # it survives restarts (degrades to memory-only if the dir isn't writable).
        data_dir = os.path.dirname(getattr(self.config, "config_path", "") or "/data/config.yaml") or "."
        self.events = EventLog(path=os.path.join(data_dir, "events.jsonl"))
        # HA discovery shape (role) last published per device — drives
        # confirm-before-emit re-publishing on role changes. Roles are persisted
        # so the correct pair shape is published on the first discovery after a
        # restart (no transient where a slave briefly appears controllable).
        self._disc_role: dict[str, str] = {}
        self._roles_path = os.path.join(data_dir, "roles.json")

    # --- Backwards-compatible accessors over the extracted state/timer stores.
    # Handlers, web.py and teach_in.py reference these names directly.
    @property
    def _states(self) -> dict[str, VentilationState]:
        return self.devices.states

    @property
    def _device_status(self) -> dict[str, DeviceStatus]:
        return self.devices.status

    @property
    def _id_to_name(self) -> dict[str, str]:
        return self.devices.id_to_name

    @property
    def _name_to_config(self) -> dict[str, DeviceConfig]:
        return self.devices.name_to_config

    @property
    def _state_known(self) -> set[str]:
        return self.devices.state_known

    @property
    def _saved_states(self) -> dict[str, VentilationState]:
        return self.timers.saved

    @property
    def _timer_end(self) -> dict[str, float]:
        return self.timers.end

    @property
    def _mode_timers(self) -> dict[str, asyncio.Task]:
        return self.timers.timers

    def dispatch(self, fn, *args) -> None:
        """Run a bridge command on the asyncio loop thread.

        MQTT command callbacks fire on the paho network thread. Bridge state
        (_states, timers, ...) and serial sends must only be touched from the
        event loop, so we marshal those callbacks onto it. Falls back to a
        direct call when no loop is running (e.g. in unit tests).
        """
        loop = getattr(self, "_loop", None)
        if loop and loop.is_running():
            loop.call_soon_threadsafe(fn, *args)
        else:
            fn(*args)

    # --- Event log helpers ---

    def _evt_lang(self) -> dict:
        return _EVENT_TEXT.get(self.config.language, _EVENT_TEXT["de"])

    def _mode_label(self, mode: VentilationMode) -> str:
        return self._evt_lang()["modes"].get(mode.value, mode.value)

    def _friendly(self, device_name: str) -> str:
        dev = self._name_to_config.get(device_name)
        return (dev.friendly_name or device_name) if dev else device_name

    def _resolve_to_master(self, device_name: str) -> str:
        """Map a slave device to its master.

        A slave physically mirrors its master's fan level (opposite airflow,
        enforced by the 27 00 sync), so it cannot be driven independently — any
        command must target the master. Returns the master's name for an active
        slave, otherwise the name unchanged. If the sync has gone stale the role
        decays to "standalone" and the device is controllable directly again."""
        ds = self._device_status.get(device_name)
        if ds and ds.detected_role == "slave" and ds.synced_from:
            master = self._id_to_name.get(ds.synced_from)
            if master and master != device_name:
                return master
        return device_name

    def record_event(self, category: str, key: str, *, device: str | None = None,
                     source: str | None = None, **kw) -> None:
        """Append a localised entry to the in-memory event log."""
        message = self._evt_lang()[key].format(**kw)
        self.events.add(category, message, device=device, source=source)

    def record_mqtt_event(self, connected: bool) -> None:
        """Called from the MQTT client thread on (re)connect / disconnect."""
        self.record_event("mqtt", "mqtt_up" if connected else "mqtt_down", source="system")

    @property
    def states(self) -> dict[str, VentilationState]:
        return self._states

    @property
    def device_status(self) -> dict[str, DeviceStatus]:
        return self._device_status

    def timer_remaining_minutes(self, device_name: str) -> int:
        """Return remaining timer minutes for a device, or 0 if no timer."""
        return self.timers.remaining_minutes(device_name)

    def _setup_device_mappings(self) -> None:
        for device in self.config.devices:
            self.devices.register(device)

        if self.config.remote.device_id:
            rls_id = self.config.remote.device_id.upper().replace(":", "")
            self.devices.id_to_name[rls_id] = self.config.remote.name

    def _handle_packet(self, pkt: dict) -> None:
        if pkt.get('type') != 'radio':
            return

        rorg = pkt.get('rorg')
        sender = pkt.get('sender', [])
        user_data = pkt.get('user_data', [])
        dest = pkt.get('dest')
        sender_str = id_to_str(sender)

        # Ignore our own transmissions (fake PP 45 ID echo)
        if self.serial.base_id:
            fake = list(self.serial.base_id)
            fake[3] = (fake[3] + 1) & 0xFF
            if sender == fake:
                return

        # Reject packets from invalid sender IDs (broadcast, noise)
        if not _is_valid_device_id(sender_str):
            logger.debug("Ignoring packet from invalid sender: %s", sender_str)
            return

        # Record receive signal strength for any telegram from a known device.
        # (New senders are resolved later in the handlers; their RSSI lands on
        # the next packet, which is fine for a slowly-changing link metric.)
        rssi = pkt.get('rssi')
        if rssi is not None:
            name = self._id_to_name.get(sender_str)
            if name:
                self._device_status.setdefault(name, DeviceStatus()).rssi = rssi

        if rorg == RORG_MSC:
            telegram = parse_msc_telegram(user_data, sender, dest)
            if not telegram:
                return

            if telegram.msg_type == MscType.STATUS_REPORT:
                dest_str = id_to_str(dest) if dest else None
                raw_hex = " ".join(f"{b:02X}" for b in user_data)
                logger.debug("27 10 raw %s → %s: %s", sender_str, dest_str, raw_hex)
                self._handle_status_report(sender_str, telegram.state, dest_str)

            elif telegram.msg_type == MscType.SET_LEVEL:
                dest_str = id_to_str(dest) if dest else "?"
                logger.debug("27 20 raw %s → %s: %s",
                             sender_str, dest_str,
                             " ".join(f"{b:02X}" for b in user_data))
                self._handle_external_command(sender_str, dest, telegram.state)

            elif telegram.msg_type == MscType.MASTER_SLAVE_SYNC:
                dest_str = id_to_str(dest) if dest else "?"
                raw_hex = " ".join(f"{b:02X}" for b in user_data)
                logger.debug("27 00 sync %s → %s: %s", sender_str, dest_str, raw_hex)
                self._handle_sync(sender_str, dest, user_data)

            elif telegram.msg_type == MscType.TEACH_IN_SCAN:
                self._handle_rls_scan(sender_str)

            elif telegram.msg_type == MscType.TEACH_IN_RESPONSE:
                self._handle_teach_in_response(sender_str, dest)

            elif telegram.msg_type == MscType.DEVICE_ANNOUNCE:
                self._handle_device_announce(sender_str)

    def _auto_discover_device(self, device_id_str: str) -> str:
        """Auto-discover a new MAICO device from traffic. Returns device name."""
        # Skip if it's the RLS remote, our own base ID, or our fake PP 45 ID
        rls_id = self.config.remote.device_id.upper().replace(":", "")
        our_base = id_to_str(self.serial.base_id) if self.serial.base_id else ""
        our_fake = ""
        if self.serial.base_id:
            fake = list(self.serial.base_id)
            fake[3] = (fake[3] + 1) & 0xFF
            our_fake = id_to_str(fake)
        if device_id_str in (rls_id, our_base, our_fake):
            return ""

        # Reject invalid/dangerous IDs (broadcast, noise, corrupt data)
        if not _is_valid_device_id(device_id_str):
            logger.debug("Auto-discovery rejected invalid ID: %s", device_id_str)
            return ""

        # Already known?
        if device_id_str in self._id_to_name:
            return self._id_to_name[device_id_str]

        # Check if already in config by device_id
        clean_id = device_id_str.replace(":", "")
        existing = self.config.get_device_by_id(clean_id)
        if existing:
            self._id_to_name[device_id_str] = existing.name
            self._name_to_config[existing.name] = existing
            return existing.name

        # Passive discovery from RF traffic is the root cause of "ghost devices":
        # corrupt/byte-shifted sender or dest IDs that pass CRC8 but belong to no
        # real unit accumulate in config and flood the air with polls. We therefore
        # only register brand-new devices during an explicit pairing window
        # (set via the Web-UI). In normal operation unknown senders are ignored;
        # known devices were already resolved above, so this never affects them.
        if not self._discovery_enabled or self._polling_paused:
            logger.debug("Auto-discovery skipped (pairing mode off): %s", device_id_str)
            return ""

        # New device — auto-register with ID as name
        name = f"PP45_{clean_id[-4:]}"
        friendly_name = f"PP 45 RC ({clean_id[-4:]})"
        device = DeviceConfig(name=name, friendly_name=friendly_name, device_id=clean_id)
        self.config.add_device(device)
        self.config.save()

        self.devices.register(device)

        # Publish HA discovery (role-aware: new device starts standalone)
        self.publish_discovery(device.name)

        # Set default level 2
        self.set_level(name, 2)

        logger.info("Auto-discovered new device: %s (%s)", name, device_id_str)
        self.mqtt.publish_event("device_discovered", {
            "device_id": device_id_str,
            "name": name,
        })
        return name

    def _handle_status_report(self, sender_str: str, state: VentilationState | None,
                              dest_str: str | None = None) -> None:
        name = self._id_to_name.get(sender_str)
        if not name:
            name = self._auto_discover_device(sender_str)
            if not name:
                return

        if state:
            # 27 10 status reports do NOT reliably encode the live airflow
            # direction — verified from traffic, they always report the exhaust
            # form (0x0X) regardless of the unit's actual reversal phase. Only
            # 27 00 sync telegrams carry the real, alternating direction, so a
            # status report must never change it. Devices without a sync partner
            # therefore stay UNKNOWN (their live direction is not observable).
            existing = self._states.get(name)
            state.direction = existing.direction if existing else AirflowDirection.UNKNOWN
            self._states[name] = state
            self._state_known.add(name)
            self.mqtt.publish_state(name, state)

            # A slave physically mirrors the master's mode and fan level (its
            # airflow runs opposite, tracked separately via 27 00 sync). Mirror
            # every change, not just on/off — a heat_exchanger<->summer switch
            # while both run would otherwise leave the slave's published state
            # stale, making downstream idempotency guards (HA) re-issue the
            # command forever.
            ds = self._device_status.get(name)
            if ds and ds.syncs_to:
                slave_name = self._id_to_name.get(ds.syncs_to)
                if slave_name:
                    slave_state = self._states.get(slave_name)
                    if slave_state and (slave_state.mode != state.mode
                                        or slave_state.fan_level != state.fan_level):
                        slave_state.mode = state.mode
                        slave_state.fan_level = state.fan_level
                        self.mqtt.publish_state(slave_name, slave_state)

        # Update connection status
        now = time.time()
        ds = self._device_status.setdefault(name, DeviceStatus())
        ds.last_seen = now

        our_base_str = id_to_str(self.serial.base_id) if self.serial.base_id else ""
        rls_id_str = self.config.remote.device_id.upper().replace(":", "")

        if dest_str and dest_str == our_base_str:
            ds.last_response_to_us = now
            ds.response_count += 1
            if ds.connection != ConnectionStatus.MANAGED:
                ds.connection = ConnectionStatus.MANAGED
                logger.info("%s is now MANAGED (responds to our polls)", name)
                self.mqtt.publish_connection_status(name, ds)
        elif dest_str and dest_str == rls_id_str:
            if ds.connection == ConnectionStatus.UNKNOWN:
                ds.connection = ConnectionStatus.PASSIVE
                logger.info("%s is PASSIVE (seen via RLS traffic)", name)
                self.mqtt.publish_connection_status(name, ds)
        elif ds.connection == ConnectionStatus.UNKNOWN:
            ds.connection = ConnectionStatus.PASSIVE
            self.mqtt.publish_connection_status(name, ds)

        if state:
            logger.info("%s [%s]: mode=%s level=%d dir=%s",
                        name, ds.connection, state.mode.value, state.fan_level,
                        state.direction.value)

        # Device is definitely reachable — update availability immediately
        self._publish_availability_if_changed(name)

    def _send_rls_status_report(self, state: VentilationState) -> None:
        """Send 27 10 status report back to RLS, pretending to be a PP 45."""
        if not self.serial.base_id or not self.config.remote.device_id:
            return
        if self._polling_paused:
            return
        fake_id = list(self.serial.base_id)
        fake_id[3] = (fake_id[3] + 1) & 0xFF
        rls_id = str_to_id(self.config.remote.device_id)
        data, optional = build_status_report(fake_id, rls_id, state.fan_level, state.mode)
        self.serial.send(data, optional)

    def _handle_external_command(self, sender_str: str, dest: list[int] | None,
                                 state: VentilationState | None) -> None:
        rls_id = self.config.remote.device_id.upper().replace(":", "")
        if sender_str != rls_id or state is None:
            return

        # Always respond with status report so the RLS knows we're alive
        self._send_rls_status_report(state)

        if not self.config.rls_global_sync:
            logger.info("RLS → level=%d (sync disabled)", state.fan_level)
            return

        # Change Detection — only react on actual changes
        last = self._last_rls_state
        if last and last.fan_level == state.fan_level and last.mode == state.mode:
            return  # Periodic broadcast, no change

        self._last_rls_state = VentilationState(
            mode=state.mode, fan_level=state.fan_level
        )

        logger.info("RLS change: level=%d mode=%s → syncing all devices",
                     state.fan_level, state.mode.value)
        self.record_event("control", "rls_change", source="rls",
                          level=state.fan_level, mode=self._mode_label(state.mode))

        # Skip polls briefly so they don't overwrite the RLS sync
        self._poll_skip_until = time.time() + self.config.poll_interval + 2

        # Sync all master/standalone devices
        for device in self.config.devices:
            dev_status = self._device_status.get(device.name)
            if dev_status and dev_status.detected_role == "slave":
                continue
            # Set mode first if changed, then level — avoid sending conflicting commands
            current = self._states.get(device.name)
            if state.mode in (VentilationMode.HEAT_EXCHANGER, VentilationMode.SUMMER):
                if not current or current.mode != state.mode:
                    self.set_mode(device.name, state.mode, source="rls")
            self.set_level(device.name, state.fan_level, source="rls")

    def discovery_role(self, name: str) -> tuple[str, DeviceConfig | None]:
        """Detected role + master config used to shape HA discovery.

        Returns ("slave", master_cfg) only when the master is known; otherwise
        the device is treated as standalone (can't group without a master)."""
        ds = self._device_status.get(name)
        role = ds.detected_role if ds else "standalone"
        if role == "slave" and ds and ds.synced_from:
            master_name = self._id_to_name.get(ds.synced_from)
            master = self._name_to_config.get(master_name) if master_name else None
            if master is not None:
                return "slave", master
            return "standalone", None
        return role, None

    def publish_discovery(self, name: str) -> None:
        """Publish HA discovery for one device in its current role shape."""
        cfg = self._name_to_config.get(name)
        if not cfg:
            return
        role, master = self.discovery_role(name)
        self._disc_role[name] = role
        self.mqtt.publish_device_discovery(cfg, role=role, master=master)

    def refresh_discovery(self, name: str) -> None:
        """Re-publish discovery only when the role shape changed (confirm-before-emit)
        and persist roles so a restart restores the shape."""
        role, _ = self.discovery_role(name)
        if self._disc_role.get(name) == role:
            return
        self.publish_discovery(name)
        self._save_roles()

    def _save_roles(self) -> None:
        import json
        data = {n: {"syncs_to": ds.syncs_to, "synced_from": ds.synced_from}
                for n, ds in self._device_status.items()
                if ds.syncs_to or ds.synced_from}
        try:
            with open(self._roles_path, "w", encoding="utf-8") as f:
                json.dump(data, f)
        except OSError:
            logger.debug("roles persist failed", exc_info=True)

    def _restore_roles(self) -> None:
        """Load persisted roles BEFORE the first discovery publish so the pair
        shape is correct from the start (persist-then-reconcile). Restored roles
        get a fresh sync grace window; live syncs re-confirm them, else they decay."""
        import json
        try:
            with open(self._roles_path, encoding="utf-8") as f:
                data = json.load(f)
        except (OSError, ValueError):
            return
        now = time.time()
        for name, rel in data.items():
            ds = self._device_status.setdefault(name, DeviceStatus())
            ds.syncs_to = rel.get("syncs_to")
            ds.synced_from = rel.get("synced_from")
            if ds.syncs_to or ds.synced_from:
                ds.last_sync = now

    def _handle_sync(self, sender_str: str, dest: list[int] | None,
                     user_data: list[int]) -> None:
        """Handle 27 00 master-slave sync. Extracts direction and detects relationships."""
        if not dest:
            return

        dest_str = id_to_str(dest)
        sender_name = self._id_to_name.get(sender_str) or self._auto_discover_device(sender_str)
        dest_name = self._id_to_name.get(dest_str) or self._auto_discover_device(dest_str)

        # Detect master-slave relationship
        now = time.time()
        if sender_name:
            ds = self._device_status.setdefault(sender_name, DeviceStatus())
            ds.last_sync = now
            if ds.syncs_to != dest_str:
                ds.syncs_to = dest_str
                logger.info("Detected: %s (%s) is master of %s (%s)",
                            sender_name, sender_str,
                            dest_name or "unknown", dest_str)
                self.mqtt.publish_connection_status(sender_name, ds)

        if dest_name:
            ds = self._device_status.setdefault(dest_name, DeviceStatus())
            ds.last_sync = now
            if ds.synced_from != sender_str:
                ds.synced_from = sender_str
                logger.info("Detected: %s (%s) is slave of %s (%s)",
                            dest_name, dest_str,
                            sender_name or "unknown", sender_str)
                self.mqtt.publish_connection_status(dest_name, ds)

        # Re-shape HA discovery if a role just changed (cheap no-op otherwise).
        if sender_name:
            self.refresh_discovery(sender_name)
        if dest_name:
            self.refresh_discovery(dest_name)

        # Decode direction + level from sync byte: 27 00 [status_byte] [timer] 00
        # Sync byte = master's direction (verified from traffic).
        # Slave direction: assumed opposite (physically required for heat exchange,
        # but not independently verified — slave sends no telemetry).
        if len(user_data) >= 3:
            status_byte = user_data[2]
            entry = _SYNC_STATUS_MAP.get(status_byte)
            if entry:
                master_dir, level = entry
                slave_dir = _OPPOSITE_DIR.get(master_dir, AirflowDirection.UNKNOWN)
                for dev_name, direction in ((sender_name, master_dir), (dest_name, slave_dir)):
                    if dev_name:
                        state = self._states.get(dev_name)
                        if not state:
                            state = VentilationState()
                            self._states[dev_name] = state
                        changed = False
                        if state.direction != direction:
                            state.direction = direction
                            changed = True
                        if level > 0 and (state.fan_level != level or state.mode == VentilationMode.OFF):
                            state.fan_level = level
                            if state.mode == VentilationMode.OFF:
                                state.mode = VentilationMode.HEAT_EXCHANGER
                            changed = True
                        if changed:
                            self._state_known.add(dev_name)
                            self.mqtt.publish_state(dev_name, state)
                            logger.info("%s from sync: dir=%s level=%d",
                                        dev_name, direction.value, level)
                        # Any sync traffic proves reachability (even for slaves)
                        self._publish_availability_if_changed(dev_name)

    def _handle_teach_in_response(self, sender_str: str, dest: list[int] | None) -> None:
        logger.info("Teach-in response from %s!", sender_str)
        self.record_event("pairing", "teach_resp", source="system", id=sender_str)
        self.mqtt.publish_event("teach_in", {
            "device_id": sender_str,
            "status": "paired",
        })

    def _handle_device_announce(self, sender_str: str) -> None:
        logger.info("Device announcement from %s (in learn mode)", sender_str)
        self.record_event("pairing", "device_found", source="system", id=sender_str)
        self.mqtt.publish_event("device_found", {
            "device_id": sender_str,
            "status": "learning",
        })

    def _handle_rls_scan(self, sender_str: str) -> None:
        """Handle 27 30 scan from RLS during RLS teach-in mode."""
        if not self._rls_teach_in_active:
            logger.debug("27 30 scan from %s (RLS teach-in not active, ignoring)", sender_str)
            return

        if not self.serial.base_id:
            return

        # Derive fake device ID from base_id + 1
        fake_id = list(self.serial.base_id)
        fake_id[3] = (fake_id[3] + 1) & 0xFF
        rls_id = str_to_id(sender_str)

        # Respond with 27 40 teach-in response
        data, optional = build_teach_in_response(fake_id, rls_id)
        self.serial.send(data, optional)
        logger.info("RLS teach-in: responded to scan from %s with fake ID %s",
                     sender_str, id_to_str(fake_id))

        # Confirm with 27 20 (set level 0)
        data, optional = build_set_level(fake_id, rls_id, 0)
        self.serial.send(data, optional)

        # Save RLS device ID
        self.config.remote.device_id = sender_str
        self._id_to_name[sender_str] = self.config.remote.name
        self.config.save()

        self._rls_teach_in_result = sender_str
        logger.info("RLS teach-in: paired with RLS %s", sender_str)
        self.record_event("pairing", "rls_paired", source="rls", id=sender_str)

    def set_level(self, device_name: str, level: int, source: str = "system") -> bool:
        device_name = self._resolve_to_master(device_name)
        device = self._name_to_config.get(device_name)
        if not device or not self.serial.base_id:
            logger.error("Cannot set level: device=%s base_id=%s", device_name, self.serial.base_id)
            return False

        if not _is_valid_device_id(device.device_id):
            logger.warning("Refusing to send to invalid device ID: %s (%s)",
                           device_name, device.device_id)
            return False

        # Manual level change cancels any active sleep/boost timer
        self._cancel_mode_timer(device_name)
        saved = self._saved_states.pop(device_name, None)

        level = max(0, min(5, level))
        current = self._states.get(device_name, VentilationState())
        # Restore saved mode if coming from boost/sleep, otherwise preserve current
        if saved and saved.mode in (VentilationMode.HEAT_EXCHANGER, VentilationMode.SUMMER):
            mode = saved.mode
        elif current.mode in (VentilationMode.HEAT_EXCHANGER, VentilationMode.SUMMER):
            mode = current.mode
        else:
            mode = VentilationMode.HEAT_EXCHANGER
        if level == 0:
            mode = VentilationMode.OFF

        device_id = str_to_id(device.device_id)
        data, optional = build_set_level(self.serial.base_id, device_id, level, mode)
        self.serial.send(data, optional)

        current.fan_level = level
        current.mode = mode
        self._states[device_name] = current
        self.mqtt.publish_state(device_name, current)
        logger.info("Set %s to level %d (%s)", device_name, level, mode.value)
        # RLS syncs every device at once; log one summary event for it instead
        # of N per-device entries (emitted at the RLS change site).
        if source != "rls":
            friendly = self._friendly(device_name)
            if level == 0:
                self.record_event("control", "level_off", device=friendly, source=source, dev=friendly)
            else:
                self.record_event("control", "level", device=friendly, source=source,
                                   dev=friendly, level=level, mode=self._mode_label(mode))
        return True

    def set_power(self, device_name: str, on: bool, source: str = "system") -> bool:
        if on:
            current = self._states.get(device_name, VentilationState())
            level = current.fan_level if current.fan_level > 0 else 1
            return self.set_level(device_name, level, source=source)
        else:
            return self.set_level(device_name, 0, source=source)

    # Timer defaults (seconds)
    BOOST_DURATION = 30 * 60   # 30 minutes
    SLEEP_DURATION = 2 * 3600  # 2 hours

    def set_mode(self, device_name: str, mode: VentilationMode, source: str = "system") -> bool:
        """Switch operating mode and send to device. Starts timer for sleep/boost."""
        device_name = self._resolve_to_master(device_name)
        device = self._name_to_config.get(device_name)
        if not device or not self.serial.base_id:
            return False

        current = self._states.get(device_name, VentilationState())

        # Cancel any existing timer for this device
        self._cancel_mode_timer(device_name)

        if mode == VentilationMode.BOOST:
            # Save current state for restore, then set level 5 in summer mode (Durchluft)
            self._saved_states[device_name] = VentilationState(
                mode=current.mode, fan_level=current.fan_level, direction=current.direction)
            base_mode = VentilationMode.SUMMER
            level = 5
            device_id = str_to_id(device.device_id)
            data, optional = build_set_level(self.serial.base_id, device_id, level, base_mode)
            self.serial.send(data, optional)
            current.mode = VentilationMode.BOOST
            current.fan_level = level
            self._start_mode_timer(device_name, self.BOOST_DURATION)

        elif mode in (VentilationMode.SLEEP_HEAT, VentilationMode.SLEEP_SUMMER):
            # Save current state, then activate sleep
            self._saved_states[device_name] = VentilationState(
                mode=current.mode, fan_level=current.fan_level, direction=current.direction)
            device_id = str_to_id(device.device_id)
            data, optional = build_set_level(self.serial.base_id, device_id, 0, mode)
            self.serial.send(data, optional)
            current.mode = mode
            current.fan_level = 0
            level = 0
            self._start_mode_timer(device_name, self.SLEEP_DURATION)

        else:
            # Normal mode switch (heat_exchanger/summer) — clear saved state
            self._saved_states.pop(device_name, None)
            level = current.fan_level if current.fan_level > 0 else 1
            device_id = str_to_id(device.device_id)
            data, optional = build_set_level(self.serial.base_id, device_id, level, mode)
            self.serial.send(data, optional)
            current.mode = mode
            current.fan_level = level

        self._states[device_name] = current
        self.mqtt.publish_state(device_name, current)
        logger.info("Set %s mode to %s (level %d)", device_name, mode.value, level)
        if source != "rls":
            friendly = self._friendly(device_name)
            self.record_event("control", "mode", device=friendly, source=source,
                               dev=friendly, mode=self._mode_label(mode))
        return True

    def _cancel_mode_timer(self, device_name: str) -> None:
        self.timers.cancel(device_name)

    def _start_mode_timer(self, device_name: str, duration: int) -> None:
        self.timers.start(device_name, duration)

    def _restore_mode(self, device_name: str) -> None:
        saved = self._saved_states.pop(device_name, None)
        if saved:
            mode = saved.mode if saved.mode in (VentilationMode.HEAT_EXCHANGER, VentilationMode.SUMMER) else VentilationMode.HEAT_EXCHANGER
            level = saved.fan_level if saved.fan_level > 0 else 1
            logger.info("%s: timer expired, restoring %s level %d", device_name, mode.value, level)
            self.set_level(device_name, level, source="timer")
        else:
            logger.info("%s: timer expired, no saved state — setting level 1", device_name)
            self.set_level(device_name, 1, source="timer")

    def send_scan(self) -> bool:
        if not self.serial.base_id:
            logger.error("Cannot scan: no base ID")
            return False
        from .maico_protocol import build_scan
        data, optional = build_scan(self.serial.base_id)
        self.serial.send(data, optional)
        logger.info("Teach-in scan sent")
        return True

    def _evaluate_device_availability(self, device_name: str, now: float | None = None) -> bool:
        """Return True if device should be considered available in HA."""
        if now is None:
            now = time.time()
        ds = self._device_status.get(device_name)
        if not ds:
            return False
        # Available if we've seen any traffic recently (status report, sync, or response)
        last = max(ds.last_seen, ds.last_sync)
        return last > 0 and (now - last) < DEVICE_OFFLINE_TIMEOUT

    def _publish_availability_if_changed(self, device_name: str) -> None:
        """Evaluate and publish device availability, only when it changes."""
        online = self._evaluate_device_availability(device_name)
        prev = self._availability.get(device_name)
        if prev != online:
            self._availability[device_name] = online
            self.mqtt.publish_device_availability(device_name, online)
            logger.info("%s availability → %s", device_name, "online" if online else "offline")
            # Skip the initial baseline publish (prev is None) so every boot
            # doesn't spam "unavailable" events before the first poll response.
            if prev is not None:
                friendly = self._friendly(device_name)
                self.record_event("connection", "available" if online else "unavailable",
                                  device=friendly, source="system", dev=friendly)

    async def _availability_loop(self) -> None:
        """Periodically expire stale roles, re-evaluate availability, republish."""
        while self._running:
            now = time.time()
            for device in self.config.devices:
                ds = self._device_status.get(device.name)
                if ds:
                    ds.expire_stale_role(now)
                    self.refresh_discovery(device.name)
                    # Republish connection status so last_seen / RSSI stay live in
                    # HA — they were previously only sent on a status *change*,
                    # which left "last seen" frozen at its first value.
                    self.mqtt.publish_connection_status(device.name, ds)
                self._publish_availability_if_changed(device.name)
            await asyncio.sleep(AVAILABILITY_CHECK_INTERVAL)

    async def _poll_loop(self) -> None:
        """Poll all configured devices with staggered 27 20 commands."""
        devices = self.config.devices
        if not devices:
            logger.warning("No devices configured, polling disabled")
            return

        interval = self.config.poll_interval
        stagger = interval / len(devices) if len(devices) > 1 else interval

        while self._running:
            for i, device in enumerate(devices):
                if not self._running:
                    break

                if self.serial.base_id and not self._polling_paused and time.time() > self._poll_skip_until:
                    # Safety: never send commands to invalid device IDs
                    if not _is_valid_device_id(device.device_id):
                        logger.debug("Poll skipped for invalid device ID: %s (%s)",
                                     device.name, device.device_id)
                        continue

                    current = self._states.get(device.name, VentilationState())
                    if device.name not in self._state_known:
                        # No state known yet — send a probe poll with safe defaults
                        # so the device responds with 27 10 and we learn its state
                        mode = VentilationMode.HEAT_EXCHANGER
                        level = current.fan_level if current.fan_level > 0 else 2
                    else:
                        level = current.fan_level
                        if current.mode == VentilationMode.BOOST:
                            mode = VentilationMode.SUMMER  # Boost = Durchluft
                        elif current.mode in (VentilationMode.HEAT_EXCHANGER, VentilationMode.SUMMER):
                            mode = current.mode
                        else:
                            mode = VentilationMode.HEAT_EXCHANGER
                    device_id = str_to_id(device.device_id)
                    data, optional = build_set_level(
                        self.serial.base_id, device_id, level, mode
                    )
                    self.serial.send(data, optional)
                    logger.debug("Poll %s (level %d, %s)", device.name, level, mode.value)

                    # Update timer sensor if active
                    remaining = self.timer_remaining_minutes(device.name)
                    if self.timers.has(device.name):
                        self.mqtt.publish_timer(device.name, remaining)

                if i < len(devices) - 1:
                    await asyncio.sleep(stagger)

            remaining = interval - stagger * (len(devices) - 1) if len(devices) > 1 else interval
            await asyncio.sleep(max(1, remaining))

    async def run(self) -> None:
        self._setup_device_mappings()
        self._restore_roles()  # persist-then-reconcile: roles known before discovery
        self._running = True
        self._loop = asyncio.get_running_loop()

        self.serial.open()
        self.serial.on_packet(self._handle_packet)
        self.mqtt.connect()

        try:
            from .web import create_web_app, start_web_server
            self._web_app = create_web_app(self)
            asyncio.create_task(start_web_server(self._web_app, self.config.web.port))
            logger.info("Web-UI starting on port %d", self.config.web.port)
        except Exception:
            logger.exception("Failed to start Web-UI")

        # In-container zeroconf is off by default: from a bridged Docker network
        # it advertises the unreachable container IP and collides with the host's
        # avahi, which already owns the hostname. Enable only on host networking.
        if self.config.web.mdns:
            try:
                from .web import register_mdns
                register_mdns(self.config.web.hostname, self.config.web.port)
            except Exception:
                logger.debug("mDNS registration failed (non-critical)")

        logger.info("MAICO EnOcean MQTT Bridge started")
        # Restart marker — gives the /logs view a clear boundary between this
        # run and any persisted events from before the restart.
        self.record_event("system", "restart", source="system", version=__version__)
        logger.info("Monitoring %d device(s), poll interval %ds",
                    len(self.config.devices), self.config.poll_interval)
        if self.serial.base_id:
            logger.info("EnOcean base ID: %s", self.serial.base_id_str)

        self._poll_task = asyncio.create_task(self._poll_loop())
        self._availability_task = asyncio.create_task(self._availability_loop())

        try:
            await self.serial.start_receive_loop()
        except asyncio.CancelledError:
            logger.info("Bridge shutting down...")
        finally:
            self._running = False
            if self._poll_task:
                self._poll_task.cancel()
            if self._availability_task:
                self._availability_task.cancel()
            try:
                self.serial.close()
            except Exception:
                logger.debug("Error closing serial", exc_info=True)
            try:
                self.mqtt.disconnect()
            except Exception:
                logger.debug("Error disconnecting MQTT", exc_info=True)

    def shutdown(self) -> None:
        self._running = False


def setup_logging() -> None:
    log_level = logging.DEBUG if "--debug" in sys.argv else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def main() -> None:
    setup_logging()

    config_path = None
    for i, arg in enumerate(sys.argv[1:], 1):
        if arg == "--config" and i < len(sys.argv) - 1:
            config_path = sys.argv[i + 1]

    try:
        config = load_config(config_path)
    except Exception as e:
        logger.warning("Config load issue: %s — starting with defaults", e)
        config = AppConfig()

    bridge = MaicoMqttBridge(config)
    loop = asyncio.new_event_loop()

    def shutdown(sig: signal.Signals) -> None:
        logger.info("Received signal %s, shutting down...", sig.name)
        bridge.shutdown()
        for task in asyncio.all_tasks(loop):
            task.cancel()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, shutdown, sig)

    try:
        loop.run_until_complete(bridge.run())
    except KeyboardInterrupt:
        pass
    finally:
        loop.close()


if __name__ == "__main__":
    main()
