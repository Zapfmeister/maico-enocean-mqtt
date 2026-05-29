"""Main entry point for maico-enocean-mqtt bridge.

Orchestrates EnOcean serial, MAICO MSC protocol, MQTT client, and Web-UI.
Uses direct MSC 27 20 commands. All devices are treated equally — master/slave
relationships are detected automatically from 27 00 sync traffic.
"""

import asyncio
import logging
import signal
import sys
import time
from dataclasses import dataclass, field

from .config import AppConfig, DeviceConfig, load_config
from .enocean_serial import EnOceanSerial, RORG_MSC
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

logger = logging.getLogger(__name__)

# Status map for 27 00 sync byte → direction + level
_SYNC_STATUS_MAP: dict[int, tuple[AirflowDirection, int]] = {
    0x00: (AirflowDirection.UNKNOWN, 0),
    # Inflow (Zuluft) — 0x2X
    0x21: (AirflowDirection.INFLOW, 1),
    0x22: (AirflowDirection.INFLOW, 2),
    0x23: (AirflowDirection.INFLOW, 3),
    0x24: (AirflowDirection.INFLOW, 4),
    0x25: (AirflowDirection.INFLOW, 5),
    # Exhaust (Abluft) — 0x0X
    0x01: (AirflowDirection.EXHAUST, 1),
    0x02: (AirflowDirection.EXHAUST, 2),
    0x03: (AirflowDirection.EXHAUST, 3),
    0x04: (AirflowDirection.EXHAUST, 4),
    0x05: (AirflowDirection.EXHAUST, 5),
    # Sleep mode (0x60 seen in sync during sleep)
    0x60: (AirflowDirection.UNKNOWN, 0),
}


_OPPOSITE_DIR: dict[AirflowDirection, AirflowDirection] = {
    AirflowDirection.INFLOW: AirflowDirection.EXHAUST,
    AirflowDirection.EXHAUST: AirflowDirection.INFLOW,
    AirflowDirection.UNKNOWN: AirflowDirection.UNKNOWN,
}


class ConnectionStatus:
    MANAGED = "managed"
    PASSIVE = "passive"
    UNKNOWN = "unknown"


SYNC_ROLE_TIMEOUT = 120  # seconds without sync before role resets
DEVICE_OFFLINE_TIMEOUT = 300  # seconds without any traffic before HA marks unavailable
AVAILABILITY_CHECK_INTERVAL = 30  # how often to re-evaluate per-device availability

# IDs that must never be treated as devices
_INVALID_DEVICE_IDS: set[str] = {
    "FFFFFFFF",  # EnOcean broadcast address
    "00000000",  # Null address
}


def _is_valid_device_id(device_id_str: str) -> bool:
    """Check if an EnOcean device ID looks like a real MAICO device.

    Rejects broadcast addresses, null addresses, and IDs with too many
    0xFF bytes (typically from RF noise or corrupt serial data).
    """
    clean = device_id_str.upper().replace(":", "")
    if clean in _INVALID_DEVICE_IDS:
        return False
    # Count 0xFF bytes — real MAICO IDs never have more than one
    ff_count = sum(1 for i in range(0, 8, 2) if clean[i:i+2] == "FF")
    if ff_count >= 2:
        return False
    return True

@dataclass
class DeviceStatus:
    connection: str = ConnectionStatus.UNKNOWN
    last_seen: float = 0.0
    last_response_to_us: float = 0.0
    response_count: int = 0
    # Detected relationships from 27 00 sync traffic
    syncs_to: str | None = None       # Device ID this device sends 27 00 to (= is master of)
    synced_from: str | None = None     # Device ID that sends 27 00 to us (= our master)
    last_sync: float = 0.0            # Timestamp of last sync traffic

    @property
    def last_seen_ago(self) -> int:
        if self.last_seen == 0:
            return -1
        return int(time.time() - self.last_seen)

    @property
    def detected_role(self) -> str:
        """Role detected from traffic, not config. Expires after timeout."""
        if self.last_sync > 0 and (time.time() - self.last_sync) > SYNC_ROLE_TIMEOUT:
            self.syncs_to = None
            self.synced_from = None
            self.last_sync = 0.0
        if self.syncs_to:
            return "master"
        if self.synced_from:
            return "slave"
        return "standalone"

    def to_dict(self) -> dict:
        return {
            "connection": self.connection,
            "last_seen_ago": self.last_seen_ago,
            "response_count": self.response_count,
            "detected_role": self.detected_role,
            "syncs_to": self.syncs_to,
            "synced_from": self.synced_from,
        }


class MaicoMqttBridge:
    """Bridge between MAICO PP 45 RC units and Home Assistant via MQTT."""

    def __init__(self, config: AppConfig):
        self.config = config
        self.serial = EnOceanSerial(config.enocean.device)
        self.mqtt = MqttClient(config, self)
        self._states: dict[str, VentilationState] = {}
        self._device_status: dict[str, DeviceStatus] = {}
        self._id_to_name: dict[str, str] = {}
        self._name_to_config: dict[str, DeviceConfig] = {}
        self._running = False
        self._poll_task: asyncio.Task | None = None
        self._web_app = None
        self._state_known: set[str] = set()  # devices with confirmed state from traffic
        self._saved_states: dict[str, VentilationState] = {}  # saved before sleep/boost
        self._mode_timers: dict[str, asyncio.Task] = {}  # active mode timers
        self._timer_end: dict[str, float] = {}  # timestamp when timer expires
        self._last_rls_state: VentilationState | None = None
        self._rls_teach_in_active: bool = False
        self._rls_teach_in_result: str | None = None
        self._polling_paused: bool = False
        self._discovery_enabled: bool = False  # only register unknown senders during explicit pairing
        self._poll_skip_until: float = 0.0  # skip polls until this timestamp (after RLS sync)
        self._availability: dict[str, bool] = {}  # last-published availability per device
        self._availability_task: asyncio.Task | None = None

    @property
    def states(self) -> dict[str, VentilationState]:
        return self._states

    @property
    def device_status(self) -> dict[str, DeviceStatus]:
        return self._device_status

    def timer_remaining_minutes(self, device_name: str) -> int:
        """Return remaining timer minutes for a device, or 0 if no timer."""
        end = self._timer_end.get(device_name)
        if end is None:
            return 0
        remaining = end - time.time()
        return max(0, int(remaining / 60))

    def _setup_device_mappings(self) -> None:
        for device in self.config.devices:
            dev_id = device.device_id.upper().replace(":", "")
            self._id_to_name[dev_id] = device.name
            self._name_to_config[device.name] = device
            self._states[device.name] = VentilationState()
            self._device_status[device.name] = DeviceStatus()

        if self.config.remote.device_id:
            rls_id = self.config.remote.device_id.upper().replace(":", "")
            self._id_to_name[rls_id] = self.config.remote.name

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

        self._id_to_name[device_id_str] = name
        self._name_to_config[name] = device
        self._states[name] = VentilationState()
        self._device_status[name] = DeviceStatus()

        # Publish HA discovery
        self.mqtt.publish_device_discovery(device)

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
            # Preserve direction from sync if status doesn't have it
            existing = self._states.get(name)
            if existing and state.direction == AirflowDirection.UNKNOWN and existing.direction != AirflowDirection.UNKNOWN:
                state.direction = existing.direction
            self._states[name] = state
            self._state_known.add(name)
            self.mqtt.publish_state(name, state)

            # Sync slave state when master turns off/on
            ds = self._device_status.get(name)
            if ds and ds.syncs_to:
                slave_name = self._id_to_name.get(ds.syncs_to)
                if slave_name:
                    slave_state = self._states.get(slave_name)
                    if slave_state:
                        if not state.is_on and slave_state.is_on:
                            slave_state.mode = VentilationMode.OFF
                            slave_state.fan_level = 0
                            self.mqtt.publish_state(slave_name, slave_state)
                        elif state.is_on and not slave_state.is_on:
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
                    self.set_mode(device.name, state.mode)
            self.set_level(device.name, state.fan_level)

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
        self.mqtt.publish_event("teach_in", {
            "device_id": sender_str,
            "status": "paired",
        })

    def _handle_device_announce(self, sender_str: str) -> None:
        logger.info("Device announcement from %s (in learn mode)", sender_str)
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

    def set_level(self, device_name: str, level: int) -> bool:
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
        return True

    def set_power(self, device_name: str, on: bool) -> bool:
        if on:
            current = self._states.get(device_name, VentilationState())
            level = current.fan_level if current.fan_level > 0 else 1
            return self.set_level(device_name, level)
        else:
            return self.set_level(device_name, 0)

    # Timer defaults (seconds)
    BOOST_DURATION = 30 * 60   # 30 minutes
    SLEEP_DURATION = 2 * 3600  # 2 hours

    def set_mode(self, device_name: str, mode: VentilationMode) -> bool:
        """Switch operating mode and send to device. Starts timer for sleep/boost."""
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
        return True

    def _cancel_mode_timer(self, device_name: str) -> None:
        task = self._mode_timers.pop(device_name, None)
        if task and not task.done():
            task.cancel()
        self._timer_end.pop(device_name, None)
        self.mqtt.publish_timer(device_name, 0)

    def _start_mode_timer(self, device_name: str, duration: int) -> None:
        self._timer_end[device_name] = time.time() + duration
        self.mqtt.publish_timer(device_name, duration // 60)

        async def _timer():
            logger.info("%s: timer started (%d min)", device_name, duration // 60)
            await asyncio.sleep(duration)
            self._timer_end.pop(device_name, None)
            self._restore_mode(device_name)

        loop = getattr(self, '_loop', None)
        if loop and loop.is_running():
            future = asyncio.run_coroutine_threadsafe(_timer(), loop)
            # Wrap in a simple object that has .done() and .cancel()
            self._mode_timers[device_name] = future
        else:
            self._mode_timers[device_name] = asyncio.create_task(_timer())

    def _restore_mode(self, device_name: str) -> None:
        saved = self._saved_states.pop(device_name, None)
        if saved:
            mode = saved.mode if saved.mode in (VentilationMode.HEAT_EXCHANGER, VentilationMode.SUMMER) else VentilationMode.HEAT_EXCHANGER
            level = saved.fan_level if saved.fan_level > 0 else 1
            logger.info("%s: timer expired, restoring %s level %d", device_name, mode.value, level)
            self.set_level(device_name, level)
        else:
            logger.info("%s: timer expired, no saved state — setting level 1", device_name)
            self.set_level(device_name, 1)

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
        if self._availability.get(device_name) != online:
            self._availability[device_name] = online
            self.mqtt.publish_device_availability(device_name, online)
            logger.info("%s availability → %s", device_name, "online" if online else "offline")

    async def _availability_loop(self) -> None:
        """Periodically re-evaluate device availability and republish changes."""
        while self._running:
            for device in self.config.devices:
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
                    if device.name in self._timer_end:
                        self.mqtt.publish_timer(device.name, remaining)

                if i < len(devices) - 1:
                    await asyncio.sleep(stagger)

            remaining = interval - stagger * (len(devices) - 1) if len(devices) > 1 else interval
            await asyncio.sleep(max(1, remaining))

    async def run(self) -> None:
        self._setup_device_mappings()
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

        try:
            from .web import register_mdns
            register_mdns(self.config.web.hostname, self.config.web.port)
        except Exception:
            logger.debug("mDNS registration failed (non-critical)")

        logger.info("MAICO EnOcean MQTT Bridge started")
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
