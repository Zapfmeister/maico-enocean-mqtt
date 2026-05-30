"""MQTT client with Home Assistant MQTT Discovery support.

Publishes per device:
  - Fan entity (On/Off, percentage 0-100 → levels 0-5, presets)
  - Sensor entity (fan speed 0-100 %, numeric, for history graphs)
  - Select entity (Wärmetauscher / Sommer mode)
  - Sensor entity (airflow direction, read-only)
  - Sensor entity (connection status: managed/passive/unknown, diagnostic)
  - Binary sensor for bridge status (LWT)
"""

import json
import logging
import secrets
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from . import __version__ as sw_version

import paho.mqtt.client as mqtt

from .config import AppConfig, DeviceConfig
from .maico_protocol import AirflowDirection, VentilationMode, VentilationState

if TYPE_CHECKING:
    from .main import DeviceStatus, MaicoMqttBridge

logger = logging.getLogger(__name__)

MODE_TO_PRESET: dict[VentilationMode, str] = {
    VentilationMode.SLEEP_HEAT: "sleep",
    VentilationMode.SLEEP_SUMMER: "sleep",
    VentilationMode.BOOST: "boost",
}

PRESET_TO_MODE: dict[str, VentilationMode] = {
    "sleep": VentilationMode.SLEEP_HEAT,
    "boost": VentilationMode.BOOST,
}

_MODE_I18N = {
    "de": {
        "options": ["Wärmetauscher", "Sommer", "Schlafen", "Stoßlüften"],
        "select_map": {
            "Wärmetauscher": VentilationMode.HEAT_EXCHANGER,
            "Sommer": VentilationMode.SUMMER,
            "Schlafen": VentilationMode.SLEEP_HEAT,
            "Stoßlüften": VentilationMode.BOOST,
        },
        "reverse_map": {
            VentilationMode.OFF: "Wärmetauscher",
            VentilationMode.HEAT_EXCHANGER: "Wärmetauscher",
            VentilationMode.SUMMER: "Sommer",
            VentilationMode.SLEEP_HEAT: "Schlafen",
            VentilationMode.SLEEP_SUMMER: "Schlafen",
            VentilationMode.BOOST: "Stoßlüften",
        },
        "direction": {"inflow": "Zuluft", "exhaust": "Abluft", "unknown": "Unbekannt", "off": "Aus", "alternating": "Wechselnd", "continuous": "Durchluft"},
        "entity_names": {
            "mode": "Modus",
            "direction": "Luftrichtung",
            "connection": "Verbindung",
            "role": "Rolle",
            "last_seen": "Letzter Kontakt",
            "signal": "Funksignal",
            "timer": "Timer",
            "speed": "Drehzahl",
        },
    },
    "en": {
        "options": ["Heat Exchanger", "Summer", "Sleep", "Boost"],
        "select_map": {
            "Heat Exchanger": VentilationMode.HEAT_EXCHANGER,
            "Summer": VentilationMode.SUMMER,
            "Sleep": VentilationMode.SLEEP_HEAT,
            "Boost": VentilationMode.BOOST,
        },
        "reverse_map": {
            VentilationMode.OFF: "Heat Exchanger",
            VentilationMode.HEAT_EXCHANGER: "Heat Exchanger",
            VentilationMode.SUMMER: "Summer",
            VentilationMode.SLEEP_HEAT: "Sleep",
            VentilationMode.SLEEP_SUMMER: "Sleep",
            VentilationMode.BOOST: "Boost",
        },
        "direction": {"inflow": "Inflow", "exhaust": "Exhaust", "unknown": "Unknown", "off": "Off", "alternating": "Alternating", "continuous": "Continuous"},
        "entity_names": {
            "mode": "Mode",
            "direction": "Airflow Direction",
            "connection": "Connection",
            "role": "Role",
            "last_seen": "Last Seen",
            "signal": "Signal Strength",
            "timer": "Timer",
            "speed": "Fan Speed",
        },
    },
}


@dataclass
class ConnectionHealth:
    """In-memory MQTT connection stability metrics, since process start.

    The bridge process stays up across brief broker/network drops, so it can
    count its own reconnects first-hand — a cheap, generic health signal that
    surfaces a flapping link (e.g. Wi-Fi power-save) without any external probe.
    All timestamps are wall-clock ``time.time()``; ``now`` is injectable for
    deterministic tests.
    """

    connected: bool = False
    connect_count: int = 0          # successful (re)connects observed
    disconnect_count: int = 0       # unexpected disconnects observed
    last_connect_at: float = 0.0
    last_disconnect_at: float = 0.0
    started_at: float = field(default_factory=time.time)

    def record_connect(self, now: float | None = None) -> None:
        self.connected = True
        self.connect_count += 1
        self.last_connect_at = time.time() if now is None else now

    def record_disconnect(self, now: float | None = None) -> None:
        self.connected = False
        self.disconnect_count += 1
        self.last_disconnect_at = time.time() if now is None else now

    @property
    def reconnect_count(self) -> int:
        """Connects beyond the first — i.e. how often the link came back."""
        return max(0, self.connect_count - 1)

    def to_dict(self, now: float | None = None) -> dict:
        now = time.time() if now is None else now
        return {
            "connected": self.connected,
            "reconnect_count": self.reconnect_count,
            "disconnect_count": self.disconnect_count,
            "connected_for": int(now - self.last_connect_at) if self.last_connect_at else -1,
            "last_disconnect_ago": int(now - self.last_disconnect_at) if self.last_disconnect_at else -1,
            "uptime": int(now - self.started_at),
        }


class MqttClient:
    """MQTT client for Home Assistant integration via MQTT Discovery."""

    def __init__(self, config: AppConfig, bridge: "MaicoMqttBridge"):
        self.config = config
        self.bridge = bridge
        self._client: mqtt.Client | None = None
        self._connected = False
        self.health = ConnectionHealth()
        self._i18n = _MODE_I18N.get(config.language, _MODE_I18N["de"])

    def connect(self) -> None:
        cfg = self.config.mqtt
        # A unique client_id avoids two instances kicking each other off the
        # broker (which looks like periodic disconnects). Stable within a
        # process so reconnects reuse the same session; override via config.
        client_id = cfg.client_id or f"maico-enocean-bridge-{secrets.token_hex(3)}"
        self._client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=client_id,
        )

        if cfg.username:
            self._client.username_pw_set(cfg.username, cfg.password)

        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.on_message = self._on_message

        # LWT
        status_topic = f"{cfg.topic_prefix}/bridge/status"
        self._client.will_set(status_topic, "offline", retain=True)

        # Auto-reconnect with capped backoff. Using connect_async + loop_start
        # means an initially-unreachable broker no longer drops all publishes
        # forever — the network loop keeps retrying and on_connect re-publishes
        # discovery once it comes up.
        self._client.reconnect_delay_set(min_delay=1, max_delay=60)

        logger.info("Connecting to MQTT %s:%d", cfg.host, cfg.port)
        self._client.loop_start()
        try:
            self._client.connect_async(cfg.host, cfg.port, keepalive=60)
        except Exception:
            logger.exception("MQTT connect_async failed")

    def disconnect(self) -> None:
        if self._client:
            status_topic = f"{self.config.mqtt.topic_prefix}/bridge/status"
            self._client.publish(status_topic, "offline", retain=True)
            self._client.loop_stop()
            self._client.disconnect()
            logger.info("MQTT disconnected")

    def _on_connect(self, client: mqtt.Client, userdata: object, flags: mqtt.ConnectFlags,
                    rc: mqtt.ReasonCode, properties: mqtt.Properties | None = None) -> None:
        if rc == 0:
            logger.info("MQTT connected")
            self._connected = True
            self.health.record_connect()
            # Only a *re*connect is event-worthy; the first connect is just startup.
            if self.health.reconnect_count >= 1:
                self.bridge.record_mqtt_event(True)
            self._publish_availability("online")
            self._publish_all_discovery()
            self._subscribe_commands()
        else:
            logger.error("MQTT connection failed: %s", rc)

    def _on_disconnect(self, client: mqtt.Client, userdata: object, flags: mqtt.DisconnectFlags,
                       rc: mqtt.ReasonCode, properties: mqtt.Properties | None = None) -> None:
        logger.warning("MQTT disconnected (rc=%s)", rc)
        self._connected = False
        self.health.record_disconnect()
        self.bridge.record_mqtt_event(False)

    def _on_message(self, client: mqtt.Client, userdata: object, msg: mqtt.MQTTMessage) -> None:
        topic = msg.topic
        try:
            payload = msg.payload.decode("utf-8")
        except (UnicodeDecodeError, AttributeError):
            logger.warning("MQTT: invalid payload encoding on %s", topic)
            return
        prefix = self.config.mqtt.topic_prefix

        logger.debug("MQTT rx: %s = %s", topic, payload)

        for device in self.config.devices:
            dt = f"{prefix}/{device.name}"

            # These callbacks run on the paho network thread; marshal the actual
            # state changes onto the asyncio loop via bridge.dispatch().
            if topic == f"{dt}/set/percentage":
                try:
                    # HA sends speed_range value (1-5) directly when speed_range_min/max is set
                    level = max(0, min(5, int(float(payload))))
                    self.bridge.dispatch(self.bridge.set_level, device.name, level, "ha")
                except ValueError:
                    logger.error("Invalid percentage: %s", payload)

            elif topic == f"{dt}/set/power":
                self.bridge.dispatch(self.bridge.set_power, device.name, payload.upper() == "ON", "ha")

            elif topic == f"{dt}/set/mode":
                mode = self._i18n["select_map"].get(payload)
                if mode:
                    self.bridge.dispatch(self.bridge.set_mode, device.name, mode, "ha")

            elif topic == f"{dt}/set/preset_mode":
                mode = PRESET_TO_MODE.get(payload.lower())
                if mode:
                    self.bridge.dispatch(self.bridge.set_mode, device.name, mode, "ha")

            # HomeKit-friendly mode controls: a Sommer on/off switch (off =
            # Wärmetauscher) plus momentary Schlafen/Stoßlüften buttons.
            elif topic == f"{dt}/set/summer":
                mode = (VentilationMode.SUMMER if payload.upper() == "ON"
                        else VentilationMode.HEAT_EXCHANGER)
                self.bridge.dispatch(self.bridge.set_mode, device.name, mode)

            elif topic == f"{dt}/set/sleep":
                self.bridge.dispatch(self.bridge.set_mode, device.name, VentilationMode.SLEEP_HEAT, "ha")

            elif topic == f"{dt}/set/boost":
                self.bridge.dispatch(self.bridge.set_mode, device.name, VentilationMode.BOOST, "ha")

    def publish_state(self, device_name: str, state: VentilationState) -> None:
        if not self._client or not self._connected:
            return

        prefix = self.config.mqtt.topic_prefix
        t = f"{prefix}/{device_name}"

        self._client.publish(f"{t}/state", "ON" if state.is_on else "OFF", retain=True)
        self._client.publish(f"{t}/percentage", state.fan_level, retain=True)
        # Numeric speed sensor (0-100 %) for the history graph: the fan entity's
        # own percentage is only an attribute, so it never shows as a line in the
        # default recorder view. Levels 0-5 map linearly to 0/20/40/60/80/100 %.
        self._client.publish(f"{t}/speed_percent", round(state.fan_level / 5 * 100), retain=True)
        dir_labels = self._i18n["direction"]
        if not state.is_on:
            direction = dir_labels["off"]
        elif state.direction != AirflowDirection.UNKNOWN:
            direction = dir_labels.get(state.direction.value, state.direction.value)
        elif state.mode == VentilationMode.HEAT_EXCHANGER:
            # Solo units reverse for heat recovery but never broadcast their live
            # phase (only master/slave pairs do, via 27 00 sync). So instead of
            # a misleading fixed value, show that the airflow alternates.
            direction = dir_labels["alternating"]
        elif state.mode in (VentilationMode.SUMMER, VentilationMode.SLEEP_SUMMER):
            # Summer mode bypasses heat recovery and runs continuously in one
            # direction ("Durchluft") — it doesn't alternate, and the status
            # telegram carries no direction byte. So show the continuous-flow
            # label instead of a meaningless "unknown".
            direction = dir_labels["continuous"]
        else:
            direction = dir_labels["unknown"]
        self._client.publish(f"{t}/direction", direction, retain=True)

        # Mode for select entity
        reverse = self._i18n["reverse_map"]
        default_mode = self._i18n["options"][0]
        self._client.publish(f"{t}/mode", reverse.get(state.mode, default_mode), retain=True)

        # HomeKit Sommer switch state (on only while in summer mode)
        self._client.publish(
            f"{t}/summer", "ON" if state.mode == VentilationMode.SUMMER else "OFF",
            retain=True,
        )

        # Full JSON
        self._client.publish(f"{t}/json", json.dumps(state.to_dict()), retain=True)

    def publish_connection_status(self, device_name: str, status: "DeviceStatus") -> None:
        """Publish connection status, role, and last_seen for a device."""
        if not self._client or not self._connected:
            return
        prefix = self.config.mqtt.topic_prefix
        t = f"{prefix}/{device_name}"
        self._client.publish(f"{t}/connection", json.dumps(status.to_dict()), retain=True)
        self._client.publish(f"{t}/role", status.detected_role, retain=True)
        # last_seen and rssi feed numeric HA sensors — only publish real values.
        # Sending the string "unknown" makes HA log an "invalid state" warning on
        # every 30 s republish (e.g. for a device not seen yet after a restart).
        last_seen = status.last_seen_ago
        if last_seen >= 0:
            self._client.publish(f"{t}/last_seen", last_seen, retain=True)
        if status.rssi is not None:
            self._client.publish(f"{t}/rssi", status.rssi, retain=True)

    def publish_device_availability(self, device_name: str, online: bool) -> None:
        """Publish per-device availability (online/offline) for HA entity state."""
        if not self._client or not self._connected:
            return
        prefix = self.config.mqtt.topic_prefix
        self._client.publish(
            f"{prefix}/{device_name}/availability",
            "online" if online else "offline",
            retain=True,
        )

    def publish_timer(self, device_name: str, remaining_minutes: int) -> None:
        """Publish remaining timer minutes for sleep/boost mode."""
        if not self._client or not self._connected:
            return
        prefix = self.config.mqtt.topic_prefix
        self._client.publish(
            f"{prefix}/{device_name}/timer",
            remaining_minutes if remaining_minutes > 0 else 0,
            retain=True,
        )

    def publish_event(self, event_type: str, data: dict) -> None:
        if not self._client or not self._connected:
            return
        prefix = self.config.mqtt.topic_prefix
        payload = json.dumps({"event": event_type, **data})
        self._client.publish(f"{prefix}/bridge/event", payload)
        logger.info("Event: %s %s", event_type, data)

    def _publish_availability(self, status: str) -> None:
        if self._client:
            self._client.publish(
                f"{self.config.mqtt.topic_prefix}/bridge/status", status, retain=True
            )

    def _publish_all_discovery(self) -> None:
        self._publish_bridge_discovery()
        for device in self.config.devices:
            self._publish_device_discovery(device)

    def _publish_device_discovery(self, device: DeviceConfig) -> None:
        """Publish all discovery entities for a single device."""
        self._publish_fan_discovery(device)
        self._publish_speed_sensor_discovery(device)
        self._publish_mode_select_discovery(device)
        self._publish_summer_switch_discovery(device)
        self._publish_mode_button_discovery(device, "sleep", self._i18n["options"][2], "mdi:power-sleep")
        self._publish_mode_button_discovery(device, "boost", self._i18n["options"][3], "mdi:weather-windy")
        self._publish_direction_sensor_discovery(device)
        self._publish_connection_sensor_discovery(device)
        self._publish_role_sensor_discovery(device)
        self._publish_last_seen_sensor_discovery(device)
        self._publish_rssi_sensor_discovery(device)
        self._publish_timer_sensor_discovery(device)

    def publish_device_discovery(self, device: DeviceConfig) -> None:
        """Publish discovery for a single device (called after pairing)."""
        self._publish_device_discovery(device)

    def remove_device_discovery(self, device: DeviceConfig) -> None:
        """Remove discovery for a device (publish empty payloads)."""
        if not self._client:
            return
        ha = self.config.mqtt.ha_discovery_prefix
        uid = f"maico_{device.name.lower()}"
        for comp, suffix in [("fan", ""), ("sensor", "_speed"), ("select", "_mode"),
                              ("switch", "_summer"),
                              ("button", "_sleep"), ("button", "_boost"),
                              ("sensor", "_direction"), ("sensor", "_connection"),
                              ("sensor", "_role"), ("sensor", "_last_seen"),
                              ("sensor", "_rssi"), ("sensor", "_timer")]:
            self._client.publish(f"{ha}/{comp}/{uid}{suffix}/config", "", retain=True)

    def clear_device_topics(self, device_name: str) -> None:
        """Clear retained MQTT state topics for a device (after rename/remove)."""
        if not self._client:
            return
        prefix = self.config.mqtt.topic_prefix
        t = f"{prefix}/{device_name}"
        for suffix in ["/state", "/percentage", "/speed_percent", "/direction",
                       "/mode", "/summer",
                       "/json", "/connection", "/role", "/last_seen", "/rssi", "/timer",
                       "/availability"]:
            self._client.publish(f"{t}{suffix}", "", retain=True)

    def _publish_bridge_discovery(self) -> None:
        if not self._client:
            return
        ha = self.config.mqtt.ha_discovery_prefix
        prefix = self.config.mqtt.topic_prefix
        web_url = f"http://{self.config.web.hostname}.local:{self.config.web.port}"

        payload = {
            "name": "MAICO Bridge",
            "unique_id": "maico_bridge",
            "object_id": "maico_bridge",
            "state_topic": f"{prefix}/bridge/status",
            "payload_on": "online",
            "payload_off": "offline",
            "device_class": "connectivity",
            "entity_category": "diagnostic",
            "device": {
                "identifiers": ["maico_bridge"],
                "name": "MAICO EnOcean Bridge",
                "manufacturer": "Gerard Zapf",
                "model": "EnOcean MQTT Bridge für MAICO PP 45 RC",
                "sw_version": sw_version,
                "configuration_url": web_url,
            },
        }
        self._client.publish(
            f"{ha}/binary_sensor/maico_bridge/config",
            json.dumps(payload), retain=True,
        )

    def _availability_block(self, device_name: str) -> dict:
        """Return availability config combining bridge + device status (both must be online)."""
        prefix = self.config.mqtt.topic_prefix
        return {
            "availability": [
                {"topic": f"{prefix}/bridge/status",
                 "payload_available": "online", "payload_not_available": "offline"},
                {"topic": f"{prefix}/{device_name}/availability",
                 "payload_available": "online", "payload_not_available": "offline"},
            ],
            "availability_mode": "all",
        }

    def _publish_fan_discovery(self, device: DeviceConfig) -> None:
        if not self._client:
            return
        prefix = self.config.mqtt.topic_prefix
        ha = self.config.mqtt.ha_discovery_prefix
        uid = f"maico_{device.name.lower()}"
        dt = f"{prefix}/{device.name}"
        web_url = f"http://{self.config.web.hostname}.local:{self.config.web.port}"

        payload = {
            # Primary entity of the device: name=None makes HA use the device
            # name verbatim (no doubling). HA prepends the device name to all
            # other entities, so those carry only their bare role label.
            "name": None,
            "unique_id": uid,
            "object_id": uid,
            "state_topic": f"{dt}/state",
            "state_value_template": "{{ value }}",
            "percentage_state_topic": f"{dt}/percentage",
            "percentage_command_topic": f"{dt}/set/percentage",
            "speed_range_min": 1,
            "speed_range_max": 5,
            "command_topic": f"{dt}/set/power",
            "payload_on": "ON",
            "payload_off": "OFF",
            **self._availability_block(device.name),
            "device": {
                "identifiers": [f"maico_{device.device_id}"],
                "name": device.friendly_name or device.name,
                "manufacturer": "MAICO",
                "model": "PP 45 RC",
                "via_device": "maico_bridge",
                "configuration_url": web_url,
            },
        }
        self._client.publish(f"{ha}/fan/{uid}/config", json.dumps(payload), retain=True)
        logger.info("Discovery: fan %s", device.friendly_name)

    def _publish_speed_sensor_discovery(self, device: DeviceConfig) -> None:
        """Numeric fan-speed sensor (0-100 %) so the speed shows as a line in history."""
        if not self._client:
            return
        prefix = self.config.mqtt.topic_prefix
        ha = self.config.mqtt.ha_discovery_prefix
        uid = f"maico_{device.name.lower()}_speed"
        dt = f"{prefix}/{device.name}"

        payload = {
            "name": self._i18n['entity_names']['speed'],
            "unique_id": uid,
            "object_id": uid,
            "state_topic": f"{dt}/speed_percent",
            "unit_of_measurement": "%",
            "state_class": "measurement",
            "icon": "mdi:fan",
            **self._availability_block(device.name),
            "device": {
                "identifiers": [f"maico_{device.device_id}"],
            },
        }
        self._client.publish(f"{ha}/sensor/{uid}/config", json.dumps(payload), retain=True)

    def _publish_mode_select_discovery(self, device: DeviceConfig) -> None:
        if not self._client:
            return
        prefix = self.config.mqtt.topic_prefix
        ha = self.config.mqtt.ha_discovery_prefix
        uid = f"maico_{device.name.lower()}_mode"
        dt = f"{prefix}/{device.name}"

        payload = {
            "name": self._i18n['entity_names']['mode'],
            "unique_id": uid,
            "object_id": uid,
            "state_topic": f"{dt}/mode",
            "command_topic": f"{dt}/set/mode",
            "options": self._i18n["options"],
            "icon": "mdi:heat-wave",
            **self._availability_block(device.name),
            "device": {
                "identifiers": [f"maico_{device.device_id}"],
            },
        }
        self._client.publish(f"{ha}/select/{uid}/config", json.dumps(payload), retain=True)

    def _publish_summer_switch_discovery(self, device: DeviceConfig) -> None:
        """Sommer on/off switch (off = Wärmetauscher). HomeKit-/Siri-compatible."""
        if not self._client:
            return
        prefix = self.config.mqtt.topic_prefix
        ha = self.config.mqtt.ha_discovery_prefix
        uid = f"maico_{device.name.lower()}_summer"
        dt = f"{prefix}/{device.name}"

        payload = {
            # Bare label; HA prepends the device name (-> "MAICO Bad Sommer").
            "name": self._i18n['options'][1],
            "unique_id": uid,
            "object_id": uid,
            "state_topic": f"{dt}/summer",
            "command_topic": f"{dt}/set/summer",
            "payload_on": "ON",
            "payload_off": "OFF",
            "icon": "mdi:weather-sunny",
            **self._availability_block(device.name),
            "device": {
                "identifiers": [f"maico_{device.device_id}"],
            },
        }
        self._client.publish(f"{ha}/switch/{uid}/config", json.dumps(payload), retain=True)

    def _publish_mode_button_discovery(self, device: DeviceConfig, key: str,
                                       label: str, icon: str) -> None:
        """Momentary button that triggers a ventilation mode (e.g. Schlafen/Stoßlüften).

        HomeKit exposes HA buttons as switches Siri can switch on, so these are
        directly voice-controllable."""
        if not self._client:
            return
        prefix = self.config.mqtt.topic_prefix
        ha = self.config.mqtt.ha_discovery_prefix
        uid = f"maico_{device.name.lower()}_{key}"
        dt = f"{prefix}/{device.name}"

        payload = {
            # Bare label; HA prepends the device name (-> "MAICO Bad Schlafen").
            "name": label,
            "unique_id": uid,
            "object_id": uid,
            "command_topic": f"{dt}/set/{key}",
            "payload_press": "PRESS",
            "icon": icon,
            **self._availability_block(device.name),
            "device": {
                "identifiers": [f"maico_{device.device_id}"],
            },
        }
        self._client.publish(f"{ha}/button/{uid}/config", json.dumps(payload), retain=True)

    def _publish_direction_sensor_discovery(self, device: DeviceConfig) -> None:
        if not self._client:
            return
        prefix = self.config.mqtt.topic_prefix
        ha = self.config.mqtt.ha_discovery_prefix
        uid = f"maico_{device.name.lower()}_direction"
        dt = f"{prefix}/{device.name}"

        payload = {
            "name": self._i18n['entity_names']['direction'],
            "unique_id": uid,
            "object_id": uid,
            "state_topic": f"{dt}/direction",
            "icon": "mdi:air-filter",
            **self._availability_block(device.name),
            "device": {
                "identifiers": [f"maico_{device.device_id}"],
            },
        }
        self._client.publish(f"{ha}/sensor/{uid}/config", json.dumps(payload), retain=True)

    def _publish_connection_sensor_discovery(self, device: DeviceConfig) -> None:
        """Publish MQTT Discovery for connection status sensor (managed/passive/unknown)."""
        if not self._client:
            return
        prefix = self.config.mqtt.topic_prefix
        ha = self.config.mqtt.ha_discovery_prefix
        uid = f"maico_{device.name.lower()}_connection"
        dt = f"{prefix}/{device.name}"

        payload = {
            "name": self._i18n['entity_names']['connection'],
            "unique_id": uid,
            "object_id": uid,
            "state_topic": f"{dt}/connection",
            "value_template": "{{ value_json.connection }}",
            "json_attributes_topic": f"{dt}/connection",
            "icon": "mdi:link-variant",
            "entity_category": "diagnostic",
            "availability_topic": f"{prefix}/bridge/status",
            "payload_available": "online",
            "payload_not_available": "offline",
            "device": {
                "identifiers": [f"maico_{device.device_id}"],
            },
        }
        self._client.publish(f"{ha}/sensor/{uid}/config", json.dumps(payload), retain=True)

    def _publish_role_sensor_discovery(self, device: DeviceConfig) -> None:
        if not self._client:
            return
        prefix = self.config.mqtt.topic_prefix
        ha = self.config.mqtt.ha_discovery_prefix
        uid = f"maico_{device.name.lower()}_role"
        dt = f"{prefix}/{device.name}"

        payload = {
            "name": self._i18n['entity_names']['role'],
            "unique_id": uid,
            "object_id": uid,
            "state_topic": f"{dt}/role",
            "icon": "mdi:account-group",
            "entity_category": "diagnostic",
            "availability_topic": f"{prefix}/bridge/status",
            "device": {
                "identifiers": [f"maico_{device.device_id}"],
            },
        }
        self._client.publish(f"{ha}/sensor/{uid}/config", json.dumps(payload), retain=True)

    def _publish_last_seen_sensor_discovery(self, device: DeviceConfig) -> None:
        if not self._client:
            return
        prefix = self.config.mqtt.topic_prefix
        ha = self.config.mqtt.ha_discovery_prefix
        uid = f"maico_{device.name.lower()}_last_seen"
        dt = f"{prefix}/{device.name}"

        payload = {
            "name": self._i18n['entity_names']['last_seen'],
            "unique_id": uid,
            "object_id": uid,
            "state_topic": f"{dt}/last_seen",
            "unit_of_measurement": "s",
            "icon": "mdi:clock-outline",
            "entity_category": "diagnostic",
            "availability_topic": f"{prefix}/bridge/status",
            "device": {
                "identifiers": [f"maico_{device.device_id}"],
            },
        }
        self._client.publish(f"{ha}/sensor/{uid}/config", json.dumps(payload), retain=True)

    def _publish_rssi_sensor_discovery(self, device: DeviceConfig) -> None:
        if not self._client:
            return
        prefix = self.config.mqtt.topic_prefix
        ha = self.config.mqtt.ha_discovery_prefix
        uid = f"maico_{device.name.lower()}_rssi"
        dt = f"{prefix}/{device.name}"

        payload = {
            "name": self._i18n['entity_names']['signal'],
            "unique_id": uid,
            "object_id": uid,
            "state_topic": f"{dt}/rssi",
            "device_class": "signal_strength",
            "unit_of_measurement": "dBm",
            "state_class": "measurement",
            "entity_category": "diagnostic",
            "availability_topic": f"{prefix}/bridge/status",
            "device": {
                "identifiers": [f"maico_{device.device_id}"],
            },
        }
        self._client.publish(f"{ha}/sensor/{uid}/config", json.dumps(payload), retain=True)

    def _publish_timer_sensor_discovery(self, device: DeviceConfig) -> None:
        if not self._client:
            return
        prefix = self.config.mqtt.topic_prefix
        ha = self.config.mqtt.ha_discovery_prefix
        uid = f"maico_{device.name.lower()}_timer"
        dt = f"{prefix}/{device.name}"

        payload = {
            "name": self._i18n['entity_names']['timer'],
            "unique_id": uid,
            "object_id": uid,
            "state_topic": f"{dt}/timer",
            "unit_of_measurement": "min",
            "icon": "mdi:timer-outline",
            **self._availability_block(device.name),
            "device": {
                "identifiers": [f"maico_{device.device_id}"],
            },
        }
        self._client.publish(f"{ha}/sensor/{uid}/config", json.dumps(payload), retain=True)

    def _subscribe_commands(self) -> None:
        if not self._client:
            return
        prefix = self.config.mqtt.topic_prefix
        for device in self.config.devices:
            topic = f"{prefix}/{device.name}/set/#"
            self._client.subscribe(topic)
            logger.info("Subscribed: %s", topic)
