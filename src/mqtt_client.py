"""MQTT client with Home Assistant MQTT Discovery support.

Publishes per device:
  - Fan entity (On/Off, percentage 0-100 → levels 0-5, presets)
  - Select entity (Wärmetauscher / Sommer mode)
  - Sensor entity (airflow direction, read-only)
  - Sensor entity (connection status: managed/passive/unknown, diagnostic)
  - Binary sensor for bridge status (LWT)
"""

import json
import logging
from typing import TYPE_CHECKING

from . import __version__ as sw_version

import paho.mqtt.client as mqtt

from .config import AppConfig, DeviceConfig
from .maico_protocol import VentilationMode, VentilationState

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
        "direction": {"inflow": "Zuluft", "exhaust": "Abluft", "unknown": "Unbekannt", "off": "Aus"},
        "entity_names": {
            "mode": "Modus",
            "direction": "Luftrichtung",
            "connection": "Verbindung",
            "role": "Rolle",
            "last_seen": "Letzter Kontakt",
            "timer": "Timer",
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
        "direction": {"inflow": "Inflow", "exhaust": "Exhaust", "unknown": "Unknown", "off": "Off"},
        "entity_names": {
            "mode": "Mode",
            "direction": "Airflow Direction",
            "connection": "Connection",
            "role": "Role",
            "last_seen": "Last Seen",
            "timer": "Timer",
        },
    },
}


class MqttClient:
    """MQTT client for Home Assistant integration via MQTT Discovery."""

    def __init__(self, config: AppConfig, bridge: "MaicoMqttBridge"):
        self.config = config
        self.bridge = bridge
        self._client: mqtt.Client | None = None
        self._connected = False
        self._i18n = _MODE_I18N.get(config.language, _MODE_I18N["de"])

    def connect(self) -> None:
        cfg = self.config.mqtt
        self._client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id="maico-enocean-bridge",
        )

        if cfg.username:
            self._client.username_pw_set(cfg.username, cfg.password)

        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.on_message = self._on_message

        # LWT
        status_topic = f"{cfg.topic_prefix}/bridge/status"
        self._client.will_set(status_topic, "offline", retain=True)

        logger.info("Connecting to MQTT %s:%d", cfg.host, cfg.port)
        try:
            self._client.connect(cfg.host, cfg.port, keepalive=60)
            self._client.loop_start()
        except Exception:
            logger.exception("MQTT connection failed")

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
            self._publish_availability("online")
            self._publish_all_discovery()
            self._subscribe_commands()
        else:
            logger.error("MQTT connection failed: %s", rc)

    def _on_disconnect(self, client: mqtt.Client, userdata: object, flags: mqtt.DisconnectFlags,
                       rc: mqtt.ReasonCode, properties: mqtt.Properties | None = None) -> None:
        logger.warning("MQTT disconnected (rc=%s)", rc)
        self._connected = False

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
                    self.bridge.dispatch(self.bridge.set_level, device.name, level)
                except ValueError:
                    logger.error("Invalid percentage: %s", payload)

            elif topic == f"{dt}/set/power":
                self.bridge.dispatch(self.bridge.set_power, device.name, payload.upper() == "ON")

            elif topic == f"{dt}/set/mode":
                mode = self._i18n["select_map"].get(payload)
                if mode:
                    self.bridge.dispatch(self.bridge.set_mode, device.name, mode)

            elif topic == f"{dt}/set/preset_mode":
                mode = PRESET_TO_MODE.get(payload.lower())
                if mode:
                    self.bridge.dispatch(self.bridge.set_mode, device.name, mode)

    def publish_state(self, device_name: str, state: VentilationState) -> None:
        if not self._client or not self._connected:
            return

        prefix = self.config.mqtt.topic_prefix
        t = f"{prefix}/{device_name}"

        self._client.publish(f"{t}/state", "ON" if state.is_on else "OFF", retain=True)
        self._client.publish(f"{t}/percentage", state.fan_level, retain=True)
        dir_labels = self._i18n["direction"]
        direction = dir_labels["off"] if not state.is_on else dir_labels.get(state.direction.value, state.direction.value)
        self._client.publish(f"{t}/direction", direction, retain=True)

        # Mode for select entity
        reverse = self._i18n["reverse_map"]
        default_mode = self._i18n["options"][0]
        self._client.publish(f"{t}/mode", reverse.get(state.mode, default_mode), retain=True)

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
        last_seen = status.last_seen_ago
        self._client.publish(f"{t}/last_seen", last_seen if last_seen >= 0 else "unknown", retain=True)

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
        self._publish_mode_select_discovery(device)
        self._publish_direction_sensor_discovery(device)
        self._publish_connection_sensor_discovery(device)
        self._publish_role_sensor_discovery(device)
        self._publish_last_seen_sensor_discovery(device)
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
        for comp, suffix in [("fan", ""), ("select", "_mode"), ("sensor", "_direction"),
                              ("sensor", "_connection"), ("sensor", "_role"),
                              ("sensor", "_last_seen"), ("sensor", "_timer")]:
            self._client.publish(f"{ha}/{comp}/{uid}{suffix}/config", "", retain=True)

    def clear_device_topics(self, device_name: str) -> None:
        """Clear retained MQTT state topics for a device (after rename/remove)."""
        if not self._client:
            return
        prefix = self.config.mqtt.topic_prefix
        t = f"{prefix}/{device_name}"
        for suffix in ["/state", "/percentage", "/direction", "/mode",
                       "/json", "/connection", "/role", "/last_seen", "/timer",
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
            "name": device.friendly_name or device.name,
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
                "name": f"MAICO {device.friendly_name or device.name}",
                "manufacturer": "MAICO",
                "model": "PP 45 RC",
                "via_device": "maico_bridge",
                "configuration_url": web_url,
            },
        }
        self._client.publish(f"{ha}/fan/{uid}/config", json.dumps(payload), retain=True)
        logger.info("Discovery: fan %s", device.friendly_name)

    def _publish_mode_select_discovery(self, device: DeviceConfig) -> None:
        if not self._client:
            return
        prefix = self.config.mqtt.topic_prefix
        ha = self.config.mqtt.ha_discovery_prefix
        uid = f"maico_{device.name.lower()}_mode"
        dt = f"{prefix}/{device.name}"

        payload = {
            "name": f"{device.friendly_name or device.name} {self._i18n['entity_names']['mode']}",
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

    def _publish_direction_sensor_discovery(self, device: DeviceConfig) -> None:
        if not self._client:
            return
        prefix = self.config.mqtt.topic_prefix
        ha = self.config.mqtt.ha_discovery_prefix
        uid = f"maico_{device.name.lower()}_direction"
        dt = f"{prefix}/{device.name}"

        payload = {
            "name": f"{device.friendly_name or device.name} {self._i18n['entity_names']['direction']}",
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
            "name": f"{device.friendly_name or device.name} {self._i18n['entity_names']['connection']}",
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
            "name": f"{device.friendly_name or device.name} {self._i18n['entity_names']['role']}",
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
            "name": f"{device.friendly_name or device.name} {self._i18n['entity_names']['last_seen']}",
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

    def _publish_timer_sensor_discovery(self, device: DeviceConfig) -> None:
        if not self._client:
            return
        prefix = self.config.mqtt.topic_prefix
        ha = self.config.mqtt.ha_discovery_prefix
        uid = f"maico_{device.name.lower()}_timer"
        dt = f"{prefix}/{device.name}"

        payload = {
            "name": f"{device.friendly_name or device.name} Timer",
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

    @staticmethod
    def _level_to_percentage(level: int) -> int:
        return int(level * 100 / 5)

    @staticmethod
    def _percentage_to_level(percentage: int) -> int:
        if percentage <= 0:
            return 0
        return max(1, min(5, round(percentage * 5 / 100)))
