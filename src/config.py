"""Configuration for maico-enocean-mqtt bridge.

Supports YAML config file with ENV var overrides (12-Factor App pattern).
Config can be modified at runtime via Web-UI and saved to config.yaml.

All devices are treated equally — master/slave relationships are
detected automatically from EnOcean traffic (27 00 sync telegrams).
"""

import logging
import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)


@dataclass
class MqttConfig:
    host: str = "localhost"
    port: int = 1883
    username: str = ""
    password: str = ""
    topic_prefix: str = "maico"
    ha_discovery_prefix: str = "homeassistant"


@dataclass
class EnoceanConfig:
    device: str = "/dev/ttyUSB0"


@dataclass
class DeviceConfig:
    name: str = ""
    friendly_name: str = ""
    device_id: str = ""


@dataclass
class RemoteConfig:
    device_id: str = ""
    name: str = "RLS 45 K"


@dataclass
class WebConfig:
    port: int = 8080
    password: str = ""
    hostname: str = "maico-controller"


@dataclass
class AppConfig:
    mqtt: MqttConfig = field(default_factory=MqttConfig)
    enocean: EnoceanConfig = field(default_factory=EnoceanConfig)
    devices: list[DeviceConfig] = field(default_factory=list)
    remote: RemoteConfig = field(default_factory=RemoteConfig)
    web: WebConfig = field(default_factory=WebConfig)
    poll_interval: int = 10
    language: str = "de"
    rls_global_sync: bool = False
    config_path: str = "/data/config.yaml"

    def get_device_by_name(self, name: str) -> DeviceConfig | None:
        for d in self.devices:
            if d.name.lower() == name.lower():
                return d
        return None

    def get_device_by_id(self, device_id: str) -> DeviceConfig | None:
        clean = device_id.upper().replace(":", "")
        for d in self.devices:
            if d.device_id.upper().replace(":", "") == clean:
                return d
        return None

    def add_device(self, device: DeviceConfig) -> None:
        # Prevent duplicates by device_id or name
        existing = self.get_device_by_id(device.device_id) or self.get_device_by_name(device.name)
        if existing:
            existing.friendly_name = device.friendly_name or existing.friendly_name
            return
        self.devices.append(device)

    def remove_device(self, name: str) -> bool:
        for i, d in enumerate(self.devices):
            if d.name.lower() == name.lower():
                self.devices.pop(i)
                return True
        return False

    def save(self) -> bool:
        """Save current config to YAML file. Returns True on success."""
        try:
            path = Path(self.config_path)
            path.parent.mkdir(parents=True, exist_ok=True)
        except OSError:
            logger.exception("Cannot create config directory %s", self.config_path)
            return False

        data = {
            "mqtt": {
                "host": self.mqtt.host,
                "port": self.mqtt.port,
                "username": self.mqtt.username,
                "password": self.mqtt.password,
                "topic_prefix": self.mqtt.topic_prefix,
                "ha_discovery_prefix": self.mqtt.ha_discovery_prefix,
            },
            "enocean": {
                "device": self.enocean.device,
            },
            "devices": [
                {
                    "name": dev.name,
                    "friendly_name": dev.friendly_name,
                    "device_id": dev.device_id,
                }
                for dev in self.devices
            ],
            "remote": {
                "device_id": self.remote.device_id,
                "name": self.remote.name,
            },
            "web": {
                "port": self.web.port,
                "password": self.web.password,
                "hostname": self.web.hostname,
            },
            "poll_interval": self.poll_interval,
            "language": self.language,
            "rls_global_sync": self.rls_global_sync,
        }

        try:
            with open(path, "w") as f:
                yaml.dump(data, f, default_flow_style=False, allow_unicode=True)
        except OSError:
            logger.exception("Failed to save config to %s", self.config_path)
            return False
        return True


def load_config(path: str | None = None) -> AppConfig:
    """Load configuration from YAML file with ENV var overrides."""
    if path is None:
        path = os.environ.get("CONFIG_PATH", "/data/config.yaml")

    cfg = AppConfig(config_path=path)
    config_path = Path(path)

    raw: dict = {}
    if config_path.exists():
        try:
            with open(config_path) as f:
                raw = yaml.safe_load(f) or {}
        except (yaml.YAMLError, OSError):
            logger.exception("Failed to load config from %s, using defaults", path)
            raw = {}

    # MQTT (ENV overrides YAML)
    mqtt_raw = raw.get("mqtt", {})
    cfg.mqtt = MqttConfig(
        host=os.environ.get("MQTT_HOST", mqtt_raw.get("host", "localhost")),
        port=int(os.environ.get("MQTT_PORT", mqtt_raw.get("port", 1883))),
        username=os.environ.get("MQTT_USERNAME", mqtt_raw.get("username", "")),
        password=os.environ.get("MQTT_PASSWORD", mqtt_raw.get("password", "")),
        topic_prefix=mqtt_raw.get("topic_prefix", "maico"),
        ha_discovery_prefix=mqtt_raw.get("ha_discovery_prefix", "homeassistant"),
    )

    # EnOcean
    eno_raw = raw.get("enocean", {})
    cfg.enocean = EnoceanConfig(
        device=os.environ.get("MAICO_SERIAL_PORT", eno_raw.get("device", "/dev/ttyUSB0")),
    )

    # Web
    web_raw = raw.get("web", {})
    cfg.web = WebConfig(
        port=int(os.environ.get("MAICO_WEB_PORT", web_raw.get("port", 8080))),
        password=os.environ.get("MAICO_WEB_PASSWORD", web_raw.get("password", "")),
        hostname=os.environ.get("MAICO_HOSTNAME", web_raw.get("hostname", "maico-controller")),
    )

    cfg.poll_interval = int(os.environ.get(
        "MAICO_POLL_INTERVAL", raw.get("poll_interval", 10),
    ))
    cfg.language = os.environ.get("MAICO_LANGUAGE", raw.get("language", "de"))
    rls_sync_env = os.environ.get("MAICO_RLS_GLOBAL_SYNC", "").lower()
    if rls_sync_env in ("true", "1", "yes"):
        cfg.rls_global_sync = True
    elif rls_sync_env in ("false", "0", "no"):
        cfg.rls_global_sync = False
    else:
        cfg.rls_global_sync = bool(raw.get("rls_global_sync", False))

    # Devices — flat list, no master/slave distinction
    for dev_raw in raw.get("devices", []):
        device_id = dev_raw.get("device_id", dev_raw.get("master_id", ""))
        cfg.devices.append(DeviceConfig(
            name=dev_raw.get("name", ""),
            friendly_name=dev_raw.get("friendly_name", ""),
            device_id=device_id,
        ))
        # Migrate old config: also add slaves as top-level devices
        for slave_raw in dev_raw.get("slaves", []):
            slave_id = slave_raw.get("device_id", "")
            slave_name = slave_raw.get("name", "")
            if slave_id and not cfg.get_device_by_id(slave_id):
                cfg.devices.append(DeviceConfig(
                    name=slave_name,
                    friendly_name=slave_name,
                    device_id=slave_id,
                ))

    # Remote
    remote_raw = raw.get("remote", {})
    cfg.remote = RemoteConfig(
        device_id=remote_raw.get("device_id", ""),
        name=remote_raw.get("name", "RLS 45 K"),
    )

    return cfg
