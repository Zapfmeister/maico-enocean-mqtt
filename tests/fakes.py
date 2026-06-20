"""Test doubles for bridge integration tests — fake serial + fake MQTT.

These let the bridge's packet handlers and command paths run end-to-end without
real hardware or a broker: FakeSerial records what was transmitted and can feed
received packets into the bridge; FakeMqtt records every publish.
"""

from src.config import AppConfig, DeviceConfig
from src.events import EventLog
from src.main import MaicoMqttBridge

DEFAULT_BASE = (0x05, 0xA2, 0xB6, 0xC1)


class FakeSerial:
    """Stand-in for EnOceanSerial: records sends, can inject received packets."""

    def __init__(self, base_id=DEFAULT_BASE):
        self._base_id = list(base_id) if base_id else None
        self.sent: list[tuple[list[int], list[int]]] = []
        self._cb = None

    @property
    def base_id(self):
        return self._base_id

    @property
    def base_id_str(self):
        if self._base_id:
            return ":".join(f"{b:02X}" for b in self._base_id)
        return "unknown"

    def send(self, data, optional):
        self.sent.append((list(data), list(optional)))

    def on_packet(self, cb):
        self._cb = cb

    def feed(self, pkt: dict):
        """Deliver a received packet to the bridge's handler."""
        if self._cb:
            self._cb(pkt)


class FakeMqtt:
    """Stand-in for MqttClient: records every publish the bridge makes."""

    def __init__(self):
        self.states: dict[str, dict] = {}
        self.connection: dict[str, dict] = {}
        self.availability: dict[str, bool] = {}
        self.timers: dict[str, int] = {}
        self.events: list[tuple[str, dict]] = []
        self.discovery: list[str] = []
        self.discovery_roles: dict[str, str] = {}

    def publish_state(self, name, state):
        self.states[name] = state.to_dict()

    def publish_connection_status(self, name, status):
        self.connection[name] = status.to_dict()

    def publish_device_availability(self, name, online):
        self.availability[name] = online

    def publish_timer(self, name, remaining_minutes):
        self.timers[name] = remaining_minutes

    def publish_event(self, event_type, data):
        self.events.append((event_type, data))

    def publish_device_discovery(self, device, *, role="standalone", master=None):
        self.discovery.append(device.name)
        self.discovery_roles[device.name] = role


def make_bridge(devices=None, base_id=DEFAULT_BASE, rls_id="", rls_global_sync=False):
    """Build a bridge wired to fake serial + mqtt, with device mappings set up."""
    cfg = AppConfig()
    cfg.rls_global_sync = rls_global_sync
    if rls_id:
        cfg.remote.device_id = rls_id
    for name, dev_id in (devices or []):
        cfg.devices.append(DeviceConfig(name=name, friendly_name=name, device_id=dev_id))
    bridge = MaicoMqttBridge(cfg)
    bridge.serial = FakeSerial(base_id)
    bridge.mqtt = FakeMqtt()
    # Keep tests hermetic: memory-only event log, no writes to the data dir.
    bridge.events = EventLog()
    bridge._setup_device_mappings()
    return bridge
