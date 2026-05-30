"""publish_connection_status must never send the string "unknown" to the
numeric rssi / last_seen sensors (that makes HA log invalid-state warnings)."""

import time

from src.config import AppConfig, DeviceConfig
from src.devices import DeviceStatus
from src.mqtt_client import MqttClient


class Recorder:
    def __init__(self):
        self.pub = []

    def publish(self, topic, payload, retain=False):
        self.pub.append((topic, payload))


def _client():
    cfg = AppConfig()
    cfg.devices = [DeviceConfig(name="Bad", friendly_name="Bad", device_id="051EF6BA")]
    c = MqttClient(cfg, bridge=None)
    c._client = Recorder()
    c._connected = True
    return c


def test_unknown_values_are_not_published():
    c = _client()
    c.publish_connection_status("Bad", DeviceStatus())  # rssi None, never seen
    topics = [t for t, _ in c._client.pub]
    assert not any(t.endswith("/rssi") for t in topics)
    assert not any(t.endswith("/last_seen") for t in topics)
    assert any(t.endswith("/connection") for t in topics)  # still published


def test_valid_values_are_published_numeric():
    c = _client()
    ds = DeviceStatus(rssi=-70)
    ds.last_seen = time.time()
    c.publish_connection_status("Bad", ds)
    pubs = dict(c._client.pub)
    assert pubs["maico/Bad/rssi"] == -70
    assert isinstance(pubs["maico/Bad/last_seen"], int) and pubs["maico/Bad/last_seen"] >= 0
