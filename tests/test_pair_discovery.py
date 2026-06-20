"""Feature: Master/Slave-Paar als eine HA-Einheit (Issue #33).

Rollen-reaktive Discovery: ein Slave bekommt KEINE steuerbaren Entities mehr;
seine read-only-Sensoren (Richtung, Rolle, Verbindung) erscheinen unter den
device.identifiers des Masters. Standalone/Master unverändert.
"""
import json

from src.config import AppConfig, DeviceConfig
from src.mqtt_client import MqttClient

LEO_ID = "051EA5D9"
SZ_ID = "05229657"


class _Rec:
    def __init__(self):
        self.pub: dict[str, str] = {}

    def publish(self, topic, payload, retain=False):
        self.pub[topic] = payload


def _mqtt(pair_grouping=True):
    cfg = AppConfig()
    cfg.pair_grouping = pair_grouping
    cfg.devices = [
        DeviceConfig(name="Leopold", friendly_name="Leopold Lüftung", device_id=LEO_ID),
        DeviceConfig(name="Schlafzimmer", friendly_name="Schlafzimmer Lüftung", device_id=SZ_ID),
    ]
    mc = MqttClient(cfg, object())
    mc._client = _Rec()
    return mc


def _cfg(mc, name):
    return next(d for d in mc.config.devices if d.name == name)


def _payload(mc, topic):
    raw = mc._client.pub.get(topic)
    return json.loads(raw) if raw else None


def test_standalone_full_discovery_own_identifiers():
    mc = _mqtt()
    mc.publish_device_discovery(_cfg(mc, "Leopold"), role="standalone")
    fan = _payload(mc, "homeassistant/fan/maico_leopold/config")
    assert fan is not None, "standalone muss steuerbaren fan publizieren"
    assert fan["device"]["identifiers"] == [f"maico_{LEO_ID}"]


def test_slave_controllable_entities_removed():
    mc = _mqtt()
    mc.publish_device_discovery(_cfg(mc, "Schlafzimmer"), role="slave", master=_cfg(mc, "Leopold"))
    # Steuerbare Entities müssen entfernt sein (leere retained config).
    for topic in [
        "homeassistant/fan/maico_schlafzimmer/config",
        "homeassistant/select/maico_schlafzimmer_mode/config",
        "homeassistant/switch/maico_schlafzimmer_summer/config",
        "homeassistant/button/maico_schlafzimmer_sleep/config",
        "homeassistant/button/maico_schlafzimmer_boost/config",
    ]:
        assert mc._client.pub.get(topic) == "", f"{topic} muss geleert sein"


def test_slave_readonly_sensors_homed_under_master():
    mc = _mqtt()
    mc.publish_device_discovery(_cfg(mc, "Schlafzimmer"), role="slave", master=_cfg(mc, "Leopold"))
    for suffix in ("direction", "role", "connection"):
        p = _payload(mc, f"homeassistant/sensor/maico_schlafzimmer_{suffix}/config")
        assert p is not None, f"{suffix}-Sensor muss publiziert sein"
        assert p["device"]["identifiers"] == [f"maico_{LEO_ID}"], f"{suffix} muss unter Master hängen"
        assert "Schlafzimmer" in (p.get("name") or ""), f"{suffix}-Name muss Slave-Präfix tragen"


def test_slave_noise_sensors_dropped():
    mc = _mqtt()
    mc.publish_device_discovery(_cfg(mc, "Schlafzimmer"), role="slave", master=_cfg(mc, "Leopold"))
    for suffix in ("rssi", "last_seen", "timer", "speed"):
        assert mc._client.pub.get(f"homeassistant/sensor/maico_schlafzimmer_{suffix}/config") == "", \
            f"{suffix} sollte am Slave entfernt sein"


def test_toggle_off_restores_full_slave_discovery():
    mc = _mqtt(pair_grouping=False)
    mc.publish_device_discovery(_cfg(mc, "Schlafzimmer"), role="slave", master=_cfg(mc, "Leopold"))
    fan = _payload(mc, "homeassistant/fan/maico_schlafzimmer/config")
    assert fan is not None, "Toggle aus → altes Verhalten (steuerbarer Slave)"
    assert fan["device"]["identifiers"] == [f"maico_{SZ_ID}"]


if __name__ == "__main__":
    import sys
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    failed = 0
    for fn in fns:
        try:
            fn(); print(f"ok  {fn.__name__}")
        except Exception as e:
            failed += 1; print(f"FAIL {fn.__name__}: {type(e).__name__}: {e}")
    sys.exit(1 if failed else 0)
