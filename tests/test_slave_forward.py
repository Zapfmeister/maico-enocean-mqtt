"""Slave commands must be forwarded to the master (a slave mirrors the master
and can't be driven independently)."""

from src.maico_protocol import VentilationMode
from tests.fakes import make_bridge
from tests.test_bridge import sync_pkt


def _paired_bridge():
    b = make_bridge([("leo", "051EA5D9"), ("schlaf", "05229657")])
    # 27 00 sync: Leopold (master) → Schlafzimmer (slave).
    b._handle_packet(sync_pkt("051EA5D9", "05229657", 0x22))
    assert b._device_status["schlaf"].detected_role == "slave"
    assert b._device_status["leo"].detected_role == "master"
    return b


def test_set_level_on_slave_controls_master():
    b = _paired_bridge()
    assert b.set_level("schlaf", 4, source="web")
    assert b.mqtt.states["leo"]["fan_level"] == 4


def test_set_mode_on_slave_controls_master():
    b = _paired_bridge()
    assert b.set_mode("schlaf", VentilationMode.SUMMER, source="web")
    assert b.mqtt.states["leo"]["mode"] == "summer"


def test_event_log_attributes_change_to_master():
    b = _paired_bridge()
    b.set_level("schlaf", 3, source="ha")
    ctrl = [e for e in b.events.recent() if e["category"] == "control"]
    # The master is what actually changed, so that's what the log shows.
    assert ctrl and ctrl[0]["device"] == "leo"


def test_master_and_standalone_not_redirected():
    b = _paired_bridge()
    assert b._resolve_to_master("leo") == "leo"
    b2 = make_bridge([("bad", "051EF6BA")])
    assert b2._resolve_to_master("bad") == "bad"
