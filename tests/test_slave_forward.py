"""Slave commands must be forwarded to the master (a slave mirrors the master
and can't be driven independently)."""

from src.maico_protocol import AirflowDirection, VentilationMode, VentilationState
from tests.fakes import make_bridge
from tests.test_bridge import BASE_STR, status_pkt, sync_pkt


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


def test_slave_mirrors_master_mode_switch_while_running():
    """A heat_exchanger->summer switch while both units run must be mirrored onto
    the slave's published state. Previously the slave was only mirrored on on/off
    transitions, so its mode stayed stale at heat_exchanger forever — which made
    HA's idempotency guard re-issue the command endlessly (logged as repeated
    master 'summer' control events)."""
    b = _paired_bridge()
    # Both running in heat_exchanger; the slave never sends its own status, so
    # seed and publish its mirrored state explicitly.
    b._states["leo"] = VentilationState(mode=VentilationMode.HEAT_EXCHANGER,
                                        fan_level=2, direction=AirflowDirection.EXHAUST)
    b._states["schlaf"] = VentilationState(mode=VentilationMode.HEAT_EXCHANGER,
                                           fan_level=2, direction=AirflowDirection.INFLOW)
    b.mqtt.publish_state("schlaf", b._states["schlaf"])
    # Master reports SUMMER level 2 (stufe 0x0A) — e.g. after HA switched it.
    b._handle_packet(status_pkt("051EA5D9", BASE_STR, 0x0A))
    assert b._states["leo"].mode == VentilationMode.SUMMER
    # The slave physically follows: its published mode must mirror summer...
    assert b._states["schlaf"].mode == VentilationMode.SUMMER
    assert b.mqtt.states["schlaf"]["mode"] == "summer"
    # ...while keeping its own (opposite) airflow direction.
    assert b._states["schlaf"].direction == AirflowDirection.INFLOW
