"""Tests for the in-memory event log and the bridge's event recording."""

from src.events import Event, EventLog
from src.maico_protocol import VentilationMode
from tests.fakes import make_bridge
from tests.test_bridge import status_pkt, BASE_STR


# --- EventLog unit tests ---

def test_recent_is_newest_first():
    log = EventLog()
    log.add("control", "first", now=1.0)
    log.add("control", "second", now=2.0)
    msgs = [e["message"] for e in log.recent()]
    assert msgs == ["second", "first"]


def test_maxlen_trims_oldest():
    log = EventLog(maxlen=3)
    for i in range(5):
        log.add("system", f"e{i}", now=float(i))
    assert len(log) == 3
    assert [e["message"] for e in log.recent()] == ["e4", "e3", "e2"]


def test_to_dict_computes_ago():
    ev = Event(ts=100.0, category="mqtt", message="x")
    d = ev.to_dict(now=130.0)
    assert d["ago"] == 30
    assert d["category"] == "mqtt"


def test_recent_limit():
    log = EventLog()
    for i in range(10):
        log.add("system", f"e{i}", now=float(i))
    assert len(log.recent(limit=3)) == 3


# --- Bridge integration ---

def test_set_level_records_control_event_with_source():
    b = make_bridge([("bad", "051EF6BA")])
    assert b.set_level("bad", 3, source="web")
    ev = b.events.recent()
    assert ev[0]["category"] == "control"
    assert ev[0]["source"] == "web"
    assert "Stufe 3" in ev[0]["message"]


def test_set_level_off_event():
    b = make_bridge([("bad", "051EF6BA")])
    b.set_level("bad", 0, source="ha")
    assert "Aus" in b.events.recent()[0]["message"]


def test_rls_source_suppresses_per_device_control_event():
    b = make_bridge([("bad", "051EF6BA")])
    b.set_level("bad", 2, source="rls")
    assert [e for e in b.events.recent() if e["category"] == "control"] == []


def test_set_mode_records_localised_event():
    b = make_bridge([("bad", "051EF6BA")])
    b.set_mode("bad", VentilationMode.SUMMER, source="web")
    ctrl = [e for e in b.events.recent() if e["category"] == "control"]
    assert ctrl and "Sommer" in ctrl[0]["message"]


def test_availability_flip_records_connection_event():
    b = make_bridge([("bad", "051EF6BA")])
    # First contact establishes the baseline (online) without an event.
    b._handle_packet(status_pkt("051EF6BA", BASE_STR, 0x03))
    assert [e for e in b.events.recent() if e["category"] == "connection"] == []
    # Make it stale, then a re-evaluation flips it to unavailable → one event.
    b._device_status["bad"].last_seen = 0
    b._device_status["bad"].last_sync = 0
    b._publish_availability_if_changed("bad")
    conn = [e for e in b.events.recent() if e["category"] == "connection"]
    assert conn and "nicht erreichbar" in conn[0]["message"]
