"""Tests for MQTT connection-health metrics and the duration formatter."""

from src.mqtt_client import ConnectionHealth
from src.web import _fmt_duration


def test_initial_state_is_disconnected_and_empty():
    h = ConnectionHealth(started_at=0.0)
    assert h.connected is False
    assert h.connect_count == 0
    assert h.reconnect_count == 0
    d = h.to_dict(now=0.0)
    assert d["connected"] is False
    assert d["reconnect_count"] == 0
    assert d["connected_for"] == -1        # never connected
    assert d["last_disconnect_ago"] == -1  # never dropped


def test_first_connect_is_not_a_reconnect():
    h = ConnectionHealth(started_at=0.0)
    h.record_connect(now=10.0)
    assert h.connected is True
    assert h.connect_count == 1
    assert h.reconnect_count == 0
    d = h.to_dict(now=15.0)
    assert d["connected_for"] == 5


def test_drop_and_recovery_counts_one_reconnect():
    h = ConnectionHealth(started_at=0.0)
    h.record_connect(now=0.0)       # initial
    h.record_disconnect(now=100.0)  # link drops
    assert h.connected is False
    h.record_connect(now=103.0)     # comes back
    assert h.connected is True
    assert h.reconnect_count == 1
    assert h.disconnect_count == 1
    d = h.to_dict(now=110.0)
    assert d["last_disconnect_ago"] == 10  # 110 - 100
    assert d["connected_for"] == 7         # 110 - 103


def test_repeated_flapping_accumulates():
    h = ConnectionHealth(started_at=0.0)
    h.record_connect(now=0.0)
    for i in range(1, 6):
        h.record_disconnect(now=i * 10.0)
        h.record_connect(now=i * 10.0 + 1)
    assert h.disconnect_count == 5
    assert h.reconnect_count == 5
    assert h.connected is True


def test_uptime_is_since_started_at():
    h = ConnectionHealth(started_at=1000.0)
    assert h.to_dict(now=1042.0)["uptime"] == 42


def test_fmt_duration_buckets():
    assert _fmt_duration(-1) == "—"
    assert _fmt_duration(0) == "0s"
    assert _fmt_duration(34) == "34s"
    assert _fmt_duration(59) == "59s"
    assert _fmt_duration(60) == "1m"
    assert _fmt_duration(305) == "5m"
    assert _fmt_duration(3600) == "1h"
    assert _fmt_duration(3600 + 240) == "1h 4m"
    assert _fmt_duration(86400) == "1d"
    assert _fmt_duration(86400 + 3 * 3600) == "1d 3h"
