"""Bridge integration tests using fake serial + fake MQTT.

These pin the current end-to-end behaviour of the packet handlers and command
paths so the God-object refactor (#5) can be verified to change nothing.
"""

import pytest

from src.enocean_serial import RORG_MSC
from src.maico_protocol import AirflowDirection, VentilationMode, str_to_id
from src.main import ConnectionStatus
from tests.fakes import make_bridge, DEFAULT_BASE

BASE_STR = "".join(f"{b:02X}" for b in DEFAULT_BASE)  # 05A2B6C1


def status_pkt(sender, dest, stufe, mode_byte=0x30):
    return {"type": "radio", "rorg": RORG_MSC, "sender": str_to_id(sender), "status": 0,
            "user_data": [0x27, 0x10, stufe, mode_byte, 0, 0, 0], "dest": str_to_id(dest)}


def sync_pkt(sender, dest, status_byte, timer=0x30):
    return {"type": "radio", "rorg": RORG_MSC, "sender": str_to_id(sender), "status": 0,
            "user_data": [0x27, 0x00, status_byte, timer, 0], "dest": str_to_id(dest)}


def cmd_pkt(sender, dest, level_byte, flag=0):
    return {"type": "radio", "rorg": RORG_MSC, "sender": str_to_id(sender), "status": 0,
            "user_data": [0x27, 0x20, level_byte, 0, flag], "dest": str_to_id(dest)}


# --- Status reports (27 10) ---

def test_status_report_to_base_marks_managed_and_publishes():
    b = make_bridge([("bad", "051EF6BA")])
    b._handle_packet(status_pkt("051EF6BA", BASE_STR, 0x03))
    assert b._device_status["bad"].connection == ConnectionStatus.MANAGED
    assert b.mqtt.states["bad"]["fan_level"] == 3
    assert b.mqtt.states["bad"]["mode"] == "heat_exchanger"
    assert b.mqtt.availability["bad"] is True


def test_status_report_keeps_direction_unknown_for_solo():
    b = make_bridge([("bad", "051EF6BA")])
    b._handle_packet(status_pkt("051EF6BA", BASE_STR, 0x03))
    # 27 10 must not set a direction (only sync does).
    assert b.mqtt.states["bad"]["direction"] == "unknown"


def test_invalid_sender_is_ignored():
    b = make_bridge([("bad", "051EF6BA")])
    b._handle_packet(status_pkt("FFFFFFFF", BASE_STR, 0x03))
    assert b.mqtt.states == {}


# --- Sync (27 00) ---

def test_sync_detects_roles_and_opposite_directions():
    b = make_bridge([("leo", "051EA5D9"), ("schlaf", "05229657")])
    b._handle_packet(sync_pkt("051EA5D9", "05229657", 0x22))  # inflow lvl 2
    assert b._device_status["leo"].detected_role == "master"
    assert b._device_status["leo"].syncs_to == "05229657"
    assert b._device_status["schlaf"].detected_role == "slave"
    assert b.mqtt.states["leo"]["direction"] == "inflow"
    assert b.mqtt.states["schlaf"]["direction"] == "exhaust"  # opposite of master


# --- Commands ---

def test_set_level_sends_telegram_and_publishes():
    b = make_bridge([("bad", "051EF6BA")])
    assert b.set_level("bad", 3) is True
    assert len(b.serial.sent) == 1
    data, _ = b.serial.sent[0]
    assert data[3] == 0xE3            # 0xE0 + level 3, heat exchanger
    assert b.mqtt.states["bad"]["fan_level"] == 3


def test_set_level_zero_turns_off():
    b = make_bridge([("bad", "051EF6BA")])
    b.set_level("bad", 0)
    data, _ = b.serial.sent[0]
    assert data[3] == 0xE0
    assert b.mqtt.states["bad"]["mode"] == "off"


def test_set_level_rejects_invalid_device_id():
    b = make_bridge([("ghost", "FFFFFFFF")])
    assert b.set_level("ghost", 2) is False
    assert b.serial.sent == []


def test_set_power_on_uses_last_level_or_one():
    b = make_bridge([("bad", "051EF6BA")])
    b.set_power("bad", True)
    assert b.mqtt.states["bad"]["fan_level"] == 1
    assert b.mqtt.states["bad"]["is_on"] is True


# --- RLS global sync ---

def test_rls_global_sync_propagates_to_devices():
    b = make_bridge([("bad", "051EF6BA")], rls_id="052200AA", rls_global_sync=True)
    b._handle_packet(cmd_pkt("052200AA", BASE_STR, 0xE3))  # RLS sets level 3
    assert b.mqtt.states["bad"]["fan_level"] == 3
    assert b.mqtt.states["bad"]["mode"] == "heat_exchanger"


def test_rls_without_global_sync_does_not_change_devices():
    b = make_bridge([("bad", "051EF6BA")], rls_id="052200AA", rls_global_sync=False)
    b._handle_packet(cmd_pkt("052200AA", BASE_STR, 0xE3))
    assert "bad" not in b.mqtt.states  # device state untouched


# --- Timers (need a running loop for asyncio.create_task) ---

@pytest.mark.asyncio
async def test_set_mode_boost_starts_timer():
    b = make_bridge([("bad", "051EF6BA")])
    b.set_mode("bad", VentilationMode.BOOST)
    try:
        assert b.mqtt.states["bad"]["mode"] == "boost"
        assert b.mqtt.states["bad"]["fan_level"] == 5
        assert "bad" in b._saved_states
        assert "bad" in b._timer_end
        assert b.mqtt.timers["bad"] == 30
        data, _ = b.serial.sent[0]
        assert data[3] == 0xED        # summer level 5 (0xE8 + 5)
    finally:
        b._cancel_mode_timer("bad")


def test_restore_mode_returns_to_saved_state():
    b = make_bridge([("bad", "051EF6BA")])
    from src.maico_protocol import VentilationState
    b._saved_states["bad"] = VentilationState(mode=VentilationMode.HEAT_EXCHANGER, fan_level=3)
    b._restore_mode("bad")
    assert b.mqtt.states["bad"]["fan_level"] == 3
    assert b.mqtt.states["bad"]["mode"] == "heat_exchanger"
    assert "bad" not in b._saved_states
