"""Tests for the MAICO MSC protocol codec (decode + encode)."""

from src.enocean_serial import parse_esp3_packets
from src.maico_protocol import (
    AirflowDirection,
    MscType,
    VentilationMode,
    build_set_level,
    build_status_report,
    id_to_str,
    parse_msc_telegram,
    str_to_id,
    _decode_status_report,
)
from tests import captures as cap


def _telegram(frame: bytes):
    pkt = parse_esp3_packets(frame).packets[0]
    return parse_msc_telegram(pkt["user_data"], pkt["sender"], pkt["dest"])


# --- ID helpers ---

def test_id_round_trip():
    assert id_to_str([0x05, 0x1E, 0xA8, 0x03]) == "051EA803"
    assert str_to_id("051EA803") == [0x05, 0x1E, 0xA8, 0x03]
    assert str_to_id("05:1E:A8:03") == [0x05, 0x1E, 0xA8, 0x03]


# --- Status report decode (27 10) ---

def test_decode_status_report_levels():
    off = _decode_status_report(0x00, 0xE0)
    assert off.mode == VentilationMode.OFF
    assert off.fan_level == 0
    assert off.direction == AirflowDirection.UNKNOWN
    s2 = _decode_status_report(0x02, 0xE0)
    assert s2.mode == VentilationMode.HEAT_EXCHANGER
    assert s2.fan_level == 2
    assert s2.direction == AirflowDirection.EXHAUST
    s_inflow = _decode_status_report(0x23, 0xE0)
    assert s_inflow.fan_level == 3
    assert s_inflow.direction == AirflowDirection.INFLOW


def test_decode_summer_and_sleep():
    summer = _decode_status_report(0x0A, 0xE0)
    assert summer.mode == VentilationMode.SUMMER
    assert summer.fan_level == 2
    assert _decode_status_report(0x40, 0xE0).mode == VentilationMode.SLEEP_HEAT
    assert _decode_status_report(0x48, 0xE0).mode == VentilationMode.SLEEP_SUMMER


def test_decode_unknown_stufe_falls_back_to_off():
    s = _decode_status_report(0x7F, 0xE0)
    assert s.mode == VentilationMode.OFF
    assert s.fan_level == 0


def test_parse_real_status_report():
    tg = _telegram(cap.STATUS_L2)
    assert tg.msg_type == MscType.STATUS_REPORT
    assert tg.state.fan_level == 2
    assert tg.state.mode == VentilationMode.HEAT_EXCHANGER
    assert tg.state.direction == AirflowDirection.EXHAUST

    off = _telegram(cap.STATUS_OFF)
    assert off.state.fan_level == 0
    assert off.state.mode == VentilationMode.OFF


# --- Set-level decode (27 20) ---

def test_parse_real_setlevel():
    tg = _telegram(cap.SETLEVEL_L2)
    assert tg.msg_type == MscType.SET_LEVEL
    assert tg.state.fan_level == 2
    assert tg.state.mode == VentilationMode.HEAT_EXCHANGER


# --- Sync decode (27 00) ---

def test_parse_real_sync():
    tg = _telegram(cap.SYNC_INFLOW_L2)
    assert tg.msg_type == MscType.MASTER_SLAVE_SYNC
    assert tg.sender == [0x05, 0x1E, 0xA5, 0xD9]
    assert tg.dest == [0x05, 0x22, 0x96, 0x57]
    assert tg.raw_data[2] == 0x22  # status byte (inflow lvl 2)


def test_parse_slave_ack_type():
    tg = _telegram(cap.SLAVE_ACK)
    assert tg.msg_type == MscType.SLAVE_ACK


def test_non_msc_returns_none():
    assert parse_msc_telegram([0xAB, 0xCD], [0, 0, 0, 0]) is None


# --- Encoding (27 20 / 27 10) ---

def test_build_set_level_heat_exchanger():
    base = [0x05, 0xA2, 0xB6, 0xC1]
    dev = [0x05, 0x1E, 0xA8, 0x03]
    data, optional = build_set_level(base, dev, 3, VentilationMode.HEAT_EXCHANGER)
    assert data[:3] == [0xD1, 0x27, 0x20]
    assert data[3] == 0xE3          # 0xE0 + level 3
    assert data[5] == 0x00          # flag byte
    assert data[6:10] == base       # sender = our base id
    assert optional == [0x03] + dev + [0xFF, 0x00]


def test_build_set_level_summer():
    data, _ = build_set_level([0]*4, [0]*4, 4, VentilationMode.SUMMER)
    assert data[3] == 0xE8 + 4      # summer offset


def test_build_set_level_sleep_sets_flag():
    data, _ = build_set_level([0]*4, [0]*4, 0, VentilationMode.SLEEP_HEAT)
    assert data[3] == 0xE0
    assert data[5] == 0x01          # sleep flag


def test_build_set_level_clamps():
    data, _ = build_set_level([0]*4, [0]*4, 99, VentilationMode.HEAT_EXCHANGER)
    assert data[3] == 0xE5          # clamped to level 5


def test_set_level_then_parse_round_trip():
    # A command we build should decode back to the same level/mode.
    base = [0x05, 0xA2, 0xB6, 0xC1]
    dev = [0x05, 0x1E, 0xA8, 0x03]
    data, optional = build_set_level(base, dev, 4, VentilationMode.SUMMER)
    # user_data is everything after RORG, before sender(4)+status(1).
    user_data = data[1:-5]
    tg = parse_msc_telegram(user_data, base, dev)
    assert tg.state.fan_level == 4
    assert tg.state.mode == VentilationMode.SUMMER


def test_build_status_report_structure():
    fake = [0x05, 0xA2, 0xB6, 0xC2]
    rls = [0x05, 0x22, 0x96, 0x57]
    data, optional = build_status_report(fake, rls, 3, VentilationMode.HEAT_EXCHANGER)
    assert data[:3] == [0xD1, 0x27, 0x10]
    assert data[3] == 3             # stufe = level for heat exchanger
    assert optional == [0x03] + rls + [0xFF, 0x00]
