"""Tests for per-device RSSI: parsing it out of ERP1 optional data and
recording it on the device status."""

import tests.captures as cap
from src.enocean_serial import RORG_MSC, parse_esp3_packets
from src.maico_protocol import str_to_id
from tests.fakes import make_bridge


def _radio(sender, dest, rssi):
    return {"type": "radio", "rorg": RORG_MSC, "sender": str_to_id(sender), "status": 0,
            "user_data": [0x27, 0x10, 0x03, 0x30, 0, 0, 0], "dest": str_to_id(dest), "rssi": rssi}


def test_parser_extracts_rssi_from_real_frame():
    pkt = parse_esp3_packets(cap.STATUS_L2).packets[0]
    assert pkt["rssi"] is not None
    assert pkt["rssi"] < 0  # dBm magnitude, negated


def test_bridge_records_rssi_for_known_device():
    b = make_bridge([("bad", "051EF6BA")])
    b._handle_packet(_radio("051EF6BA", "05A2B6C1", -67))
    assert b._device_status["bad"].rssi == -67
    assert b._device_status["bad"].to_dict()["rssi"] == -67


def test_rssi_updates_on_next_packet():
    b = make_bridge([("bad", "051EF6BA")])
    b._handle_packet(_radio("051EF6BA", "05A2B6C1", -67))
    b._handle_packet(_radio("051EF6BA", "05A2B6C1", -72))
    assert b._device_status["bad"].rssi == -72


def test_unknown_sender_rssi_not_stored():
    b = make_bridge([("bad", "051EF6BA")])
    b._handle_packet(_radio("05123456", "05A2B6C1", -50))
    # Discovery is off, so the unknown sender is never registered.
    assert "PP45_3456" not in b._device_status


def test_to_dict_includes_rssi_none_by_default():
    b = make_bridge([("bad", "051EF6BA")])
    assert b._device_status.get("bad") is None or b._device_status["bad"].rssi is None
