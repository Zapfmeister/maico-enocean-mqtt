"""Tests for the raw ESP3 serial layer: CRC8, packet building and parsing."""

from src.enocean_serial import (
    crc8,
    build_esp3_packet,
    parse_esp3_packets,
    PACKET_TYPE_RADIO,
    RORG_MSC,
)
from tests import captures as cap


# --- CRC8 ---

def test_crc8_known_header():
    # Header bytes [00 0B 07 01] of a real frame carry header-CRC 0x80.
    assert crc8([0x00, 0x0B, 0x07, 0x01]) == 0x80


def test_crc8_empty():
    assert crc8([]) == 0x00


def test_crc8_matches_real_frame_data():
    # Last byte of a real frame is its data-CRC over data+optional.
    frame = cap.SETLEVEL_L2
    # data_len/opt_len from header
    data_len = (frame[1] << 8) | frame[2]
    opt_len = frame[3]
    payload = frame[6:6 + data_len + opt_len]
    assert crc8(list(payload)) == frame[-1]


# --- Parsing real frames ---

def test_parse_setlevel():
    res = parse_esp3_packets(cap.SETLEVEL_L2)
    assert res.consumed == len(cap.SETLEVEL_L2)
    assert len(res.packets) == 1
    pkt = res.packets[0]
    assert pkt["type"] == "radio"
    assert pkt["rorg"] == RORG_MSC
    assert pkt["sender"] == [0x05, 0xA2, 0xB6, 0xC1]
    assert pkt["dest"] == [0x05, 0x1E, 0xA8, 0x03]
    assert pkt["user_data"][:3] == [0x27, 0x20, 0xE2]


def test_parse_status_report():
    pkt = parse_esp3_packets(cap.STATUS_L2).packets[0]
    assert pkt["sender"] == [0x05, 0x1E, 0xA8, 0x03]
    assert pkt["user_data"][:4] == [0x27, 0x10, 0x02, 0xE0]


def test_parse_multiple_packets_in_one_buffer():
    res = parse_esp3_packets(cap.THREE_SETLEVEL)
    assert len(res.packets) == 3
    assert res.consumed == len(cap.THREE_SETLEVEL)
    dests = [p["dest"] for p in res.packets]
    assert dests == [
        [0x05, 0x1E, 0xA8, 0x03],
        [0x05, 0x1E, 0xA5, 0xD9],
        [0x05, 0x1E, 0xF6, 0xBA],
    ]


# --- Partial buffers ---

def test_partial_packet_is_not_consumed():
    # Feed all but the last 3 bytes: the trailing incomplete packet must be
    # kept for the next read cycle (consumed stops before it).
    full = cap.SETLEVEL_L2
    partial = full[:-3]
    res = parse_esp3_packets(partial)
    assert res.packets == []
    assert res.consumed == 0  # nothing safely parsed yet


def test_partial_then_complete():
    # Simulate two reads: first half, then the rest appended.
    full = cap.STATUS_L2
    first = full[:8]
    res1 = parse_esp3_packets(first)
    assert res1.packets == []
    # Buffer keeps unconsumed bytes; append remainder.
    buf = first[res1.consumed:] + full[8:]
    res2 = parse_esp3_packets(buf)
    assert len(res2.packets) == 1
    assert res2.consumed == len(buf)


def test_leading_garbage_before_sync_is_skipped():
    noise = bytes([0x12, 0x34, 0xAB])
    res = parse_esp3_packets(noise + cap.SETLEVEL_L2)
    assert len(res.packets) == 1
    assert res.consumed == len(noise) + len(cap.SETLEVEL_L2)


# --- Corruption resilience ---

def test_bad_header_crc_is_rejected():
    frame = bytearray(cap.SETLEVEL_L2)
    frame[5] ^= 0xFF  # corrupt header CRC
    res = parse_esp3_packets(bytes(frame))
    assert res.packets == []


def test_bad_data_crc_is_rejected():
    frame = bytearray(cap.SETLEVEL_L2)
    frame[-1] ^= 0xFF  # corrupt data CRC
    res = parse_esp3_packets(bytes(frame))
    assert res.packets == []


def test_absurd_length_does_not_hang():
    # A sync byte followed by a huge declared length must be skipped, not
    # trusted (guards against runaway buffer reads from RF noise).
    frame = bytes([0x55, 0xFF, 0xFF, 0xFF, 0x01]) + cap.SETLEVEL_L2
    res = parse_esp3_packets(frame)
    # The real frame after the bogus sync still parses.
    assert len(res.packets) == 1


# --- Round trip ---

def test_build_then_parse_round_trip():
    data = [RORG_MSC, 0x27, 0x20, 0xE3, 0x00, 0x00, 0x05, 0x1E, 0xA8, 0x03, 0x00]
    optional = [0x03, 0x05, 0xA2, 0xB6, 0xC1, 0xFF, 0x00]
    raw = build_esp3_packet(data, optional, PACKET_TYPE_RADIO)
    res = parse_esp3_packets(raw)
    assert len(res.packets) == 1
    pkt = res.packets[0]
    assert pkt["rorg"] == RORG_MSC
    assert pkt["sender"] == [0x05, 0x1E, 0xA8, 0x03]
    assert pkt["dest"] == [0x05, 0xA2, 0xB6, 0xC1]
    assert pkt["user_data"][:3] == [0x27, 0x20, 0xE3]
