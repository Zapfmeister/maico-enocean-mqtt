"""Tests for serial reconnect resilience and base-ID reading."""

import src.enocean_serial as es
from src.enocean_serial import (
    EnOceanSerial,
    build_esp3_packet,
    CO_RD_IDBASE,
    PACKET_TYPE_COMMON_COMMAND,
    PACKET_TYPE_RESPONSE,
    RETURN_CODE_OK,
)

BASE = (0x05, 0xA2, 0xB6, 0xC1)


def _base_id_response() -> bytes:
    return build_esp3_packet([RETURN_CODE_OK, *BASE], [], PACKET_TYPE_RESPONSE)


class FakeSerial:
    """Minimal serial stub that answers CO_RD_IDBASE with a base-ID response."""

    def __init__(self, port, baudrate=57600, timeout=0.5):
        self.is_open = True
        self._rx = bytearray()

    def reset_input_buffer(self):
        self._rx.clear()

    def write(self, packet):
        if (len(packet) >= 7 and packet[4] == PACKET_TYPE_COMMON_COMMAND
                and packet[6] == CO_RD_IDBASE):
            self._rx.extend(_base_id_response())

    @property
    def in_waiting(self):
        return len(self._rx)

    def read(self, n):
        data = bytes(self._rx[:n])
        del self._rx[:n]
        return data

    def close(self):
        self.is_open = False


def test_open_does_not_raise_when_port_unavailable(monkeypatch):
    def boom(*a, **k):
        raise es.serial.SerialException("no such device")

    monkeypatch.setattr(es.serial, "Serial", boom)
    s = EnOceanSerial("/dev/ghost")
    s.open()  # must not raise — resilient startup
    assert s.base_id is None
    assert s._open_port() is False


def test_open_port_reads_base_id(monkeypatch):
    monkeypatch.setattr(es.serial, "Serial", FakeSerial)
    s = EnOceanSerial("/dev/fake")
    assert s._open_port() is True
    assert s.base_id == list(BASE)
    assert s.base_id_str == "05:A2:B6:C1"


def test_open_port_retries_until_success(monkeypatch):
    state = {"fail": 2}

    def factory(*a, **k):
        if state["fail"] > 0:
            state["fail"] -= 1
            raise es.serial.SerialException("flaky usb")
        return FakeSerial(*a, **k)

    monkeypatch.setattr(es.serial, "Serial", factory)
    s = EnOceanSerial("/dev/fake")
    assert s._open_port() is False   # 1st attempt fails
    assert s._open_port() is False   # 2nd attempt fails
    assert s._open_port() is True    # 3rd attempt succeeds
    assert s.base_id == list(BASE)


def test_open_port_false_when_no_base_id(monkeypatch):
    class NoAnswer(FakeSerial):
        def write(self, packet):
            pass  # never answers CO_RD_IDBASE

    monkeypatch.setattr(es.serial, "Serial", NoAnswer)
    s = EnOceanSerial("/dev/fake")
    # Port opens but stick is mute -> treated as not ready so the loop retries.
    assert s._open_port() is False
    assert s.base_id is None
