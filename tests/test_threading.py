"""Tests for the thread-safety fixes: serial write lock and loop dispatch."""

import asyncio
import threading

import pytest

from src.config import AppConfig
from src.enocean_serial import EnOceanSerial
from src.main import MaicoMqttBridge


class _FakeSerial:
    """Serial stub whose write() asserts the caller holds the write lock."""

    def __init__(self, lock: threading.Lock):
        self.is_open = True
        self._lock = lock
        self.writes = 0

    def write(self, packet):
        # If the lock isn't held during write, two threads could interleave.
        assert self._lock.locked(), "write() called without holding _write_lock"
        self.writes += 1


def test_send_holds_write_lock():
    s = EnOceanSerial("/dev/null")
    s._ser = _FakeSerial(s._write_lock)
    s.send([0xD1, 0x27, 0x20, 0xE2, 0x00, 0x00, 0, 0, 0, 0, 0],
           [0x03, 0, 0, 0, 0, 0xFF, 0x00])
    assert s._ser.writes == 1
    # Lock released again afterwards.
    assert not s._write_lock.locked()


def test_dispatch_without_loop_calls_directly():
    bridge = MaicoMqttBridge(AppConfig())
    seen = []
    bridge.dispatch(seen.append, "x")
    assert seen == ["x"]


@pytest.mark.asyncio
async def test_dispatch_with_running_loop_schedules_on_loop():
    bridge = MaicoMqttBridge(AppConfig())
    bridge._loop = asyncio.get_running_loop()
    result = {}

    def record():
        result["thread"] = threading.current_thread()

    main_thread = threading.current_thread()

    # Dispatch from a *different* thread; it must run on the loop thread.
    def from_other_thread():
        bridge.dispatch(record)

    t = threading.Thread(target=from_other_thread)
    t.start()
    t.join()
    # Give the loop a tick to run the scheduled callback.
    await asyncio.sleep(0.05)
    assert result["thread"] is main_thread
