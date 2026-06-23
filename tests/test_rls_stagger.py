"""Issue #34: the RLS global-sync must stagger its EnOcean sends so a
whole-house boost reaches every device. Firing all sends back-to-back collides
on the radio and some devices miss the command."""
import asyncio

import src.main as main
from src.maico_protocol import VentilationMode, VentilationState
from tests.fakes import make_bridge
from tests.test_bridge import sync_pkt


def test_rls_sync_staggers_and_covers_all_non_slaves(monkeypatch):
    b = make_bridge([("leo", "051EA5D9"), ("schlaf", "05229657"),
                     ("buero", "051EA803"), ("bad", "051EF6BA")])
    # Pair leo -> schlaf so schlaf is a slave: it must be skipped (follows master).
    b._handle_packet(sync_pkt("051EA5D9", "05229657", 0x22))

    sleeps: list[float] = []

    async def fake_sleep(d):
        sleeps.append(d)

    monkeypatch.setattr(main.asyncio, "sleep", fake_sleep)

    b.serial.sent.clear()
    asyncio.run(b._rls_sync_all(VentilationState(mode=VentilationMode.SUMMER, fan_level=5)))

    # All non-slave devices (leo, buero, bad) got a send; the slave (schlaf) was skipped.
    assert len(b.serial.sent) >= 3
    # Sends are spaced out, not fired back-to-back.
    assert len(sleeps) >= 2
    assert all(s > 0 for s in sleeps)
