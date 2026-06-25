"""Issue #35: an active boost/sleep timer must not be cut short by the HA cooling
automations; the latest cooling intent is applied when the timer ends."""
import time

from src.maico_protocol import VentilationMode, VentilationState
from tests.fakes import make_bridge


def test_ha_cooling_deferred_during_active_timer():
    b = make_bridge([("bad", "051EF6BA")])
    # Simulate an active boost timer (without the real asyncio task).
    b.timers.end["bad"] = time.time() + 1800
    b.serial.sent.clear()

    # An HA cooling command must NOT send / cancel the timer — it is deferred.
    assert b.set_mode("bad", VentilationMode.SUMMER, source="ha") is True
    assert b.serial.sent == [], "ha-Kommando bei aktivem Boost darf nicht senden"
    assert b.timers.has("bad"), "Boost-Timer muss aktiv bleiben"
    assert b._pending_after_timer["bad"].mode == VentilationMode.SUMMER

    # A deliberate RLS press is NOT deferred — it acts immediately.
    b.set_mode("bad", VentilationMode.HEAT_EXCHANGER, source="rls")
    assert b.serial.sent, "RLS-Kommando muss sofort wirken"


def test_ha_level_deferred_during_active_timer():
    b = make_bridge([("bad", "051EF6BA")])
    b.timers.end["bad"] = time.time() + 1800
    b.serial.sent.clear()
    assert b.set_level("bad", 1, source="ha") is True
    assert b.serial.sent == []
    assert b.timers.has("bad")
    assert b._pending_after_timer["bad"].fan_level == 1


def test_pending_intent_applied_on_restore():
    b = make_bridge([("bad", "051EF6BA")])
    # Latest cooling intent captured during the boost beats the stale pre-boost state.
    b._pending_after_timer["bad"] = VentilationState(mode=VentilationMode.SUMMER, fan_level=3)
    b._saved_states["bad"] = VentilationState(mode=VentilationMode.HEAT_EXCHANGER, fan_level=2)
    b._restore_mode("bad")
    assert b._states["bad"].mode == VentilationMode.SUMMER
    assert b._states["bad"].fan_level == 3


def test_restore_falls_back_to_saved_without_pending():
    b = make_bridge([("bad", "051EF6BA")])
    b._saved_states["bad"] = VentilationState(mode=VentilationMode.HEAT_EXCHANGER, fan_level=2)
    b._restore_mode("bad")
    assert b._states["bad"].mode == VentilationMode.HEAT_EXCHANGER
    assert b._states["bad"].fan_level == 2
