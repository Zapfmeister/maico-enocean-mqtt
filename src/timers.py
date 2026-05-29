"""Sleep/boost mode timers for the MAICO bridge.

Encapsulates the per-device countdown timers (boost ~30 min, sleep ~2 h), the
saved pre-timer state used to restore the device afterwards, and the asyncio
tasks. The bridge supplies two callbacks: one to restore a device when its timer
expires, and one to publish the remaining minutes to MQTT.
"""

import asyncio
import logging
import time
from collections.abc import Callable

from .maico_protocol import VentilationState

logger = logging.getLogger(__name__)


class TimerManager:
    def __init__(self, on_expire: Callable[[str], None],
                 on_publish: Callable[[str, int], None]) -> None:
        self.timers: dict[str, asyncio.Task] = {}
        self.end: dict[str, float] = {}            # name -> expiry timestamp
        self.saved: dict[str, VentilationState] = {}  # state to restore after timer
        self._on_expire = on_expire
        self._on_publish = on_publish

    def save_state(self, name: str, state: VentilationState) -> None:
        self.saved[name] = state

    def pop_saved(self, name: str) -> VentilationState | None:
        return self.saved.pop(name, None)

    def has(self, name: str) -> bool:
        return name in self.end

    def remaining_minutes(self, name: str) -> int:
        end = self.end.get(name)
        if end is None:
            return 0
        return max(0, int((end - time.time()) / 60))

    def start(self, name: str, duration: int) -> None:
        self.end[name] = time.time() + duration
        self._on_publish(name, duration // 60)

        async def _timer():
            logger.info("%s: timer started (%d min)", name, duration // 60)
            await asyncio.sleep(duration)
            self.end.pop(name, None)
            self._on_expire(name)

        # Commands run on the event loop thread (MQTT callbacks are marshalled
        # via bridge.dispatch), so a plain Task is correct and cancellable.
        self.timers[name] = asyncio.create_task(_timer())

    def cancel(self, name: str) -> None:
        task = self.timers.pop(name, None)
        if task and not task.done():
            task.cancel()
        self.end.pop(name, None)
        self._on_publish(name, 0)
