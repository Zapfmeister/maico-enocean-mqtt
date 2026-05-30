"""In-memory event log for the web UI.

A small bounded ring buffer of human-readable events (level/mode changes,
pairing, connection transitions, MQTT reconnects) so the dashboard can show
"what happened" without SSH + ``docker logs``. Kept deliberately generic: the
caller passes an already-localised ``message`` plus a ``category``/``source``;
this module only stores, ages and trims. Memory-only by design — entries do not
survive a restart, which keeps the hot path free of disk writes.
"""

import time
from collections import deque
from dataclasses import dataclass

# Categories used by the UI for filtering / colour-coding.
CATEGORIES = ("control", "pairing", "connection", "mqtt", "system")


@dataclass
class Event:
    ts: float
    category: str
    message: str
    device: str | None = None
    source: str | None = None  # "ha" | "web" | "rls" | "timer" | "system"

    def to_dict(self, now: float | None = None) -> dict:
        now = time.time() if now is None else now
        return {
            "ts": self.ts,
            "ago": int(now - self.ts),
            "category": self.category,
            "message": self.message,
            "device": self.device,
            "source": self.source,
        }


class EventLog:
    """Bounded, newest-last ring buffer of :class:`Event`."""

    def __init__(self, maxlen: int = 200):
        self._events: deque[Event] = deque(maxlen=maxlen)

    def add(self, category: str, message: str, device: str | None = None,
            source: str | None = None, now: float | None = None) -> Event:
        ev = Event(
            ts=time.time() if now is None else now,
            category=category, message=message, device=device, source=source,
        )
        self._events.append(ev)
        return ev

    def recent(self, limit: int | None = None, now: float | None = None) -> list[dict]:
        """Return events newest-first, optionally capped at ``limit``."""
        items = list(reversed(self._events))
        if limit is not None:
            items = items[:limit]
        return [e.to_dict(now=now) for e in items]

    def __len__(self) -> int:
        return len(self._events)
