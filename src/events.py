"""Event log for the web UI, with optional on-disk persistence.

A bounded ring buffer of human-readable events (level/mode changes, pairing,
connection transitions, MQTT reconnects) so the dashboard can show "what
happened" without SSH + ``docker logs``. The caller passes an already-localised
``message`` plus a ``category``/``source``; this module stores, ages, paginates
and trims.

When constructed with a ``path`` the log is also persisted as JSON Lines so it
survives restarts: each event is appended immediately, the newest ``maxlen``
entries are reloaded on startup, and the file is compacted back to the ring
buffer every ``maxlen/2`` writes so it never grows unbounded. The in-memory
deque stays the query source, so pagination never touches disk. Persistence
failures (read-only/missing data dir) are swallowed — logging must never break
the bridge — and the log degrades to memory-only.
"""

import json
import logging
import os
import time
from collections import deque
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# Categories used by the UI for filtering / colour-coding.
CATEGORIES = ("control", "pairing", "connection", "mqtt", "system")


@dataclass
class Event:
    ts: float
    category: str
    message: str
    device: str | None = None
    source: str | None = None

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

    def _persist(self) -> dict:
        return {"ts": self.ts, "category": self.category, "message": self.message,
                "device": self.device, "source": self.source}

    @classmethod
    def _from_persist(cls, d: dict) -> "Event":
        return cls(ts=d["ts"], category=d["category"], message=d["message"],
                   device=d.get("device"), source=d.get("source"))


class EventLog:
    """Bounded, newest-last ring buffer of :class:`Event`, optionally persisted."""

    def __init__(self, maxlen: int = 2000, path: str | None = None):
        self._maxlen = maxlen
        self._events: deque[Event] = deque(maxlen=maxlen)
        self._path = path
        self._writes_since_trim = 0
        if path:
            self._load()

    # --- persistence ---

    def _load(self) -> None:
        try:
            with open(self._path, encoding="utf-8") as f:
                lines = f.readlines()
        except FileNotFoundError:
            return
        except OSError:
            logger.debug("event log load failed", exc_info=True)
            return
        for line in lines[-self._maxlen:]:
            line = line.strip()
            if not line:
                continue
            try:
                self._events.append(Event._from_persist(json.loads(line)))
            except (json.JSONDecodeError, KeyError, TypeError):
                continue
        # File grew past the cap on a previous run → compact it once now.
        if len(lines) > self._maxlen:
            self._rewrite()

    def _append(self, ev: Event) -> None:
        if not self._path:
            return
        try:
            with open(self._path, "a", encoding="utf-8") as f:
                f.write(json.dumps(ev._persist(), ensure_ascii=False) + "\n")
        except OSError:
            logger.debug("event log append failed", exc_info=True)
            return
        self._writes_since_trim += 1
        if self._writes_since_trim >= max(1, self._maxlen // 2):
            self._rewrite()

    def _rewrite(self) -> None:
        """Atomically rewrite the file from the (capped) in-memory deque."""
        if not self._path:
            return
        tmp = self._path + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                for ev in self._events:
                    f.write(json.dumps(ev._persist(), ensure_ascii=False) + "\n")
            os.replace(tmp, self._path)
            self._writes_since_trim = 0
        except OSError:
            logger.debug("event log rewrite failed", exc_info=True)

    # --- public API ---

    def add(self, category: str, message: str, device: str | None = None,
            source: str | None = None, now: float | None = None) -> Event:
        ev = Event(ts=time.time() if now is None else now, category=category,
                   message=message, device=device, source=source)
        self._events.append(ev)
        self._append(ev)
        return ev

    def query(self, limit: int | None = 50, offset: int = 0,
              category: str | None = None, now: float | None = None) -> dict:
        """Return a newest-first page plus paging metadata.

        ``category`` of None or "all" returns every category. Slicing is done in
        memory, so this is cheap to call repeatedly for infinite scroll.
        """
        items = list(reversed(self._events))
        if category and category != "all":
            items = [e for e in items if e.category == category]
        total = len(items)
        page = items[offset:] if limit is None else items[offset:offset + limit]
        return {
            "events": [e.to_dict(now=now) for e in page],
            "total": total,
            "offset": offset,
            "has_more": (offset + len(page)) < total,
        }

    def recent(self, limit: int | None = None, now: float | None = None) -> list[dict]:
        """Newest-first events (back-compat helper over :meth:`query`)."""
        return self.query(limit=limit, now=now)["events"]

    def __len__(self) -> int:
        return len(self._events)
