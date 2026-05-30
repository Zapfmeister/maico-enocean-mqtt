"""Tests for event-log persistence, rotation and paginated/filtered queries."""

import json

from src.events import EventLog


def test_persists_and_reloads(tmp_path):
    p = tmp_path / "events.jsonl"
    log = EventLog(maxlen=100, path=str(p))
    log.add("control", "first", source="web", now=1.0)
    log.add("connection", "second", source="system", now=2.0)
    assert p.exists()

    reloaded = EventLog(maxlen=100, path=str(p))
    got = reloaded.recent()
    assert [e["message"] for e in got] == ["second", "first"]  # newest first
    assert got[0]["source"] == "system"


def test_rotation_caps_file_and_memory(tmp_path):
    p = tmp_path / "events.jsonl"
    log = EventLog(maxlen=10, path=str(p))
    for i in range(50):
        log.add("system", f"e{i}", now=float(i))
    assert len(log) == 10
    lines = [ln for ln in p.read_text().splitlines() if ln.strip()]
    assert len(lines) <= 15  # compacted, never unbounded

    reloaded = EventLog(maxlen=10, path=str(p))
    assert len(reloaded) == 10
    assert reloaded.recent()[0]["message"] == "e49"


def test_load_compacts_oversized_file(tmp_path):
    p = tmp_path / "events.jsonl"
    with open(p, "w", encoding="utf-8") as f:
        for i in range(100):
            f.write(json.dumps({"ts": float(i), "category": "system", "message": f"e{i}"}) + "\n")
    log = EventLog(maxlen=10, path=str(p))
    assert len(log) == 10
    lines = [ln for ln in p.read_text().splitlines() if ln.strip()]
    assert len(lines) == 10  # file trimmed on load


def test_corrupt_lines_skipped_on_load(tmp_path):
    p = tmp_path / "events.jsonl"
    p.write_text('not json\n{"ts":1.0,"category":"system","message":"ok"}\n{bad}\n')
    log = EventLog(maxlen=100, path=str(p))
    assert [e["message"] for e in log.recent()] == ["ok"]


def test_missing_data_dir_degrades_to_memory(tmp_path):
    # Non-existent directory → append/rewrite swallow OSError, log still works.
    p = tmp_path / "nope" / "events.jsonl"
    log = EventLog(maxlen=10, path=str(p))
    log.add("system", "x", now=1.0)
    assert [e["message"] for e in log.recent()] == ["x"]
    assert not p.exists()


def test_query_pagination():
    log = EventLog(maxlen=100)
    for i in range(10):
        log.add("control", f"e{i}", now=float(i))
    page0 = log.query(limit=3, offset=0)
    assert [e["message"] for e in page0["events"]] == ["e9", "e8", "e7"]
    assert page0["total"] == 10 and page0["has_more"] is True

    last = log.query(limit=3, offset=9)
    assert [e["message"] for e in last["events"]] == ["e0"]
    assert last["has_more"] is False


def test_query_category_filter():
    log = EventLog(maxlen=100)
    log.add("control", "c1", now=1.0)
    log.add("mqtt", "m1", now=2.0)
    log.add("control", "c2", now=3.0)
    res = log.query(category="control")
    assert [e["message"] for e in res["events"]] == ["c2", "c1"]
    assert res["total"] == 2
    assert log.query(category="all")["total"] == 3
