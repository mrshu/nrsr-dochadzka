from __future__ import annotations

import json
from pathlib import Path

import nrsr_attendance.pipelines as pipelines
import pytest
from nrsr_attendance.pipelines import RawJsonPipeline


def test_pipeline_writes_vote_json_and_updates_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setattr(pipelines, "_repo_root", lambda: tmp_path)

    pipeline = RawJsonPipeline()
    item = {
        "kind": "vote",
        "vote_id": 123,
        "source_url": "https://example.test/vote",
        "fetched_at_utc": "2026-01-01T00:00:00+00:00",
        "http_status": 200,
        "mp_votes": [],
    }

    pipeline.process_item(item)

    vote_path = tmp_path / "data" / "raw" / "votes" / "123.json"
    assert vote_path.exists()
    saved = json.loads(vote_path.read_text(encoding="utf-8"))
    assert saved["vote_id"] == 123

    pipeline._on_spider_closed(spider=None, reason="finished")

    state_path = tmp_path / "data" / "raw" / "_state.json"
    assert state_path.exists()
    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert state["votes"]["last_seen_id"] == 123


def test_pipeline_does_not_overwrite_unless_force(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(pipelines, "_repo_root", lambda: tmp_path)

    pipeline = RawJsonPipeline()
    item = {"kind": "vote", "vote_id": 1, "payload": {"v": 1}}
    pipeline.process_item(item)

    vote_path = tmp_path / "data" / "raw" / "votes" / "1.json"
    before = vote_path.read_text(encoding="utf-8")

    # No overwrite by default.
    pipeline.process_item({"kind": "vote", "vote_id": 1, "payload": {"v": 2}})
    after = vote_path.read_text(encoding="utf-8")
    assert after == before

    # Overwrite when force is set.
    pipeline.process_item(
        {"kind": "vote", "vote_id": 1, "payload": {"v": 3}, "force_overwrite": True}
    )
    overwritten = vote_path.read_text(encoding="utf-8")
    assert overwritten != before
    assert json.loads(overwritten)["payload"]["v"] == 3
