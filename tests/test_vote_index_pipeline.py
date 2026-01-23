from __future__ import annotations

import json
from pathlib import Path

import nrsr_attendance.pipelines as pipelines
import pytest


def test_vote_index_pipeline_writes_sorted_deduped_shard(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setattr(pipelines, "_repo_root", lambda: tmp_path)

    p = pipelines.VoteIndexJsonlPipeline()
    p.process_item(
        {
            "kind": "vote_index",
            "vote_id": 2,
            "term_id": 9,
            "meeting_id": 43,
            "title": "two",
        }
    )
    p.process_item(
        {
            "kind": "vote_index",
            "vote_id": 1,
            "term_id": 9,
            "meeting_id": 43,
            "title": "one",
        }
    )
    # duplicate vote_id should overwrite
    p.process_item(
        {
            "kind": "vote_index",
            "vote_id": 2,
            "term_id": 9,
            "meeting_id": 43,
            "title": "two-updated",
        }
    )

    p._on_spider_closed(spider=None, reason="finished")

    shard = tmp_path / "data" / "raw" / "vote_index" / "9" / "43.jsonl"
    lines = shard.read_text(encoding="utf-8").splitlines()
    assert [json.loads(line)["vote_id"] for line in lines] == [1, 2]
    assert json.loads(lines[1])["title"] == "two-updated"
