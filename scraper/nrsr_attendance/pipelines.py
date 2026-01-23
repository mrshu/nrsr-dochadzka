import json
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scrapy import signals


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _atomic_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(text, encoding="utf-8")
    tmp_path.replace(path)


def _state_path() -> Path:
    return _repo_root() / "data" / "raw" / "_state.json"


def _read_state() -> dict[str, Any]:
    path = _state_path()
    if not path.exists():
        return {"schema_version": 1, "votes": {}}
    state = json.loads(path.read_text(encoding="utf-8"))
    state.setdefault("schema_version", 1)
    state.setdefault("votes", {})
    return state


def _write_state(state: dict[str, Any]) -> None:
    state["schema_version"] = 1
    payload = json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    _atomic_write(_state_path(), payload)


_VOTE_ID_RE = re.compile(r"^[0-9]+$")


class RawJsonPipeline:
    def __init__(self) -> None:
        self._max_vote_id: int | None = None

    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        crawler.signals.connect(pipeline._on_spider_closed, signal=signals.spider_closed)
        return pipeline

    def process_item(self, item: dict[str, Any]):
        if item.get("kind") != "vote":
            return item

        vote_id = str(item.get("vote_id") or "")
        if not _VOTE_ID_RE.match(vote_id):
            raise ValueError(f"Invalid vote_id: {vote_id!r}")

        vote_id_int = int(vote_id)
        if self._max_vote_id is None:
            self._max_vote_id = vote_id_int
        else:
            self._max_vote_id = max(self._max_vote_id, vote_id_int)

        out_path = _repo_root() / "data" / "raw" / "votes" / f"{vote_id}.json"
        if out_path.exists() and not item.get("force_overwrite"):
            return item

        payload = json.dumps(item, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
        _atomic_write(out_path, payload)
        return item

    def _on_spider_closed(self, spider, reason: str) -> None:
        if reason != "finished":
            return
        if self._max_vote_id is None:
            return
        state = _read_state()
        votes = state.setdefault("votes", {})
        last_seen = int(votes.get("last_seen_id") or 0)
        if self._max_vote_id > last_seen:
            votes["last_seen_id"] = self._max_vote_id
        votes["updated_at_utc"] = datetime.now(UTC).replace(microsecond=0).isoformat()
        _write_state(state)
