from __future__ import annotations

import json
from collections.abc import Iterable
from pathlib import Path

import scrapy

from nrsr_attendance.spiders.votes import VotesSpider


class VoteDetailsSpider(VotesSpider):
    """
    Fetch vote details (hlasklub pages) for vote_ids discovered by VoteIndexSpider.

    Reads per-meeting index shards from `data/raw/vote_index/**.jsonl`.
    """

    name = "vote_details"
    start_urls: list[str] = []

    def __init__(
        self,
        *args,
        force: str | None = None,
        terms: str | None = None,
        meetings: str | None = None,
        **kwargs,
    ):
        super().__init__(*args, force=force, **kwargs)
        self._terms_filter = _parse_int_set(terms)
        self._meetings_filter = _parse_int_set(meetings)

    async def start(self) -> Iterable:
        repo_root = Path(__file__).resolve().parents[3]
        index_root = repo_root / "data" / "raw" / "vote_index"
        votes_root = repo_root / "data" / "raw" / "votes"

        if not index_root.exists():
            self.logger.warning("No vote_index directory found at %s", index_root)
            return

        shard_paths = sorted(index_root.glob("*/*.jsonl"))
        for shard_path in shard_paths:
            try:
                term_id = int(shard_path.parent.name)
                meeting_id = int(shard_path.stem)
            except ValueError:
                continue

            if self._terms_filter is not None and term_id not in self._terms_filter:
                continue
            if self._meetings_filter is not None and meeting_id not in self._meetings_filter:
                continue

            for rec in _iter_jsonl(shard_path):
                vote_id = rec.get("vote_id")
                if not isinstance(vote_id, int):
                    continue

                out_path = votes_root / f"{vote_id}.json"
                if out_path.exists() and not self._force:
                    continue

                url = rec.get("hlasklub_url")
                if not isinstance(url, str) or not url:
                    url = (
                        "https://www.nrsr.sk/web/Default.aspx?sid=schodze/hlasovanie/hlasklub"
                        f"&ID={vote_id}"
                    )

                meta = {
                    "vote_id": vote_id,
                    "term_id": rec.get("term_id"),
                    "meeting_nr": rec.get("meeting_nr") or rec.get("meeting_id"),
                    "cpt_id": rec.get("cpt_id"),
                    "title_from_listing": rec.get("title"),
                    "force_overwrite": self._force,
                }

                yield scrapy.Request(url=url, callback=self.parse_vote, meta=meta, dont_filter=True)


def _parse_int_set(value: str | None) -> set[int] | None:
    if not value:
        return None
    items = {int(v.strip()) for v in value.split(",") if v.strip()}
    return items or None


def _iter_jsonl(path: Path) -> Iterable[dict]:
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)
