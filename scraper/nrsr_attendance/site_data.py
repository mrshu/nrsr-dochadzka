from __future__ import annotations

import json
import re
import unicodedata
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

_RESERVED_CLUB_KEYS = {
    "(no_club)": "no-club",
    "(unknown)": "unknown",
}


def slugify(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    normalized = normalized.casefold()
    normalized = re.sub(r"[^a-z0-9]+", "-", normalized)
    normalized = re.sub(r"-{2,}", "-", normalized).strip("-")
    return normalized or "unknown"


def build_club_keys(clubs: list[str]) -> dict[str, str]:
    # Deterministic mapping: sort labels, then allocate slugs and resolve collisions with -2/-3...
    labels = sorted({c for c in clubs if isinstance(c, str)})
    used: dict[str, int] = {}
    mapping: dict[str, str] = {}

    for label in labels:
        if label in _RESERVED_CLUB_KEYS:
            key = _RESERVED_CLUB_KEYS[label]
        else:
            key = slugify(label)
            if key in _RESERVED_CLUB_KEYS.values():
                key = f"{key}-2"

        n = used.get(key, 0) + 1
        used[key] = n
        if n > 1:
            key = f"{key}-{n}"
        mapping[label] = key

    return mapping


@dataclass(frozen=True)
class TermOverview:
    term_id: int
    generated_at_utc: str
    mps: list[dict[str, object]]
    clubs: list[dict[str, object]]

    def as_dict(self) -> dict[str, object]:
        return {
            "term_id": self.term_id,
            "generated_at_utc": self.generated_at_utc,
            "mps": self.mps,
            "clubs": self.clubs,
        }


def build_site_data(
    processed_dir: Path,
    out_assets_data_dir: Path,
    *,
    terms: list[int] | None = None,
    include_mp_pages: bool = False,
    include_vote_pages: bool = False,
    recent_votes_per_mp: int = 20,
) -> None:
    metadata_path = processed_dir / "metadata.json"
    if not metadata_path.exists():
        raise FileNotFoundError(f"Missing processed metadata: {metadata_path}")

    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    last_updated_utc = metadata.get("last_updated_utc")
    schema_version = int(metadata.get("schema_version") or 1)

    mp_attendance_path = processed_dir / "mp_attendance.jsonl"
    club_attendance_path = processed_dir / "club_attendance.jsonl"
    if not mp_attendance_path.exists():
        raise FileNotFoundError(f"Missing processed mp_attendance: {mp_attendance_path}")
    if not club_attendance_path.exists():
        raise FileNotFoundError(f"Missing processed club_attendance: {club_attendance_path}")

    votes_path = processed_dir / "votes.jsonl"
    if not votes_path.exists():
        raise FileNotFoundError(f"Missing processed votes: {votes_path}")

    mps_df = pl.read_ndjson(mp_attendance_path)
    clubs_df = pl.read_ndjson(club_attendance_path)
    votes_df = pl.read_ndjson(votes_path)

    discovered_terms = sorted(
        {int(t) for t in mps_df.get_column("term_id").unique().to_list() if t is not None},
        reverse=True,
    )
    if terms is None:
        terms = discovered_terms
    else:
        allow = set(discovered_terms)
        terms = [t for t in terms if t in allow]

    if not terms:
        raise ValueError("No term_id values found in mp_attendance.jsonl")

    default_term_id = max(terms)
    manifest = {
        "schema_version": schema_version,
        "last_updated_utc": last_updated_utc,
        "terms": terms,
        "default_term_id": default_term_id,
    }

    _write_json(out_assets_data_dir / "manifest.json", manifest)

    generated_at_utc = datetime.now(UTC).isoformat()
    for term_id in terms:
        term_votes_df = (
            votes_df.filter(pl.col("term_id") == term_id)
            .sort(["vote_datetime_utc", "vote_id"], descending=[True, True])
            .select(
                [
                    "vote_id",
                    "vote_datetime_local",
                    "vote_datetime_utc",
                    "meeting_nr",
                    "vote_number",
                    "title",
                    "result",
                ]
            )
        )

        term_mps = (
            mps_df.filter(pl.col("term_id") == term_id)
            .sort(["participation_rate", "mp_id"], descending=[True, False])
            .to_dicts()
        )

        term_clubs_df = clubs_df.filter(pl.col("term_id") == term_id).sort(
            ["participation_rate", "club"], descending=[True, False]
        )
        club_labels = [row.get("club") for row in term_clubs_df.select("club").to_dicts()]
        club_key_map = build_club_keys([c for c in club_labels if isinstance(c, str)])

        term_clubs: list[dict[str, object]] = []
        for row in term_clubs_df.to_dicts():
            club = row.get("club")
            if isinstance(club, str):
                row["club_key"] = club_key_map.get(club, slugify(club))
            else:
                row["club_key"] = "unknown"
            term_clubs.append(row)

        overview = TermOverview(
            term_id=term_id,
            generated_at_utc=generated_at_utc,
            mps=term_mps,
            clubs=term_clubs,
        )
        _write_json(
            out_assets_data_dir / "term" / str(term_id) / "overview.json", overview.as_dict()
        )
        _write_json(
            out_assets_data_dir / "term" / str(term_id) / "votes.json",
            term_votes_df.to_dicts(),
        )

        if not (include_mp_pages or include_vote_pages):
            continue

        term_mp_votes_df = _load_mp_votes_for_term(processed_dir, term_id)

        if include_mp_pages:
            _write_mp_pages(
                out_assets_data_dir / "term" / str(term_id) / "mp",
                term_id=term_id,
                mp_attendance_df=mps_df.filter(pl.col("term_id") == term_id),
                mp_votes_df=term_mp_votes_df,
                votes_df=term_votes_df,
                recent_votes_per_mp=recent_votes_per_mp,
            )

        if include_vote_pages:
            _write_vote_pages(
                out_assets_data_dir / "term" / str(term_id) / "vote",
                term_id=term_id,
                votes_df=term_votes_df,
                mp_votes_df=term_mp_votes_df,
            )


def _load_mp_votes_for_term(processed_dir: Path, term_id: int) -> pl.DataFrame:
    index_path = processed_dir / "mp_votes" / "index.json"
    if index_path.exists():
        index = json.loads(index_path.read_text(encoding="utf-8"))
        shard_paths: list[Path] = []
        for shard in index.get("shards") or []:
            if shard.get("term_id") != term_id:
                continue
            rel = shard.get("path")
            if isinstance(rel, str) and rel:
                shard_paths.append(processed_dir / rel)

        if not shard_paths:
            return pl.DataFrame()

        return pl.concat([pl.read_ndjson(p) for p in shard_paths], how="vertical")

    legacy = processed_dir / "mp_votes.jsonl"
    if legacy.exists():
        return pl.read_ndjson(legacy).filter(pl.col("term_id") == term_id)

    raise FileNotFoundError(
        "Missing processed mp_votes shards (data/processed/mp_votes/index.json)."
    )


def _write_mp_pages(
    out_dir: Path,
    *,
    term_id: int,
    mp_attendance_df: pl.DataFrame,
    mp_votes_df: pl.DataFrame,
    votes_df: pl.DataFrame,
    recent_votes_per_mp: int,
) -> None:
    votes_lookup = votes_df.select(
        ["vote_id", "vote_datetime_utc", "vote_datetime_local", "title", "result", "meeting_nr"]
    )

    mp_ids = mp_attendance_df.select(["mp_id", "mp_name"]).unique().to_dicts()
    for mp in mp_ids:
        mp_id = mp.get("mp_id")
        mp_name = mp.get("mp_name")
        if not isinstance(mp_id, int):
            continue

        rows = mp_attendance_df.filter(pl.col("mp_id") == mp_id).to_dicts()
        summary = rows[0] if rows else {"term_id": term_id, "mp_id": mp_id, "mp_name": mp_name}

        mp_rows = mp_votes_df.filter(pl.col("mp_id") == mp_id)
        clubs = (
            mp_rows.group_by("club")
            .agg(
                total_votes=pl.len(),
                present_count=pl.col("is_present").cast(pl.Int8).sum(),
                voted_count=pl.col("is_voted").cast(pl.Int8).sum(),
                absent_count=pl.col("vote_code").is_in(["0", "N"]).cast(pl.Int8).sum(),
            )
            .with_columns(
                participation_rate=(pl.col("present_count") / pl.col("total_votes")).round(6)
            )
            .sort(["participation_rate", "club"], descending=[True, False])
            .to_dicts()
        )

        recent = (
            mp_rows.select(["vote_id", "vote_code"])
            .join(votes_lookup, on="vote_id", how="left")
            .sort(["vote_datetime_utc", "vote_id"], descending=[True, True])
            .head(recent_votes_per_mp)
            .to_dicts()
        )

        payload = {
            "term_id": term_id,
            "mp_id": mp_id,
            "mp_name": mp_name,
            "summary": summary,
            "clubs_at_vote_time": clubs,
            "recent_votes": recent,
        }
        _write_json(out_dir / f"{mp_id}.json", payload)


def _write_vote_pages(
    out_dir: Path,
    *,
    term_id: int,
    votes_df: pl.DataFrame,
    mp_votes_df: pl.DataFrame,
) -> None:
    votes_lookup: dict[int, dict[str, object]] = {}
    for row in votes_df.to_dicts():
        vote_id = row.get("vote_id")
        if isinstance(vote_id, int):
            votes_lookup[vote_id] = row

    partitions = mp_votes_df.partition_by(["vote_id"], maintain_order=True, as_dict=True)
    for key, shard_df in partitions.items():
        vote_id = key[0] if isinstance(key, tuple) else key
        if not isinstance(vote_id, int):
            continue

        vote = votes_lookup.get(vote_id) or {"vote_id": vote_id, "term_id": term_id}
        mps = shard_df.select(["mp_id", "mp_name", "club", "vote_code"]).to_dicts()
        clubs = (
            shard_df.group_by("club")
            .agg(
                total=pl.len(),
                absent=pl.col("vote_code").is_in(["0", "N"]).cast(pl.Int8).sum(),
                present=pl.col("is_present").cast(pl.Int8).sum(),
            )
            .with_columns(presence_rate=(pl.col("present") / pl.col("total")).round(6))
            .sort(["presence_rate", "club"], descending=[True, False])
            .to_dicts()
        )

        payload = {
            "term_id": term_id,
            "vote": vote,
            "clubs": clubs,
            "mps": mps,
        }
        _write_json(out_dir / f"{vote_id}.json", payload)


def _write_json(path: Path, obj: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(obj, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
