from __future__ import annotations

import json
import re
import unicodedata
from dataclasses import dataclass
from datetime import UTC, datetime
from datetime import timedelta
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
    window: dict[str, object]
    club_attribution: str
    mps: list[dict[str, object]]
    clubs: list[dict[str, object]]

    def as_dict(self) -> dict[str, object]:
        return {
            "term_id": self.term_id,
            "generated_at_utc": self.generated_at_utc,
            "window": self.window,
            "club_attribution": self.club_attribution,
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
    votes_df = pl.read_ndjson(
        votes_path,
        schema_overrides={
            "vote_id": pl.Int64,
            "term_id": pl.Int64,
            "meeting_nr": pl.Int64,
            "vote_number": pl.Int64,
            "vote_datetime_local": pl.Utf8,
            "vote_datetime_utc": pl.Utf8,
            "title": pl.Utf8,
            "result": pl.Utf8,
        },
    )

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

        term_mp_votes_df = _load_mp_votes_for_term(processed_dir, term_id)
        mp_primary_club_by_id: dict[int, str] = {}
        mp_current_club_by_id: dict[int, str] = {}
        if not term_mp_votes_df.is_empty():
            slim = term_mp_votes_df.select(["mp_id", "club", "vote_datetime_utc", "vote_id"])

            primary_clubs = (
                slim.group_by(["mp_id", "club"], maintain_order=True)
                .agg(n=pl.len())
                .sort(["mp_id", "n", "club"], descending=[False, True, False])
                .group_by("mp_id", maintain_order=True)
                .agg(pl.first("club").alias("club"))
                .to_dicts()
            )
            for row in primary_clubs:
                mp_id = row.get("mp_id")
                club = row.get("club")
                if isinstance(mp_id, int) and isinstance(club, str) and club:
                    mp_primary_club_by_id[mp_id] = club

            current_clubs = (
                slim.sort(
                    ["mp_id", "vote_datetime_utc", "vote_id"],
                    descending=[False, True, True],
                )
                .group_by("mp_id", maintain_order=True)
                .agg(pl.first("club").alias("club"))
                .to_dicts()
            )
            for row in current_clubs:
                mp_id = row.get("mp_id")
                club = row.get("club")
                if isinstance(mp_id, int) and isinstance(club, str) and club:
                    mp_current_club_by_id[mp_id] = club

        term_mps_raw = (
            mps_df.filter(pl.col("term_id") == term_id)
            .sort(["participation_rate", "mp_id"], descending=[True, False])
            .to_dicts()
        )

        # Stable club keys are derived from all clubs seen in the full-term data.
        full_term_clubs = (
            clubs_df.filter(pl.col("term_id") == term_id).select("club").unique().to_dicts()
        )
        club_labels = [row.get("club") for row in full_term_clubs]
        club_key_map = build_club_keys([c for c in club_labels if isinstance(c, str)])

        def attach_clubs(rows: list[dict[str, object]]) -> list[dict[str, object]]:
            out: list[dict[str, object]] = []
            for mp in rows:
                mp_id = mp.get("mp_id")
                if not isinstance(mp_id, int):
                    mp["club"] = "(unknown)"
                    mp["club_key"] = "unknown"
                    mp["primary_club"] = "(unknown)"
                    mp["primary_club_key"] = "unknown"
                    mp["current_club"] = "(unknown)"
                    mp["current_club_key"] = "unknown"
                    out.append(mp)
                    continue

                primary_club = mp_primary_club_by_id.get(mp_id) or "(unknown)"
                current_club = mp_current_club_by_id.get(mp_id) or primary_club or "(unknown)"

                mp["primary_club"] = primary_club
                mp["primary_club_key"] = club_key_map.get(primary_club, slugify(primary_club))
                mp["current_club"] = current_club
                mp["current_club_key"] = club_key_map.get(current_club, slugify(current_club))

                # For leaderboards/filtering, use the club "as of now" (latest known vote).
                mp["club"] = current_club
                mp["club_key"] = mp["current_club_key"]
                out.append(mp)
            return out

        def clubs_from_mps(rows: list[dict[str, object]]) -> list[dict[str, object]]:
            df = pl.DataFrame(rows)
            if df.is_empty():
                return []
            needed = {"club", "club_key", "term_id", "present_count", "absent_count", "total_votes", "voted_count"}
            for col in needed:
                if col not in df.columns:
                    df = df.with_columns(pl.lit(None).alias(col))
            clubs = (
                df.group_by(["club", "club_key"], maintain_order=True)
                .agg(
                    term_id=pl.lit(term_id),
                    total_votes=pl.col("total_votes").cast(pl.Int64).sum(),
                    present_count=pl.col("present_count").cast(pl.Int64).sum(),
                    absent_count=pl.col("absent_count").cast(pl.Int64).sum(),
                    voted_count=pl.col("voted_count").cast(pl.Int64).sum(),
                )
                .with_columns(
                    participation_rate=(pl.col("present_count") / pl.col("total_votes")).round(6)
                )
                .sort(["participation_rate", "club"], descending=[True, False])
                .to_dicts()
            )
            return clubs

        term_mps_full = attach_clubs([dict(row) for row in term_mps_raw])
        term_clubs_full = clubs_from_mps(term_mps_full)

        # Rolling window: last 180 days anchored at the latest vote in this term (deterministic).
        to_utc = None
        from_utc = None
        if not term_votes_df.is_empty():
            latest = term_votes_df.select("vote_datetime_utc").to_dicts()[0].get("vote_datetime_utc")
            if isinstance(latest, str) and latest:
                to_utc = datetime.fromisoformat(latest)
                from_utc = to_utc - timedelta(days=180)

        term_mps_180: list[dict[str, object]] = []
        term_clubs_180: list[dict[str, object]] = []
        votes_in_window = 0
        if to_utc and from_utc and not term_mp_votes_df.is_empty():
            from_str = from_utc.isoformat()
            window_votes_df = term_votes_df.filter(pl.col("vote_datetime_utc") >= from_str)
            votes_in_window = int(window_votes_df.height)

            window_mp_votes_df = term_mp_votes_df.filter(pl.col("vote_datetime_utc") >= from_str)
            mp_window = (
                window_mp_votes_df.group_by(["mp_id", "mp_name"], maintain_order=True)
                .agg(
                    term_id=pl.lit(term_id),
                    total_votes=pl.len(),
                    present_count=pl.col("is_present").cast(pl.Int64).sum(),
                    voted_count=pl.col("is_voted").cast(pl.Int64).sum(),
                    absent_count=pl.col("vote_code").is_in(["0", "N"]).cast(pl.Int64).sum(),
                    for_count=(pl.col("vote_code") == "Z").cast(pl.Int64).sum(),
                    against_count=(pl.col("vote_code") == "P").cast(pl.Int64).sum(),
                    abstain_count=(pl.col("vote_code") == "?").cast(pl.Int64).sum(),
                    not_voting_count=(pl.col("vote_code") == "N").cast(pl.Int64).sum(),
                )
                .with_columns(
                    participation_rate=(pl.col("present_count") / pl.col("total_votes")).round(6)
                )
                .sort(["participation_rate", "mp_id"], descending=[True, False])
                .to_dicts()
            )
            term_mps_180 = attach_clubs([dict(row) for row in mp_window])
            term_clubs_180 = clubs_from_mps(term_mps_180)

        overview_full = TermOverview(
            term_id=term_id,
            generated_at_utc=generated_at_utc,
            window={"kind": "full"},
            club_attribution="current",
            mps=term_mps_full,
            clubs=term_clubs_full,
        )
        overview_180 = TermOverview(
            term_id=term_id,
            generated_at_utc=generated_at_utc,
            window={
                "kind": "rolling",
                "days": 180,
                "from_utc": from_utc.isoformat() if from_utc else None,
                "to_utc": to_utc.isoformat() if to_utc else None,
                "votes_in_window": votes_in_window,
            },
            club_attribution="current",
            mps=term_mps_180,
            clubs=term_clubs_180,
        )

        _write_json(
            out_assets_data_dir / "term" / str(term_id) / "overview.full.json",
            overview_full.as_dict(),
        )
        _write_json(
            out_assets_data_dir / "term" / str(term_id) / "overview.180d.json",
            overview_180.as_dict(),
        )
        # Back-compat (default to full-term).
        _write_json(
            out_assets_data_dir / "term" / str(term_id) / "overview.json",
            overview_full.as_dict(),
        )

        _write_json(
            out_assets_data_dir / "term" / str(term_id) / "votes.json",
            term_votes_df.to_dicts(),
        )

        if not (include_mp_pages or include_vote_pages):
            continue

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
