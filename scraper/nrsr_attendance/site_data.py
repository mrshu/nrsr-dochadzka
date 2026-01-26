from __future__ import annotations

import json
import re
import unicodedata
from dataclasses import dataclass
from datetime import UTC, datetime
from datetime import timedelta
from pathlib import Path

import polars as pl

from .club_colors import club_colors_for_term

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


def _invalid_club_vote_ids(df: pl.DataFrame) -> set[int]:
    if df.is_empty():
        return set()
    summary = (
        df.group_by("vote_id")
        .agg(any_real=(~pl.col("club").is_in(["(no_club)", "(unknown)"])).any())
        .filter(pl.col("any_real") == False)  # noqa: E712
    )
    if summary.is_empty():
        return set()
    return set(summary.get_column("vote_id").to_list())


@dataclass(frozen=True)
class TermOverview:
    term_id: int
    generated_at_utc: str
    window: dict[str, object]
    absence: dict[str, object]
    club_attribution: str
    mps: list[dict[str, object]]
    clubs: list[dict[str, object]]

    def as_dict(self) -> dict[str, object]:
        return {
            "term_id": self.term_id,
            "generated_at_utc": self.generated_at_utc,
            "window": self.window,
            "absence": self.absence,
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
        invalid_club_vote_ids = _invalid_club_vote_ids(term_mp_votes_df)
        club_votes_df = term_mp_votes_df
        if invalid_club_vote_ids:
            club_votes_df = term_mp_votes_df.filter(
                ~pl.col("vote_id").is_in(list(invalid_club_vote_ids))
            )
        mp_primary_club_by_id: dict[int, str] = {}
        mp_current_club_by_id: dict[int, str] = {}
        if not club_votes_df.is_empty():
            slim = club_votes_df.select(["mp_id", "club", "vote_datetime_utc", "vote_id"])

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

        # Stable club keys are derived from all clubs seen in the full-term data.
        full_term_clubs = (
            clubs_df.filter(pl.col("term_id") == term_id).select("club").unique().to_dicts()
        )
        club_labels = [row.get("club") for row in full_term_clubs]
        club_key_map = build_club_keys([c for c in club_labels if isinstance(c, str)])
        club_color_map = club_colors_for_term(term_id)

        def resolve_club_color(club: object) -> str | None:
            if not isinstance(club, str):
                return None
            return club_color_map.get(club)

        def attach_clubs(
            rows: list[dict[str, object]],
            *,
            current_by_id: dict[int, str],
        ) -> list[dict[str, object]]:
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
                current_club = (
                    current_by_id.get(mp_id)
                    or mp_current_club_by_id.get(mp_id)
                    or primary_club
                    or "(unknown)"
                )

                mp["primary_club"] = primary_club
                mp["primary_club_key"] = club_key_map.get(primary_club, slugify(primary_club))
                mp["current_club"] = current_club
                mp["current_club_key"] = club_key_map.get(current_club, slugify(current_club))

                # For leaderboards/filtering, use the club "as of now" (latest known vote).
                mp["club"] = current_club
                mp["club_key"] = mp["current_club_key"]
                mp["club_color"] = resolve_club_color(mp["club"])
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
            for row in clubs:
                row["club_color"] = resolve_club_color(row.get("club"))
            return clubs

        full_club_rows: list[dict[str, object]] = []
        for label in sorted({c for c in club_labels if isinstance(c, str)}):
            full_club_rows.append(
                {
                    "club": label,
                    "club_key": club_key_map.get(label, slugify(label)),
                    "club_color": resolve_club_color(label),
                }
            )

        def fill_missing_clubs(clubs_rows: list[dict[str, object]]) -> list[dict[str, object]]:
            seen = {r.get("club_key") for r in clubs_rows}
            out = list(clubs_rows)
            for base in full_club_rows:
                key = base.get("club_key")
                if key in seen:
                    continue
                out.append(
                    {
                        "term_id": term_id,
                        "club": base.get("club"),
                        "club_key": key,
                        "club_color": base.get("club_color"),
                        "total_votes": 0,
                        "present_count": 0,
                        "absent_count": 0,
                        "voted_count": 0,
                        "participation_rate": None,
                    }
                )
            out.sort(
                key=lambda r: (
                    -(r.get("participation_rate") or -1),
                    str(r.get("club") or ""),
                )
            )
            return out

        def current_club_for_votes(df: pl.DataFrame) -> dict[int, str]:
            if df.is_empty():
                return {}
            cur = (
                df.select(["mp_id", "club", "vote_datetime_utc", "vote_id"])
                .sort(
                    ["mp_id", "vote_datetime_utc", "vote_id"],
                    descending=[False, True, True],
                )
                .group_by("mp_id", maintain_order=True)
                .agg(pl.first("club").alias("club"))
                .to_dicts()
            )
            out: dict[int, str] = {}
            for row in cur:
                mp_id = row.get("mp_id")
                club = row.get("club")
                if isinstance(mp_id, int) and isinstance(club, str) and club:
                    out[mp_id] = club
            return out

        def mps_from_votes(
            df: pl.DataFrame,
            *,
            absent_codes: list[str],
            current_by_id: dict[int, str],
        ) -> list[dict[str, object]]:
            if df.is_empty():
                return []
            agg = (
                df.group_by(["mp_id", "mp_name"], maintain_order=True)
                .agg(
                    term_id=pl.lit(term_id),
                    total_votes=pl.len(),
                    absent_count=pl.col("vote_code").is_in(absent_codes).cast(pl.Int64).sum(),
                    voted_count=pl.col("vote_code").is_in(["Z", "P", "?"]).cast(pl.Int64).sum(),
                    for_count=(pl.col("vote_code") == "Z").cast(pl.Int64).sum(),
                    against_count=(pl.col("vote_code") == "P").cast(pl.Int64).sum(),
                    abstain_count=(pl.col("vote_code") == "?").cast(pl.Int64).sum(),
                    not_voting_count=(pl.col("vote_code") == "N").cast(pl.Int64).sum(),
                )
                .with_columns(present_count=(pl.col("total_votes") - pl.col("absent_count")))
                .with_columns(
                    participation_rate=(pl.col("present_count") / pl.col("total_votes")).round(6)
                )
                .sort(["participation_rate", "mp_id"], descending=[True, False])
                .to_dicts()
            )
            return attach_clubs([dict(row) for row in agg], current_by_id=current_by_id)

        def _filter_invalid_club_votes(df: pl.DataFrame) -> pl.DataFrame:
            if df.is_empty() or not invalid_club_vote_ids:
                return df
            return df.filter(~pl.col("vote_id").is_in(list(invalid_club_vote_ids)))

        def build_variant(
            *,
            mp_votes_df: pl.DataFrame,
            window: dict[str, object],
            absent_codes: list[str],
            absence_kind: str,
        ) -> TermOverview:
            current_by_id = current_club_for_votes(_filter_invalid_club_votes(mp_votes_df))
            mps_rows = mps_from_votes(
                mp_votes_df,
                absent_codes=absent_codes,
                current_by_id=current_by_id,
            )
            clubs_rows = fill_missing_clubs(clubs_from_mps(mps_rows))
            return TermOverview(
                term_id=term_id,
                generated_at_utc=generated_at_utc,
                window=window,
                absence={"kind": absence_kind, "absent_codes": absent_codes},
                club_attribution="current",
                mps=mps_rows,
                clubs=clubs_rows,
            )

        # Rolling window: last 180 days anchored at the latest vote in this term (deterministic).
        to_utc = None
        from_utc = None
        if not term_votes_df.is_empty():
            latest = term_votes_df.select("vote_datetime_utc").to_dicts()[0].get("vote_datetime_utc")
            if isinstance(latest, str) and latest:
                to_utc = datetime.fromisoformat(latest)
                from_utc = to_utc - timedelta(days=180)

        window_full = {"kind": "full"}

        votes_in_window = 0
        window_mp_votes_df = pl.DataFrame()
        window_meta = {"kind": "rolling", "days": 180, "from_utc": None, "to_utc": None, "votes_in_window": 0}
        if to_utc and from_utc and not term_mp_votes_df.is_empty():
            from_str = from_utc.isoformat()
            window_votes_df = term_votes_df.filter(pl.col("vote_datetime_utc") >= from_str)
            votes_in_window = int(window_votes_df.height)
            window_mp_votes_df = term_mp_votes_df.filter(pl.col("vote_datetime_utc") >= from_str)
            window_meta = {
                "kind": "rolling",
                "days": 180,
                "from_utc": from_utc.isoformat(),
                "to_utc": to_utc.isoformat(),
                "votes_in_window": votes_in_window,
            }

        # absence variants
        abs0n = ["0", "N"]
        abs0 = ["0"]

        overview_full_abs0n = build_variant(
            mp_votes_df=term_mp_votes_df,
            window=window_full,
            absent_codes=abs0n,
            absence_kind="abs0n",
        )
        overview_full_abs0 = build_variant(
            mp_votes_df=term_mp_votes_df,
            window=window_full,
            absent_codes=abs0,
            absence_kind="abs0",
        )
        overview_180_abs0n = build_variant(
            mp_votes_df=window_mp_votes_df,
            window=window_meta,
            absent_codes=abs0n,
            absence_kind="abs0n",
        )
        overview_180_abs0 = build_variant(
            mp_votes_df=window_mp_votes_df,
            window=window_meta,
            absent_codes=abs0,
            absence_kind="abs0",
        )

        out_term = out_assets_data_dir / "term" / str(term_id)
        _write_json(out_term / "overview.full.abs0n.json", overview_full_abs0n.as_dict())
        _write_json(out_term / "overview.full.abs0.json", overview_full_abs0.as_dict())
        _write_json(out_term / "overview.180d.abs0n.json", overview_180_abs0n.as_dict())
        _write_json(out_term / "overview.180d.abs0.json", overview_180_abs0.as_dict())

        # Back-compat (default = full term, abs0n).
        _write_json(out_term / "overview.full.json", overview_full_abs0n.as_dict())
        _write_json(out_term / "overview.180d.json", overview_180_abs0n.as_dict())
        _write_json(out_term / "overview.json", overview_full_abs0n.as_dict())

        _write_json(
            out_assets_data_dir / "term" / str(term_id) / "votes.json",
            term_votes_df.to_dicts(),
        )

        if not (include_mp_pages or include_vote_pages):
            continue

        if include_mp_pages:
            mp_root = out_assets_data_dir / "term" / str(term_id) / "mp"
            _write_mp_pages(
                mp_root / "full.abs0n",
                term_id=term_id,
                mp_rows=overview_full_abs0n.mps,
                mp_votes_df=term_mp_votes_df,
                invalid_club_vote_ids=invalid_club_vote_ids,
                votes_df=term_votes_df,
                club_color_map=club_color_map,
                recent_votes_per_mp=recent_votes_per_mp,
            )
            _write_mp_pages(
                mp_root / "full.abs0",
                term_id=term_id,
                mp_rows=overview_full_abs0.mps,
                mp_votes_df=term_mp_votes_df,
                invalid_club_vote_ids=invalid_club_vote_ids,
                votes_df=term_votes_df,
                club_color_map=club_color_map,
                recent_votes_per_mp=recent_votes_per_mp,
            )
            invalid_window_votes = _invalid_club_vote_ids(window_mp_votes_df)
            _write_mp_pages(
                mp_root / "180d.abs0n",
                term_id=term_id,
                mp_rows=overview_180_abs0n.mps,
                mp_votes_df=window_mp_votes_df,
                invalid_club_vote_ids=invalid_window_votes,
                votes_df=term_votes_df,
                club_color_map=club_color_map,
                recent_votes_per_mp=recent_votes_per_mp,
            )
            _write_mp_pages(
                mp_root / "180d.abs0",
                term_id=term_id,
                mp_rows=overview_180_abs0.mps,
                mp_votes_df=window_mp_votes_df,
                invalid_club_vote_ids=invalid_window_votes,
                votes_df=term_votes_df,
                club_color_map=club_color_map,
                recent_votes_per_mp=recent_votes_per_mp,
            )
            # Back-compat default.
            _write_mp_pages(
                mp_root,
                term_id=term_id,
                mp_rows=overview_full_abs0n.mps,
                mp_votes_df=term_mp_votes_df,
                invalid_club_vote_ids=invalid_club_vote_ids,
                votes_df=term_votes_df,
                club_color_map=club_color_map,
                recent_votes_per_mp=recent_votes_per_mp,
            )

        if include_vote_pages:
            _write_vote_pages(
                out_assets_data_dir / "term" / str(term_id) / "vote",
                term_id=term_id,
                votes_df=term_votes_df,
                mp_votes_df=term_mp_votes_df,
                invalid_club_vote_ids=invalid_club_vote_ids,
                club_color_map=club_color_map,
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
    mp_rows: list[dict[str, object]],
    mp_votes_df: pl.DataFrame,
    invalid_club_vote_ids: set[int],
    votes_df: pl.DataFrame,
    club_color_map: dict[str, str],
    recent_votes_per_mp: int,
) -> None:
    votes_lookup = votes_df.select(
        ["vote_id", "vote_datetime_utc", "vote_datetime_local", "title", "result", "meeting_nr"]
    )

    for mp in mp_rows:
        mp_id = mp.get("mp_id")
        mp_name = mp.get("mp_name")
        if not isinstance(mp_id, int):
            continue

        summary = dict(mp) if isinstance(mp, dict) else {"term_id": term_id, "mp_id": mp_id, "mp_name": mp_name}

        mp_rows = mp_votes_df.filter(pl.col("mp_id") == mp_id)
        club_rows = mp_rows
        if invalid_club_vote_ids:
            club_rows = mp_rows.filter(~pl.col("vote_id").is_in(list(invalid_club_vote_ids)))
        clubs = (
            club_rows.group_by("club")
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
        for row in clubs:
            club_name = row.get("club")
            if isinstance(club_name, str):
                row["club_color"] = club_color_map.get(club_name)

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
    invalid_club_vote_ids: set[int],
    club_color_map: dict[str, str],
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
        if invalid_club_vote_ids and vote_id in invalid_club_vote_ids:
            clubs = []
        else:
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
            for row in clubs:
                club_name = row.get("club")
                if isinstance(club_name, str):
                    row["club_color"] = club_color_map.get(club_name)

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
