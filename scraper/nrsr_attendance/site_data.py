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


def build_site_data(processed_dir: Path, out_assets_data_dir: Path) -> None:
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

    mps_df = pl.read_ndjson(mp_attendance_path)
    clubs_df = pl.read_ndjson(club_attendance_path)

    terms = sorted(
        {int(t) for t in mps_df.get_column("term_id").unique().to_list() if t is not None},
        reverse=True,
    )
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


def _write_json(path: Path, obj: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(obj, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
