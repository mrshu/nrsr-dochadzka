from __future__ import annotations

import json
import re
import shutil
import unicodedata
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import polars as pl

BRATISLAVA_TZ = ZoneInfo("Europe/Bratislava")


def _normalize_key(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    normalized = normalized.casefold()
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


_SK_DT_RE = re.compile(
    r"^\s*(\d{1,2})\.\s*(\d{1,2})\.\s*(\d{4})\s+(\d{1,2}):(\d{2})(?::(\d{2}))?\s*$"
)


def parse_sk_datetime_to_utc(value: str | None) -> datetime | None:
    local_dt = parse_sk_datetime_to_local(value)
    if not local_dt:
        return None
    return local_dt.astimezone(UTC)


def parse_sk_datetime_to_local(value: str | None) -> datetime | None:
    if not value:
        return None
    match = _SK_DT_RE.match(value)
    if not match:
        return None
    day, month, year, hour, minute, second = match.groups()
    return datetime(
        int(year),
        int(month),
        int(day),
        int(hour),
        int(minute),
        int(second) if second is not None else 0,
        tzinfo=BRATISLAVA_TZ,
    )


def _stats_get(stats: dict[str, int] | None, key: str) -> int | None:
    if not stats:
        return None
    target = _normalize_key(key)
    for k, v in stats.items():
        if _normalize_key(k) == target:
            return v
    return None


def _stats_get_code(stats: dict[str, int] | None, code: str) -> int | None:
    if not stats:
        return None
    code_prefix = f"[{code}]"
    for k, v in stats.items():
        if k.strip().startswith(code_prefix):
            return v
    return None


def _safe_int(value: str | int | None) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    value = value.strip()
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        return None


_NO_CLUB_RAW = "Poslanci, ktorí nie sú členmi poslaneckých klubov"
_NO_CLUB_INTERNAL = "(no_club)"
_UNKNOWN_CLUB_INTERNAL = "(unknown)"

_VOTES_SCHEMA_OVERRIDES = {
    "vote_id": pl.Int64,
    "term_id": pl.Int64,
    "meeting_nr": pl.Int64,
    "vote_number": pl.Int64,
    "vote_datetime_local": pl.Utf8,
    "vote_datetime_utc": pl.Utf8,
    "title": pl.Utf8,
    "result": pl.Utf8,
    "cpt_id": pl.Int64,
    "present": pl.Int64,
    "voting": pl.Int64,
    "for": pl.Int64,
    "against": pl.Int64,
    "abstain": pl.Int64,
    "not_voting": pl.Int64,
    "absent": pl.Int64,
    "source_url": pl.Utf8,
    "http_status": pl.Int64,
    "fetched_at_utc": pl.Utf8,
}

_MP_VOTES_SCHEMA_OVERRIDES = {
    "vote_id": pl.Int64,
    "term_id": pl.Int64,
    "meeting_nr": pl.Int64,
    "vote_number": pl.Int64,
    "vote_datetime_local": pl.Utf8,
    "vote_datetime_utc": pl.Utf8,
    "mp_id": pl.Int64,
    "mp_name": pl.Utf8,
    "club": pl.Utf8,
    "vote_code": pl.Utf8,
    "is_present": pl.Boolean,
    "is_voted": pl.Boolean,
}

def _df_from_records(records: list[dict], *, schema_overrides: dict[str, pl.DataType]) -> pl.DataFrame:
    if records:
        return pl.DataFrame(records, schema_overrides=schema_overrides)
    return pl.DataFrame(schema=schema_overrides)


def _normalize_club(value: object) -> str:
    if not isinstance(value, str):
        return _UNKNOWN_CLUB_INTERNAL
    normalized = " ".join(value.split())
    if not normalized:
        return _UNKNOWN_CLUB_INTERNAL
    if normalized == _NO_CLUB_RAW:
        return _NO_CLUB_INTERNAL
    return normalized


_ABSENT_VOTE_CODES = {"0", "N"}


@dataclass(frozen=True)
class ProcessResult:
    schema_version: int
    last_updated_utc: str
    raw_vote_files: int
    votes_rows: int
    mp_votes_rows: int

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "last_updated_utc": self.last_updated_utc,
            "raw_vote_files": self.raw_vote_files,
            "votes_rows": self.votes_rows,
            "mp_votes_rows": self.mp_votes_rows,
        }


def _write_jsonl(df: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in df.to_dicts():
            f.write(json.dumps(row, ensure_ascii=False, sort_keys=True))
            f.write("\n")

def _write_json(path: Path, obj: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(obj, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )


def _mp_votes_shard_path(out_dir: Path, *, term_id: int | None, meeting_nr: int | None) -> Path:
    term_part = str(term_id) if term_id is not None else "unknown"
    meeting_part = str(meeting_nr) if meeting_nr is not None else "unknown"
    return out_dir / "mp_votes" / f"term={term_part}" / f"meeting={meeting_part}.jsonl"


def _write_mp_votes_shards(mp_votes_df: pl.DataFrame, out_dir: Path) -> None:
    root = out_dir / "mp_votes"
    if root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True, exist_ok=True)

    shards_index: list[dict[str, object]] = []

    if mp_votes_df.height:
        partitions = mp_votes_df.partition_by(
            ["term_id", "meeting_nr"],
            maintain_order=True,
            as_dict=True,
        )

        for (term_id, meeting_nr), shard_df in sorted(partitions.items(), key=lambda kv: kv[0]):
            path = _mp_votes_shard_path(out_dir, term_id=term_id, meeting_nr=meeting_nr)
            _write_jsonl(shard_df, path)
            shards_index.append(
                {
                    "term_id": term_id,
                    "meeting_nr": meeting_nr,
                    "path": str(path.relative_to(out_dir)).replace("\\", "/"),
                    "rows": shard_df.height,
                    "bytes": path.stat().st_size,
                }
            )

    _write_json(root / "index.json", {"shards": shards_index})


def process_votes(raw_votes_dir: Path, out_dir: Path, *, schema_version: int = 1) -> ProcessResult:
    vote_files = sorted(
        p for p in raw_votes_dir.glob("*.json") if p.is_file() and p.name != ".gitkeep"
    )

    votes: list[dict[str, object]] = []
    mp_votes: list[dict[str, object]] = []

    for path in vote_files:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if payload.get("kind") != "vote":
            continue

        summary = payload.get("summary") or {}
        stats = payload.get("stats") or {}

        vote_id = int(payload["vote_id"])
        term_id = int(payload["term_id"]) if payload.get("term_id") is not None else None
        meeting_nr = int(payload["meeting_nr"]) if payload.get("meeting_nr") is not None else None

        vote_number = _safe_int(summary.get("Číslo hlasovania"))
        dt_local = parse_sk_datetime_to_local(summary.get("Dátum a čas"))
        dt_utc = dt_local.astimezone(UTC) if dt_local else None

        votes.append(
            {
                "vote_id": vote_id,
                "term_id": term_id,
                "meeting_nr": meeting_nr,
                "vote_number": vote_number,
                "vote_datetime_local": dt_local.isoformat() if dt_local else None,
                "vote_datetime_utc": dt_utc.isoformat() if dt_utc else None,
                "title": summary.get("Názov hlasovania") or payload.get("title_from_listing"),
                "result": summary.get("Výsledok hlasovania"),
                "cpt_id": payload.get("cpt_id"),
                "present": _stats_get(stats, "Prítomní"),
                "voting": _stats_get(stats, "Hlasujúcich"),
                "for": _stats_get_code(stats, "Z"),
                "against": _stats_get_code(stats, "P"),
                "abstain": _stats_get_code(stats, "?"),
                "not_voting": _stats_get_code(stats, "N"),
                "absent": _stats_get_code(stats, "0"),
                "source_url": payload.get("source_url"),
                "http_status": payload.get("http_status"),
                "fetched_at_utc": payload.get("fetched_at_utc"),
            }
        )

        for mv in payload.get("mp_votes") or []:
            vote_code = mv.get("vote_code")
            is_present = vote_code is not None and vote_code not in _ABSENT_VOTE_CODES
            is_voted = vote_code in {"Z", "P", "?"}
            mp_votes.append(
                {
                    "vote_id": vote_id,
                    "term_id": term_id,
                    "meeting_nr": meeting_nr,
                    "vote_number": vote_number,
                    "vote_datetime_local": dt_local.isoformat() if dt_local else None,
                    "vote_datetime_utc": dt_utc.isoformat() if dt_utc else None,
                    "mp_id": mv.get("mp_id"),
                    "mp_name": mv.get("mp_name"),
                    "club": _normalize_club(mv.get("club")),
                    "vote_code": vote_code,
                    "is_present": is_present,
                    "is_voted": is_voted,
                }
            )

    out_dir.mkdir(parents=True, exist_ok=True)

    # Clean legacy outputs from older versions of the pipeline.
    for legacy in (
        out_dir / "votes.csv",
        out_dir / "mp_votes.csv",
        out_dir / "mp_attendance.csv",
        out_dir / "club_attendance.csv",
        out_dir / "mp_votes.jsonl",
    ):
        legacy.unlink(missing_ok=True)

    votes_df = _df_from_records(votes, schema_overrides=_VOTES_SCHEMA_OVERRIDES).sort(
        ["vote_datetime_utc", "vote_id"]
    )
    mp_votes_df = _df_from_records(mp_votes, schema_overrides=_MP_VOTES_SCHEMA_OVERRIDES).sort(
        ["vote_datetime_utc", "vote_id", "mp_id"]
    )

    _write_jsonl(votes_df, out_dir / "votes.jsonl")
    _write_mp_votes_shards(mp_votes_df, out_dir)

    if mp_votes_df.height:
        mp_summary = (
            mp_votes_df.group_by(["term_id", "mp_id", "mp_name"])
            .agg(
                total_votes=pl.len(),
                present_count=pl.col("is_present").cast(pl.Int8).sum(),
                voted_count=pl.col("is_voted").cast(pl.Int8).sum(),
                absent_count=pl.col("vote_code").is_in(["0", "N"]).cast(pl.Int8).sum(),
                not_voting_count=(pl.col("vote_code") == "N").cast(pl.Int8).sum(),
                abstain_count=(pl.col("vote_code") == "?").cast(pl.Int8).sum(),
                for_count=(pl.col("vote_code") == "Z").cast(pl.Int8).sum(),
                against_count=(pl.col("vote_code") == "P").cast(pl.Int8).sum(),
            )
            .with_columns(
                participation_rate=(pl.col("present_count") / pl.col("total_votes")).round(6)
            )
            .sort(["term_id", "participation_rate", "mp_id"])
        )

        club_summary = (
            mp_votes_df.group_by(["term_id", "club"])
            .agg(
                total_votes=pl.len(),
                absent_count=pl.col("vote_code").is_in(["0", "N"]).cast(pl.Int8).sum(),
            )
            .with_columns(
                # Club-level "attendance" should treat vote_code "0" (absent) and "N" (not voting)
                # as absent.
                present_count=(pl.col("total_votes") - pl.col("absent_count")),
            )
            .with_columns(
                participation_rate=(pl.col("present_count") / pl.col("total_votes")).round(6)
            )
            .sort(["term_id", "participation_rate", "club"])
        )

        _write_jsonl(mp_summary, out_dir / "mp_attendance.jsonl")
        _write_jsonl(club_summary, out_dir / "club_attendance.jsonl")
    else:
        _write_jsonl(
            pl.DataFrame(
                {
                    "term_id": [],
                    "mp_id": [],
                    "mp_name": [],
                    "total_votes": [],
                    "present_count": [],
                    "voted_count": [],
                    "absent_count": [],
                    "not_voting_count": [],
                    "abstain_count": [],
                    "for_count": [],
                    "against_count": [],
                    "participation_rate": [],
                }
            ),
            out_dir / "mp_attendance.jsonl",
        )
        _write_jsonl(
            pl.DataFrame(
                {
                    "term_id": [],
                    "club": [],
                    "total_votes": [],
                    "absent_count": [],
                    "present_count": [],
                    "participation_rate": [],
                }
            ),
            out_dir / "club_attendance.jsonl",
        )

    result = ProcessResult(
        schema_version=schema_version,
        last_updated_utc=datetime.now(tz=UTC).isoformat(),
        raw_vote_files=len(vote_files),
        votes_rows=votes_df.height,
        mp_votes_rows=mp_votes_df.height,
    )

    (out_dir / "metadata.json").write_text(
        json.dumps(result.as_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    return result
