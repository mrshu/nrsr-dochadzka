import json
from pathlib import Path

import polars as pl
from nrsr_attendance.processing import process_votes


def test_process_votes_writes_expected_outputs(tmp_path: Path):
    raw_votes_dir = tmp_path / "raw" / "votes"
    out_dir = tmp_path / "processed"
    raw_votes_dir.mkdir(parents=True)

    (raw_votes_dir / "1.json").write_text(
        json.dumps(
            {
                "kind": "vote",
                "vote_id": 1,
                "term_id": 9,
                "meeting_nr": 43,
                "cpt_id": None,
                "source_url": "http://example.test/vote/1",
                "http_status": 200,
                "fetched_at_utc": "2026-01-01T00:00:00+00:00",
                "summary": {
                    "Dátum a čas": "12. 12. 2025 10:06",
                    "Číslo hlasovania": 1,
                    "Názov hlasovania": "Test vote 1",
                    "Výsledok hlasovania": "Návrh prešiel",
                },
                "stats": {
                    "Prítomní": 2,
                    "Hlasujúcich": 2,
                    "[Z] Za hlasovalo": 1,
                    "[P] Proti hlasovalo": 1,
                    "[?] Zdržalo sa hlasovania": 0,
                    "[N] Nehlasovalo": 0,
                    "[0] Neprítomní": 148,
                },
                "mp_votes": [
                    {
                        "mp_id": 10,
                        "mp_name": "Alpha, A",
                        "mp_url": "http://example.test/mp/10",
                        "club": "Club A",
                        "vote_code": "Z",
                    },
                    {
                        "mp_id": 11,
                        "mp_name": "Beta, B",
                        "mp_url": "http://example.test/mp/11",
                        "club": "Club A",
                        "vote_code": "0",
                    },
                    {
                        "mp_id": 12,
                        "mp_name": "Gamma, G",
                        "mp_url": "http://example.test/mp/12",
                        "club": "Club A",
                        "vote_code": "N",
                    },
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    result = process_votes(raw_votes_dir, out_dir, schema_version=1)
    assert result.raw_vote_files == 1
    assert (out_dir / "votes.jsonl").exists()
    assert (out_dir / "mp_votes").exists()
    assert (out_dir / "mp_votes" / "index.json").exists()
    assert (out_dir / "mp_votes" / "term=9" / "meeting=43.jsonl").exists()
    assert (out_dir / "mp_attendance.jsonl").exists()
    assert (out_dir / "club_attendance.jsonl").exists()
    assert (out_dir / "metadata.json").exists()

    votes = pl.read_ndjson(out_dir / "votes.jsonl")
    assert votes.height == 1
    assert votes.select("vote_id").item() == 1
    assert votes.select("vote_datetime_local").item() == "2025-12-12T10:06:00+01:00"
    assert votes.select("vote_datetime_utc").item() == "2025-12-12T09:06:00+00:00"
    assert votes.select("present").item() == 2
    assert votes.select("for").item() == 1
    assert votes.select("against").item() == 1

    mp_votes = pl.read_ndjson(out_dir / "mp_votes" / "term=9" / "meeting=43.jsonl")
    assert mp_votes.height == 3
    present_flags = dict(zip(mp_votes["mp_id"].to_list(), mp_votes["is_present"].to_list()))
    assert present_flags == {10: True, 11: False, 12: False}

    mp_attendance = pl.read_ndjson(out_dir / "mp_attendance.jsonl")
    assert mp_attendance.height == 3
    alpha = mp_attendance.filter(pl.col("mp_id") == 10).row(0, named=True)
    beta = mp_attendance.filter(pl.col("mp_id") == 11).row(0, named=True)
    assert alpha["present_count"] == 1
    assert beta["absent_count"] == 1

    club_attendance = pl.read_ndjson(out_dir / "club_attendance.jsonl")
    assert club_attendance.height == 1
    club = club_attendance.row(0, named=True)
    assert club["club"] == "Club A"
    assert club["total_votes"] == 3
    assert club["absent_count"] == 2
    assert club["present_count"] == 1

    metadata_text = (out_dir / "metadata.json").read_text(encoding="utf-8")
    metadata = json.loads(metadata_text)
    assert list(metadata.keys()) == sorted(metadata.keys())


def test_process_votes_normalizes_no_club_label(tmp_path: Path):
    raw_votes_dir = tmp_path / "raw" / "votes"
    out_dir = tmp_path / "processed"
    raw_votes_dir.mkdir(parents=True)

    (raw_votes_dir / "1.json").write_text(
        json.dumps(
            {
                "kind": "vote",
                "vote_id": 1,
                "term_id": 9,
                "meeting_nr": 1,
                "summary": {"Dátum a čas": "12. 12. 2025 10:06", "Číslo hlasovania": 1},
                "stats": {},
                "mp_votes": [
                    {
                        "mp_id": 10,
                        "mp_name": "Alpha, A",
                        "club": "Poslanci, ktorí nie sú členmi poslaneckých klubov",
                        "vote_code": "Z",
                    }
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    process_votes(raw_votes_dir, out_dir, schema_version=1)

    mp_votes = pl.read_ndjson(out_dir / "mp_votes" / "term=9" / "meeting=1.jsonl")
    assert mp_votes.select("club").item() == "(no_club)"

    club_attendance = pl.read_ndjson(out_dir / "club_attendance.jsonl")
    assert club_attendance.height == 1
    assert club_attendance.select("club").item() == "(no_club)"


def test_process_votes_does_not_depend_on_polars_inference(tmp_path: Path):
    raw_votes_dir = tmp_path / "raw" / "votes"
    out_dir = tmp_path / "processed"
    raw_votes_dir.mkdir(parents=True)

    # Polars defaults to inferring schema from the first 100 rows. If a column is null
    # for those rows, it can be inferred as Null and later string values can crash
    # construction in strict mode. This regression test ensures we pin schemas.
    for i in range(1, 102):
        result = None if i <= 100 else "Návrh neprešiel"
        (raw_votes_dir / f"{i}.json").write_text(
            json.dumps(
                {
                    "kind": "vote",
                    "vote_id": i,
                    "term_id": 9,
                    "meeting_nr": 1,
                    "summary": {
                        "Dátum a čas": "12. 12. 2025 10:06",
                        "Číslo hlasovania": i,
                        "Výsledok hlasovania": result,
                    },
                    "stats": {},
                    "mp_votes": [],
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

    process_votes(raw_votes_dir, out_dir, schema_version=1)
    non_null_results = 0
    with (out_dir / "votes.jsonl").open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            obj = json.loads(line)
            if obj.get("result") is not None:
                non_null_results += 1
    assert non_null_results == 1
