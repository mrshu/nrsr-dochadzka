import json
from pathlib import Path

from nrsr_attendance.site_data import build_club_keys, build_site_data, slugify


def test_slugify_basic():
    assert slugify("Poslanecký klub OĽaNO") == "poslanecky-klub-olano"
    assert slugify("   ") == "unknown"


def test_build_club_keys_collision_and_reserved():
    mapping = build_club_keys(
        [
            "(no_club)",
            "(unknown)",
            "Foo Bar",
            "Foo-Bar",
        ]
    )
    assert mapping["(no_club)"] == "no-club"
    assert mapping["(unknown)"] == "unknown"
    assert mapping["Foo Bar"].startswith("foo-bar")
    assert mapping["Foo-Bar"].startswith("foo-bar")
    assert mapping["Foo Bar"] != mapping["Foo-Bar"]


def test_build_site_data_writes_manifest_and_term_overviews(tmp_path: Path):
    processed = tmp_path / "processed"
    processed.mkdir()

    (processed / "metadata.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "last_updated_utc": "2026-01-24T00:00:00+00:00",
                "raw_vote_files": 1,
                "votes_rows": 1,
                "mp_votes_rows": 1,
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    (processed / "mp_attendance.jsonl").write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "term_id": 9,
                        "mp_id": 1,
                        "mp_name": "Alpha, A",
                        "total_votes": 10,
                        "present_count": 7,
                        "absent_count": 3,
                        "participation_rate": 0.7,
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                json.dumps(
                    {
                        "term_id": 8,
                        "mp_id": 2,
                        "mp_name": "Beta, B",
                        "total_votes": 5,
                        "present_count": 5,
                        "absent_count": 0,
                        "participation_rate": 1.0,
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    (processed / "club_attendance.jsonl").write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "term_id": 9,
                        "club": "(no_club)",
                        "total_votes": 2,
                        "present_count": 1,
                        "absent_count": 1,
                        "participation_rate": 0.5,
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                json.dumps(
                    {
                        "term_id": 8,
                        "club": "Club A",
                        "total_votes": 2,
                        "present_count": 2,
                        "absent_count": 0,
                        "participation_rate": 1.0,
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    (processed / "votes.jsonl").write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "term_id": 9,
                        "vote_id": 100,
                        "vote_datetime_local": "2025-01-01T00:00:00+01:00",
                        "vote_datetime_utc": "2024-12-31T23:00:00+00:00",
                        "meeting_nr": 1,
                        "vote_number": 1,
                        "title": "X",
                        "result": None,
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                json.dumps(
                    {
                        "term_id": 8,
                        "vote_id": 200,
                        "vote_datetime_local": "2023-01-01T00:00:00+01:00",
                        "vote_datetime_utc": "2022-12-31T23:00:00+00:00",
                        "meeting_nr": 1,
                        "vote_number": 1,
                        "title": "Y",
                        "result": None,
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    out_dir = tmp_path / "site_assets_data"
    build_site_data(processed, out_dir)

    manifest = json.loads((out_dir / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["terms"] == [9, 8]
    assert manifest["default_term_id"] == 9

    overview9 = json.loads((out_dir / "term" / "9" / "overview.json").read_text(encoding="utf-8"))
    assert overview9["term_id"] == 9
    assert overview9["clubs"][0]["club_key"] == "no-club"
    assert (out_dir / "term" / "9" / "votes.json").exists()
