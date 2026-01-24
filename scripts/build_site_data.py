from __future__ import annotations

import argparse
import sys
from pathlib import Path


def _add_scraper_to_path() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(repo_root / "scraper"))


def _parse_terms(value: str) -> list[int] | None:
    value = value.strip()
    if not value:
        return None
    out: list[int] = []
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        out.append(int(part))
    return out or None


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build a browser-friendly site data bundle from data/processed/."
    )
    parser.add_argument("--processed-dir", type=Path, default=Path("data/processed"))
    parser.add_argument("--out-dir", type=Path, default=Path("data/site/assets/data"))
    parser.add_argument(
        "--terms",
        default="",
        help="Comma-separated term IDs to include (empty means all discovered terms).",
    )
    parser.add_argument(
        "--include-mp-pages",
        action="store_true",
        help="Generate per-MP JSON payloads under assets/data/term/<term>/mp/.",
    )
    parser.add_argument(
        "--include-vote-pages",
        action="store_true",
        help="Generate per-vote JSON payloads under assets/data/term/<term>/vote/.",
    )
    parser.add_argument(
        "--recent-votes-per-mp",
        type=int,
        default=20,
        help="How many recent votes to include on each MP payload.",
    )
    args = parser.parse_args()

    _add_scraper_to_path()
    from nrsr_attendance.site_data import build_site_data  # noqa: I001

    build_site_data(
        args.processed_dir,
        args.out_dir,
        terms=_parse_terms(args.terms),
        include_mp_pages=bool(args.include_mp_pages),
        include_vote_pages=bool(args.include_vote_pages),
        recent_votes_per_mp=int(args.recent_votes_per_mp),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
