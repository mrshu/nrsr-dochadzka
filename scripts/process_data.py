from __future__ import annotations

import argparse
import sys
from pathlib import Path


def _add_scraper_to_path() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(repo_root / "scraper"))


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Process raw NRSR data into analysis-ready tables."
    )
    parser.add_argument("--raw-dir", type=Path, default=Path("data/raw"))
    parser.add_argument("--out-dir", type=Path, default=Path("data/processed"))
    parser.add_argument("--schema-version", type=int, default=1)
    args = parser.parse_args()

    _add_scraper_to_path()
    from nrsr_attendance.processing import process_votes  # noqa: I001

    process_votes(args.raw_dir / "votes", args.out_dir, schema_version=args.schema_version)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
