from __future__ import annotations

import argparse
import sys
from pathlib import Path


def _add_scraper_to_path() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(repo_root / "scraper"))


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build a browser-friendly site data bundle from data/processed/."
    )
    parser.add_argument("--processed-dir", type=Path, default=Path("data/processed"))
    parser.add_argument("--out-dir", type=Path, default=Path("data/site/assets/data"))
    args = parser.parse_args()

    _add_scraper_to_path()
    from nrsr_attendance.site_data import build_site_data  # noqa: I001

    build_site_data(args.processed_dir, args.out_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
