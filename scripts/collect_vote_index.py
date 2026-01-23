import argparse
import os
import sys
from pathlib import Path

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode",
        choices=["update", "backfill"],
        default="update",
        help="Update writes latest vote IDs; backfill enumerates all terms/meetings/pages.",
    )
    parser.add_argument(
        "--terms",
        default="",
        help="Comma-separated term IDs to include (e.g. 9 or 9,8,7). Empty means all.",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    scraper_dir = repo_root / "scraper"

    os.environ.setdefault("SCRAPY_SETTINGS_MODULE", "nrsr_attendance.settings")
    sys.path.insert(0, str(scraper_dir))
    os.chdir(scraper_dir)

    process = CrawlerProcess(get_project_settings())
    process.crawl("vote_index", mode=args.mode, terms=args.terms or None)
    process.start()


if __name__ == "__main__":
    main()
