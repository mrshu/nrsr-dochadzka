import argparse
import os
import sys
from pathlib import Path

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--force",
        action="store_true",
        help=(
            "Re-scrape the current vote listing page even if state says it's up to date. "
            "Overwrites existing vote JSON files for discovered votes."
        ),
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    scraper_dir = repo_root / "scraper"

    os.environ.setdefault("SCRAPY_SETTINGS_MODULE", "nrsr_attendance.settings")
    sys.path.insert(0, str(scraper_dir))
    os.chdir(scraper_dir)

    process = CrawlerProcess(get_project_settings())
    process.crawl("votes", force="1" if args.force else "0")
    process.start()


if __name__ == "__main__":
    main()
