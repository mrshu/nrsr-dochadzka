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
        help="Re-fetch and overwrite vote JSON files even if they already exist.",
    )
    parser.add_argument(
        "--terms",
        default="",
        help="Comma-separated term IDs to include (e.g. 9 or 9,8,7). Empty means all.",
    )
    parser.add_argument(
        "--meetings",
        default="",
        help="Comma-separated meeting IDs to include (e.g. 43 or 43,44,1001). Empty means all.",
    )
    parser.add_argument(
        "--concurrent",
        type=int,
        default=2,
        help="CONCURRENT_REQUESTS_PER_DOMAIN (higher is faster, but be polite).",
    )
    parser.add_argument(
        "--autothrottle",
        action="store_true",
        default=True,
        help="Enable AutoThrottle (default: enabled).",
    )
    parser.add_argument(
        "--no-autothrottle",
        action="store_false",
        dest="autothrottle",
        help="Disable AutoThrottle.",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    scraper_dir = repo_root / "scraper"

    os.environ.setdefault("SCRAPY_SETTINGS_MODULE", "nrsr_attendance.settings")
    sys.path.insert(0, str(scraper_dir))
    os.chdir(scraper_dir)

    settings = get_project_settings()
    settings.set("CONCURRENT_REQUESTS_PER_DOMAIN", args.concurrent, priority="cmdline")
    settings.set("AUTOTHROTTLE_ENABLED", bool(args.autothrottle), priority="cmdline")

    process = CrawlerProcess(settings)
    process.crawl(
        "vote_details",
        force="1" if args.force else "0",
        terms=args.terms or None,
        meetings=args.meetings or None,
    )
    process.start()


if __name__ == "__main__":
    main()
