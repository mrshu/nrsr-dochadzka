"""Generate sitemap.xml from manifest and overview data."""
from __future__ import annotations

import argparse
import json
import re
import unicodedata
from pathlib import Path
from xml.etree.ElementTree import Element, SubElement, ElementTree


BASE_URL = "https://mrshu.github.io/nrsr-dochadzka"


def slugify(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    normalized = normalized.casefold()
    normalized = re.sub(r"[^a-z0-9]+", "-", normalized)
    normalized = re.sub(r"-{2,}", "-", normalized).strip("-")
    return normalized or "unknown"


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate sitemap.xml.")
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("site/assets/data"),
        help="Root data dir containing manifest.json.",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("site/sitemap.xml"),
        help="Output sitemap path.",
    )
    args = parser.parse_args()

    manifest = json.loads((args.data_dir / "manifest.json").read_text(encoding="utf-8"))
    last_updated = manifest.get("last_updated_utc", "")
    # Extract date portion (YYYY-MM-DD) for lastmod
    lastmod = last_updated[:10] if len(last_updated) >= 10 else ""

    terms = manifest.get("terms") or []

    # Collect MP IDs and names
    mp_ids: dict[int, str] = {}
    for term_id in terms:
        overview_path = args.data_dir / "term" / str(term_id) / "overview.full.abs0n.json"
        if not overview_path.exists():
            continue
        overview = json.loads(overview_path.read_text(encoding="utf-8"))
        for mp in overview.get("mps", []):
            mp_id = mp.get("mp_id")
            mp_name = mp.get("mp_name")
            if isinstance(mp_id, int):
                mp_ids[mp_id] = str(mp_name or mp_ids.get(mp_id) or "")

    # Build XML
    urlset = Element("urlset")
    urlset.set("xmlns", "http://www.sitemaps.org/schemas/sitemap/0.9")

    # Homepage
    url_el = SubElement(urlset, "url")
    SubElement(url_el, "loc").text = f"{BASE_URL}/"
    if lastmod:
        SubElement(url_el, "lastmod").text = lastmod
    SubElement(url_el, "changefreq").text = "daily"
    SubElement(url_el, "priority").text = "1.0"

    # MP pages
    for mp_id in sorted(mp_ids):
        mp_name = mp_ids.get(mp_id) or ""
        slug = slugify(mp_name)
        url_el = SubElement(urlset, "url")
        SubElement(url_el, "loc").text = f"{BASE_URL}/mp/{mp_id}-{slug}/"
        if lastmod:
            SubElement(url_el, "lastmod").text = lastmod
        SubElement(url_el, "priority").text = "0.7"

    tree = ElementTree(urlset)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    tree.write(str(args.out), xml_declaration=True, encoding="UTF-8")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
