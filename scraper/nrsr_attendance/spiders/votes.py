import re
from datetime import UTC, datetime
from urllib.parse import parse_qs, urljoin, urlparse

import scrapy

_VOTE_ID_RE = re.compile(r"[?&]ID=(?P<id>[0-9]+)")


def _parse_int(value: str | None) -> int | None:
    if value is None:
        return None
    value = value.strip()
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _extract_vote_id(href: str) -> int | None:
    match = _VOTE_ID_RE.search(href)
    if not match:
        return None
    return int(match.group("id"))


def _extract_query_int(url: str, key: str) -> int | None:
    parsed = urlparse(url)
    values = parse_qs(parsed.query).get(key)
    if not values:
        return None
    return _parse_int(values[0])


class VotesSpider(scrapy.Spider):
    name = "votes"
    allowed_domains = ["www.nrsr.sk", "nrsr.sk"]
    start_urls = ["https://www.nrsr.sk/web/default.aspx?SectionId=108"]

    custom_settings = {
        "ITEM_PIPELINES": {"nrsr_attendance.pipelines.RawJsonPipeline": 100},
        "DOWNLOAD_TIMEOUT": 60,
        "RETRY_TIMES": 5,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 1.0,
        "AUTOTHROTTLE_MAX_DELAY": 10.0,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 2,
    }

    def __init__(self, *args, force: str | None = None, **kwargs):
        super().__init__(*args, **kwargs)
        self._force = (force or "").strip().lower() in {"1", "true", "yes", "y"}
        self._last_seen_id = self._load_last_seen_id()

    def _load_last_seen_id(self) -> int:
        # Keep this dependency-free (no imports from repo root).
        # The pipeline will update the state file on successful runs.
        import json
        from pathlib import Path

        state_path = Path(__file__).resolve().parents[3] / "data" / "raw" / "_state.json"
        if not state_path.exists():
            return 0
        try:
            state = json.loads(state_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return 0
        votes = state.get("votes") or {}
        try:
            return int(votes.get("last_seen_id") or 0)
        except (TypeError, ValueError):
            return 0

    def parse(self, response: scrapy.http.Response):
        term_id = _parse_int(
            response.css(
                "#_sectionLayoutContainer_ctl01__termNrCombo option[selected]::attr(value)"
            ).get()
        )

        rows = response.css("#_sectionLayoutContainer_ctl01__resultGrid2 tr")[1:]
        found: list[dict] = []
        for row in rows:
            vote_link = row.css(
                "a[href*='sid=schodze/hlasovanie/hlasovanie&ID=']::attr(href)"
            ).get()
            if not vote_link:
                continue
            vote_id = _extract_vote_id(vote_link)
            if vote_id is None:
                continue

            meeting_nr = _parse_int(row.css("td:nth-child(1)::text").get())

            cpt_link = row.css("a[href*='sid=zakony/cpt']::attr(href)").get()
            cpt_id = _extract_vote_id(cpt_link) if cpt_link else None

            title = " ".join(
                t.strip() for t in row.css("td:nth-child(5) *::text").getall() if t.strip()
            )

            found.append(
                {
                    "vote_id": vote_id,
                    "term_id": term_id,
                    "meeting_nr": meeting_nr,
                    "cpt_id": cpt_id,
                    "title_from_listing": title or None,
                }
            )

        found.sort(key=lambda x: x["vote_id"], reverse=True)

        cutoff = 0 if self._force else self._last_seen_id
        new_votes = [v for v in found if v["vote_id"] > cutoff]
        if not new_votes:
            if self._force:
                self.logger.info("Force enabled, but no votes were discovered on the page.")
            else:
                self.logger.info("No new votes found (last_seen_id=%s).", self._last_seen_id)
            return

        self.logger.info(
            "Found %s new vote(s) (max_id=%s, last_seen_id=%s).",
            len(new_votes),
            new_votes[0]["vote_id"],
            self._last_seen_id,
        )

        for v in new_votes:
            url = f"https://www.nrsr.sk/web/Default.aspx?sid=schodze/hlasovanie/hlasklub&ID={v['vote_id']}"
            v["force_overwrite"] = self._force
            yield response.follow(url, callback=self.parse_vote, meta=v)

    def parse_vote(self, response: scrapy.http.Response):
        vote_id = _extract_query_int(response.url, "ID")
        if vote_id is None:
            return

        summary_panels = response.css(".voting_stats_summary_panel")
        if len(summary_panels) > 0:
            meta_summary = self._parse_summary_panel(summary_panels[0])
        else:
            meta_summary = {}

        if len(summary_panels) > 1:
            stats_summary = self._parse_summary_panel(summary_panels[1])
        else:
            stats_summary = {}

        mp_votes: list[dict] = []
        current_club: str | None = None
        for row in response.css("#_sectionLayoutContainer_ctl01__resultsTable tr"):
            header = row.css("td.hpo_result_block_title::text").get()
            if header:
                current_club = header.strip() or None
                continue

            for cell in row.css("td"):
                link = cell.css("a")
                if not link:
                    continue
                mp_name = (link.css("::text").get() or "").strip()
                href = link.attrib.get("href", "")
                mp_id = _extract_query_int(urljoin(response.url, href), "PoslanecID")

                code_text = "".join(t.strip() for t in cell.xpath("text()").getall() if t.strip())
                code_match = re.search(r"\[(?P<code>.)\]", code_text)
                vote_code = code_match.group("code") if code_match else None

                mp_votes.append(
                    {
                        "mp_id": mp_id,
                        "mp_name": mp_name,
                        "club": current_club,
                        "vote_code": vote_code,
                        "mp_url": urljoin(response.url, href),
                    }
                )

        mp_votes.sort(
            key=lambda m: (
                (m.get("club") or ""),
                (m.get("mp_name") or ""),
                (m.get("mp_id") or 0),
                (m.get("vote_code") or ""),
            )
        )

        item = {
            "kind": "vote",
            "source_url": response.url,
            "fetched_at_utc": datetime.now(UTC).replace(microsecond=0).isoformat(),
            "http_status": response.status,
            "vote_id": vote_id,
            "term_id": response.meta.get("term_id"),
            "meeting_nr": response.meta.get("meeting_nr"),
            "cpt_id": response.meta.get("cpt_id"),
            "title_from_listing": response.meta.get("title_from_listing"),
            "force_overwrite": bool(response.meta.get("force_overwrite")),
            "summary": meta_summary,
            "stats": self._coerce_stats(stats_summary),
            "mp_votes": mp_votes,
        }
        yield item

    @staticmethod
    def _parse_summary_panel(panel: scrapy.selector.Selector) -> dict[str, str]:
        out: dict[str, str] = {}
        for div in panel.css("div.voting_stats_summary_full > div"):
            label = "".join(div.css("strong::text").getall()).strip()
            value = " ".join(
                t.strip() for t in div.css("span *::text, span::text").getall() if t.strip()
            )
            if label and value:
                out[label] = value
        return out

    @staticmethod
    def _coerce_stats(stats: dict[str, str]) -> dict[str, int | str]:
        out: dict[str, int | str] = {}
        for k, v in stats.items():
            num = _parse_int(v)
            out[k] = num if num is not None else v
        return out
