import re
from datetime import UTC, datetime
from urllib.parse import parse_qs, urljoin, urlparse

import scrapy
from scrapy.http import FormRequest

_EVENT_TARGET_TERM = "_sectionLayoutContainer$ctl01$_termNrCombo"
_EVENT_TARGET_GRID = "_sectionLayoutContainer$ctl01$_resultGrid2"
_NAME_TERM = "_sectionLayoutContainer$ctl01$_termNrCombo"
_NAME_MEETING = "_sectionLayoutContainer$ctl01$_meetingNrCombo"
_ID_TERM = "_sectionLayoutContainer_ctl01__termNrCombo"
_ID_MEETING = "_sectionLayoutContainer_ctl01__meetingNrCombo"

_VOTE_ID_RE = re.compile(r"[?&]ID=(?P<id>[0-9]+)")
_POSTBACK_ARG_RE = re.compile(r"__doPostBack\('(?P<target>[^']+)','(?P<arg>[^']+)'\)")


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


def _extract_vote_id(href: str | None) -> int | None:
    if not href:
        return None
    match = _VOTE_ID_RE.search(href)
    if not match:
        return None
    return int(match.group("id"))


def _postback_arg(href: str | None) -> str | None:
    if not href:
        return None
    match = _POSTBACK_ARG_RE.search(href)
    if not match:
        return None
    return match.group("arg")


class VoteIndexSpider(scrapy.Spider):
    """
    Discovery spider that produces per-meeting vote index shards (JSONL).

    Modes:
    - update: scrape the latest listing page and write/update meeting shards for discovered votes
    - backfill: enumerate all terms, then all meetings in each term, and paginate through all votes
    """

    name = "vote_index"
    allowed_domains = ["www.nrsr.sk", "nrsr.sk"]
    start_urls = ["https://www.nrsr.sk/web/default.aspx?SectionId=108"]

    custom_settings = {
        "ITEM_PIPELINES": {"nrsr_attendance.pipelines.VoteIndexJsonlPipeline": 100},
        "DOWNLOAD_TIMEOUT": 60,
        "RETRY_TIMES": 5,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 1.0,
        "AUTOTHROTTLE_MAX_DELAY": 10.0,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 2,
    }

    def __init__(
        self,
        *args,
        mode: str | None = None,
        terms: str | None = None,
        meetings: str | None = None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self._mode = (mode or "update").strip().lower()
        if self._mode not in {"update", "backfill"}:
            raise ValueError(f"Invalid mode: {self._mode!r}")

        self._terms_filter: set[int] | None = None
        if terms:
            self._terms_filter = {int(t.strip()) for t in terms.split(",") if t.strip()}

        self._meetings_filter: set[int] | None = None
        if meetings:
            self._meetings_filter = {int(m.strip()) for m in meetings.split(",") if m.strip()}

    def parse(self, response: scrapy.http.Response):
        if self._mode == "update":
            yield from self._parse_latest_listing(response)
            return

        # backfill
        term_values = response.css(f"#{_ID_TERM} option::attr(value)").getall()
        term_ids = sorted({int(v) for v in term_values if v and v.isdigit()}, reverse=True)
        if self._terms_filter is not None:
            term_ids = [t for t in term_ids if t in self._terms_filter]

        selected_term = _parse_int(response.css(f"#{_ID_TERM} option[selected]::attr(value)").get())

        for term_id in term_ids:
            if term_id == selected_term:
                yield from self._schedule_meetings_for_term(response, term_id)
                continue
            yield FormRequest.from_response(
                response,
                formdata={
                    "__EVENTTARGET": _EVENT_TARGET_TERM,
                    "__EVENTARGUMENT": "",
                    _NAME_TERM: str(term_id),
                },
                callback=self._parse_term_page,
                cb_kwargs={"term_id": term_id},
            )

    def _parse_term_page(self, response: scrapy.http.Response, *, term_id: int):
        yield from self._schedule_meetings_for_term(response, term_id)

    def _schedule_meetings_for_term(self, response: scrapy.http.Response, term_id: int):
        options = response.css(f"#{_ID_MEETING} option")
        meetings: list[tuple[int, str]] = []
        for opt in options:
            value = (opt.attrib.get("value") or "").strip()
            label = " ".join(t.strip() for t in opt.css("::text").getall() if t.strip())
            if not value.isdigit():
                continue
            meeting_id = int(value)
            if meeting_id == 0:
                continue
            if self._meetings_filter is not None and meeting_id not in self._meetings_filter:
                continue
            meetings.append((meeting_id, label))

        for meeting_id, meeting_label in meetings:
            url = (
                "https://www.nrsr.sk/web/Default.aspx"
                "?sid=schodze/hlasovanie/vyhladavanie_vysledok"
                f"&ZakZborID=13&CisObdobia={term_id}&CisSchodze={meeting_id}"
                "&ShowCisloSchodze=False"
            )
            yield response.follow(
                url,
                callback=self._parse_meeting_results,
                cb_kwargs={
                    "term_id": term_id,
                    "meeting_id": meeting_id,
                    "meeting_label": meeting_label,
                    "page": 1,
                },
            )

    def _parse_meeting_results(
        self,
        response: scrapy.http.Response,
        *,
        term_id: int,
        meeting_id: int,
        meeting_label: str,
        page: int,
    ):
        yield from self._parse_results_table(
            response,
            term_id=term_id,
            meeting_id=meeting_id,
            meeting_label=meeting_label,
        )

        max_page = self._max_page(response)
        if max_page and page < max_page:
            next_page = page + 1
            yield FormRequest.from_response(
                response,
                formdata={
                    "__EVENTTARGET": _EVENT_TARGET_GRID,
                    "__EVENTARGUMENT": f"Page${next_page}",
                },
                callback=self._parse_meeting_results,
                cb_kwargs={
                    "term_id": term_id,
                    "meeting_id": meeting_id,
                    "meeting_label": meeting_label,
                    "page": next_page,
                },
            )

    @staticmethod
    def _max_page(response: scrapy.http.Response) -> int | None:
        args: list[int] = []
        for href in response.css(
            "#_sectionLayoutContainer_ctl01__resultGrid2 tr.pager a::attr(href)"
        ):
            arg = _postback_arg(href.get())
            if not arg:
                continue
            if arg.startswith("Page$"):
                n = _parse_int(arg.split("$", 1)[1])
                if n is not None:
                    args.append(n)
        return max(args) if args else None

    def _parse_latest_listing(self, response: scrapy.http.Response):
        term_id = _parse_int(response.css(f"#{_ID_TERM} option[selected]::attr(value)").get())

        rows = response.css("#_sectionLayoutContainer_ctl01__resultGrid2 tr")[1:]
        for row in rows:
            vote_link = row.css(
                "a[href*='sid=schodze/hlasovanie/hlasovanie&ID=']::attr(href)"
            ).get()
            vote_id = _extract_vote_id(vote_link)
            if vote_id is None:
                continue

            meeting_nr = _parse_int(row.css("td:nth-child(1)::text").get())
            cpt_link = row.css("a[href*='sid=zakony/cpt']::attr(href)").get()
            cpt_id = _extract_vote_id(cpt_link) if cpt_link else None
            vote_number = _parse_int(row.css("td:nth-child(3) a::text").get())
            date_time_text = " ".join(
                t.strip()
                for t in row.css("td:nth-child(2) *::text, td:nth-child(2)::text").getall()
                if t.strip()
            )
            title = " ".join(
                t.strip() for t in row.css("td:nth-child(5) *::text").getall() if t.strip()
            )

            hlasovanie_url = urljoin(response.url, vote_link)
            hlasklub_link = row.css(
                "a[href*='sid=schodze/hlasovanie/hlasklub&ID=']::attr(href)"
            ).get()
            hlasklub_url = urljoin(response.url, hlasklub_link) if hlasklub_link else None

            if meeting_nr is None:
                continue

            yield {
                "kind": "vote_index",
                "vote_id": vote_id,
                "term_id": term_id,
                "meeting_id": meeting_nr,
                "meeting_label": f"{meeting_nr}. schôdza",
                "meeting_nr": meeting_nr,
                "vote_number": vote_number,
                "date_time_text": date_time_text or None,
                "cpt_id": cpt_id,
                "title": title or None,
                "hlasovanie_url": hlasovanie_url,
                "hlasklub_url": hlasklub_url
                or f"https://www.nrsr.sk/web/Default.aspx?sid=schodze/hlasovanie/hlasklub&ID={vote_id}",
                "source_url": response.url,
                "http_status": response.status,
                "fetched_at_utc": datetime.now(UTC).replace(microsecond=0).isoformat(),
            }

    def _parse_results_table(
        self,
        response: scrapy.http.Response,
        *,
        term_id: int,
        meeting_id: int,
        meeting_label: str,
    ):
        header_texts = [
            " ".join(t.strip() for t in th.css("::text").getall() if t.strip())
            for th in response.css(
                "#_sectionLayoutContainer_ctl01__resultGrid2 tr.tab_zoznam_header th"
            )
        ]
        has_meeting_col = any("Číslo schôdze" in t for t in header_texts)

        # Some result views hide the meeting number column (ShowCisloSchodze=False). In that case,
        # columns shift left by 1 and we should rely on meeting_id from context.
        if has_meeting_col:
            idx_meeting = 1
            idx_date = 2
            idx_vote = 3
            idx_cpt = 4
            idx_title = 5
            idx_hlasklub = 6
        else:
            idx_meeting = None
            idx_date = 1
            idx_vote = 2
            idx_cpt = 3
            idx_title = 4
            idx_hlasklub = 5

        for row in response.css(
            "#_sectionLayoutContainer_ctl01__resultGrid2 tr.tab_zoznam_alt, "
            "#_sectionLayoutContainer_ctl01__resultGrid2 tr.tab_zoznam_nonalt"
        ):
            meeting_nr = (
                meeting_id
                if idx_meeting is None
                else _parse_int(row.css(f"td:nth-child({idx_meeting})::text").get())
            )
            date_time_text = " ".join(
                t.strip()
                for t in row.css(
                    f"td:nth-child({idx_date}) *::text, td:nth-child({idx_date})::text"
                ).getall()
                if t.strip()
            )

            vote_href = row.css(
                "a[href*='sid=schodze/hlasovanie/hlasovanie&ID=']::attr(href)"
            ).get()
            vote_id = _extract_vote_id(vote_href)
            if vote_id is None:
                continue
            hlasovanie_url = urljoin(response.url, vote_href)

            vote_number = _parse_int(row.css(f"td:nth-child({idx_vote}) a::text").get())

            cpt_href = row.css(
                f"td:nth-child({idx_cpt}) a[href*='sid=zakony/cpt']::attr(href)"
            ).get()
            cpt_id = None
            if cpt_href:
                parsed = urlparse(urljoin(response.url, cpt_href))
                query = parse_qs(parsed.query)
                values = query.get("ID")
                cpt_id = _parse_int(values[0]) if values else None

            title = " ".join(
                t.strip()
                for t in row.css(
                    f"td:nth-child({idx_title}) *::text, td:nth-child({idx_title})::text"
                ).getall()
                if t.strip()
            )

            hlasklub_href = row.css(
                f"td:nth-child({idx_hlasklub}) "
                "a[href*='sid=schodze/hlasovanie/hlasklub&ID=']::attr(href)"
            ).get()
            hlasklub_url = (
                urljoin(response.url, hlasklub_href)
                if hlasklub_href
                else f"https://www.nrsr.sk/web/Default.aspx?sid=schodze/hlasovanie/hlasklub&ID={vote_id}"
            )

            yield {
                "kind": "vote_index",
                "vote_id": vote_id,
                "term_id": term_id,
                "meeting_id": meeting_id,
                "meeting_label": meeting_label,
                "meeting_nr": meeting_nr,
                "vote_number": vote_number,
                "date_time_text": date_time_text or None,
                "cpt_id": cpt_id,
                "title": title or None,
                "hlasovanie_url": hlasovanie_url,
                "hlasklub_url": hlasklub_url,
                "source_url": response.url,
                "http_status": response.status,
                "fetched_at_utc": datetime.now(UTC).replace(microsecond=0).isoformat(),
            }
