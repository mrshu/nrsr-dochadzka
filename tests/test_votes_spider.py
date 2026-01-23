from __future__ import annotations

from typing import Any

import pytest
from nrsr_attendance.spiders.votes import VotesSpider
from scrapy.http import HtmlResponse, Request


def _html_response(url: str, body: str, *, meta: dict[str, Any] | None = None) -> HtmlResponse:
    request = Request(url=url, meta=meta or {})
    return HtmlResponse(url=url, request=request, body=body.encode("utf-8"), encoding="utf-8")


LISTING_HTML = """
<html>
  <body>
    <select id="_sectionLayoutContainer_ctl01__termNrCombo">
      <option selected="selected" value="9">9</option>
    </select>
    <table id="_sectionLayoutContainer_ctl01__resultGrid2">
      <tr><th>Číslo schôdze</th><th>Dátum</th><th>Číslo</th><th>ČPT</th><th>Názov</th></tr>
      <tr>
        <td nowrap="nowrap">43</td>
        <td>12.12.2025<br/>10:06:00</td>
        <td><a href="?sid=schodze/hlasovanie/hlasovanie&ID=57432">149</a></td>
        <td></td>
        <td>Hlasovanie o procedurálnom návrhu.</td>
      </tr>
      <tr>
        <td nowrap="nowrap">43</td>
        <td>12.12.2025<br/>10:05:00</td>
        <td><a href="?sid=schodze/hlasovanie/hlasovanie&ID=57431">148</a></td>
        <td></td>
        <td>Staršie hlasovanie.</td>
      </tr>
    </table>
  </body>
</html>
""".strip()


HLASKLUB_HTML = """
<html>
  <body>
    <div class="voting_stats_summary_panel">
      <div class="voting_stats_summary_full">
        <div><strong>Schôdza</strong><span>Schôdza č. 43</span></div>
        <div><strong>Dátum a čas</strong><span>12. 12. 2025 10:06</span></div>
        <div><strong>Číslo hlasovania</strong><span>149</span></div>
        <div><strong>Názov hlasovania</strong><span>Hlasovanie o procedurálnom návrhu.</span></div>
        <div><strong>Výsledok hlasovania</strong><span>Návrh prešiel</span></div>
      </div>
    </div>

    <div class="voting_stats_summary_panel">
      <div class="voting_stats_summary_full">
        <div><strong>Prítomní</strong><span>81</span></div>
        <div><strong>Hlasujúcich</strong><span>81</span></div>
        <div><strong>[Z] Za hlasovalo</strong><span>77</span></div>
        <div><strong>[P] Proti hlasovalo</strong><span>4</span></div>
        <div><strong>[0] Neprítomní</strong><span>69</span></div>
      </div>
    </div>

    <table id="_sectionLayoutContainer_ctl01__resultsTable" class="hpo_result_table">
      <tr><td class="hpo_result_block_title" colspan="4">Klub TEST</td></tr>
      <tr>
        <td>[Z] <a href="?sid=poslanci/poslanec&amp;PoslanecID=1">Alpha, A</a></td>
        <td>[0] <a href="?sid=poslanci/poslanec&amp;PoslanecID=2">Beta, B</a></td>
      </tr>
      <tr>
        <td class="hpo_result_block_title" colspan="4">
          Poslanci, ktorí nie sú členmi poslaneckých klubov
        </td>
      </tr>
      <tr>
        <td>[P] <a href="?sid=poslanci/poslanec&amp;PoslanecID=3">Gamma, G</a></td>
        <td></td>
      </tr>
    </table>
  </body>
</html>
""".strip()

HLASKLUB_HTML_UNSORTED = """
<html>
  <body>
    <table id="_sectionLayoutContainer_ctl01__resultsTable" class="hpo_result_table">
      <tr><td class="hpo_result_block_title" colspan="4">Klub B</td></tr>
      <tr>
        <td>[0] <a href="?sid=poslanci/poslanec&amp;PoslanecID=2">Beta, B</a></td>
        <td>[Z] <a href="?sid=poslanci/poslanec&amp;PoslanecID=1">Alpha, A</a></td>
      </tr>
      <tr><td class="hpo_result_block_title" colspan="4">Klub A</td></tr>
      <tr>
        <td>[P] <a href="?sid=poslanci/poslanec&amp;PoslanecID=3">Gamma, G</a></td>
      </tr>
    </table>
  </body>
</html>
""".strip()


def test_votes_spider_parse_is_incremental():
    spider = VotesSpider()
    spider._last_seen_id = 57431

    response = _html_response("https://www.nrsr.sk/web/default.aspx?SectionId=108", LISTING_HTML)
    out = list(spider.parse(response))

    # Only the new vote should be followed.
    assert len(out) == 1
    req = out[0]
    assert req.url.endswith("sid=schodze/hlasovanie/hlasklub&ID=57432")
    assert req.meta["vote_id"] == 57432
    assert req.meta["term_id"] == 9
    assert req.meta["meeting_nr"] == 43


def test_votes_spider_parse_yields_nothing_when_no_new_votes():
    spider = VotesSpider()
    spider._last_seen_id = 999999

    response = _html_response("https://www.nrsr.sk/web/default.aspx?SectionId=108", LISTING_HTML)
    out = list(spider.parse(response))
    assert out == []


def test_votes_spider_force_fetches_listing_page_even_if_state_is_up_to_date():
    spider = VotesSpider(force="1")
    spider._last_seen_id = 999999  # should be ignored when force is enabled

    response = _html_response("https://www.nrsr.sk/web/default.aspx?SectionId=108", LISTING_HTML)
    out = list(spider.parse(response))

    assert {r.meta["vote_id"] for r in out} == {57432, 57431}
    assert all(r.meta["force_overwrite"] is True for r in out)


@pytest.mark.parametrize(
    ("url", "expected_id"),
    [
        ("https://www.nrsr.sk/web/Default.aspx?sid=schodze/hlasovanie/hlasklub&ID=57432", 57432),
        ("https://www.nrsr.sk/web/Default.aspx?sid=schodze/hlasovanie/hlasklub&ID=1", 1),
    ],
)
def test_votes_spider_parse_vote_extracts_codes(url: str, expected_id: int):
    spider = VotesSpider()
    response = _html_response(
        url,
        HLASKLUB_HTML,
        meta={"term_id": 9, "meeting_nr": 43, "cpt_id": None, "title_from_listing": None},
    )
    item = next(spider.parse_vote(response))

    assert item["vote_id"] == expected_id
    assert item["stats"]["Prítomní"] == 81
    assert len(item["mp_votes"]) == 3
    assert {m["vote_code"] for m in item["mp_votes"]} == {"Z", "0", "P"}
    assert {m["mp_id"] for m in item["mp_votes"]} == {1, 2, 3}


def test_votes_spider_parse_vote_sorts_mp_votes_deterministically():
    spider = VotesSpider()
    response = _html_response(
        "https://www.nrsr.sk/web/Default.aspx?sid=schodze/hlasovanie/hlasklub&ID=1",
        HLASKLUB_HTML_UNSORTED,
        meta={"term_id": 9, "meeting_nr": 43, "cpt_id": None, "title_from_listing": None},
    )
    item = next(spider.parse_vote(response))
    assert [(m["club"], m["mp_id"]) for m in item["mp_votes"]] == [
        ("Klub A", 3),
        ("Klub B", 1),
        ("Klub B", 2),
    ]
