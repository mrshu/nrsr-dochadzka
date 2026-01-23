from __future__ import annotations

from typing import Any

from nrsr_attendance.spiders.vote_index import VoteIndexSpider
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
      <tr>
        <th>Číslo schôdze</th>
        <th>Dátum</th>
        <th>Číslo</th>
        <th>ČPT</th>
        <th>Názov</th>
        <th>Hlasovanie</th>
      </tr>
      <tr>
        <td nowrap="nowrap">43</td>
        <td>12.12.2025<br/>10:06:00</td>
        <td><a href="Default.aspx?sid=schodze/hlasovanie/hlasovanie&ID=57432">149</a></td>
        <td></td>
        <td>Hlasovanie o procedurálnom návrhu.</td>
        <td>
          <a href="Default.aspx?sid=schodze/hlasovanie/hlasklub&ID=57432">Hlasovanie č. 149</a>
        </td>
      </tr>
    </table>
  </body>
</html>
""".strip()


MEETING_PAGE_1 = """
<html>
  <body>
    <form id="_f" method="post">
      <input type="hidden" name="__VIEWSTATE" value="x" />
      <input type="hidden" name="__EVENTVALIDATION" value="y" />
      <table id="_sectionLayoutContainer_ctl01__resultGrid2">
        <tr class="pager">
          <td><span>1</span></td>
          <td>
            <a
              href="javascript:__doPostBack('_sectionLayoutContainer$ctl01$_resultGrid2','Page$2')"
            >2</a>
          </td>
        </tr>
        <tr class="tab_zoznam_header">
          <th>Číslo schôdze</th>
          <th>Dátum</th>
          <th>Číslo</th>
          <th>ČPT</th>
          <th>Názov</th>
          <th>Hlasovanie</th>
        </tr>
        <tr class="tab_zoznam_nonalt">
          <td>43</td>
          <td>25.11.2025<br/>13:21:08</td>
          <td><a href="Default.aspx?sid=schodze/hlasovanie/hlasovanie&ID=57277">1</a></td>
          <td></td>
          <td>Prezentácia č. 1</td>
          <td>
            <a href="Default.aspx?sid=schodze/hlasovanie/hlasklub&ID=57277">Hlasovanie č. 1</a>
          </td>
        </tr>
      </table>
    </form>
  </body>
</html>
""".strip()


def test_vote_index_update_mode_yields_vote_index_items():
    spider = VoteIndexSpider(mode="update")
    response = _html_response("https://www.nrsr.sk/web/default.aspx?SectionId=108", LISTING_HTML)
    out = list(spider.parse(response))
    assert out[0]["kind"] == "vote_index"
    assert out[0]["vote_id"] == 57432
    assert out[0]["term_id"] == 9
    assert out[0]["meeting_id"] == 43


def test_vote_index_meeting_results_paginates_via_postback():
    spider = VoteIndexSpider(mode="backfill")
    response = _html_response(
        "https://www.nrsr.sk/web/Default.aspx?sid=schodze/hlasovanie/vyhladavanie_vysledok&ZakZborID=13&CisObdobia=9&CisSchodze=43&ShowCisloSchodze=False",
        MEETING_PAGE_1,
    )
    out = list(
        spider._parse_meeting_results(
            response, term_id=9, meeting_id=43, meeting_label="43. schôdza", page=1
        )
    )
    items = [x for x in out if isinstance(x, dict)]
    reqs = [x for x in out if not isinstance(x, dict)]
    assert len(items) == 1
    assert items[0]["vote_id"] == 57277
    assert reqs and reqs[0].method == "POST"
