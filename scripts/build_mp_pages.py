from __future__ import annotations

import argparse
import json
from pathlib import Path


TEMPLATE = """<!doctype html>
<html lang="sk">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>NRSR Attendance — Poslanec</title>
    <link rel="stylesheet" href="../../assets/base.css" />
    <link rel="stylesheet" href="../../assets/theme-a.css" />
  </head>
  <body data-mp-id="{mp_id}">
    <header class="container">
      <div class="topbar">
        <h1>NRSR attendance</h1>
        <nav class="nav">
          <a href="../../">Späť</a>
        </nav>
      </div>
      <div class="note" id="subtitle">Profil poslanca</div>

      <div class="hero hero-home">
        <div class="controls">
          <label for="termSelect">Volebné obdobie</label>
          <select id="termSelect" disabled></select>
          <label for="windowSelect">Obdobie</label>
          <select id="windowSelect" disabled>
            <option value="full">Celé obdobie</option>
            <option value="180d" selected>Posledných 180 dní</option>
          </select>
          <label for="absenceSelect">Absencia</label>
          <select id="absenceSelect" disabled>
            <option value="abs0n" selected>0 + N</option>
            <option value="abs0">iba 0</option>
          </select>
          <div class="meta" id="meta"></div>
        </div>
        <div class="kpis" id="kpis"></div>
      </div>
    </header>

    <main class="container">
      <section class="panel-a">
        <div class="panel-head">
          <h2 id="mpTitle">Poslanec</h2>
          <div class="hint" id="mpHint"></div>
        </div>
        <div class="profile" id="profile"></div>
      </section>

      <div class="home-grid" style="margin-top: 14px">
        <section class="panel-a">
          <div class="panel-head">
            <h2>Kluby (podľa hlasovaní)</h2>
            <div class="hint">Rozpad účasti podľa klubu</div>
          </div>
          <div class="profile" id="clubs"></div>
        </section>

        <section class="panel-a">
          <div class="panel-head">
            <h2>Posledné hlasovania</h2>
            <div class="hint">Najnovšie v dátach</div>
          </div>
          <div class="profile" id="recent"></div>
        </section>
      </div>
    </main>

    <footer class="container footer">
      <span>Data: nrsr.sk</span>
    </footer>

    <script src="../../assets/mp.js" type="module"></script>
  </body>
</html>
"""


def _parse_terms(value: str) -> list[int] | None:
    value = value.strip()
    if not value:
        return None
    out: list[int] = []
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        out.append(int(part))
    return out or None


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate static MP profile pages.")
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("site/assets/data"),
        help="Root data dir containing manifest.json.",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("site/mp"),
        help="Output directory for mp/<id>/index.html.",
    )
    parser.add_argument(
        "--terms",
        default="",
        help="Comma-separated term IDs (empty uses manifest terms).",
    )
    args = parser.parse_args()

    manifest = json.loads((args.data_dir / "manifest.json").read_text(encoding="utf-8"))
    terms = _parse_terms(args.terms) or manifest.get("terms") or []
    mp_ids: set[int] = set()

    for term_id in terms:
        overview_path = args.data_dir / "term" / str(term_id) / "overview.full.abs0n.json"
        if not overview_path.exists():
            continue
        overview = json.loads(overview_path.read_text(encoding="utf-8"))
        for mp in overview.get("mps", []):
            mp_id = mp.get("mp_id")
            if isinstance(mp_id, int):
                mp_ids.add(mp_id)

    for mp_id in sorted(mp_ids):
        dest = args.out_dir / str(mp_id) / "index.html"
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(TEMPLATE.format(mp_id=mp_id), encoding="utf-8")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
