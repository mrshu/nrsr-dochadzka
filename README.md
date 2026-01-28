# nrsr-dochadzka

Minimal Python/Scrapy pipeline to track Slovak parliament (NRSR) attendance/activity:

1) collect raw data → `data/raw/`  
2) process datasets → `data/processed/`  
3) publish static site → `gh-pages`

## Requirements

- Python 3.12
- `uv`

## Local setup

```bash
uv sync --python 3.12
```

## Scraper

This repo uses a Scrapy project under `scraper/`.

```bash
cd scraper
uv run scrapy crawl votes
```

For incremental vote collection (tracks `data/raw/_state.json`):

```bash
uv run python scripts/collect_votes.py
```

For the two-stage flow (index → details):

```bash
uv run python scripts/collect_vote_index.py --mode update
uv run python scripts/collect_vote_details.py
```

For a full backfill of the vote index (all terms/meetings/pages):

```bash
uv run python scripts/collect_vote_index.py --mode backfill
uv run python scripts/collect_vote_details.py
```

Both commands accept tuning flags to speed up large runs (use responsibly):

- `--concurrent N` (default `2`)
- `--no-autothrottle`
- `--terms 9,8,7` and `--meetings 43,44,1001` to backfill in chunks (e.g. per term)

## Processing

Turn `data/raw/votes/*.json` into analysis-ready JSONL tables under `data/processed/` (plus
`metadata.json`):

```bash
uv run python scripts/process_data.py
```

## Site data bundle

Build a browser-friendly JSON bundle under `data/site/assets/data/`:

```bash
uv run python scripts/build_site_data.py
```

Preview the static site locally (after building the bundle):

```bash
uv run python scripts/build_site_data.py --out-dir site/assets/data
python -m http.server --directory site 8000
```

## Tooling

```bash
uv run ruff check .
uv run ruff format .
uv run pytest
```
