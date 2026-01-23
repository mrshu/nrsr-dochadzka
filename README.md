# nrsr-attendance

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

## Processing

Turn `data/raw/votes/*.json` into analysis-ready CSVs under `data/processed/`:

```bash
uv run python scripts/process_data.py
```

## Tooling

```bash
uv run ruff check .
uv run ruff format .
uv run pytest
```
