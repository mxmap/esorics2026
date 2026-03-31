# Municipality Email Domain Collection

Automated pipeline to collect and verify email domains for municipalities in Switzerland, Germany, and Austria.

## Development

```bash
uv sync --group dev           # install all dependencies
uv run ruff check src tests   # lint
uv run ruff format src tests  # format
uv run pytest --cov --cov-report=term-missing  # tests with coverage
```

## Running

```bash
uv run resolve ch             # resolve Swiss municipalities
uv run resolve de             # resolve German municipalities
uv run resolve at             # resolve Austrian municipalities
uv run resolve ch --dry-run   # statistics only, no scraping
uv run resolve ch -v          # verbose logging
```

## Architecture

- `src/municipality_email/` — main package
  - `schemas.py` — Pydantic models
  - `pipeline.py` — 6-phase orchestrator (collect, validate, scrape, mx, decide, export)
  - `scraping.py` — web scraping, email extraction, TYPO3 decryption
  - `dns.py` — multi-resolver DNS with fallback
  - `log.py` — loguru setup
  - `cli.py` — typer CLI
  - `clients/` — API clients (BFS, OpenPLZ, Wikidata, static file loaders)
  - `countries/` — country configs (base ABC + CH/DE/AT implementations)
- `data/{cc}/` — input data and overrides per country
- `domains/` — output directory (gitignored)
- `tests/` — pytest suite (90% coverage required)

## Key conventions

- All async code uses `httpx.AsyncClient` for HTTP
- DNS uses dnspython with System/Quad9/Cloudflare fallback
- Scraping is always exhaustive (all subpages)
- Decision algorithm is scraping-first, unified across countries
- Output: three tiers per country (minimal, detailed, review)
