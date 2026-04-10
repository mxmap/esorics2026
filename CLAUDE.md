# Municipality Email Infrastructure

Automated pipeline to collect email domains, classify email providers, and analyze security for municipalities in Switzerland, Germany, and Austria.

## Development

```bash
uv sync --group dev           # install all dependencies
uv run ruff check src tests   # lint
uv run ruff format src tests  # format
uv run pytest --cov --cov-report=term-missing  # tests with coverage
uv run pyright --level=warning --warnings src tests  # type check
```

## Running

```bash
# Stage 1: Resolve email domains
uv run resolve ch             # resolve Swiss municipalities
uv run resolve de             # resolve German municipalities
uv run resolve at             # resolve Austrian municipalities
uv run resolve ch --dry-run   # statistics only, no scraping
uv run resolve ch -v          # verbose logging

# Stage 2: Classify email providers
uv run classify ch            # classify Swiss municipality providers
uv run classify de            # classify German municipality providers
uv run classify at            # classify Austrian municipality providers

# Stage 3: Security scan (requires Docker)
uv run scan ch                # scan Swiss municipality email domains
uv run scan de                # scan German municipality email domains
uv run scan at                # scan Austrian municipality email domains
uv run scan ch -v             # verbose logging (streams Docker output)

# Stage 4: Analyze results
uv run analyze providers output/providers/providers_ch.json          # single country console report
uv run analyze providers output/providers/providers_ch.json --latex   # single country LaTeX export
uv run analyze providers --all                                        # combined summary (console)
uv run analyze providers --all --latex                                # combined multi-country LaTeX table
uv run analyze security output/security/security_ch.json             # single country console report
uv run analyze security output/security/security_ch.json --latex     # single country LaTeX export
uv run analyze security --all                                         # combined summary (console)
uv run analyze security --all --latex                                 # combined multi-country LaTeX table
```

## Architecture

- `src/mail_municipalities/` -- main package
  - `cli.py` -- unified Typer CLI (resolve, classify, scan, analyze commands)
  - `analysis/` -- shared analysis and LaTeX export
    - `helpers.py` -- shared LaTeX formatting helpers
    - `provider_latex.py` -- single-country provider LaTeX tables
    - `provider_combined.py` -- combined multi-country provider tables (pandas)
    - `security_latex.py` -- single-country security LaTeX tables
    - `security_combined.py` -- combined multi-country security tables (pandas)
  - `core/` -- shared infrastructure
    - `dns.py` -- multi-resolver DNS with fallback
    - `log.py` -- loguru setup
  - `domain_resolver/` -- Stage 1: email domain collection
    - `pipeline.py` -- 8-phase orchestrator (collect, validate, scrape, mx, decide, export)
    - `schemas.py` -- Pydantic models
    - `scraping.py` -- web scraping, email extraction, TYPO3 decryption
    - `content.py` -- homepage classification
    - `filtering.py` -- email domain filtering
    - `cache.py` -- SQLite async cache
    - `clients/` -- API clients (BFS, OpenPLZ, Wikidata, static file loaders)
    - `countries/` -- country configs (base ABC + CH/DE/AT implementations)
  - `provider_classification/` -- Stage 2: email provider classification
    - `classifier.py` -- classify domains by aggregating DNS/probe evidence
    - `probes.py` -- 10 async DNS probe functions
    - `signatures.py` -- provider fingerprint patterns
    - `models.py` -- Provider, SignalKind, Evidence, ClassificationResult
    - `runner.py` -- classification pipeline orchestration
    - `analyze.py` -- statistical analysis console reports
    - `constants.py` -- canton/region abbreviations
  - `security_analysis/` -- Stage 3: security scanning (DANE, SPF, DKIM, DMARC)
    - `runner.py` -- orchestrates Docker scanner/evaluator, transforms I/O
    - `models.py` -- Pydantic models (DaneSummary, DssSummary, MunicipalitySecurity)
    - `defaults.py` -- default .env configuration for Docker scanner
- `src/security_test/` -- Kotlin/Docker security scanner (external tool)
  - `scanner/` -- TLS/DANE/DSS scanning via testssl.sh, gotls, dss
  - `evaluator/` -- aggregates scan results into per-domain assessments
  - `docker-compose.yaml` -- orchestrates scanner and evaluator containers
- `data/{cc}/` -- input data, overrides per country and cached network data
- `output/` -- output directory (gitignored)
  - `domains/` -- domain resolver output (domains_{cc}.json, _detailed, _review)
  - `providers/` -- provider classification output (providers_{cc}.json, .min.json)
  - `security/` -- security scan output (security_{cc}.json)
- `tests/` -- pytest suite (90% coverage required)

## Key conventions

- All async code uses `httpx.AsyncClient` for HTTP
- DNS uses dnspython with System/Quad9/Cloudflare fallback
- Scraping is always exhaustive (all subpages)
- Decision algorithm is scraping-first, unified across countries
- Provider classification uses 10 concurrent DNS probes with weighted evidence aggregation
- Output: three tiers per country for domains, full + minified for providers
- Security scan wraps Kotlin/Docker tool; requires Docker to be running
- Security scan reads from output/domains/, writes to output/security/

## What NOT to do
- modify JSON files in `output/`
- if you do not know or have no evidence, rather say so.
