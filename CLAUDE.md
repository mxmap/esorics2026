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
uv run analyze security output/security/security_ch.json             # single country console report
uv run analyze security output/security/security_ch.json --latex     # single country LaTeX export
uv run analyze security --all                                         # combined summary (console)
uv run analyze merged                                                 # merged provider+security summary (console)
uv run analyze merged --latex                                         # merged multi-country LaTeX table
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
    - `merged_combined.py` -- merged provider+security LaTeX table (pandas)
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

Behavioral guidelines to reduce common LLM coding mistakes. Merge with project-specific instructions as needed.

**Tradeoff:** These guidelines bias toward caution over speed. For trivial tasks, use judgment.

## 1. Think Before Coding

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:
- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them - don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

## 2. Simplicity First

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

## 3. Surgical Changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:
- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it - don't delete it.

When your changes create orphans:
- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: Every changed line should trace directly to the user's request.

## 4. Goal-Driven Execution

**Define success criteria. Loop until verified.**

Transform tasks into verifiable goals:
- "Add validation" → "Write tests for invalid inputs, then make them pass"
- "Fix the bug" → "Write a test that reproduces it, then make it pass"
- "Refactor X" → "Ensure tests pass before and after"

For multi-step tasks, state a brief plan:
```
1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]
```

Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.

---

**These guidelines are working if:** fewer unnecessary changes in diffs, fewer rewrites due to overcomplication, and clarifying questions come before implementation rather than after mistakes.
