# Security Scanner

Kotlin/Docker-based email security scanner that tests DANE/DNSSEC and email authentication (SPF, DKIM, DMARC) for municipality email domains.

## Usage

This tool is invoked via the project CLI — see the [project root](../../CLAUDE.md) for setup instructions.

```bash
uv run scan ch    # scan Swiss municipalities
uv run scan de    # scan German municipalities
uv run scan at    # scan Austrian municipalities
uv run scan ch -v # verbose (streams Docker output)
```

## Prerequisites

- Docker (with `docker compose` v2 or `docker-compose` v1)
- Domain resolver output in `output/domains/domains_{cc}.json`

## Architecture

- **scanner/** — Kotlin app that resolves MX records and runs TLS (testssl.sh), DANE (gotls), and DSS (SPF/DKIM/DMARC) probes against each domain
- **evaluator/** — Kotlin app that aggregates raw scan results into per-domain security assessments
- **docker-compose.yaml** — Orchestrates both containers with shared volumes

The Python wrapper in `src/mail_municipalities/security_analysis/runner.py` handles input/output transformation and Docker invocation.

## Configuration

Default scan parameters are auto-generated in `.env` on first run. See `.env.template` for all available options (DNS rate limits, timeouts, thread counts).
