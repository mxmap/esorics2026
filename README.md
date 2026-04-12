# Municipality Email Infrastructure — ESORICS 2026

![CI](https://github.com/davidhuser/paper-municipality-emails/actions/workflows/ci.yml/badge.svg)

Automated pipeline to collect email domains, classify email providers, and analyze email security for municipalities in Switzerland (~2,100), Germany (~9,000), and Austria (~2,350).

## Pipeline

1. **Resolve** — Collect municipality websites from official registries and Wikidata, scrape for email addresses, validate via DNS/MX, and produce a municipality-to-domain mapping.
2. **Classify** — Fingerprint DNS records (MX, SPF, autodiscover, SMTP banners, ASN) to determine email provider (Microsoft 365, Google Workspace, AWS, domestic hosting, etc.).
3. **Scan** — Evaluate DANE/DNSSEC and email authentication (SPF, DMARC) per domain via a Kotlin/Docker scanner.

## How to run

Install [`uv`](https://docs.astral.sh/uv/getting-started/installation/) then:

```bash
uv sync                       # install dependencies

# Stage 1: Resolve email domains
uv run resolve ch             # resolve Swiss municipalities
uv run resolve de             # resolve German municipalities
uv run resolve at             # resolve Austrian municipalities
uv run resolve --all          # resolve all three countries
uv run resolve ch --dry-run   # statistics only, no scraping
uv run resolve ch -v          # verbose logging
uv run resolve ch --no-cache  # ignore cached network results

# Stage 2: Classify email providers
uv run classify ch            # classify Swiss municipality providers
uv run classify de            # classify German municipality providers
uv run classify at            # classify Austrian municipality providers

# Stage 3: Security scan (requires Docker)
uv run scan ch                # scan Swiss municipalities
uv run scan de                # scan German municipalities
uv run scan at                # scan Austrian municipalities
uv run scan ch -v             # verbose (streams Docker output)
```

> [!IMPORTANT]
> This tool requires unrestricted outbound port 25 (SMTP). Most residential ISPs and laptops block this.
> For best results, run from a cloud VM with port 25 access opened.

## Output files

Results are written to `output/`:

| Stage | Files | Description |
|-------|-------|-------------|
| Resolve | `domains/domains_{cc}.json` | Minimal: code, name, region, website, email domain |
| | `domains/domains_{cc}_detailed.json` | Full: includes source, confidence, flags |
| | `domains/domains_{cc}_review.json` | Low-confidence entries for manual review |
| Classify | `providers/providers_{cc}.json` | Provider, confidence, evidence signals, gateway |
| | `providers/providers_{cc}.min.json` | Minified for frontend consumption |
| Scan | `security/security_{cc}.json` | DANE, SPF, DMARC assessment per municipality |
| Export | `export.xlsx` | Combined workbook: all municipalities + statistics |

## Maps

Interactive Leaflet maps visualize provider classification per country:

```bash
python3 -m http.server              # from the project root
# then open http://localhost:8000/maps/
```

- [Switzerland](maps/ch.html) — ~2,100 municipalities
- [Austria](maps/at.html) — ~2,100 municipalities
- [Germany](maps/de.html) — ~11,100 municipalities

Each map colors municipalities by email jurisdiction (domestic, US cloud, foreign) with confidence-level shading and interactive popups showing MX/SPF records and classification signals.
