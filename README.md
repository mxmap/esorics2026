# Municipality Email Infrastructure

Automated pipeline to collect email domains, classify email providers, and analyze email infrastructure for municipalities in Switzerland (~2,100), Germany (~9,000), and Austria (~2,350).

The system has three stages:

1. **Domain Resolver** -- Cross-references official registries, Wikidata, and static datasets to build a list of candidate website domains per municipality. Scrapes those websites for email addresses, validates them via DNS/MX lookups, and applies filtering heuristics to produce a final mapping of municipality code to email domain.
2. **Provider Classification** -- Fingerprints DNS records (MX, SPF, DKIM, autodiscover, SMTP banners, ASN lookups) to determine whether each municipality uses Microsoft 365, Google Workspace, AWS, Infomaniak, a Swiss ISP, or independent hosting.
3. **Security Analysis** -- Wraps a Kotlin/Docker scanner to evaluate DANE/DNSSEC and email authentication (SPF, DKIM, DMARC) for each municipality's email domain.

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
> This tool requires unrestricted outbound port 25 (SMTP). Most residential ISPs and Laptops block this.
> For best results, run from a cloud VM with port 25 access opened.


## Output files

Results are written to `output/` with separate directories per stage:

### `output/domains/` -- Domain Resolver Output

Three tiers per country:

**`domains_{cc}.json`** -- Minimal output. One entry per municipality with code, name, region, website, and email domain(s).

```json
{
  "generated": "2026-04-01T14:57:10.807954Z",
  "total": 2110,
  "municipalities": [
    {
      "code": "1",
      "name": "Aeugst am Albis",
      "region": "Kanton Zurich",
      "website": "aeugst-albis.ch",
      "emails": ["aeugst-albis.ch"]
    }
  ]
}
```

**`domains_{cc}_detailed.json`** -- Same records plus `source`, `confidence`, `sources_detail`, and `flags`.

**`domains_{cc}_review.json`** -- Subset of entries with low/no confidence or non-empty flags.

### `output/providers/` -- Provider Classification Output

**`providers_{cc}.json`** -- Full classification results per municipality including provider, confidence, evidence signals, and gateway detection.

**`providers_{cc}.min.json`** -- Minified version for frontend consumption.

### `output/security/` -- Security Analysis Output

**`security_{cc}.json`** -- Per-municipality security assessment with aggregate counts.

```json
{
  "generated": "2026-04-08T14:47:39Z",
  "commit": "fa40a79",
  "total": 2110,
  "counts": {
    "scanned": 2109,
    "dane_supported": 0,
    "spf": 2096,
    "good_spf": 1690,
    "dmarc": 1175,
    "good_dmarc": 284,
    "dkim": 597
  },
  "municipalities": [
    {
      "code": "1",
      "name": "Aeugst am Albis",
      "region": "Kanton Zürich",
      "domain": "aeugst-albis.ch",
      "mx_records": ["mailgw01.zii.ch", "mailgw02.zii.ch"],
      "dane": { "supported": false, "partial": false },
      "dss": {
        "has_spf": true,
        "has_good_spf": true,
        "has_dmarc": false,
        "has_good_dmarc": false,
        "has_dkim": false
      },
      "scan_valid": true
    }
  ]
}
```

## Maps

Interactive Leaflet maps visualize the provider classification per country. After running Stages 1 and 2, serve the maps locally:

```bash
python3 -m http.server              # from the project root
# then open http://localhost:8000/maps/
```

- [Switzerland](maps/ch.html) -- ~2,100 municipalities
- [Austria](maps/at.html) -- ~2,100 municipalities
- [Germany](maps/de.html) -- ~11,100 municipalities

Each map colors municipalities by email jurisdiction (domestic, US cloud, foreign) with confidence-level shading, and provides interactive popups with MX/SPF records and classification signals.
