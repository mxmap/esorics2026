# Municipality Email Domain Collection

Automated pipeline to collect and verify email domains for municipalities in Switzerland (~2,100), Germany (~9,000), and Austria (~2,350).

The pipeline cross-references official registries, Wikidata, and static datasets to build a list of candidate website domains per municipality. It then scrapes those websites for email addresses, validates them via DNS/MX lookups, and applies filtering heuristics to produce a final mapping of municipality code to email domain.

## How it works

The pipeline runs eight phases for each country:

1. **Collect** — Gather municipality records from multiple sources (APIs, static files, Wikidata) and merge them into a unified candidate list. Manual overrides are applied here.
2. **DNS pre-filter** — Eliminate candidate domains that don't resolve (no A/AAAA records). Uses system, Quad9, and Cloudflare resolvers with fallback.
3. **Validate** — HTTP HEAD requests to check which domains are accessible and detect redirects.
4. **Content classification** — Fetch homepage HTML and classify it using keyword heuristics (municipality keywords in DE/FR/IT, parked-domain indicators).
5. **Scrape** — Extract email domains from municipality websites. Handles plain-text emails, HTML entity encoding, TYPO3 Caesar cipher, Cloudflare email protection, Joomla payloads, ROT13, and other obfuscation methods. Falls back to Playwright (headless browser) for JavaScript-rendered pages.
6. **MX validation** — DNS MX record lookups for all discovered email domains.
7. **Decide** — Pick the final email domain(s) per municipality using a priority chain: override > scraped email (with MX) > static source (with MX) > guessed domain (with MX) > none. Applies frequency blocklisting (domains appearing across many municipalities are likely ISPs, not municipality-specific), TLD whitelisting, and relevance scoring.
8. **Export** — Write three JSON output files per country.

All network results are cached in a per-country SQLite database (`data/{cc}/cache.db`) so re-runs skip already-fetched data.

### Country-specific details

#### Switzerland (CH)

- **Municipality code**: BFS number (Federal Statistical Office)
- **Sources**: BFS REST API (canonical list), OpenPLZ API (canton info), Wikidata (websites)
- **~2,100 municipalities** (communes)

#### Germany (DE)

- **Municipality code**: AGS (Amtlicher Gemeindeschlüssel)
- **Sources**: Livenson dataset (websites + domains), b42labs dataset, Alex CSV (email addresses, fuzzy name-matched), Wikidata
- **~9,000 municipalities** — the largest dataset, relies heavily on static sources since many small Gemeinden share Amt/Verbandsgemeinde websites with other municipalities

#### Austria (AT)

- **Municipality code**: GKZ (Gemeindekennzahl)
- **Sources**: BRESU dataset (websites + email domains), OpenPLZ API, Wikidata
- Auto-generates `.gv.at` domain variants to ensure government domains are always checked
- Prioritises government TLDs (`.gv.at`, `.gde.at`) in the decision step

## How to run

```bash
uv sync                       # install dependencies
uv run resolve --help         # see CLI options

uv run resolve ch             # resolve Swiss municipalities
uv run resolve de             # resolve German municipalities
uv run resolve at             # resolve Austrian municipalities
uv run resolve --all          # resolve all three countries

uv run resolve ch --dry-run   # statistics only, no scraping
uv run resolve ch -v          # verbose logging

uv run resolve ch --no-cache  # ignore cached network results

uv run resolve ch -o /tmp/out # custom output directory
```

## Output files

Results are written to `domains/` with three tiers per country:

### `domains/{cc}.json` — Minimal

The primary output. One entry per municipality with code, name, region, website, and email domain(s).

```json
{
  "generated": "2026-04-01T14:57:10.807954Z",
  "total": 2110,
  "municipalities": [
    {
      "code": "1",
      "name": "Aeugst am Albis",
      "region": "Kanton Zürich",
      "website": "aeugst-albis.ch",
      "emails": ["aeugst-albis.ch"]
    }
  ]
}
```

### `domains/{cc}_detailed.json` — With metadata

Same records plus `source` (override/scrape/wikidata/static/guess/none), `confidence` (high/medium/low/none), `sources_detail` (which sources proposed which domains), and `flags` (data quality issues).

```json
{
  "code": "1",
  "name": "Aeugst am Albis",
  "region": "Kanton Zürich",
  "website": "aeugst-albis.ch",
  "emails": ["aeugst-albis.ch"],
  "source": "scrape",
  "confidence": "high",
  "sources_detail": { "wikidata": ["aeugst-albis.ch"] },
  "flags": []
}
```

### `domains/{cc}_review.json` — Needs manual review

Subset of entries with low/no confidence or non-empty flags. Typical flags:

| Flag | Meaning |
|---|---|
| `no_mx` | Domain has no MX records |
| `unverified` | Static source domain not confirmed by scraping |
| `guess_only` | Domain was generated from the municipality name, not found in any source |
| `no_municipality_keywords` | Website homepage doesn't look like a municipality site |
| `website_mismatch` | Email domain differs significantly from the website domain |

## How to add overrides

If you find a municipality with an incorrect or missing email domain, add an entry to the override file for that country:

```
data/ch/overrides.json
data/de/overrides.json
data/at/overrides.json
```

Overrides are keyed by municipality code and take absolute priority over all other sources.

```json
{
  "261": {
    "email_domain": "zuerich.ch"
  },
  "230": {
    "email_domain": "win.ch",
    "website": "stadt.winterthur.ch"
  }
}
```

Supported fields:

| Field | Description |
|---|---|
| `email_domain` | The correct email domain for this municipality (required) |
| `website` | Override the website domain used for scraping |
| `name` | Override the municipality name |

After adding overrides, re-run the pipeline for that country to regenerate the output files.
