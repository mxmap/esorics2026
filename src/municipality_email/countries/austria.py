"""Austria country configuration."""

from __future__ import annotations

import re
from pathlib import Path

from loguru import logger

from municipality_email.clients.openplz import fetch_openplz_municipalities
from municipality_email.clients.static import load_bresu
from municipality_email.clients.wikidata import fetch_wikidata
from municipality_email.countries.base import CountryConfig
from municipality_email.schemas import Country, DomainCandidate, MunicipalityRecord
from municipality_email.scraping import load_overrides, url_to_domain

SPARQL_QUERY_AT = """
SELECT DISTINCT ?gkz ?itemLabel ?website WHERE {
  ?item wdt:P31/wdt:P279* wd:Q667509 .
  ?item wdt:P964 ?gkz .
  ?item wdt:P17 wd:Q40 .
  FILTER NOT EXISTS {
    ?item wdt:P576 ?dissolved .
    FILTER(?dissolved <= NOW())
  }
  FILTER NOT EXISTS {
    ?item wdt:P1366 ?successor .
  }
  OPTIONAL { ?item wdt:P856 ?website . }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "de,en" . }
}
ORDER BY ?gkz
"""

BUNDESLAND_BY_PREFIX_AT = {
    "1": "Burgenland",
    "2": "Kärnten",
    "3": "Niederösterreich",
    "4": "Oberösterreich",
    "5": "Salzburg",
    "6": "Steiermark",
    "7": "Tirol",
    "8": "Vorarlberg",
    "9": "Wien",
}

REGIONAL_DOMAIN_SUFFIXES_AT: dict[str, list[str]] = {
    "1": ["bgld.gv.at"],
    "2": ["ktn.gde.at"],
    "4": ["ooe.gv.at"],
    "7": ["tirol.gv.at"],
    "9": ["wien.gv.at"],
}

SKIP_DOMAINS_AT = {
    "example.com",
    "example.ch",
    "sentry.io",
    "w3.org",
    "gstatic.com",
    "googleapis.com",
    "schema.org",
    "gmail.com",
    "hotmail.com",
    "hotmail.ch",
    "outlook.com",
    "gmx.ch",
    "bluewin.ch",
    "yahoo.com",
    "domain.com",
    "google.com",
    "mail.com",
    "wordpress.org",
    # Austrian personal email providers
    "aon.at",
    "gmx.at",
    "gmx.net",
    "chello.at",
    "a1.net",
    "three.at",
    "inode.at",
    "wvnet.at",
    "cnv.at",
    "netway.at",
    "utanet.at",
    "tele2.at",
    "speed.at",
    "liwest.at",
    "kabsi.at",
    "wavenet.at",
    "sbg.at",
    # CMS / web agencies
    "riskommunal.net",
    "gem2go.at",
    "kommunalnet.at",
    # Generic personal email
    "icloud.com",
    "protonmail.com",
    "tutanota.com",
    "web.de",
    "gmx.de",
    "t-online.de",
    "live.at",
    "live.com",
    "hotmail.de",
    "hotmail.at",
    "yahoo.de",
    "yahoo.at",
    "outlook.at",
    # ISPs
    "silverserver.at",
    # CMS / app providers
    "citiesapps.com",
}

SUBPAGES_AT = [
    "/kontakt",
    "/kontakt/",
    "/impressum",
    "/impressum/",
    "/rathaus",
    "/gemeinde",
    "/verwaltung",
    "/buergerservice",
    "/buergerservice/",
    "/service",
]


def gkz_to_bundesland(gkz: str) -> str:
    """Map GKZ first digit to Bundesland name."""
    return BUNDESLAND_BY_PREFIX_AT.get(gkz[0], f"??({gkz[0]})")


def _is_gov_domain(domain: str) -> bool:
    """Check if a domain is an Austrian government domain."""
    return domain.endswith(".gv.at") or domain.endswith(".gde.at")


class AustriaConfig(CountryConfig):
    country = Country.AT
    code_field = "gkz"
    tlds = [".at", ".gv.at", ".gde.at"]
    government_tlds = [".gv.at", ".gde.at"]
    skip_domains = SKIP_DOMAINS_AT
    subpages = SUBPAGES_AT
    concurrency = 50

    async def collect_candidates(self, data_dir: Path) -> list[MunicipalityRecord]:
        # Load BRESU (website + email data)
        bresu = load_bresu(data_dir / "bresu_oe_gemeinden.json")
        logger.info("Bresu: {} municipalities", len(bresu))

        # Fetch OpenPLZ (canonical list)
        openplz = await fetch_openplz_municipalities()

        # Fetch Wikidata (websites)
        wikidata = await fetch_wikidata(SPARQL_QUERY_AT, code_field="gkz")

        # Load overrides
        overrides = load_overrides(data_dir / "overrides.json")

        # OpenPLZ is canonical
        all_gkz = set(openplz)

        records: list[MunicipalityRecord] = []
        for gkz in sorted(all_gkz):
            # Name priority: OpenPLZ > Wikidata > BRESU
            name = ""
            if gkz in openplz:
                name = openplz[gkz]["name"]
            elif gkz in wikidata:
                name = wikidata[gkz]["name"]
            elif gkz in bresu:
                name = bresu[gkz]["name"]

            rec = MunicipalityRecord(
                code=gkz,
                name=name,
                region=gkz_to_bundesland(gkz),
                country=Country.AT,
            )

            # Override
            if gkz in overrides:
                override = overrides[gkz]
                rec.override_domain = override.get("domain", "")
                if override.get("name"):
                    rec.override_name = override["name"]

            # BRESU website domain
            bresu_wd = bresu.get(gkz, {}).get("website_domain")
            if bresu_wd and bresu_wd.lower() not in self.skip_domains:
                d = bresu_wd.lower()
                rec.candidates.append(DomainCandidate(domain=d, source="bresu"))
                if not rec.website_domain:
                    rec.website_domain = d

            # BRESU email domain (separate — direct email signal)
            bresu_ed = bresu.get(gkz, {}).get("email_domain")
            if bresu_ed and bresu_ed.lower() not in self.skip_domains:
                d = bresu_ed.lower()
                rec.candidates.append(
                    DomainCandidate(domain=d, source="bresu_email", is_email_domain=True)
                )

            # Wikidata website domain
            wiki_entry = wikidata.get(gkz)
            if wiki_entry and wiki_entry.get("website"):
                wd = url_to_domain(wiki_entry["website"])
                if wd and wd.lower() not in self.skip_domains:
                    d = wd.lower()
                    rec.candidates.append(DomainCandidate(domain=d, source="wikidata"))
                    if not rec.website_domain:
                        rec.website_domain = d

            records.append(rec)

        # Auto-add .gv.at variants for all municipalities (ensures gov domains are scraped)
        gv_added = 0
        for rec in records:
            if not rec.name or rec.override_domain is not None:
                continue
            existing = {c.domain for c in rec.candidates}
            for slug in self.slugify_name(rec.name):
                gv = f"{slug}.gv.at"
                if gv not in existing:
                    rec.candidates.append(DomainCandidate(domain=gv, source="guess"))
                    gv_added += 1
        if gv_added:
            logger.info("Added {} .gv.at guess domains", gv_added)

        logger.info("AT: {} municipalities collected", len(records))
        return records

    def guess_domains(self, name: str, region: str) -> list[str]:
        parts = [p.strip() for p in name.split("/") if p.strip()]
        candidates: set[str] = set()
        bundesland_code = ""
        for prefix, bl in BUNDESLAND_BY_PREFIX_AT.items():
            if bl == region:
                bundesland_code = prefix
                break

        for part in parts:
            slugs = self.slugify_name(part)
            for slug in slugs:
                candidates.add(f"{slug}.at")
                candidates.add(f"{slug}.gv.at")
                candidates.add(f"gemeinde-{slug}.at")
                candidates.add(f"stadt-{slug}.at")
                candidates.add(f"marktgemeinde-{slug}.at")

                if bundesland_code:
                    for suffix in REGIONAL_DOMAIN_SUFFIXES_AT.get(bundesland_code, []):
                        candidates.add(f"{slug}.{suffix}")

        return sorted(candidates)

    def domain_matches_name(self, name: str, domain: str) -> bool:
        if not name or not domain:
            return False
        slugs = self.slugify_name(name)
        domain_lower = domain.lower()

        # Strip compound TLD
        domain_base = domain_lower
        for suffix in (".gv.at", ".gde.at", ".or.at", ".co.at", ".at"):
            if domain_base.endswith(suffix):
                domain_base = domain_base[: -len(suffix)]
                break

        for prefix in ("stadt-", "gemeinde-", "markt-", "marktgemeinde-", "stadtgemeinde-"):
            if domain_base.startswith(prefix):
                domain_base = domain_base[len(prefix) :]
                break

        for slug in slugs:
            if slug in domain_lower or slug == domain_base:
                return True
        return False

    def slugify_name(self, name: str) -> set[str]:
        raw = name.lower().strip()
        raw = re.sub(r"\s*\(.*?\)\s*", "", raw)

        de = raw.replace("ä", "ae").replace("ö", "oe").replace("ü", "ue").replace("ß", "ss")

        def _slug(s: str) -> str:
            s = re.sub(r"['\u2019`]", "", s)
            s = re.sub(r"[^a-z0-9]+", "-", s)
            return s.strip("-")

        slugs = {_slug(de), _slug(raw)} - {""}

        joined = _slug(de).replace("-", "")
        if joined and joined not in slugs:
            slugs.add(joined)

        return slugs

    def pick_best_email(self, emails: set[str], name: str, static_domains: set[str]) -> list[str]:
        """Austrian government domain preference."""

        def _sort_key(d: str) -> tuple[int, bool, str]:
            is_gov = _is_gov_domain(d)
            name_match = self.domain_matches_name(name, d)
            # Lower = better: gov+name(0), gov(1), name(2), other(3)
            if is_gov and name_match:
                tier = 0
            elif is_gov:
                tier = 1
            elif name_match:
                tier = 2
            else:
                tier = 3
            return (tier, not name_match, d)

        return sorted(emails, key=_sort_key)

    def regional_suffixes(self, region: str) -> list[str]:
        for prefix, bl in BUNDESLAND_BY_PREFIX_AT.items():
            if bl == region:
                return REGIONAL_DOMAIN_SUFFIXES_AT.get(prefix, [])
        return []
