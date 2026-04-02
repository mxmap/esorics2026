"""Germany country configuration."""

from __future__ import annotations

import re
from pathlib import Path

from loguru import logger

from mail_municipalities.domain_resolver.clients.static import (
    load_b42labs,
    load_csv_alex,
    load_livenson,
    normalize_csv_name,
)
from mail_municipalities.domain_resolver.clients.wikidata import fetch_wikidata
from mail_municipalities.domain_resolver.countries.base import CountryConfig
from mail_municipalities.domain_resolver.schemas import (
    Country,
    DomainCandidate,
    MunicipalityRecord,
)
from mail_municipalities.domain_resolver.scraping import load_overrides, url_to_domain

SPARQL_QUERY_DE = """
SELECT DISTINCT ?ags ?itemLabel ?website WHERE {
  ?item wdt:P31/wdt:P279* wd:Q262166 .
  ?item wdt:P439 ?ags .
  ?item wdt:P17 wd:Q183 .
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
ORDER BY ?ags
"""

BUNDESLAND_BY_PREFIX = {
    "01": "Schleswig-Holstein",
    "02": "Hamburg",
    "03": "Niedersachsen",
    "04": "Bremen",
    "05": "Nordrhein-Westfalen",
    "06": "Hessen",
    "07": "Rheinland-Pfalz",
    "08": "Baden-Württemberg",
    "09": "Bayern",
    "10": "Saarland",
    "11": "Berlin",
    "12": "Brandenburg",
    "13": "Mecklenburg-Vorpommern",
    "14": "Sachsen",
    "15": "Sachsen-Anhalt",
    "16": "Thüringen",
}

SKIP_DOMAINS_DE = {
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
    # German personal email providers
    "web.de",
    "gmx.de",
    "gmx.net",
    "gmx.at",
    "t-online.de",
    "freenet.de",
    "arcor.de",
    "aol.de",
    "aol.com",
    "yahoo.de",
    "posteo.de",
    "mailbox.org",
    "tutanota.com",
    "protonmail.com",
    "icloud.com",
    "teleworm.us",
    "example.de",
    # Web agencies / CMS providers
    "hirsch-woelfl.de",
    "inixmedia.de",
    "nolis.de",
    "b-ite.de",
    "die-netzwerkstatt.de",
    "chamaeleon.de",
    "daik.de",
    "advantic.de",
    "realrgb.de",
    "egotec.com",
    "mustermail.de",
    "riewerts.io",
}

SUBPAGES_DE = [
    "/kontakt",
    "/kontakt/",
    "/impressum",
    "/impressum/",
    "/rathaus",
    "/verwaltung",
    "/buergerservice",
]


def ags_to_bundesland(ags: str) -> str:
    """Map AGS prefix (first 2 digits) to Bundesland name."""
    return BUNDESLAND_BY_PREFIX.get(ags[:2], f"??({ags[:2]})")


class GermanyConfig(CountryConfig):
    country = Country.DE
    code_field = "ags"
    tlds = [
        ".de",
        ".bayern",
        ".nrw",
        ".saarland",
        ".berlin",
        ".hamburg",
        ".koeln",
        ".cologne",
        ".ruhr",
        ".org",
        ".com",
        ".net",
    ]
    government_tlds: list[str] = []
    skip_domains = SKIP_DOMAINS_DE
    subpages = SUBPAGES_DE
    concurrency = 60

    async def collect_candidates(self, data_dir: Path) -> list[MunicipalityRecord]:
        # Load static sources
        livenson = load_livenson(data_dir / "municipalities_de_livenson.json")
        b42labs = load_b42labs(data_dir / "data_b42labs_de.json")
        csv_data = load_csv_alex(data_dir / "E-Mail-Liste_alex.csv", self.skip_domains)

        # Fetch Wikidata
        wikidata = await fetch_wikidata(SPARQL_QUERY_DE, code_field="ags")

        # Load overrides
        overrides = load_overrides(data_dir / "overrides.json")

        # Union of all AGS codes
        all_ags = set(livenson) | set(b42labs) | set(wikidata)

        records: list[MunicipalityRecord] = []
        for ags in sorted(all_ags):
            # Name priority: livenson > wikidata > b42labs
            name = ""
            if ags in livenson:
                name = livenson[ags]["name"]
            elif ags in wikidata:
                name = wikidata[ags]["name"]
            elif ags in b42labs:
                name = b42labs[ags]["name"]

            rec = MunicipalityRecord(
                code=ags,
                name=name,
                region=ags_to_bundesland(ags),
                country=Country.DE,
            )

            # Override
            if ags in overrides:
                override = overrides[ags]
                rec.override_domain = override.get("email_domain", "")
                if override.get("website"):
                    rec.website_domain = override["website"]
                if override.get("name"):
                    rec.override_name = override["name"]

            # Livenson domain
            liv_domain = livenson.get(ags, {}).get("domain", "")
            if liv_domain and liv_domain.lower() not in self.skip_domains:
                d = liv_domain.lower()
                rec.candidates.append(DomainCandidate(domain=d, source="livenson", is_email_domain=True))

            # b42labs domain
            b42_domain = b42labs.get(ags, {}).get("domain", "")
            if b42_domain and b42_domain.lower() not in self.skip_domains:
                d = b42_domain.lower()
                rec.candidates.append(DomainCandidate(domain=d, source="b42labs", is_email_domain=True))

            # Wikidata website domain
            wiki_entry = wikidata.get(ags)
            if wiki_entry and wiki_entry.get("website"):
                wd = url_to_domain(wiki_entry["website"])
                if wd and wd.lower() not in self.skip_domains:
                    d = wd.lower()
                    rec.candidates.append(DomainCandidate(domain=d, source="wikidata"))
                    rec.website_domain = d

            # CSV email domain (name-based matching, constrained by Bundesland)
            nn = normalize_csv_name(name) if name else ""
            if nn and nn in csv_data:
                for ed, csv_bl in csv_data[nn]:
                    if ed in self.skip_domains:
                        continue
                    if csv_bl and csv_bl != rec.region:
                        continue
                    rec.candidates.append(DomainCandidate(domain=ed, source="csv_email", is_email_domain=True))
                    break

            records.append(rec)

        logger.info("DE: {} municipalities collected", len(records))
        return records

    def guess_domains(self, name: str, region: str) -> list[str]:
        parts = [p.strip() for p in name.split("/") if p.strip()]
        candidates: set[str] = set()
        for part in parts:
            slugs = self.slugify_name(part)
            for slug in slugs:
                candidates.add(f"{slug}.de")
                candidates.add(f"stadt-{slug}.de")
                candidates.add(f"gemeinde-{slug}.de")
                candidates.add(f"markt-{slug}.de")
                candidates.add(f"vg-{slug}.de")
                candidates.add(f"samtgemeinde-{slug}.de")
                candidates.add(f"amt-{slug}.de")
                candidates.add(f"{slug}-online.de")
                candidates.add(f"{slug}-info.de")
        return sorted(candidates)

    def domain_matches_name(self, name: str, domain: str) -> bool:
        if not name or not domain:
            return False
        slugs = self.slugify_name(name)
        domain_lower = domain.lower()
        domain_base = domain_lower.rsplit(".", 1)[0] if "." in domain_lower else domain_lower

        for prefix in ("stadt-", "gemeinde-", "markt-", "vg-", "samtgemeinde-", "amt-"):
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
