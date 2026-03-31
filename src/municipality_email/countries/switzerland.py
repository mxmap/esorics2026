"""Switzerland country configuration."""

from __future__ import annotations

import re
from pathlib import Path

from loguru import logger

from municipality_email.clients.bfs import fetch_bfs_municipalities
from municipality_email.clients.wikidata import fetch_wikidata
from municipality_email.countries.base import CountryConfig
from municipality_email.schemas import Country, DomainCandidate, MunicipalityRecord
from municipality_email.scraping import load_overrides, url_to_domain

SPARQL_QUERY_CH = """
SELECT ?item ?itemLabel ?bfs ?website ?cantonLabel WHERE {
  ?item wdt:P31 wd:Q70208 .
  ?item wdt:P771 ?bfs .
  FILTER NOT EXISTS {
    ?item wdt:P576 ?dissolved .
    FILTER(?dissolved <= NOW())
  }
  FILTER NOT EXISTS {
    ?item p:P31 ?stmt .
    ?stmt ps:P31 wd:Q70208 .
    ?stmt pq:P582 ?endTime .
    FILTER(?endTime <= NOW())
  }
  FILTER NOT EXISTS {
    ?item wdt:P1366 ?successor .
  }
  OPTIONAL { ?item wdt:P856 ?website . }
  OPTIONAL { ?item wdt:P131+ ?canton .
             ?canton wdt:P31 wd:Q23058 . }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "de,fr,it,rm,en" . }
}
ORDER BY xsd:integer(?bfs)
"""

CANTON_ABBREVIATIONS = {
    "Kanton Zürich": "zh",
    "Kanton Bern": "be",
    "Kanton Luzern": "lu",
    "Kanton Uri": "ur",
    "Kanton Schwyz": "sz",
    "Kanton Obwalden": "ow",
    "Kanton Nidwalden": "nw",
    "Kanton Glarus": "gl",
    "Kanton Zug": "zg",
    "Kanton Freiburg": "fr",
    "Kanton Solothurn": "so",
    "Kanton Basel-Stadt": "bs",
    "Kanton Basel-Landschaft": "bl",
    "Kanton Schaffhausen": "sh",
    "Kanton Appenzell Ausserrhoden": "ar",
    "Kanton Appenzell Innerrhoden": "ai",
    "Kanton St. Gallen": "sg",
    "Kanton Graubünden": "gr",
    "Kanton Aargau": "ag",
    "Kanton Thurgau": "tg",
    "Kanton Tessin": "ti",
    "Kanton Waadt": "vd",
    "Kanton Wallis": "vs",
    "Kanton Neuenburg": "ne",
    "Kanton Genf": "ge",
    "Kanton Jura": "ju",
}

SKIP_DOMAINS_CH = {
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
    "netconsult.ch",
    "bbf.ch",
    "dp-wired.de",
    "google.com",
    "group.calendar.google.com",
    "mail.com",
    "wordpress.org",
    "defiant.com",
    "schedulista.com",
    "zurich-airport.com",
    "avasad.ch",
}

SUBPAGES_CH = [
    "/kontakt",
    "/contact",
    "/impressum",
    "/kontakt/",
    "/contact/",
    "/impressum/",
    "/de/kontakt",
    "/fr/contact",
    "/it/contatto",
    "/verwaltung",
    "/administration",
    "/autorites",
    "/gemeinde",
    "/commune",
    "/comune",
]


class SwitzerlandConfig(CountryConfig):
    country = Country.CH
    code_field = "bfs"
    tlds = [".ch"]
    government_tlds: list[str] = []
    skip_domains = SKIP_DOMAINS_CH
    subpages = SUBPAGES_CH
    concurrency = 30

    async def collect_candidates(self, data_dir: Path) -> list[MunicipalityRecord]:
        # BFS API is canonical
        bfs_municipalities = await fetch_bfs_municipalities()

        # Wikidata provides website URLs
        wikidata = await fetch_wikidata(
            SPARQL_QUERY_CH,
            code_field="bfs",
            name_field="itemLabel",
        )

        # Load overrides
        overrides = load_overrides(data_dir / "overrides.json")

        # Merge: BFS is canonical, Wikidata supplements
        bfs_only = set(bfs_municipalities) - set(wikidata)
        if bfs_only:
            logger.warning("{} municipalities in BFS but missing from Wikidata", len(bfs_only))

        records: list[MunicipalityRecord] = []
        for bfs, bfs_entry in sorted(bfs_municipalities.items(), key=lambda kv: int(kv[0])):
            canton = bfs_entry["canton"]
            rec = MunicipalityRecord(
                code=bfs,
                name=bfs_entry["name"],
                region=canton,
                country=Country.CH,
            )

            # Override
            if bfs in overrides:
                override = overrides[bfs]
                rec.override_domain = override.get("domain", "")
                if override.get("name"):
                    rec.override_name = override["name"]
                if override.get("canton"):
                    rec.region = override["canton"]

            # Wikidata website
            wiki_entry = wikidata.get(bfs)
            if wiki_entry and wiki_entry.get("website"):
                wd = url_to_domain(wiki_entry["website"])
                if wd and wd.lower() not in self.skip_domains:
                    rec.candidates.append(DomainCandidate(domain=wd.lower(), source="wikidata"))
                    rec.website_domain = wd.lower()

            records.append(rec)

        # Add override-only municipalities (missing from BFS + Wikidata)
        known_codes = {r.code for r in records}
        for bfs, override in overrides.items():
            if bfs not in known_codes and "name" in override:
                rec = MunicipalityRecord(
                    code=bfs,
                    name=override["name"],
                    region=override.get("canton", ""),
                    country=Country.CH,
                    override_domain=override.get("domain", ""),
                )
                records.append(rec)
                logger.info("Added override-only municipality: {} {}", bfs, override["name"])

        logger.info("CH: {} municipalities collected", len(records))
        return records

    def guess_domains(self, name: str, region: str) -> list[str]:
        parts = [p.strip() for p in name.split("/") if p.strip()]

        all_slugs: set[str] = set()
        all_extras: set[str] = set()

        slugs, extras = self._slugs_for(name)
        all_slugs |= slugs
        all_extras |= extras

        if len(parts) > 1:
            for part in parts:
                slugs, extras = self._slugs_for(part)
                all_slugs |= slugs
                all_extras |= extras

        candidates: set[str] = set()
        canton_abbrev = CANTON_ABBREVIATIONS.get(region, "")

        for slug in all_slugs:
            candidates.add(f"{slug}.ch")
            candidates.add(f"gemeinde-{slug}.ch")
            candidates.add(f"commune-de-{slug}.ch")
            candidates.add(f"comune-di-{slug}.ch")
            candidates.add(f"stadt-{slug}.ch")
            if canton_abbrev:
                candidates.add(f"{slug}.{canton_abbrev}.ch")

        for joined in all_extras:
            candidates.add(f"{joined}.ch")

        return sorted(candidates)

    def _slugs_for(self, text: str) -> tuple[set[str], set[str]]:
        slugs = self.slugify_name(text)

        extras: set[str] = set()
        raw = text.lower().strip()
        raw = re.sub(r"\s*\(.*?\)\s*", "", raw)
        de = raw.replace("ü", "ue").replace("ä", "ae").replace("ö", "oe")
        fr = raw
        for a, b in [
            ("é", "e"),
            ("è", "e"),
            ("ê", "e"),
            ("ë", "e"),
            ("à", "a"),
            ("â", "a"),
            ("ô", "o"),
            ("î", "i"),
            ("ù", "u"),
            ("û", "u"),
            ("ç", "c"),
            ("ï", "i"),
        ]:
            fr = fr.replace(a, b)

        def _slug(s):
            s = re.sub(r"['\u2019`]", "", s)
            s = re.sub(r"[^a-z0-9]+", "-", s)
            return s.strip("-")

        for variant in [de, fr, raw]:
            joined = _slug(variant).replace("-", "")
            if joined and joined not in slugs:
                extras.add(joined)

        return slugs, extras

    def domain_matches_name(self, name: str, domain: str) -> bool:
        if not name or not domain:
            return False
        slugs = self.slugify_name(name)
        domain_lower = domain.lower()

        prefixes = ["stadt-", "gemeinde-", "commune-de-", "comune-di-"]
        domain_stripped = domain_lower
        for prefix in prefixes:
            if domain_stripped.startswith(prefix):
                domain_stripped = domain_stripped[len(prefix) :]
                break

        domain_base = (
            domain_stripped.rsplit(".", 1)[0] if "." in domain_stripped else domain_stripped
        )
        parts = domain_base.split(".")
        domain_base_first = parts[0] if parts else domain_base

        for slug in slugs:
            if slug in domain_lower or slug == domain_base_first:
                return True
        return False

    def slugify_name(self, name: str) -> set[str]:
        raw = name.lower().strip()
        raw = re.sub(r"\s*\(.*?\)\s*", "", raw)

        de = raw.replace("ü", "ue").replace("ä", "ae").replace("ö", "oe")
        fr = raw
        for a, b in [
            ("é", "e"),
            ("è", "e"),
            ("ê", "e"),
            ("ë", "e"),
            ("à", "a"),
            ("â", "a"),
            ("ô", "o"),
            ("î", "i"),
            ("ù", "u"),
            ("û", "u"),
            ("ç", "c"),
            ("ï", "i"),
        ]:
            fr = fr.replace(a, b)

        def _slug(s):
            s = re.sub(r"['\u2019`]", "", s)
            s = re.sub(r"[^a-z0-9]+", "-", s)
            return s.strip("-")

        return {_slug(de), _slug(fr), _slug(raw)} - {""}
