"""Switzerland country configuration."""

from __future__ import annotations

import re
from pathlib import Path

from loguru import logger

from municipality_email.clients.bfs import CANTON_SHORT_TO_FULL, fetch_bfs_municipalities
from municipality_email.clients.openplz import fetch_openplz_ch_municipalities
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

CANTON_ABBREVIATIONS = {v: k for k, v in CANTON_SHORT_TO_FULL.items()}

SKIP_DOMAINS_CH = {
    # Generic / test
    "example.com",
    "example.ch",
    "domain.com",
    # Tech / tracking
    "sentry.io",
    "w3.org",
    "gstatic.com",
    "googleapis.com",
    "schema.org",
    "google.com",
    "group.calendar.google.com",
    "wordpress.org",
    "defiant.com",
    "schedulista.com",
    # Personal email providers
    "gmail.com",
    "hotmail.com",
    "hotmail.ch",
    "hotmail.fr",
    "hotmail.it",
    "outlook.com",
    "gmx.ch",
    "gmx.net",
    "bluewin.ch",
    "bluemail.ch",
    "yahoo.com",
    "yahoo.fr",
    "mail.com",
    "windowslive.com",
    "ymail.com",
    "bluemail.ch",
    "protonmail.com",
    "protonmail.ch",
    "icloud.com",
    # ISPs / telcos
    "sunrise.ch",
    "hispeed.ch",
    "swissonline.ch",
    "vtxnet.ch",
    "netplus.ch",
    # Web agencies / CMS providers
    "netconsult.ch",
    "bbf.ch",
    "dp-wired.de",
    "talus.ch",
    "hemmer.ch",
    "contactmail.ch",
    # Misc known noise
    "zurich-airport.com",
    "avasad.ch",
    "post.ch",
    "hin.ch",
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
    tlds = [".ch", ".swiss", ".zuerich", ".org", ".com", ".net"]
    government_tlds: list[str] = []
    skip_domains = SKIP_DOMAINS_CH
    subpages = SUBPAGES_CH
    concurrency = 50

    def regional_suffixes(self, region: str) -> list[str]:
        abbrev = CANTON_ABBREVIATIONS.get(region, "")
        return [f"{abbrev}.ch"] if abbrev else []

    async def collect_candidates(self, data_dir: Path) -> list[MunicipalityRecord]:
        bfs_municipalities = await fetch_bfs_municipalities()
        openplz = await fetch_openplz_ch_municipalities()

        # Log discrepancies between authoritative BFS and OpenPLZ
        openplz_only = set(openplz) - set(bfs_municipalities)
        if openplz_only:
            details = ", ".join(
                f"{c} ({openplz[c]['name']})" for c in sorted(openplz_only, key=int)
            )
            logger.warning(
                "{} municipalities in OpenPLZ but not in BFS (excluded): {}",
                len(openplz_only),
                details,
            )
        bfs_only_codes = set(bfs_municipalities) - set(openplz)
        if bfs_only_codes:
            details = ", ".join(
                f"{c} ({bfs_municipalities[c]['name']})" for c in sorted(bfs_only_codes, key=int)
            )
            logger.warning(
                "{} municipalities in BFS but not in OpenPLZ: {}",
                len(bfs_only_codes),
                details,
            )

        # Wikidata provides website URLs
        wikidata = await fetch_wikidata(
            SPARQL_QUERY_CH,
            code_field="bfs",
            name_field="itemLabel",
        )

        # Load overrides
        overrides = load_overrides(data_dir / "overrides.json")

        # Merge: BFS is authoritative, OpenPLZ + Wikidata enrich
        bfs_only = set(bfs_municipalities) - set(wikidata)
        if bfs_only:
            logger.warning("{} municipalities in BFS but missing from Wikidata", len(bfs_only))

        records: list[MunicipalityRecord] = []
        for bfs, bfs_entry in sorted(bfs_municipalities.items(), key=lambda kv: int(kv[0])):
            canton = bfs_entry["canton"]
            if not canton:
                # Try OpenPLZ for canton
                plz_entry = openplz.get(bfs)
                if plz_entry and plz_entry.get("canton"):
                    canton = plz_entry["canton"]
                    logger.debug(
                        "Using OpenPLZ canton for {} ({}): {}", bfs, bfs_entry["name"], canton
                    )
            if not canton:
                wiki_entry = wikidata.get(bfs)
                if wiki_entry and wiki_entry.get("cantonLabel"):
                    canton = wiki_entry["cantonLabel"]
                    logger.debug(
                        "Using Wikidata canton for {} ({}): {}", bfs, bfs_entry["name"], canton
                    )
            rec = MunicipalityRecord(
                code=bfs,
                name=bfs_entry["name"],
                region=canton,
                country=Country.CH,
            )

            # Override
            if bfs in overrides:
                override = overrides[bfs]
                rec.override_domain = override.get("email_domain", "")
                if override.get("website"):
                    rec.website_domain = override["website"]
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
                    override_domain=override.get("email_domain", ""),
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
                candidates.add(f"{slug}-{canton_abbrev}.ch")
                candidates.add(f"{canton_abbrev}-{slug}.ch")
                candidates.add(f"{slug}{canton_abbrev}.ch")

        for joined in all_extras:
            candidates.add(f"{joined}.ch")

        for slug in all_slugs:
            candidates.add(f"{slug}-online.ch")
            candidates.add(f"{slug}-info.ch")

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

        slugs = {_slug(de), _slug(fr), _slug(raw)} - {""}

        # Dashless variants (e.g. "uetikon-am-see" → "uetikonamsee")
        for s in list(slugs):
            joined = s.replace("-", "")
            if joined and joined not in slugs:
                slugs.add(joined)

        return slugs
