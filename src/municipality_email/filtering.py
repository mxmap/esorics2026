"""Filtering layers for scraped email domains."""

from __future__ import annotations

from collections import Counter
from typing import TYPE_CHECKING

import tldextract
from loguru import logger

if TYPE_CHECKING:
    from municipality_email.countries.base import CountryConfig
    from municipality_email.schemas import MunicipalityRecord


def is_valid_tld(domain: str) -> bool:
    """Check if domain has a recognized public suffix."""
    ext = tldextract.extract(domain)
    return bool(ext.suffix)


def build_frequency_blocklist(
    records: list[MunicipalityRecord],
    threshold_pct: float = 0.005,
    threshold_floor: int = 5,
) -> set[str]:
    """Build blocklist of email domains appearing across too many municipality sites.

    Domains scraped from many different municipality websites are almost certainly
    generic services (ISPs, NGOs, CMS providers), not municipality-specific.
    """
    counts: Counter[str] = Counter()
    for rec in records:
        # Collect unique email domains found across all scraped pages for this municipality
        rec_domains: set[str] = set()
        for email_list in rec.scraped_emails.values():
            rec_domains.update(email_list)
        for domain in rec_domains:
            counts[domain] += 1

    scraped_count = sum(1 for rec in records if rec.scraped_emails)
    threshold = max(threshold_floor, int(scraped_count * threshold_pct))
    blocklist = {domain for domain, count in counts.items() if count >= threshold}
    if blocklist:
        logger.debug("Frequency blocklist: {} domains (threshold={})", len(blocklist), threshold)
    return blocklist


def _is_municipality_domain(
    domain: str,
    municipality_name: str,
    config: CountryConfig,
) -> bool:
    """Strict check: domain base (after stripping TLD/prefix) matches a municipality slug.

    Matches: baden.ch, gemeinde-baden.ch, gemeindesursee.ch, baden.ag.ch, stadt-baden.ch, stadtsursee.ch
    Rejects: feuerwehr-baden.ch, schule-baden.ch, apothekedrkunz.ch
    """
    slugs = config.slugify_name(municipality_name)
    if not slugs:
        return False

    ext = tldextract.extract(domain)
    # The registered domain part (e.g. "baden" from "baden.ch", "gemeinde-baden" from
    # "gemeinde-baden.ch", "baden" from "baden.ag.ch")
    base = ext.domain.lower()

    # Direct match: baden.ch
    if base in slugs:
        return True

    # Standard prefixes: gemeinde-baden.ch, stadt-baden.ch, etc.
    for prefix in (
        "gemeinde-",
        "gemeinde",
        "stadt-",
        "stadt",
        "commune-de-",
        "comune-di-",
        "markt-",
        "markt",
        "marktgemeinde-",
        "marktgemeinde",
        "stadtgemeinde-",
        "stadtgemeinde",
    ):
        if base.startswith(prefix) and base[len(prefix) :] in slugs:
            return True

    # Cantonal/regional subdomain: baden.ag.ch — ext.domain is "baden",
    # ext.subdomain is "" when registered under ag.ch
    # tldextract may parse "baden.ag.ch" as domain="baden", suffix="ag.ch"
    # or domain="ag", subdomain="baden", suffix="ch" depending on PSL.
    # Handle both: check if subdomain matches a slug.
    if ext.subdomain and ext.subdomain.lower() in slugs:
        return True

    return False


def score_domain_relevance(
    domain: str,
    municipality_name: str,
    config: CountryConfig,
    candidate_domains: set[str],
    region: str = "",
) -> float:
    """Score how relevant an email domain is to a specific municipality.

    Returns 0.0-1.0:
      1.0 — domain IS the municipality (strict base match with standard prefixes)
      0.5 — domain is a cantonal/regional suffix for the municipality's region
      0.4 — domain is a known candidate from static sources
      0.0 — no affinity
    """
    if _is_municipality_domain(domain, municipality_name, config):
        return 1.0

    if region and domain in set(config.regional_suffixes(region)):
        return 0.5

    if domain in candidate_domains:
        return 0.4

    return 0.0


def filter_scraped_pool(
    pool: set[str],
    municipality_name: str,
    config: CountryConfig,
    frequency_blocklist: set[str],
    candidate_domains: set[str],
    region: str = "",
) -> set[str]:
    """Apply all filtering layers to a scraped email pool for one municipality.

    1. Remove frequency-blocklisted domains (exempt if candidate, name-match, or regional).
    2. Keep only municipality domains (strict name match), known candidates, or regional.
       Empty result is valid — lets decide phase fall through to static/guess.
    """
    regional_domains = set(config.regional_suffixes(region)) if region else set()

    # Layer 2: frequency blocklist with exemptions
    filtered: set[str] = set()
    for domain in pool:
        if domain not in frequency_blocklist:
            filtered.add(domain)
        elif domain in candidate_domains:
            filtered.add(domain)  # exempt: known candidate
        elif config.domain_matches_name(municipality_name, domain):
            filtered.add(domain)  # exempt: matches municipality name
        elif domain in regional_domains:
            filtered.add(domain)  # exempt: cantonal/regional domain
        # else: blocked by frequency filter

    # Layer 3: relevance scoring — keep only municipality domains or known candidates
    scored = filtered.copy()
    filtered = set()
    for domain in scored:
        score = score_domain_relevance(domain, municipality_name, config, candidate_domains, region=region)
        if score >= 0.4:
            filtered.add(domain)

    return filtered
