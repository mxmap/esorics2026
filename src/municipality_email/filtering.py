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
        logger.debug(
            "Frequency blocklist: {} domains (threshold={})", len(blocklist), threshold
        )
    return blocklist


def score_domain_relevance(
    domain: str,
    municipality_name: str,
    config: CountryConfig,
    candidate_domains: set[str],
) -> float:
    """Score how relevant an email domain is to a specific municipality.

    Returns 0.0-1.0:
      1.0 — domain_matches_name (direct match, cantonal pattern like herisau.ar.ch)
      0.8 — domain contains a name slug as substring
      0.4 — domain is a known candidate from static sources
      0.2 — domain has a country-appropriate TLD
      0.0 — no affinity
    """
    if config.domain_matches_name(municipality_name, domain):
        return 1.0

    # Partial substring match
    slugs = config.slugify_name(municipality_name)
    domain_lower = domain.lower()
    for slug in slugs:
        if slug and slug in domain_lower:
            return 0.8

    if domain in candidate_domains:
        return 0.4

    if any(domain_lower.endswith(tld) for tld in config.tlds):
        return 0.2

    return 0.0


def filter_scraped_pool(
    pool: set[str],
    municipality_name: str,
    config: CountryConfig,
    frequency_blocklist: set[str],
    candidate_domains: set[str],
) -> set[str]:
    """Apply all filtering layers to a scraped email pool for one municipality.

    1. Remove frequency-blocklisted domains (exempt if candidate or name-match).
    2. When >3 domains remain, prune to those with relevance score > 0.0.
    """
    # Layer 2: frequency blocklist with exemptions
    filtered: set[str] = set()
    for domain in pool:
        if domain not in frequency_blocklist:
            filtered.add(domain)
        elif domain in candidate_domains:
            filtered.add(domain)  # exempt: known candidate
        elif config.domain_matches_name(municipality_name, domain):
            filtered.add(domain)  # exempt: matches municipality name
        # else: blocked by frequency filter

    # Layer 3: relevance scoring (only prune when many candidates)
    # Require score >= 0.4 (name match or known candidate), not just correct TLD
    if len(filtered) > 3:
        scored = filtered.copy()
        filtered = set()
        for domain in scored:
            score = score_domain_relevance(
                domain, municipality_name, config, candidate_domains
            )
            if score >= 0.4:
                filtered.add(domain)
        # Safety: if scoring removed everything, fall back to original
        if not filtered:
            filtered = scored

    return filtered
