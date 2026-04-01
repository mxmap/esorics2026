"""Six-phase pipeline orchestrator for municipality email domain collection."""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import httpx
from loguru import logger

from municipality_email.cache import CacheDB
from municipality_email.countries.base import CountryConfig
from municipality_email.dns import lookup_mx
from municipality_email.schemas import (
    Confidence,
    DomainCandidate,
    MunicipalityDetailedOutput,
    MunicipalityOutput,
    MunicipalityRecord,
    PipelineOutput,
    Source,
)
from municipality_email.filtering import build_frequency_blocklist, filter_scraped_pool
from municipality_email.scraping import (
    detect_website_mismatch,
    scrape_email_domains,
    validate_domain_accessibility,
)

_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.10 Safari/605.1.1"
)


# ── Phase 1: Collect candidates ─────────────────────────────────────


async def phase_collect(config: CountryConfig, data_dir: Path) -> list[MunicipalityRecord]:
    """Phase 1: Collect candidates from all sources, apply overrides and guessing."""
    t0 = time.time()
    records = await config.collect_candidates(data_dir)

    # Guess domains only for municipalities with zero real candidates
    guessed = 0
    for rec in records:
        if rec.override_domain is not None:
            continue
        has_real = any(c.source != "guess" for c in rec.candidates)
        if not has_real and rec.name:
            for g in config.guess_domains(rec.name, rec.region):
                rec.candidates.append(DomainCandidate(domain=g, source="guess"))
            if any(c.source == "guess" for c in rec.candidates):
                guessed += 1

    if guessed:
        logger.info("Generated guess domains for {} municipalities", guessed)

    # Build work set statistics
    all_domains = {c.domain for r in records for c in r.candidates}
    logger.info(
        "[1/6] Collect: {} municipalities, {} unique domains ({:.1f}s)",
        len(records),
        len(all_domains),
        time.time() - t0,
    )
    return records


# ── Phase 2: Validate websites (HEAD) ──────────────────────────────


async def phase_validate(
    records: list[MunicipalityRecord],
    config: CountryConfig,
    cache: CacheDB | None = None,
) -> dict[str, tuple[bool, str | None, bool]]:
    """Phase 2: HEAD requests to check domain accessibility.

    Returns dict mapping domain -> (accessible, redirect_target, ssl_failed).
    """
    t0 = time.time()
    all_domains = {c.domain for r in records for c in r.candidates}
    for r in records:
        if r.override_domain:
            all_domains.add(r.override_domain)

    # Check cache
    cached: dict[str, tuple[bool, str | None, bool]] = {}
    if cache is not None:
        cached = await cache.get_head_many(all_domains)
    to_validate = sorted(all_domains - set(cached))

    if cached:
        logger.info("HEAD cache: {} cached, {} to validate", len(cached), len(to_validate))

    results: dict[str, tuple[bool, str | None, bool]] = dict(cached)
    total = len(to_validate)
    done = 0
    sem = asyncio.Semaphore(config.concurrency)

    async def check_one(client: httpx.AsyncClient, domain: str) -> None:
        nonlocal done
        async with sem:
            accessible, redirect, ssl_failed = await validate_domain_accessibility(client, domain)
            results[domain] = (accessible, redirect, ssl_failed)
            done += 1
            if done % 500 == 0 or done == total:
                logger.info("HEAD validation: {}/{}", done, total)

    if to_validate:
        async with httpx.AsyncClient(
            headers={"User-Agent": _USER_AGENT}, follow_redirects=True, timeout=10
        ) as client:
            tasks = [check_one(client, d) for d in to_validate]
            await asyncio.gather(*tasks)

        # Persist new results
        new_results = {d: results[d] for d in to_validate if d in results}
        if cache is not None and new_results:
            await cache.put_head_many(new_results)

    accessible_count = sum(1 for a, _, _ in results.values() if a)
    logger.info(
        "[2/6] Validate: {}/{} accessible ({:.1f}s)",
        accessible_count,
        len(all_domains),
        time.time() - t0,
    )

    # Update records with accessibility info
    for rec in records:
        for cand in rec.candidates:
            if cand.domain in results:
                accessible, redirect, _ = results[cand.domain]
                rec.accessible[cand.domain] = accessible
                if redirect:
                    rec.redirects[cand.domain] = redirect

    return results


# ── Phase 3: Scrape emails ──────────────────────────────────────────


async def phase_scrape(
    records: list[MunicipalityRecord],
    config: CountryConfig,
    validation: dict[str, tuple[bool, str | None, bool]],
    cache: CacheDB | None = None,
) -> dict[str, tuple[set[str], str | None, bool]]:
    """Phase 3: Scrape accessible domains for email addresses."""
    t0 = time.time()

    # Only scrape accessible domains
    all_domains = {c.domain for r in records for c in r.candidates}
    accessible_domains = {d for d in all_domains if validation.get(d, (False,))[0]}

    # Check cache
    cached: dict[str, tuple[set[str], str | None, bool]] = {}
    if cache is not None:
        cached = await cache.get_scrape_many(accessible_domains)
    to_scrape = sorted(accessible_domains - set(cached))

    if cached:
        logger.info("Scrape cache: {} cached, {} to scrape", len(cached), len(to_scrape))

    results: dict[str, tuple[set[str], str | None, bool]] = dict(cached)
    total = len(to_scrape)
    if total == 0:
        logger.info("[3/6] Scrape: all {} domains served from cache", len(results))
        _update_records_from_scrape(records, results)
        return results

    done = 0
    timeout_count = 0
    timeout_domains: set[str] = set()
    sem = asyncio.Semaphore(config.concurrency)
    domain_timeout = 120  # max seconds per domain

    async def scrape_one(client: httpx.AsyncClient, domain: str) -> None:
        nonlocal done, timeout_count
        _, _, ssl_failed = validation.get(domain, (False, None, False))
        async with sem:
            try:
                email_domains, redirect, accessible = await asyncio.wait_for(
                    scrape_email_domains(
                        client,
                        domain,
                        config.subpages,
                        config.skip_domains,
                        exhaustive=True,
                        ssl_failed=ssl_failed,
                    ),
                    timeout=domain_timeout,
                )
                results[domain] = (email_domains, redirect, accessible)
            except asyncio.TimeoutError:
                logger.warning("Scrape timeout for {} (>{}s)", domain, domain_timeout)
                results[domain] = (set(), None, False)
                timeout_count += 1
                timeout_domains.add(domain)
            except Exception as exc:
                logger.debug("Scrape failed for {}: {!r}", domain, exc)
                results[domain] = (set(), None, False)

            # Log result
            emails, redir, acc = results[domain]
            if emails:
                logger.debug(
                    "[{}/{}] {} -> {} email(s): {}",
                    done + 1,
                    total,
                    domain,
                    len(emails),
                    ", ".join(sorted(emails)),
                )
            else:
                logger.debug("[{}/{}] {} -> no emails", done + 1, total, domain)

            # Persist each result immediately
            if cache is not None:
                await cache.put_scrape(domain, emails, redir, acc)

            done += 1
            if done % 200 == 0 or done == total:
                elapsed = time.time() - t0
                rate = done / elapsed if elapsed > 0 else 0
                eta = (total - done) / rate if rate > 0 else 0
                logger.info(
                    "Scraping: {}/{} ({:.0f}%) ~{:.0f}s remaining",
                    done,
                    total,
                    done / total * 100,
                    eta,
                )

    pool = httpx.Limits(
        max_connections=config.concurrency + 10,
        max_keepalive_connections=config.concurrency,
    )
    async with httpx.AsyncClient(
        headers={"User-Agent": _USER_AGENT},
        follow_redirects=True,
        timeout=15,
        limits=pool,
    ) as client:
        tasks = [scrape_one(client, d) for d in to_scrape]
        await asyncio.gather(*tasks)

    scraped_with_emails = sum(1 for eds, _, _ in results.values() if eds)
    logger.info(
        "[3a/6] Scrape (httpx): {}/{} had emails, {} timeouts ({:.1f}s)",
        scraped_with_emails,
        len(results),
        timeout_count,
        time.time() - t0,
    )

    # Phase 3b: Playwright fallback for domains with no emails
    no_email_domains = sorted(d for d in to_scrape if not results.get(d, (set(),))[0])
    if no_email_domains:
        await _playwright_fallback(no_email_domains, results, config, cache, t0, timeout_domains)

    _update_records_from_scrape(records, results)
    return results


async def _playwright_fallback(
    domains: list[str],
    results: dict[str, tuple[set[str], str | None, bool]],
    config: CountryConfig,
    cache: CacheDB | None,
    t0: float,
    slow_domains: set[str] | None = None,
) -> None:
    """Phase 3b: Use Chromium to render JS-heavy sites that httpx missed."""
    from municipality_email.scraping import scrape_with_playwright

    if slow_domains is None:
        slow_domains = set()

    total = len(domains)
    logger.info("[3b/6] JS fallback: {} domains to retry with Chromium", total)

    sem = asyncio.Semaphore(3)
    done = 0
    found_count = 0
    timeout_count = 0

    async def try_one(domain: str) -> None:
        nonlocal done, found_count, timeout_count
        domain_timeout = 45 if domain in slow_domains else 30
        async with sem:
            try:
                emails, redirect = await asyncio.wait_for(
                    scrape_with_playwright(domain, config.subpages, config.skip_domains),
                    timeout=domain_timeout,
                )
                if emails:
                    results[domain] = (emails, redirect, True)
                    found_count += 1
                    logger.info(
                        "[3b] {} -> {} email(s): {}",
                        domain,
                        len(emails),
                        ", ".join(sorted(emails)),
                    )
                    if cache is not None:
                        await cache.put_scrape(domain, emails, redirect, True)
            except asyncio.TimeoutError:
                logger.warning("[3b] {} timed out (>{}s)", domain, domain_timeout)
                timeout_count += 1
            except Exception as exc:
                logger.debug("[3b] {} failed: {!r}", domain, exc)

            done += 1
            if done % 10 == 0 or done == total:
                logger.info("[3b] Progress: {}/{}", done, total)

    tasks = [try_one(d) for d in domains]
    await asyncio.gather(*tasks)

    logger.info(
        "[3b/6] JS fallback: found emails on {}/{} domains, {} timeouts ({:.1f}s)",
        found_count,
        total,
        timeout_count,
        time.time() - t0,
    )


def _update_records_from_scrape(
    records: list[MunicipalityRecord],
    scrape_results: dict[str, tuple[set[str], str | None, bool]],
) -> None:
    """Push scrape results back into municipality records."""
    for rec in records:
        for cand in rec.candidates:
            if cand.domain in scrape_results:
                emails, redirect, accessible = scrape_results[cand.domain]
                if emails:
                    rec.scraped_emails[cand.domain] = sorted(emails)
                if redirect:
                    rec.redirects[cand.domain] = redirect
                rec.accessible[cand.domain] = accessible


# ── Phase 4: MX validation ──────────────────────────────────────────


async def phase_mx(
    records: list[MunicipalityRecord],
    scrape_results: dict[str, tuple[set[str], str | None, bool]],
    config: CountryConfig,
    cache: CacheDB | None = None,
) -> dict[str, bool]:
    """Phase 4: Batch MX validation for all relevant domains."""
    t0 = time.time()

    # Collect all domains needing MX checks
    domains_to_validate: set[str] = set()

    for rec in records:
        for cand in rec.candidates:
            domains_to_validate.add(cand.domain)
        if rec.override_domain:
            domains_to_validate.add(rec.override_domain)

    for emails, redirect, _ in scrape_results.values():
        domains_to_validate |= emails
        if redirect:
            domains_to_validate.add(redirect)

    domains_to_validate = {d for d in domains_to_validate if d not in config.skip_domains}

    # Check cache
    cached_mx: dict[str, bool] = {}
    if cache is not None:
        cached_mx = await cache.get_mx_many(domains_to_validate)
    to_lookup = sorted(domains_to_validate - set(cached_mx))

    if cached_mx:
        logger.info("MX cache: {} cached, {} to lookup", len(cached_mx), len(to_lookup))

    mx_results: dict[str, bool] = dict(cached_mx)
    total = len(to_lookup)
    done = 0
    sem = asyncio.Semaphore(20)

    async def check_one(domain: str) -> None:
        nonlocal done
        async with sem:
            try:
                mx = await lookup_mx(domain)
                mx_results[domain] = bool(mx)
            except Exception:
                mx_results[domain] = False
            done += 1
            if done % 1000 == 0 or done == total:
                logger.info("MX validation: {}/{}", done, total)

    if to_lookup:
        tasks = [check_one(d) for d in to_lookup]
        await asyncio.gather(*tasks)

        # Persist new results
        new_mx = {d: mx_results[d] for d in to_lookup if d in mx_results}
        if cache is not None and new_mx:
            await cache.put_mx_many(new_mx)

    valid = sum(1 for v in mx_results.values() if v)
    logger.info(
        "[4/6] MX: {}/{} have MX records ({:.1f}s)",
        valid,
        len(domains_to_validate),
        time.time() - t0,
    )

    # Update records
    for rec in records:
        for cand in rec.candidates:
            if cand.domain in mx_results:
                rec.mx_valid[cand.domain] = mx_results[cand.domain]

    return mx_results


# ── Phase 5: Decide ─────────────────────────────────────────────────


def phase_decide(
    records: list[MunicipalityRecord],
    config: CountryConfig,
    mx_valid: dict[str, bool],
    validation: dict[str, tuple[bool, str | None, bool]],
) -> None:
    """Phase 5: Decide email domain(s) for each municipality."""
    t0 = time.time()
    frequency_blocklist = build_frequency_blocklist(records)
    for rec in records:
        _decide_one(rec, config, mx_valid, validation, frequency_blocklist)
    logger.info("[5/6] Decide: done ({:.1f}s)", time.time() - t0)


def _decide_one(
    rec: MunicipalityRecord,
    config: CountryConfig,
    mx_valid: dict[str, bool],
    validation: dict[str, tuple[bool, str | None, bool]],
    frequency_blocklist: set[str] | None = None,
) -> None:
    """Decide email domain(s) for a single municipality."""
    # Build source detail for provenance
    source_map: dict[str, set[str]] = {}
    for cand in rec.candidates:
        source_map.setdefault(cand.source, set()).add(cand.domain)

    # Collect domain pools
    scraped_pool: set[str] = set()
    for domain, email_list in rec.scraped_emails.items():
        for ed in email_list:
            if ed not in config.skip_domains and mx_valid.get(ed, False):
                scraped_pool.add(ed)
    # Add redirect targets with MX
    for domain, redirect in rec.redirects.items():
        if redirect and redirect not in config.skip_domains and mx_valid.get(redirect, False):
            scraped_pool.add(redirect)

    # Apply filtering layers
    if frequency_blocklist is not None and scraped_pool:
        candidate_domains = {c.domain for c in rec.candidates}
        scraped_pool = filter_scraped_pool(
            scraped_pool,
            municipality_name=rec.name,
            config=config,
            frequency_blocklist=frequency_blocklist,
            candidate_domains=candidate_domains,
        )

    static_pool: set[str] = set()
    for cand in rec.candidates:
        if cand.source not in ("guess",) and mx_valid.get(cand.domain, False):
            static_pool.add(cand.domain)

    guess_pool: set[str] = set()
    for cand in rec.candidates:
        if cand.source == "guess" and mx_valid.get(cand.domain, False):
            guess_pool.add(cand.domain)

    sources_detail = {k: sorted(v) for k, v in source_map.items()}
    rec.sources_detail = sources_detail

    # 1. Override
    if rec.override_domain is not None:
        domain = rec.override_domain
        if domain and mx_valid.get(domain, False):
            rec.emails = [domain]
            rec.confidence = Confidence.HIGH
            rec.source = Source.OVERRIDE
        elif domain:
            rec.emails = [domain]
            rec.confidence = Confidence.MEDIUM
            rec.source = Source.OVERRIDE
            rec.flags.append("no_mx")
        else:
            rec.confidence = Confidence.NONE
            rec.source = Source.OVERRIDE
        _set_website(rec, validation)
        return

    # 2. Scraped emails with MX
    if scraped_pool:
        rec.emails = config.pick_best_email(scraped_pool, rec.name, static_pool)
        rec.confidence = Confidence.HIGH
        rec.source = Source.SCRAPE
        # Check for disagreement with static sources
        static_only = static_pool - scraped_pool
        if static_only:
            rec.flags.append("sources_disagree")
        _set_website(rec, validation)
        return

    # 3. Static source domains with MX (unconfirmed by scraping)
    unconfirmed_static = static_pool - scraped_pool
    if unconfirmed_static:
        rec.emails = config.pick_best_email(unconfirmed_static, rec.name, static_pool)
        best = rec.emails[0] if rec.emails else ""
        # If best domain matches municipality name, treat as verified
        if best and config.domain_matches_name(rec.name, best):
            rec.confidence = Confidence.HIGH
        else:
            rec.confidence = Confidence.MEDIUM
            rec.flags.append("unverified")
        rec.source = _find_source(best, rec.candidates)
        _set_website(rec, validation)
        return

    # 4. Guess domains with MX
    if guess_pool:
        rec.emails = config.pick_best_email(guess_pool, rec.name, set())
        rec.confidence = Confidence.LOW
        rec.flags.append("guess_only")
        rec.source = Source.GUESS
        _set_website(rec, validation)
        return

    # 5. Nothing
    rec.confidence = Confidence.NONE
    rec.source = Source.NONE
    _set_website(rec, validation)


def _find_source(domain: str, candidates: list[DomainCandidate]) -> Source:
    """Find which Source enum a domain came from."""
    for cand in candidates:
        if cand.domain == domain:
            source_map = {
                "wikidata": Source.WIKIDATA,
                "livenson": Source.STATIC,
                "b42labs": Source.STATIC,
                "csv_email": Source.STATIC,
                "bresu": Source.STATIC,
                "bresu_email": Source.STATIC,
                "guess": Source.GUESS,
                "scrape": Source.SCRAPE,
                "redirect": Source.REDIRECT,
                "override": Source.OVERRIDE,
            }
            return source_map.get(cand.source, Source.STATIC)
    return Source.NONE


def _set_website(
    rec: MunicipalityRecord,
    validation: dict[str, tuple[bool, str | None, bool]],
) -> None:
    """Determine website domain from validation results."""
    # If already set from source data, validate it
    if rec.website_domain:
        accessible, redirect, _ = validation.get(rec.website_domain, (False, None, False))
        if accessible:
            if redirect:
                rec.website_domain = redirect
            return
        # Website not accessible — clear it
        rec.website_domain = None

    # Find first accessible candidate domain
    for cand in rec.candidates:
        accessible, redirect, _ = validation.get(cand.domain, (False, None, False))
        if accessible:
            rec.website_domain = redirect if redirect else cand.domain
            return

    # Check website mismatch flag
    if rec.website_domain and detect_website_mismatch(rec.name, rec.website_domain):
        rec.flags.append("website_mismatch")


# ── Phase 6: Export ─────────────────────────────────────────────────


def phase_export(
    records: list[MunicipalityRecord],
    output_dir: Path,
    cc: str,
) -> None:
    """Phase 6: Write three-tier output files."""
    t0 = time.time()
    now = datetime.now(tz=timezone.utc)

    # Sort by code
    records = sorted(records, key=lambda r: r.code)

    # Minimal output
    minimal = PipelineOutput(
        generated=now,
        total=len(records),
        municipalities=[
            MunicipalityOutput(
                code=r.code,
                name=r.override_name or r.name,
                region=r.region,
                website=r.website_domain or "",
                emails=r.emails,
            )
            for r in records
        ],
    )

    # Detailed output
    detailed = PipelineOutput(
        generated=now,
        total=len(records),
        municipalities=[
            MunicipalityDetailedOutput(
                code=r.code,
                name=r.override_name or r.name,
                region=r.region,
                website=r.website_domain or "",
                emails=r.emails,
                source=r.source.value,
                confidence=r.confidence.value,
                sources_detail=r.sources_detail,
                flags=r.flags,
            )
            for r in records
        ],
    )

    # Review: entries with low/none confidence or non-empty flags
    review_entries = [
        MunicipalityDetailedOutput(
            code=r.code,
            name=r.override_name or r.name,
            region=r.region,
            website=r.website_domain or "",
            emails=r.emails,
            source=r.source.value,
            confidence=r.confidence.value,
            sources_detail=r.sources_detail,
            flags=r.flags,
        )
        for r in records
        if r.confidence in (Confidence.LOW, Confidence.NONE) or r.flags
    ]
    review = PipelineOutput(
        generated=now,
        total=len(review_entries),
        municipalities=review_entries,
    )

    output_dir.mkdir(parents=True, exist_ok=True)

    for name, data in [
        (f"{cc}.json", minimal),
        (f"{cc}_detailed.json", detailed),
        (f"{cc}_review.json", review),
    ]:
        path = output_dir / name
        with open(path, "w", encoding="utf-8") as f:
            f.write(data.model_dump_json(indent=2))
        logger.info("Wrote {}", path)

    # Log summary
    source_counts = Counter(r.source.value for r in records)
    confidence_counts = Counter(r.confidence.value for r in records)
    domain_coverage = sum(1 for r in records if r.emails)

    logger.info("--- {} Summary ---", cc.upper())
    logger.info(
        "Total: {}, with domain: {} ({:.1f}%)",
        len(records),
        domain_coverage,
        domain_coverage / max(len(records), 1) * 100,
    )
    logger.info("By source: {}", dict(source_counts.most_common()))
    logger.info("By confidence: {}", dict(confidence_counts.most_common()))
    logger.info("Review entries: {}", len(review_entries))
    logger.info("[6/6] Export done ({:.1f}s)", time.time() - t0)


# ── Main orchestrator ───────────────────────────────────────────────


async def run_pipeline(
    config: CountryConfig,
    data_dir: Path,
    output_dir: Path,
    *,
    dry_run: bool = False,
    no_cache: bool = False,
) -> None:
    """Run the full 6-phase pipeline for a country."""
    cc = config.country.value
    start = time.time()
    logger.info("=== Starting pipeline for {} ===", cc.upper())

    # Phase 1: Collect
    records = await phase_collect(config, data_dir)

    if dry_run:
        _print_dry_run(records, config)
        return

    # Open cache (if enabled)
    async with contextlib.AsyncExitStack() as stack:
        cache: CacheDB | None = None
        if not no_cache:
            cache = CacheDB(data_dir / "cache.db")
            await stack.enter_async_context(cache)

        # Phase 2: Validate
        validation = await phase_validate(records, config, cache)

        # Phase 3: Scrape
        scrape_results = await phase_scrape(records, config, validation, cache)

        # Phase 4: MX
        mx_valid = await phase_mx(records, scrape_results, config, cache)

    # Phase 5: Decide
    phase_decide(records, config, mx_valid, validation)

    # Phase 6: Export
    phase_export(records, output_dir, cc)

    logger.info("=== Pipeline complete for {} in {:.1f}s ===", cc.upper(), time.time() - start)


def _print_dry_run(records: list[MunicipalityRecord], config: CountryConfig) -> None:
    """Print statistics for dry-run mode."""
    all_domains = {c.domain for r in records for c in r.candidates}

    print(f"\n=== DRY RUN -- {config.country.value.upper()} ===")
    print(f"Total municipalities: {len(records)}")

    src_counts: Counter[str] = Counter()
    for rec in records:
        sources_seen = set()
        for cand in rec.candidates:
            if cand.source not in sources_seen:
                src_counts[cand.source] += 1
                sources_seen.add(cand.source)

    for src in sorted(src_counts):
        print(f"  With {src} domain: {src_counts[src]}")

    print(f"  Unique domains to process: {len(all_domains)}")
    print(f"  Overrides: {sum(1 for r in records if r.override_domain is not None)}")
    print()
