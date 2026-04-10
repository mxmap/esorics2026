"""Classify domains by aggregating DNS/probe evidence into provider + confidence.

Algorithm:
1. **Domestic/foreign MX override** — when MX hosts exist but none matched a
   known cloud provider and there is no gateway, classify by ASN country
   evidence (DOMESTIC / FOREIGN).  Confidence is a flat base from
   ``_DOMESTIC_RULES`` / ``_FOREIGN_RULES`` (no per-signal boost).
2. **Winner** — sum primary signal weights (MX, SPF, DKIM, AUTODISCOVER) per
   provider; highest total wins.  No primary signals → UNKNOWN.
3. **Confidence** — match winner's signals against ``_PROVIDER_RULES`` (first
   match wins); extra signals add +0.02 each; capped at 1.0.

Signal tiers:
- **Primary** (MX, SPF, DKIM, AUTODISCOVER): elect a winner.
- **Confirmation** (TENANT, ASN, SPF_IP, TXT_VERIFICATION, …): boost provider
  confidence only (+0.02 per extra signal); ignored for country classification.
- **Gateway**: rule-matching flag from MX hostnames, not a SignalKind.
  Behind a gateway, DKIM providers get +0.06 to beat SPF-from-DNS-host.
"""

from __future__ import annotations

import asyncio
from collections import Counter, defaultdict
from typing import NamedTuple
from collections.abc import AsyncIterator

from loguru import logger

from mail_municipalities.core.dns import lookup_mx
from .models import ClassificationResult, Evidence, Provider, SignalKind
from .probes import (
    WEIGHTS,
    detect_gateway,
    extract_spf_evidence,
    lookup_spf_raw,
    probe_asn,
    probe_autodiscover,
    probe_cname_chain,
    probe_dkim,
    probe_dmarc,
    probe_mx,
    probe_smtp,
    probe_spf_ip,
    probe_tenant,
    probe_txt_verification,
)

# Primary signals that can stand on their own
_PRIMARY_KINDS = frozenset({SignalKind.MX, SignalKind.SPF, SignalKind.DKIM, SignalKind.AUTODISCOVER})

# Boost per additional signal beyond the matched rule
_BOOST_PER_SIGNAL = 0.02

# Behind a gateway, boost DKIM provider scores so DKIM (0.15 + 0.06 = 0.21)
# beats SPF-only (0.20) from a DNS-hosting provider.
_GATEWAY_DKIM_BOOST = 0.06


class _Rule(NamedTuple):
    """Confidence rule matched via ``rule.signals <= present_signals``."""

    name: str
    signals: frozenset[SignalKind]  # required signal kinds (subset check)
    needs_gateway: bool  # gateway is not a SignalKind, needs dedicated flag
    base: float  # base confidence before boost


_S = SignalKind  # local alias for compact table

# fmt: off
_PROVIDER_RULES: tuple[_Rule, ...] = (
    # rule name         signals                         gw?      base
    _Rule("mx_spf",    frozenset({_S.MX, _S.SPF}),      False,   0.90),  # routing + authorization
    _Rule("mx_only",   frozenset({_S.MX}),              False,   0.80),  # routing alone
    _Rule("spf_gw",    frozenset({_S.SPF}),             True,    0.70),  # SPF visible through gateway
    _Rule("dkim_gw",   frozenset({_S.DKIM}),            True,    0.65),  # DKIM proves signer behind gw
    _Rule("dkim_spf",  frozenset({_S.DKIM, _S.SPF}),    False,   0.60),  # two signals, no routing
    _Rule("spf_only",  frozenset({_S.SPF}),             False,   0.50),  # authorization only
    _Rule("fallback",  frozenset(),                     False,   0.40),  # catch-all
)
# fmt: on

_rule_hits: Counter[str] = Counter()

# fmt: off
# Domestic rules: IP confirmed in target country via Cymru CC.
# Used both by the domestic MX override (primary path) and the country
# fallback (no primary signals at all).
_DOMESTIC_RULES: tuple[tuple[str, float], ...] = (
    ("dom_mx_spf",     0.80),  # MX routes domestic + SPF present
    ("dom_mx_only",    0.70),  # MX routes domestic, no SPF
    ("dom_secondary",  0.20),  # secondary evidence only (no MX)
    ("dom_none",       0.00),  # nothing
)

# Foreign rules: IP confirmed in a different country — weaker signal because
# a non-cloud foreign IP is more ambiguous (CDN, shared hosting).
_FOREIGN_RULES: tuple[tuple[str, float], ...] = (
    ("frgn_mx_spf",     0.60),  # MX routes foreign + SPF present
    ("frgn_mx_only",    0.50),  # MX routes foreign, no SPF
    ("frgn_secondary",  0.10),  # secondary evidence only (no MX)
    ("frgn_none",       0.00),  # nothing
)
# fmt: on

_ALL_RULE_NAMES: tuple[str, ...] = (
    tuple(r.name for r in _PROVIDER_RULES)
    + ("gateway_no_primary",)
    + tuple(name for name, _ in _DOMESTIC_RULES)
    + tuple(name for name, _ in _FOREIGN_RULES)
)


def _rule_confidence(provider: Provider, signals: set[SignalKind], gateway: str | None) -> tuple[float, str]:
    """Return ``(confidence, rule_name)`` for a winning provider.

    Iterates ``_PROVIDER_RULES`` (first match wins) via subset check:
    ``rule.signals <= present``.  Only MX, SPF, DKIM participate in rule
    matching; all other signals (TENANT, AD, ASN, …) contribute via the
    per-signal boost only.
    """
    present: set[SignalKind] = set()
    for kind in (SignalKind.MX, SignalKind.SPF, SignalKind.DKIM):
        if kind in signals:
            present.add(kind)
    has_gateway = gateway is not None

    for rule in _PROVIDER_RULES:
        if rule.signals <= present and (not rule.needs_gateway or has_gateway):
            _rule_hits[rule.name] += 1
            logger.debug(
                "rule={} base={:.2f} provider={}",
                rule.name,
                rule.base,
                provider.value,
            )
            boost = len(signals - rule.signals) * _BOOST_PER_SIGNAL
            return min(1.0, rule.base + boost), rule.name

    # Unreachable: fallback rule matches everything
    return 0.40, "fallback"  # pragma: no cover


def _country_confidence(
    mx_hosts: list[str],
    spf_raw: str,
    evidence: list[Evidence],
    rules: tuple[tuple[str, float], ...],
) -> tuple[float, str]:
    """Return ``(confidence, rule_name)`` for a DOMESTIC or FOREIGN domain.

    Uses ``_DOMESTIC_RULES`` or ``_FOREIGN_RULES`` depending on the caller.
    Returns a flat base confidence (no per-signal boost) because cloud provider
    signals (TENANT, TXT_VERIFICATION, etc.) are irrelevant to the country
    classification.
    """
    has_mx = bool(mx_hosts) or any(e.kind == SignalKind.MX for e in evidence)
    has_spf = bool(spf_raw) or any(e.kind == SignalKind.SPF for e in evidence)

    if has_mx and has_spf:
        name, base = rules[0]
    elif has_mx:
        name, base = rules[1]
    elif evidence:
        name, base = rules[2]
    else:
        name, base = rules[3]
        _rule_hits[name] += 1
        logger.debug("rule={} base=0.00", name)
        return 0.0, name

    _rule_hits[name] += 1
    logger.debug("rule={} base={:.2f}", name, base)
    return base, name


def _aggregate(
    evidence: list[Evidence],
    *,
    gateway: str | None = None,
    mx_hosts: list[str] | None = None,
    spf_raw: str = "",
) -> tuple[ClassificationResult, str]:
    """Aggregate evidence → ``(ClassificationResult, rule_name)``.

    1. Deduplicate by ``(provider, kind)``; exclude UNKNOWN.
    2. Domestic/foreign MX override if MX is non-cloud and non-gateway.
    3. Elect winner by highest primary-signal weight sum.
    4. Score via ``_rule_confidence`` (provider) or ``_country_confidence``.
    """
    _mx_hosts = mx_hosts or []

    # Deduplicate by (provider, kind) — each signal type counts once per provider
    by_provider: dict[Provider, set[SignalKind]] = defaultdict(set)
    for e in evidence:
        if e.provider == Provider.UNKNOWN:
            continue
        by_provider[e.provider].add(e.kind)

    # Winner = provider with highest sum of primary signal weights
    primary_scores: dict[Provider, float] = {}
    for provider, kinds in by_provider.items():
        score = sum(WEIGHTS[k] for k in kinds if k in _PRIMARY_KINDS)
        if score > 0:
            primary_scores[provider] = score

    # Behind a gateway, DKIM is a stronger signal than SPF because DKIM
    # proves the actual email-signing provider, while SPF can be auto-
    # inherited from DNS hosting infrastructure.
    if gateway and len(primary_scores) > 1:
        for provider, kinds in by_provider.items():
            if SignalKind.DKIM in kinds and provider in primary_scores:
                primary_scores[provider] += _GATEWAY_DKIM_BOOST

    # Domestic MX override: when MX hosts exist but none matched a known
    # cloud provider, and there is no gateway, the MX hostname is the
    # authoritative signal for where inbound email routes.  SPF/DKIM/TENANT
    # may reflect general cloud usage (e.g., MS365 for Teams) rather than
    # email-specific infrastructure.
    has_cloud_mx = any(e.kind == SignalKind.MX for e in evidence)
    if not gateway and _mx_hosts and not has_cloud_mx:
        if Provider.DOMESTIC in by_provider:
            confidence, rule_name = _country_confidence(_mx_hosts, spf_raw, evidence, _DOMESTIC_RULES)
            logger.debug("domestic MX override: rule={} conf={:.2f}", rule_name, confidence)
            return ClassificationResult(
                provider=Provider.DOMESTIC,
                confidence=confidence,
                evidence=list(evidence),
                gateway=gateway,
                mx_hosts=_mx_hosts,
                spf_raw=spf_raw,
            ), rule_name
        if Provider.FOREIGN in by_provider:
            confidence, rule_name = _country_confidence(_mx_hosts, spf_raw, evidence, _FOREIGN_RULES)
            logger.debug("foreign MX override: rule={} conf={:.2f}", rule_name, confidence)
            return ClassificationResult(
                provider=Provider.FOREIGN,
                confidence=confidence,
                evidence=list(evidence),
                gateway=gateway,
                mx_hosts=_mx_hosts,
                spf_raw=spf_raw,
            ), rule_name

    if primary_scores:
        winner = max(primary_scores, key=lambda p: primary_scores[p])
        confidence, rule_name = _rule_confidence(winner, by_provider[winner], gateway)
    else:
        # Behind a gateway with no primary provider signals, we cannot
        # determine the email provider — classify as UNKNOWN.
        if gateway:
            winner = Provider.UNKNOWN
            confidence, rule_name = 0.0, "gateway_no_primary"
            _rule_hits["gateway_no_primary"] += 1
            logger.debug("rule=gateway_no_primary (gateway blocks primary signals)")
        # Country-based fallback from Cymru CC evidence:
        # DOMESTIC = IP in target country, FOREIGN = IP in another country,
        # UNKNOWN = no Cymru data available.
        elif Provider.DOMESTIC in by_provider:
            winner = Provider.DOMESTIC
            confidence, rule_name = _country_confidence(_mx_hosts, spf_raw, evidence, _DOMESTIC_RULES)
        elif Provider.FOREIGN in by_provider:
            winner = Provider.FOREIGN
            confidence, rule_name = _country_confidence(_mx_hosts, spf_raw, evidence, _FOREIGN_RULES)
        else:
            winner = Provider.UNKNOWN
            confidence, rule_name = 0.0, "no_country_data"

    return ClassificationResult(
        provider=winner,
        confidence=confidence,
        evidence=list(evidence),
        gateway=gateway,
        mx_hosts=_mx_hosts,
        spf_raw=spf_raw,
    ), rule_name


async def classify(domain: str, *, country_code: str | None = None) -> ClassificationResult:
    """Classify a single domain: resolve MX, run probes concurrently, aggregate."""
    # Lookup ALL MX hosts first (robust, multi-resolver), then pattern-match
    all_mx_hosts = await lookup_mx(domain)
    mx_evidence = probe_mx(all_mx_hosts)

    # Gateway detection (sync, no I/O)
    gateway = detect_gateway(all_mx_hosts)

    # SPF raw record (awaited separately to preserve str type through gather)
    spf_raw = await lookup_spf_raw(domain)

    # Run remaining probes concurrently, using ALL MX hosts
    (
        dkim_ev,
        dmarc_ev,
        auto_ev,
        cname_ev,
        smtp_ev,
        tenant_ev,
        asn_ev,
        txt_ev,
        spf_ip_ev,
    ) = await asyncio.gather(
        probe_dkim(domain),
        probe_dmarc(domain),
        probe_autodiscover(domain),
        probe_cname_chain(domain, all_mx_hosts),
        probe_smtp(all_mx_hosts),
        probe_tenant(domain),
        probe_asn(all_mx_hosts, country_code=country_code),
        probe_txt_verification(domain),
        probe_spf_ip(domain, country_code=country_code),
    )

    # Derive SPF evidence from the raw record (no second DNS query)
    spf_ev = extract_spf_evidence(spf_raw)

    if not spf_raw:
        logger.warning("classify({}): no SPF record retrieved", domain)

    all_evidence = (
        mx_evidence
        + spf_ev
        + dkim_ev
        + dmarc_ev
        + auto_ev
        + cname_ev
        + smtp_ev
        + tenant_ev
        + asn_ev
        + txt_ev
        + spf_ip_ev
    )
    result, rule = _aggregate(all_evidence, gateway=gateway, mx_hosts=all_mx_hosts, spf_raw=spf_raw)
    logger.debug(
        "classify({}): provider={} confidence={:.2f} rule={} signals={}",
        domain,
        result.provider.value,
        result.confidence,
        rule,
        len(result.evidence),
    )
    return result


async def classify_many(
    domains: list[str],
    max_concurrency: int = 20,
    *,
    country_code: str | None = None,
) -> AsyncIterator[tuple[str, ClassificationResult]]:
    """Classify domains concurrently (semaphore-bounded), yield in completion order.

    Failures are logged and skipped.  Clears/logs ``_rule_hits`` around the batch.
    """
    _rule_hits.clear()
    semaphore = asyncio.Semaphore(max_concurrency)

    async def _bounded(domain: str) -> tuple[str, ClassificationResult] | None:
        async with semaphore:
            try:
                result = await classify(domain, country_code=country_code)
                return (domain, result)
            except Exception:
                logger.exception("Classification failed for {}", domain)
                return None

    tasks = [asyncio.create_task(_bounded(d)) for d in domains]
    for coro in asyncio.as_completed(tasks):
        pair = await coro
        if pair is None:
            continue
        yield pair

    summary = "\n".join(
        f"  {name:20s} {_rule_hits[name]:>5}"
        for name in sorted(_ALL_RULE_NAMES, key=lambda n: _rule_hits[n], reverse=True)
    )
    logger.info("Rule hit summary:\n{}", summary)
