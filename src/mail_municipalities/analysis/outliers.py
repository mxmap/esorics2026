"""Investigate outliers and potential errors in classification and security data.

Loads provider and security JSON output for all countries, runs targeted checks
to surface misclassifications, logical contradictions, and cross-pipeline
inconsistencies.  Optionally verifies a sample of findings with live DNS queries.

Run via CLI:  uv run analyze outliers [--country ch] [--verify]
"""

from __future__ import annotations

import subprocess
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

from rich.console import Console
from rich.table import Table

from mail_municipalities.analysis.helpers import COUNTRIES
from mail_municipalities.export import load_json
from mail_municipalities.provider_classification.signatures import (
    GATEWAY_KEYWORDS,
    SIGNATURES,
    match_patterns,
)

# ── Types ────────────────────────────────────────────────────────────

SEVERITY_ORDER: dict[str, int] = {"ERROR": 0, "WARNING": 1, "INFO": 2}

COUNTRY_ORDER: dict[str, int] = {cc.upper(): i for i, cc in enumerate(COUNTRIES)}

CATEGORIES: dict[str, tuple[str, str]] = {
    "logical_contradiction": ("Logical Contradiction", "red"),
    "dane_inconsistency": ("DANE Inconsistency", "red"),
    "domain_mismatch": ("Domain Mismatch", "red"),
    "mx_provider_mismatch": ("MX-Provider Mismatch", "red"),
    "scan_spf_mismatch": ("Scan SPF Mismatch (DNS-verified)", "red"),
    "missing_municipality": ("Missing Municipality", "red"),
    "contradictory_signals": ("Contradictory Signals", "yellow"),
    "low_confidence": ("Low Confidence (<60%)", "yellow"),
    "mx_divergence": ("MX Divergence", "yellow"),
    "missing_spf": ("Missing SPF Record", "yellow"),
    "expected_security_missing": ("Expected Security Missing", "yellow"),
    "unknown_classification": ("Unknown Classification", "cyan"),
    "invalid_scan": ("Invalid Scan", "cyan"),
}

# Primary signal kinds used in classification scoring
_PRIMARY_KINDS = {"mx", "spf", "dkim", "autodiscover"}

# Map provider output names back to signature objects
_SIG_BY_PROVIDER: dict[str, Any] = {}
for _sig in SIGNATURES:
    _SIG_BY_PROVIDER[_sig.provider.value] = _sig
# Provider output names used in JSON differ from enum values
_PROVIDER_NAME_MAP = {"ms365": "microsoft"}
for _enum_name, _output_name in _PROVIDER_NAME_MAP.items():
    if _enum_name in _SIG_BY_PROVIDER:
        _SIG_BY_PROVIDER[_output_name] = _SIG_BY_PROVIDER.pop(_enum_name)


@dataclass(frozen=True)
class Finding:
    severity: Literal["ERROR", "WARNING", "INFO"]
    category: str
    country: str
    code: str
    name: str
    region: str
    domain: str
    message: str
    details: dict[str, Any] = field(default_factory=dict)


# ── Data loading ─────────────────────────────────────────────────────

console = Console()


def load_all_data(
    providers_dir: Path,
    security_dir: Path,
    country: str | None = None,
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, list[dict[str, Any]]]]:
    """Load provider and security JSONs for each country."""
    countries = (country,) if country else COUNTRIES
    providers: dict[str, list[dict[str, Any]]] = {}
    security: dict[str, list[dict[str, Any]]] = {}

    for cc in countries:
        prov_path = providers_dir / f"providers_{cc}.json"
        sec_path = security_dir / f"security_{cc}.json"

        if prov_path.exists():
            data = load_json(prov_path)
            providers[cc] = data.get("municipalities", [])
        else:
            console.print(f"  [yellow]Warning:[/yellow] {prov_path} not found, skipping")

        if sec_path.exists():
            data = load_json(sec_path)
            security[cc] = data.get("municipalities", [])
        else:
            console.print(f"  [yellow]Warning:[/yellow] {sec_path} not found, skipping")

    return providers, security


# ── Provider checks ──────────────────────────────────────────────────


def check_low_confidence(
    providers: dict[str, list[dict[str, Any]]],
    **_: Any,
) -> list[Finding]:
    """Flag municipalities with classification confidence < 60% (excluding unknowns)."""
    findings: list[Finding] = []
    for cc, munis in providers.items():
        cc_upper = cc.upper()
        for m in munis:
            conf = m.get("classification_confidence", 0.0)
            provider = m.get("provider", "unknown")
            if provider != "unknown" and conf < 60.0:
                findings.append(
                    Finding(
                        severity="WARNING",
                        category="low_confidence",
                        country=cc_upper,
                        code=m["code"],
                        name=m["name"],
                        region=m.get("region", ""),
                        domain=m.get("domain", ""),
                        message=f"provider={provider} confidence={conf:.0f}%",
                        details={"provider": provider, "confidence": conf},
                    )
                )
    return findings


def check_contradictory_signals(
    providers: dict[str, list[dict[str, Any]]],
    **_: Any,
) -> list[Finding]:
    """Flag municipalities where primary signals point to different providers."""
    findings: list[Finding] = []
    for cc, munis in providers.items():
        cc_upper = cc.upper()
        for m in munis:
            signals = m.get("classification_signals", [])
            if not signals:
                continue

            # Aggregate primary signal weight per provider (dedup by provider+kind,
            # matching the classifier's behavior where each kind counts once)
            seen: set[tuple[str, str]] = set()
            primary_weights: dict[str, float] = defaultdict(float)
            for sig in signals:
                kind = sig.get("kind", "")
                prov = sig.get("provider", "")
                if kind in _PRIMARY_KINDS and (prov, kind) not in seen:
                    seen.add((prov, kind))
                    primary_weights[prov] += sig.get("weight", 0.0)

            if len(primary_weights) < 2:
                continue

            winner = m.get("provider", "unknown")
            sorted_provs = sorted(primary_weights.items(), key=lambda x: x[1], reverse=True)
            top_provider, top_weight = sorted_provs[0]
            runner_provider, runner_weight = sorted_provs[1]

            if runner_weight > 0 and (top_weight < 1.5 * runner_weight):
                severity: Literal["ERROR", "WARNING"] = "ERROR" if top_weight <= runner_weight else "WARNING"
                findings.append(
                    Finding(
                        severity=severity,
                        category="contradictory_signals",
                        country=cc_upper,
                        code=m["code"],
                        name=m["name"],
                        region=m.get("region", ""),
                        domain=m.get("domain", ""),
                        message=(
                            f"winner={winner} "
                            f"({top_provider}={top_weight:.2f} vs {runner_provider}={runner_weight:.2f})"
                        ),
                        details={
                            "winner": winner,
                            "primary_weights": dict(primary_weights),
                        },
                    )
                )
    return findings


def check_unknown_classification(
    providers: dict[str, list[dict[str, Any]]],
    **_: Any,
) -> list[Finding]:
    """Subcategorize unknown classifications: no domain, gateway-blocked, has signals, bare."""
    findings: list[Finding] = []
    for cc, munis in providers.items():
        cc_upper = cc.upper()
        for m in munis:
            if m.get("provider") != "unknown":
                continue

            domain = m.get("domain", "")
            gateway = m.get("gateway")
            signals = m.get("classification_signals", [])

            if not domain:
                reason = "no domain resolved"
                severity: Literal["ERROR", "WARNING", "INFO"] = "INFO"
            elif gateway:
                # Gateway blocking all primary signals is expected classifier behavior
                continue
            elif signals:
                reason = f"has {len(signals)} signal(s) but no clear winner"
                severity = "WARNING"
            else:
                reason = "no signals at all"
                severity = "INFO"

            findings.append(
                Finding(
                    severity=severity,
                    category="unknown_classification",
                    country=cc_upper,
                    code=m["code"],
                    name=m["name"],
                    region=m.get("region", ""),
                    domain=domain,
                    message=reason,
                    details={"gateway": gateway, "n_signals": len(signals)},
                )
            )
    return findings


def _is_gateway_mx(mx_list: list[str]) -> bool:
    """Check if any MX host matches a known gateway pattern."""
    all_gw_patterns = [p for patterns in GATEWAY_KEYWORDS.values() for p in patterns]
    return any(match_patterns(mx, all_gw_patterns) for mx in mx_list)


def check_mx_provider_mismatch(
    providers: dict[str, list[dict[str, Any]]],
    **_: Any,
) -> list[Finding]:
    """Flag cloud-provider municipalities whose MX records don't match expected patterns.

    Skips municipalities with a gateway (gateway-routed MX is expected to differ).
    """
    findings: list[Finding] = []
    for cc, munis in providers.items():
        cc_upper = cc.upper()
        for m in munis:
            provider = m.get("provider", "")
            if provider not in _SIG_BY_PROVIDER:
                continue

            # Gateway-routed domains are expected to have non-provider MX
            if m.get("gateway") or _is_gateway_mx(m.get("mx", [])):
                continue

            sig = _SIG_BY_PROVIDER[provider]
            mx_list = m.get("mx", [])
            if not mx_list:
                findings.append(
                    Finding(
                        severity="ERROR",
                        category="mx_provider_mismatch",
                        country=cc_upper,
                        code=m["code"],
                        name=m["name"],
                        region=m.get("region", ""),
                        domain=m.get("domain", ""),
                        message=f"provider={provider} but no MX records",
                        details={"provider": provider, "mx": []},
                    )
                )
                continue

            has_match = any(match_patterns(mx, sig.mx_patterns) for mx in mx_list)
            if not has_match:
                findings.append(
                    Finding(
                        severity="ERROR",
                        category="mx_provider_mismatch",
                        country=cc_upper,
                        code=m["code"],
                        name=m["name"],
                        region=m.get("region", ""),
                        domain=m.get("domain", ""),
                        message=f"provider={provider} but MX={', '.join(mx_list[:3])}",
                        details={"provider": provider, "mx": mx_list},
                    )
                )
    return findings


def check_missing_spf(
    providers: dict[str, list[dict[str, Any]]],
    **_: Any,
) -> list[Finding]:
    """Flag cloud-provider municipalities with no SPF record."""
    findings: list[Finding] = []
    for cc, munis in providers.items():
        cc_upper = cc.upper()
        for m in munis:
            provider = m.get("provider", "")
            if provider in ("unknown", "domestic"):
                continue
            spf = m.get("spf", "")
            if not spf or not spf.strip():
                findings.append(
                    Finding(
                        severity="WARNING",
                        category="missing_spf",
                        country=cc_upper,
                        code=m["code"],
                        name=m["name"],
                        region=m.get("region", ""),
                        domain=m.get("domain", ""),
                        message=f"provider={provider} but no SPF record",
                        details={"provider": provider},
                    )
                )
    return findings


# ── Security checks ──────────────────────────────────────────────────


def check_invalid_scans(
    security: dict[str, list[dict[str, Any]]],
    **_: Any,
) -> list[Finding]:
    """Flag municipalities with invalid security scans."""
    findings: list[Finding] = []
    for cc, munis in security.items():
        cc_upper = cc.upper()
        for m in munis:
            if not m.get("scan_valid", True):
                findings.append(
                    Finding(
                        severity="INFO",
                        category="invalid_scan",
                        country=cc_upper,
                        code=m["code"],
                        name=m["name"],
                        region=m.get("region", ""),
                        domain=m.get("domain", ""),
                        message="scan_valid=False",
                        details={},
                    )
                )
    return findings


def check_logical_contradictions(
    security: dict[str, list[dict[str, Any]]],
    **_: Any,
) -> list[Finding]:
    """Flag impossible combinations: good_spf without spf, good_dmarc without dmarc."""
    findings: list[Finding] = []
    checks = [
        ("has_good_spf", "has_spf", "has_good_spf=True but has_spf=False"),
        ("has_good_dmarc", "has_dmarc", "has_good_dmarc=True but has_dmarc=False"),
    ]
    for cc, munis in security.items():
        cc_upper = cc.upper()
        for m in munis:
            dss = m.get("dss") or {}
            for good_field, base_field, msg in checks:
                if dss.get(good_field) and not dss.get(base_field):
                    findings.append(
                        Finding(
                            severity="ERROR",
                            category="logical_contradiction",
                            country=cc_upper,
                            code=m["code"],
                            name=m["name"],
                            region=m.get("region", ""),
                            domain=m.get("domain", ""),
                            message=msg,
                            details={good_field: True, base_field: False},
                        )
                    )
    return findings


def check_dane_inconsistency(
    security: dict[str, list[dict[str, Any]]],
    **_: Any,
) -> list[Finding]:
    """Flag DANE supported=True but partial=False (logically impossible)."""
    findings: list[Finding] = []
    for cc, munis in security.items():
        cc_upper = cc.upper()
        for m in munis:
            dane = m.get("dane") or {}
            if dane.get("supported") and not dane.get("partial"):
                findings.append(
                    Finding(
                        severity="ERROR",
                        category="dane_inconsistency",
                        country=cc_upper,
                        code=m["code"],
                        name=m["name"],
                        region=m.get("region", ""),
                        domain=m.get("domain", ""),
                        message="dane.supported=True but dane.partial=False",
                        details={"dane": dane},
                    )
                )
    return findings


def check_scan_spf_mismatch(
    security: dict[str, list[dict[str, Any]]],
    **_: Any,
) -> list[Finding]:
    """Flag municipalities where security scan reports no SPF but DNS has one.

    Catches scanner failures (e.g. TXT query timeout on domains with many records).
    Verifies every candidate via DNS.
    """
    from rich.progress import Progress

    findings: list[Finding] = []
    all_candidates: list[tuple[str, dict[str, Any]]] = []
    for cc, munis in security.items():
        for m in munis:
            if m.get("scan_valid") and m.get("domain") and not (m.get("dss") or {}).get("has_spf"):
                all_candidates.append((cc, m))

    if not all_candidates:
        return findings

    with Progress(console=console) as progress:
        task = progress.add_task("DNS-verifying no-SPF municipalities", total=len(all_candidates))
        for cc, m in all_candidates:
            domain = m["domain"]
            progress.update(task, description=f"[dim]dig TXT {domain}[/dim]")
            live_spf = _dig_has_spf(domain)
            if live_spf is True:
                findings.append(
                    Finding(
                        severity="ERROR",
                        category="scan_spf_mismatch",
                        country=cc.upper(),
                        code=m["code"],
                        name=m["name"],
                        region=m.get("region", ""),
                        domain=domain,
                        message="scan says no SPF but DNS has SPF record",
                        details={"scan_has_spf": False, "dns_has_spf": True},
                    )
                )
            progress.advance(task)

    console.print(f"  [dim]Verified {len(all_candidates)} no-SPF municipalities, {len(findings)} mismatches[/dim]")
    return findings


# ── Cross-pipeline checks ───────────────────────────────────────────


def _build_lookup(
    munis: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Build {code: municipality} lookup."""
    return {m["code"]: m for m in munis}


def check_domain_mismatch(
    providers: dict[str, list[dict[str, Any]]],
    security: dict[str, list[dict[str, Any]]],
) -> list[Finding]:
    """Flag municipalities where the domain differs between providers and security."""
    findings: list[Finding] = []
    for cc in providers:
        if cc not in security:
            continue
        cc_upper = cc.upper()
        prov_lookup = _build_lookup(providers[cc])
        sec_lookup = _build_lookup(security[cc])

        for code in prov_lookup:
            if code not in sec_lookup:
                continue
            p_domain = prov_lookup[code].get("domain", "")
            s_domain = sec_lookup[code].get("domain", "")
            if p_domain and s_domain and p_domain != s_domain:
                findings.append(
                    Finding(
                        severity="ERROR",
                        category="domain_mismatch",
                        country=cc_upper,
                        code=code,
                        name=prov_lookup[code].get("name", ""),
                        region=prov_lookup[code].get("region", ""),
                        domain=p_domain,
                        message=f"providers={p_domain} vs security={s_domain}",
                        details={"provider_domain": p_domain, "security_domain": s_domain},
                    )
                )
    return findings


def check_mx_divergence(
    providers: dict[str, list[dict[str, Any]]],
    security: dict[str, list[dict[str, Any]]],
) -> list[Finding]:
    """Flag municipalities where MX records differ significantly between pipelines."""
    findings: list[Finding] = []
    for cc in providers:
        if cc not in security:
            continue
        cc_upper = cc.upper()
        prov_lookup = _build_lookup(providers[cc])
        sec_lookup = _build_lookup(security[cc])

        for code in prov_lookup:
            if code not in sec_lookup:
                continue
            p_mx = {h.lower().rstrip(".") for h in prov_lookup[code].get("mx", [])}
            s_mx = {h.lower().rstrip(".") for h in sec_lookup[code].get("mx_records", [])}

            if not p_mx or not s_mx:
                continue

            union = p_mx | s_mx
            diff = p_mx.symmetric_difference(s_mx)
            if len(diff) > len(union) * 0.5:
                findings.append(
                    Finding(
                        severity="WARNING",
                        category="mx_divergence",
                        country=cc_upper,
                        code=code,
                        name=prov_lookup[code].get("name", ""),
                        region=prov_lookup[code].get("region", ""),
                        domain=prov_lookup[code].get("domain", ""),
                        message=f"providers MX={sorted(p_mx)} vs security MX={sorted(s_mx)}",
                        details={"provider_mx": sorted(p_mx), "security_mx": sorted(s_mx)},
                    )
                )
    return findings


def check_missing_municipalities(
    providers: dict[str, list[dict[str, Any]]],
    security: dict[str, list[dict[str, Any]]],
) -> list[Finding]:
    """Flag municipalities present in one pipeline but missing from the other."""
    findings: list[Finding] = []
    all_cc = set(providers) | set(security)

    for cc in all_cc:
        cc_upper = cc.upper()
        prov_codes = {m["code"] for m in providers.get(cc, [])}
        sec_codes = {m["code"] for m in security.get(cc, [])}
        prov_lookup = _build_lookup(providers.get(cc, []))
        sec_lookup = _build_lookup(security.get(cc, []))

        for code in prov_codes - sec_codes:
            m = prov_lookup[code]
            findings.append(
                Finding(
                    severity="WARNING",
                    category="missing_municipality",
                    country=cc_upper,
                    code=code,
                    name=m.get("name", ""),
                    region=m.get("region", ""),
                    domain=m.get("domain", ""),
                    message="in providers but missing from security",
                    details={"present_in": "providers"},
                )
            )

        for code in sec_codes - prov_codes:
            m = sec_lookup[code]
            findings.append(
                Finding(
                    severity="ERROR",
                    category="missing_municipality",
                    country=cc_upper,
                    code=code,
                    name=m.get("name", ""),
                    region=m.get("region", ""),
                    domain=m.get("domain", ""),
                    message="in security but missing from providers",
                    details={"present_in": "security"},
                )
            )
    return findings


def _dig_has_spf(domain: str) -> bool | None:
    """Check if domain currently has an SPF record via dig. Returns None on error."""
    try:
        result = subprocess.run(
            ["dig", "@8.8.8.8", "+short", "TXT", domain],
            capture_output=True,
            text=True,
            timeout=10,
        )
        return "v=spf1" in result.stdout.lower()
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return None


def check_expected_security_missing(
    providers: dict[str, list[dict[str, Any]]],
    security: dict[str, list[dict[str, Any]]],
) -> list[Finding]:
    """Flag MS365 municipalities that lack SPF in the security scan.

    Verifies each candidate with live DNS -- if SPF exists now, the scan
    data is simply stale and the finding is suppressed.
    """
    findings: list[Finding] = []
    for cc in providers:
        if cc not in security:
            continue
        cc_upper = cc.upper()
        sec_lookup = _build_lookup(security[cc])

        for m in providers[cc]:
            if m.get("provider") != "microsoft":
                continue
            code = m["code"]
            sec = sec_lookup.get(code)
            if not sec or not sec.get("scan_valid"):
                continue
            dss = sec.get("dss") or {}
            if not dss.get("has_spf"):
                domain = m.get("domain", "")
                if domain:
                    live_spf = _dig_has_spf(domain)
                    if live_spf is True:
                        # SPF exists now but was missing at scan time -- stale data, not an error
                        continue
                findings.append(
                    Finding(
                        severity="WARNING",
                        category="expected_security_missing",
                        country=cc_upper,
                        code=code,
                        name=m["name"],
                        region=m.get("region", ""),
                        domain=domain,
                        message="provider=microsoft but no SPF (verified with DNS)",
                        details={"provider": "microsoft", "dss": dss},
                    )
                )
    return findings


# ── Orchestrator ─────────────────────────────────────────────────────

_ALL_CHECKS = [
    # Provider checks (only need providers kwarg)
    check_low_confidence,
    check_contradictory_signals,
    check_unknown_classification,
    check_mx_provider_mismatch,
    check_missing_spf,
    # Security checks (only need security kwarg)
    check_invalid_scans,
    check_logical_contradictions,
    check_dane_inconsistency,
    check_scan_spf_mismatch,
    # Cross-pipeline checks (need both)
    check_domain_mismatch,
    check_mx_divergence,
    check_missing_municipalities,
    check_expected_security_missing,
]


def run_all_checks(
    providers_dir: Path = Path("output/providers"),
    security_dir: Path = Path("output/security"),
    country: str | None = None,
) -> list[Finding]:
    """Run all checks and return sorted findings."""
    providers, security = load_all_data(providers_dir, security_dir, country)

    findings: list[Finding] = []
    for check_fn in _ALL_CHECKS:
        findings.extend(check_fn(providers=providers, security=security))

    findings.sort(
        key=lambda f: (
            SEVERITY_ORDER.get(f.severity, 9),
            list(CATEGORIES).index(f.category) if f.category in CATEGORIES else 99,
            COUNTRY_ORDER.get(f.country, 9),
            f.code,
        )
    )
    return findings


# ── DNS verification ─────────────────────────────────────────────────


def verify_dns(findings: list[Finding], sample_size: int = 10) -> None:
    """Spot-check a sample of ERROR findings with live DNS queries."""
    errors = [f for f in findings if f.severity == "ERROR" and f.domain]
    sample = errors[:sample_size]

    if not sample:
        console.print("  [dim]No ERROR findings with domains to verify.[/dim]")
        return

    console.rule("[bold]DNS Verification[/bold]")
    table = Table(show_edge=False, pad_edge=False)
    table.add_column("Domain", style="bold")
    table.add_column("Category")
    table.add_column("Stored MX")
    table.add_column("Current MX")
    table.add_column("Match")

    for f in sample:
        stored_mx = ", ".join(f.details.get("mx", f.details.get("provider_mx", [])))
        try:
            result = subprocess.run(
                ["dig", "+short", "MX", f.domain],
                capture_output=True,
                text=True,
                timeout=5,
            )
            lines = result.stdout.strip().splitlines()
            current_mx = ", ".join(
                line.split(None, 1)[1].rstrip(".") if " " in line else line.rstrip(".")
                for line in lines
                if line.strip()
            )
        except (subprocess.TimeoutExpired, FileNotFoundError):
            current_mx = "[red]timeout/error[/red]"

        matches = (
            "?" if not stored_mx else ("[green]yes[/green]" if current_mx == stored_mx else "[yellow]differs[/yellow]")
        )
        label, color = CATEGORIES.get(f.category, (f.category, "white"))
        table.add_row(f.domain, f"[{color}]{label}[/{color}]", stored_mx or "-", current_mx or "-", matches)

    console.print(table)
    console.print()


# ── Report ───────────────────────────────────────────────────────────

_SEVERITY_STYLE = {"ERROR": "red", "WARNING": "yellow", "INFO": "cyan"}


def print_report(findings: list[Finding]) -> None:
    """Print a structured Rich report of all findings."""
    if not findings:
        console.print("\n  [green]No findings.[/green]\n")
        return

    # Summary table
    console.rule("[bold]Summary[/bold]")
    cat_counts: dict[str, Counter[str]] = defaultdict(Counter)
    for f in findings:
        cat_counts[f.category][f.severity] += 1

    summary = Table(show_edge=False, pad_edge=False)
    summary.add_column("Category", style="bold")
    summary.add_column("Total", justify="right")
    summary.add_column("Error", justify="right", style="red")
    summary.add_column("Warning", justify="right", style="yellow")
    summary.add_column("Info", justify="right", style="cyan")

    for cat_key in CATEGORIES:
        counts = cat_counts.get(cat_key)
        if not counts:
            continue
        label, _ = CATEGORIES[cat_key]
        total = sum(counts.values())
        summary.add_row(
            label,
            str(total),
            str(counts.get("ERROR", 0)) or "-",
            str(counts.get("WARNING", 0)) or "-",
            str(counts.get("INFO", 0)) or "-",
        )

    console.print(summary)
    console.print()

    # Per-category detail tables
    for cat_key in CATEGORIES:
        cat_findings = [f for f in findings if f.category == cat_key]
        if not cat_findings:
            continue

        label, color = CATEGORIES[cat_key]
        console.rule(f"[bold {color}]{label}[/bold {color}] ({len(cat_findings)})")

        table = Table(show_edge=False, pad_edge=False, row_styles=["", "dim"])
        table.add_column("Sev", width=4)
        table.add_column("CC", width=3)
        table.add_column("Code", width=8)
        table.add_column("Name", max_width=28)
        table.add_column("Domain", max_width=30)
        table.add_column("Message")

        limit = 50
        for f in cat_findings[:limit]:
            sev_style = _SEVERITY_STYLE.get(f.severity, "white")
            table.add_row(
                f"[{sev_style}]{f.severity[:3]}[/{sev_style}]",
                f.country,
                f.code,
                f.name,
                f.domain,
                f.message,
            )
        console.print(table)
        if len(cat_findings) > limit:
            console.print(f"  [dim]... and {len(cat_findings) - limit} more[/dim]")
        console.print()

    # Bottom summary
    total = len(findings)
    errors = sum(1 for f in findings if f.severity == "ERROR")
    warnings = sum(1 for f in findings if f.severity == "WARNING")
    infos = sum(1 for f in findings if f.severity == "INFO")
    console.rule("[bold]Totals[/bold]")
    console.print(
        f"  {total} findings: "
        f"[red]{errors} errors[/red], "
        f"[yellow]{warnings} warnings[/yellow], "
        f"[cyan]{infos} info[/cyan]"
    )
    console.print()


# ── Entry point ──────────────────────────────────────────────────────


def main(
    country: str | None = None,
    providers_dir: Path = Path("output/providers"),
    security_dir: Path = Path("output/security"),
    verify: bool = False,
) -> None:
    """Run all outlier checks and print report."""
    console.rule("[bold]Outlier Investigation[/bold]")
    scope = country.upper() if country else "all countries"
    console.print(f"  Scope: {scope}\n")

    findings = run_all_checks(providers_dir, security_dir, country)
    print_report(findings)

    if verify:
        verify_dns(findings)


if __name__ == "__main__":  # pragma: no cover
    main()
