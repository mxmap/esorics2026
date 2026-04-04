"""Statistical analysis of municipality email classification data."""

from __future__ import annotations

import json
import os
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from .constants import REGION_ABBREVIATIONS
from .runner import _build_category_map

# ---------------------------------------------------------------------------
# ANSI color helpers (respect NO_COLOR convention and pipe detection)
# ---------------------------------------------------------------------------

_NO_COLOR = os.environ.get("NO_COLOR") is not None or not os.isatty(sys.stdout.fileno())


def _c(code: str, text: str) -> str:
    if _NO_COLOR:
        return str(text)
    return f"\033[{code}m{text}\033[0m"


def _bold(t: str) -> str:
    return _c("1", t)


def _dim(t: str) -> str:
    return _c("2", t)


def _red(t: str) -> str:
    return _c("31", t)


def _green(t: str) -> str:
    return _c("32", t)


def _yellow(t: str) -> str:
    return _c("33", t)


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

_BAR_FULL = "\u2588"
_BAR_EMPTY = "\u2591"


def _bar(value: float, max_value: float, width: int = 25) -> str:
    if max_value == 0:
        return _BAR_EMPTY * width
    filled = int(round(value / max_value * width))
    return _BAR_FULL * filled + _BAR_EMPTY * (width - filled)


def _pct(n: int, total: int) -> str:
    if total == 0:
        return "  0.0%"
    return f"{n / total * 100:5.1f}%"


def _header(title: str) -> None:
    line = "\u2550" * 66
    print(f"\n{_bold(line)}")
    print(f"  {_bold(title)}")
    print(_bold(line))


def _sep() -> None:
    print("  " + "\u2500" * 62)


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------


def load_data(path: Path) -> dict[str, Any]:
    """Load providers JSON and return the full dict."""
    if not path.exists():
        print(f"Error: {path} not found. Run the pipeline first.", file=sys.stderr)
        sys.exit(1)
    with open(path, encoding="utf-8") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_PROVIDERS_ORDERED = ["microsoft", "google", "aws", "domestic", "foreign", "unknown"]

_PRIMARY_SIGNAL_KINDS = {"mx", "spf", "dkim", "autodiscover"}


def _infer_country(path: Path) -> str:
    """Infer country code from filename like ``providers_ch.json``."""
    match = re.search(r"providers_(\w{2})", path.stem)
    return match.group(1) if match else "ch"


def _make_region_lookup(country_code: str) -> dict[str, str]:
    abbrevs = REGION_ABBREVIATIONS.get(country_code, {})
    return {k: v.upper() for k, v in abbrevs.items()}


def _region_abbr(region: str, region_lookup: dict[str, str]) -> str:
    return region_lookup.get(region, region[:4] if region else "??")


def _category(provider: str, category_map: dict[str, str]) -> str:
    return category_map.get(provider, "unknown")


# ---------------------------------------------------------------------------
# 1. Overall Summary
# ---------------------------------------------------------------------------


def report_overall_summary(
    data: dict[str, Any], munis: dict[str, Any], category_map: dict[str, str], domestic_label: str
) -> None:
    _header("OVERALL SUMMARY")
    total = len(munis)
    generated = data.get("generated", "?")
    commit = data.get("commit", "?")
    print(f"  Generated: {generated}  (commit {commit})")
    print(f"  Total municipalities: {total:,}")

    # Category split
    cat_counts: Counter[str] = Counter()
    for m in munis.values():
        cat_counts[_category(m["provider"], category_map)] += 1

    print()
    print(f"  {'Category':<16} {'Count':>6}  {'%':>6}  Bar")
    _sep()
    for cat in ["us-cloud", domestic_label]:
        cnt = cat_counts[cat]
        color = _red if cat == "us-cloud" else _green
        label = "US Cloud" if cat == "us-cloud" else "Domestic"
        print(f"  {color(f'{label:<16}')} {cnt:>6,}  {_pct(cnt, total)}  {color(_bar(cnt, total))}")

    # Provider distribution
    prov_counts: Counter[str] = Counter()
    for m in munis.values():
        prov_counts[m["provider"]] += 1

    print()
    print(f"  {'Provider':<16} {'Count':>6}  {'%':>6}  Bar")
    _sep()
    for prov in _PROVIDERS_ORDERED:
        cnt = prov_counts.get(prov, 0)
        color = _red if _category(prov, category_map) == "us-cloud" else _green
        print(f"  {color(f'{prov:<16}')} {cnt:>6,}  {_pct(cnt, total)}  {color(_bar(cnt, max(prov_counts.values())))}")


# ---------------------------------------------------------------------------
# 2. Regional Breakdown
# ---------------------------------------------------------------------------


def report_regional(munis: dict[str, Any], category_map: dict[str, str], region_lookup: dict[str, str]) -> None:
    _header("REGIONAL BREAKDOWN (sorted by US-Cloud %)")

    # Group by region
    by_region: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for m in munis.values():
        by_region[_region_abbr(m.get("region", ""), region_lookup)].append(m)

    # Build rows
    rows: list[tuple[str, int, dict[str, int], float]] = []
    for abbr, entries in by_region.items():
        total = len(entries)
        prov_counts: Counter[str] = Counter(e["provider"] for e in entries)
        us_cloud = sum(prov_counts.get(p, 0) for p in _PROVIDERS_ORDERED if _category(p, category_map) == "us-cloud")
        us_pct = us_cloud / total * 100 if total else 0
        rows.append((abbr, total, dict(prov_counts), us_pct))

    rows.sort(key=lambda r: r[3], reverse=True)

    hdr = (
        f"  {'Region':<8}{'Total':>5}{'MSFT':>6}{'Goog':>6}{'AWS':>5}"
        f"{'Dom':>5}{'Frgn':>5}{'Unkn':>5}  {'US%':>6}  {'Dom%':>6}"
    )
    print(hdr)
    _sep()

    for abbr, total, pc, us_pct in rows:
        domestic_pct = 100 - us_pct
        color = _red if us_pct >= 70 else (_yellow if us_pct >= 50 else _green)
        print(
            f"  {abbr:<8}{total:>5}"
            f"{pc.get('microsoft', 0):>6}"
            f"{pc.get('google', 0):>6}"
            f"{pc.get('aws', 0):>5}"
            f"{pc.get('domestic', 0):>5}"
            f"{pc.get('foreign', 0):>5}"
            f"{pc.get('independent', 0):>5}"
            f"  {color(f'{us_pct:5.1f}%')}"
            f"  {f'{domestic_pct:5.1f}%':>6}"
        )


# ---------------------------------------------------------------------------
# 3. Confidence Distribution
# ---------------------------------------------------------------------------


def report_confidence(munis: dict[str, Any]) -> None:
    _header("CONFIDENCE DISTRIBUTION")

    confidences = [m["classification_confidence"] for m in munis.values()]
    total = len(confidences)

    # Histogram buckets (high to low)
    buckets = [(90, 100), (80, 90), (70, 80), (60, 70), (50, 60), (0, 50)]
    bucket_counts = []
    for lo, hi in buckets:
        cnt = sum(1 for c in confidences if lo <= c <= (hi if hi == 100 else hi - 0.01))
        bucket_counts.append(cnt)

    max_cnt = max(bucket_counts) if bucket_counts else 1
    print(f"  {'Range':<12} {'Count':>6}  {'%':>6}  Bar")
    _sep()
    for (lo, hi), cnt in zip(buckets, bucket_counts):
        label = f"{lo}-{hi}%"
        print(f"  {label:<12} {cnt:>6,}  {_pct(cnt, total)}  {_bar(cnt, max_cnt)}")

    avg = sum(confidences) / total if total else 0
    print(f"\n  Average confidence: {_bold(f'{avg:.1f}%')}")

    # Per-provider stats
    by_prov: dict[str, list[float]] = defaultdict(list)
    for m in munis.values():
        by_prov[m["provider"]].append(m["classification_confidence"])

    print()
    print(f"  {'Provider':<16} {'Avg':>6}  {'Min':>6}  {'<60':>5}")
    _sep()
    for prov in _PROVIDERS_ORDERED:
        confs = by_prov.get(prov, [])
        if not confs:
            continue
        avg_p = sum(confs) / len(confs)
        min_p = min(confs)
        low = sum(1 for c in confs if c < 60)
        low_str = _red(f"{low:>5}") if low > 0 else f"{low:>5}"
        print(f"  {prov:<16} {avg_p:>5.1f}%  {min_p:>5.1f}%  {low_str}")


# ---------------------------------------------------------------------------
# 4. Signal Analysis
# ---------------------------------------------------------------------------


def report_signals(munis: dict[str, Any]) -> None:
    _header("SIGNAL ANALYSIS")

    total = len(munis)

    # Signal coverage
    signal_counts: Counter[str] = Counter()
    combo_counts: Counter[str] = Counter()
    single_signal: list[dict[str, Any]] = []
    zero_signal: list[dict[str, Any]] = []

    for m in munis.values():
        signals = m.get("classification_signals", [])
        kinds = sorted({s["kind"] for s in signals})
        for k in kinds:
            signal_counts[k] += 1
        if kinds:
            combo_counts["+".join(kinds)] += 1
        if len(kinds) == 1:
            single_signal.append(m)
        elif len(kinds) == 0:
            zero_signal.append(m)

    print("  Signal coverage (% of municipalities with each signal):\n")
    print(f"  {'Signal':<20} {'Count':>6}  {'%':>6}")
    _sep()
    for kind, cnt in signal_counts.most_common():
        print(f"  {kind:<20} {cnt:>6,}  {_pct(cnt, total)}")

    print("\n  Top 15 signal combinations:\n")
    print(f"  {'#':<4} {'Combination':<50} {'Count':>6}")
    _sep()
    for i, (combo, cnt) in enumerate(combo_counts.most_common(15), 1):
        print(f"  {i:<4} {combo:<50} {cnt:>6,}")

    print(f"\n  Single-signal municipalities: {_yellow(str(len(single_signal)))}")
    for m in single_signal[:5]:
        sig = m["classification_signals"][0]
        print(f"    {m['code']:>5}  {m['name']:<30} {sig['kind']}:{sig['provider']}")
    if len(single_signal) > 5:
        print(f"    {_dim(f'... and {len(single_signal) - 5} more')}")

    print(f"\n  Zero-signal municipalities: {_yellow(str(len(zero_signal)))}")
    for m in zero_signal[:5]:
        print(f"    {m['code']:>5}  {m['name']:<30} provider={m['provider']}")
    if len(zero_signal) > 5:
        print(f"    {_dim(f'... and {len(zero_signal) - 5} more')}")


# ---------------------------------------------------------------------------
# 5. Gateway Report
# ---------------------------------------------------------------------------


def report_gateways(munis: dict[str, Any]) -> None:
    _header("GATEWAY REPORT")

    total = len(munis)
    with_gw = {c: m for c, m in munis.items() if m.get("gateway")}
    without_gw = {c: m for c, m in munis.items() if not m.get("gateway")}

    print(f"  Municipalities with gateway: {_bold(str(len(with_gw)))} / {total} ({len(with_gw) / total * 100:.1f}%)")

    # Per-gateway counts
    gw_counts: Counter[str] = Counter(m["gateway"] for m in with_gw.values())
    print()
    print(f"  {'Gateway':<20} {'Count':>6}")
    _sep()
    for gw, cnt in gw_counts.most_common():
        print(f"  {gw:<20} {cnt:>6,}")

    # Provider distribution with/without gateway
    print("\n  Provider distribution:\n")
    print(f"  {'Provider':<16}  {'With GW':>8} {'%':>6}  {'No GW':>8} {'%':>6}")
    _sep()
    for prov in _PROVIDERS_ORDERED:
        cnt_w = sum(1 for m in with_gw.values() if m["provider"] == prov)
        cnt_wo = sum(1 for m in without_gw.values() if m["provider"] == prov)
        print(f"  {prov:<16}  {cnt_w:>8,} {_pct(cnt_w, len(with_gw))}  {cnt_wo:>8,} {_pct(cnt_wo, len(without_gw))}")


# ---------------------------------------------------------------------------
# 6. Domain Sharing
# ---------------------------------------------------------------------------


def report_domain_sharing(munis: dict[str, Any]) -> None:
    _header("SHARED DOMAINS")

    by_domain: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for m in munis.values():
        if m.get("domain"):
            by_domain[m["domain"]].append(m)

    shared = {d: ms for d, ms in by_domain.items() if len(ms) > 1}
    shared_sorted = sorted(shared.items(), key=lambda x: len(x[1]), reverse=True)

    print(f"  Domains used by multiple municipalities: {_bold(str(len(shared)))}")
    if not shared_sorted:
        return

    print()
    print(f"  {'Domain':<30} {'Count':>5}  {'Provider':<14} Municipalities")
    _sep()
    for domain, ms in shared_sorted:
        names = ", ".join(m["name"] for m in ms[:4])
        suffix = f", +{len(ms) - 4}" if len(ms) > 4 else ""
        print(f"  {domain:<30} {len(ms):>5}  {ms[0]['provider']:<14} {names}{suffix}")


# ---------------------------------------------------------------------------
# 7. Low-Confidence / Review Candidates
# ---------------------------------------------------------------------------


def report_low_confidence(munis: dict[str, Any], region_lookup: dict[str, str]) -> None:
    _header("LOW-CONFIDENCE / REVIEW CANDIDATES")

    # Low confidence
    low = [m for m in munis.values() if m["classification_confidence"] < 60]
    low.sort(key=lambda m: m["classification_confidence"])

    print(f"  Municipalities with confidence < 60%: {_red(str(len(low)))}")
    if low:
        print()
        print(f"  {'Code':>5}  {'Name':<28} {'Region':<6} {'Provider':<14} {'Conf':>5}  Signals")
        _sep()
        for m in low:
            signals = "+".join(sorted({s["kind"] for s in m.get("classification_signals", [])}))
            print(
                f"  {m['code']:>5}  {m['name']:<28} "
                f"{_region_abbr(m.get('region', ''), region_lookup):>4}  "
                f"{m['provider']:<14} "
                f"{m['classification_confidence']:>4.0f}%  {signals}"
            )

    # Conflicting primary signals
    conflicts: list[tuple[dict[str, Any], str, set[str]]] = []
    for m in munis.values():
        signals = m.get("classification_signals", [])
        winner = m["provider"]
        primary_by_other: dict[str, set[str]] = defaultdict(set)
        for s in signals:
            if s["kind"] in _PRIMARY_SIGNAL_KINDS and s["provider"] != winner:
                primary_by_other[s["provider"]].add(s["kind"])
        for other_prov, kinds in primary_by_other.items():
            conflicts.append((m, other_prov, kinds))

    print(f"\n  Conflicting primary signals (non-winner has MX/SPF/DKIM/AD): {_yellow(str(len(conflicts)))}")
    if conflicts:
        conflicts.sort(key=lambda x: len(x[2]), reverse=True)
        print()
        print(f"  {'Code':>5}  {'Name':<28} {'Winner':<14} {'Conflict':>14}  Signals")
        _sep()
        for m, other, kinds in conflicts[:20]:
            print(f"  {m['code']:>5}  {m['name']:<28} {m['provider']:<14} {other:>14}  {'+'.join(sorted(kinds))}")
        if len(conflicts) > 20:
            print(f"    {_dim(f'... and {len(conflicts) - 20} more')}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main(data_path: Path | None = None, *, country_code: str | None = None) -> None:
    path = data_path or Path("output/providers/providers_ch.json")
    cc = country_code or _infer_country(path)
    category_map = _build_category_map(cc)
    domestic_label = f"{cc}-based"
    region_lookup = _make_region_lookup(cc)

    data = load_data(path)
    munis = {m["code"]: m for m in data["municipalities"]}

    report_overall_summary(data, munis, category_map, domestic_label)
    report_regional(munis, category_map, region_lookup)
    report_confidence(munis)
    report_signals(munis)
    report_gateways(munis)
    report_domain_sharing(munis)
    report_low_confidence(munis, region_lookup)

    print()
