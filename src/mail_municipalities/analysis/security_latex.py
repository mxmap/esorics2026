"""Export security analysis tables as LNCS-formatted LaTeX fragments."""

from __future__ import annotations

import json
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .helpers import (
    COUNTRY_NAMES,
    divider,
    esc,
    make_region_lookup,
    num,
    pct,
)

# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------


def load_security_data(path: Path) -> dict[str, Any]:
    """Load security JSON and return the full dict."""
    if not path.exists():
        print(f"Error: {path} not found. Run the scan first.", file=sys.stderr)
        sys.exit(1)
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _infer_country(path: Path) -> str:
    """Infer country code from filename like ``security_ch.json``."""
    match = re.search(r"security_(\w{2})", path.stem)
    return match.group(1) if match else "ch"


def _region_abbr(region: str, region_lookup: dict[str, str]) -> str:
    return region_lookup.get(region, region[:4] if region else "??")


# ---------------------------------------------------------------------------
# Metric extraction helpers
# ---------------------------------------------------------------------------


def _scan_valid(munis: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return only municipalities with valid scan results."""
    return [m for m in munis if m.get("scan_valid", False)]


def _has(m: dict[str, Any], field: str) -> bool:
    """Check a boolean field in the dss or dane sub-dict."""
    if field.startswith("dane_"):
        dane = m.get("dane") or {}
        return bool(dane.get(field.removeprefix("dane_"), False))
    dss = m.get("dss") or {}
    return bool(dss.get(field, False))


# ---------------------------------------------------------------------------
# 1. Overall Security Summary
# ---------------------------------------------------------------------------

_METRICS = [
    ("has_spf", "SPF"),
    ("has_good_spf", "SPF (well-configured)"),
    ("has_dmarc", "DMARC"),
    ("has_good_dmarc", "DMARC (well-configured)"),
    ("dane_supported", "DANE (full)"),
    ("dane_partial", "DANE (partial)"),
]
# DKIM intentionally excluded (see feedback_no_dkim.md)


def latex_security_summary(
    munis: list[dict[str, Any]],
    country_code: str,
) -> str:
    valid = _scan_valid(munis)
    total = len(valid)
    country = COUNTRY_NAMES.get(country_code, country_code.upper())

    rows: list[str] = []
    for field, label in _METRICS:
        cnt = sum(1 for m in valid if _has(m, field))
        rows.append(f"        {esc(label)} & {num(cnt)} & {pct(cnt, total)}\\% \\\\")

    return (
        f"\\begin{{table}}[t]\n"
        f"    \\centering\n"
        f"    \\caption{{Email security adoption for {country} "
        f"($n={num(total)}$ municipalities).}}\n"
        f"    \\label{{tab:security-{country_code}}}\n"
        f"    \\small\n"
        f"    \\begin{{tabular}}{{lrr}}\n"
        f"        \\toprule\n"
        f"        \\textbf{{Metric}} & \\textbf{{Count}} & \\textbf{{\\%}} \\\\\n"
        f"        \\midrule\n" + "\n".join(rows) + "\n"
        "        \\bottomrule\n"
        "    \\end{tabular}\n"
        "\\end{table}\n"
    )


# ---------------------------------------------------------------------------
# 2. Regional Security Breakdown
# ---------------------------------------------------------------------------

_REGIONAL_METRICS = [
    ("has_spf", "SPF\\%"),
    ("has_good_spf", "gSPF\\%"),
    ("has_dmarc", "DMARC\\%"),
    ("has_good_dmarc", "gDMARC\\%"),
    ("dane_supported", "DANE\\%"),
]


def latex_security_regional(
    munis: list[dict[str, Any]],
    region_lookup: dict[str, str],
    country_code: str,
) -> str:
    country = COUNTRY_NAMES.get(country_code, country_code.upper())
    valid = _scan_valid(munis)

    by_region: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for m in valid:
        by_region[_region_abbr(m.get("region", ""), region_lookup)].append(m)

    # Build row data: (abbr, total, {field: count}, dmarc_pct)
    rows_data: list[tuple[str, int, dict[str, int], float]] = []
    for abbr, entries in by_region.items():
        total = len(entries)
        counts = {field: sum(1 for m in entries if _has(m, field)) for field, _ in _REGIONAL_METRICS}
        dmarc_pct = counts["has_dmarc"] / total * 100 if total else 0
        rows_data.append((abbr, total, counts, dmarc_pct))

    rows_data.sort(key=lambda r: r[3], reverse=True)

    rows: list[str] = []
    for abbr, total, counts, _ in rows_data:
        pcts = " & ".join(f"{counts[f] / total * 100:.1f}\\%" for f, _ in _REGIONAL_METRICS)
        rows.append(f"        {esc(abbr)} & {num(total)} & {pcts} \\\\")

    header_cols = " & ".join(f"\\textbf{{{label}}}" for _, label in _REGIONAL_METRICS)

    return (
        f"\\begin{{table}}[t]\n"
        f"    \\centering\n"
        f"    \\caption{{Regional email security breakdown for {country},"
        f" sorted by DMARC adoption.}}\n"
        f"    \\label{{tab:security-regional-{country_code}}}\n"
        f"    \\small\n"
        f"    \\renewcommand{{\\arraystretch}}{{0.85}}\n"
        f"    \\begin{{tabularx}}{{\\textwidth}}{{Xr{('r' * len(_REGIONAL_METRICS))}}}\n"
        f"        \\toprule\n"
        f"        \\textbf{{Region}} & \\textbf{{$n$}} & {header_cols} \\\\\n"
        f"        \\midrule\n" + "\n".join(rows) + "\n"
        "        \\bottomrule\n"
        "    \\end{tabularx}\n"
        "\\end{table}\n"
    )


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def export_security_latex(
    data: dict[str, Any],
    country_code: str,
    output_path: Path,
) -> Path:
    """Generate all security LaTeX tables and write to *output_path*."""
    generated = data.get("generated", "unknown")
    commit = data.get("commit", "unknown")
    country = COUNTRY_NAMES.get(country_code, country_code.upper())
    munis = data["municipalities"]

    region_lookup = make_region_lookup(country_code)

    header = (
        f"% Auto-generated security LaTeX tables for {country}\n"
        f"% Generated: {generated}\n"
        f"% Commit: {commit}\n"
        f"% Export date: {datetime.now(tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}\n"
        f"% Total municipalities: {data.get('total', len(munis))}\n"
        f"%\n"
        f"% This file is a fragment — include it with \\input{{{output_path.stem}}}\n"
        f"% Required packages: booktabs, tabularx\n"
    )

    sections = [
        (divider("1. Overall Security Summary"), latex_security_summary(munis, country_code)),
        (
            divider("2. Regional Security Breakdown"),
            latex_security_regional(munis, region_lookup, country_code),
        ),
    ]

    content = header + "\n".join(d + table for d, table in sections)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(content, encoding="utf-8")
    return output_path


def main(data_path: Path | None = None, *, latex: bool = False) -> None:
    """Print security summary and optionally export LaTeX."""
    path = data_path or Path("output/security/security_ch.json")
    cc = _infer_country(path)
    data = load_security_data(path)
    munis = data["municipalities"]
    valid = _scan_valid(munis)
    total = len(valid)
    country = COUNTRY_NAMES.get(cc, cc.upper())

    print(f"\n  Security summary for {country} ({num(total)} scanned municipalities):\n")
    for field, label in _METRICS:
        cnt = sum(1 for m in valid if _has(m, field))
        print(f"  {label:<28} {cnt:>6,}  {cnt / total * 100:5.1f}%")

    if latex:
        timestamp = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
        output_dir = path.parent
        tex_path = output_dir / f"tables_security_{cc}_{timestamp}.tex"
        result = export_security_latex(data, cc, tex_path)
        print(f"\n  LaTeX tables written to: {result}")

    print()
