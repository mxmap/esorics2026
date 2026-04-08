"""Export analysis tables as LNCS-formatted LaTeX fragments."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .analyze import (
    _PROVIDERS_ORDERED,
    _category,
    _region_abbr,
)

# ---------------------------------------------------------------------------
# LaTeX helpers
# ---------------------------------------------------------------------------

_COUNTRY_NAMES = {"ch": "Switzerland", "de": "Germany", "at": "Austria"}


def _num(n: int) -> str:
    """Format integer with LaTeX thousands separator."""
    if n < 1_000:
        return str(n)
    s = f"{n:,}"
    return s.replace(",", "{,}")


def _pct(n: int, total: int) -> str:
    if total == 0:
        return "0.0"
    return f"{n / total * 100:.1f}"


def _esc(text: str) -> str:
    """Escape special LaTeX characters in text."""
    for ch, repl in [("&", "\\&"), ("%", "\\%"), ("_", "\\_"), ("#", "\\#")]:
        text = text.replace(ch, repl)
    return text


def _divider(title: str) -> str:
    line = "%" + " " + "-" * 70
    return f"\n{line}\n% {title}\n{line}\n"


# ---------------------------------------------------------------------------
# 1. Overall Summary
# ---------------------------------------------------------------------------


def latex_overall_summary(
    munis: dict[str, Any],
    category_map: dict[str, str],
    country_code: str,
) -> str:
    total = len(munis)
    country = _COUNTRY_NAMES.get(country_code, country_code.upper())

    cat_counts: Counter[str] = Counter()
    for m in munis.values():
        cat_counts[_category(m["provider"], category_map)] += 1

    prov_counts: Counter[str] = Counter()
    for m in munis.values():
        prov_counts[m["provider"]] += 1

    rows: list[str] = []
    for prov in _PROVIDERS_ORDERED:
        cnt = prov_counts.get(prov, 0)
        cat = _category(prov, category_map)
        cat_label = "US Cloud" if cat == "us-cloud" else cat.replace("-", " ").title()
        rows.append(f"        {_esc(prov.title())} & {cat_label} & {_num(cnt)} & {_pct(cnt, total)}\\% \\\\")

    us = cat_counts.get("us-cloud", 0)
    domestic_key = f"{country_code}-based"
    dom = cat_counts.get(domestic_key, 0)

    return (
        f"\\begin{{table}}[t]\n"
        f"    \\centering\n"
        f"    \\caption{{Provider distribution for {country} "
        f"($n={_num(total)}$ municipalities).}}\n"
        f"    \\label{{tab:overall-{country_code}}}\n"
        f"    \\small\n"
        f"    \\begin{{tabular}}{{llrr}}\n"
        f"        \\toprule\n"
        f"        \\textbf{{Provider}} & \\textbf{{Category}} & \\textbf{{Count}} & \\textbf{{\\%}} \\\\\n"
        f"        \\midrule\n" + "\n".join(rows) + "\n"
        f"        \\midrule\n"
        f"        US Cloud (total) & & {_num(us)} & {_pct(us, total)}\\% \\\\\n"
        f"        Domestic (total) & & {_num(dom)} & {_pct(dom, total)}\\% \\\\\n"
        f"        \\bottomrule\n"
        f"    \\end{{tabular}}\n"
        f"\\end{{table}}\n"
    )


# ---------------------------------------------------------------------------
# 2. Regional Breakdown
# ---------------------------------------------------------------------------


def latex_regional(
    munis: dict[str, Any],
    category_map: dict[str, str],
    region_lookup: dict[str, str],
    country_code: str,
) -> str:
    country = _COUNTRY_NAMES.get(country_code, country_code.upper())

    by_region: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for m in munis.values():
        by_region[_region_abbr(m.get("region", ""), region_lookup)].append(m)

    rows_data: list[tuple[str, int, Counter[str], float, float]] = []
    for abbr, entries in by_region.items():
        total = len(entries)
        pc: Counter[str] = Counter(e["provider"] for e in entries)
        us = sum(pc.get(p, 0) for p in _PROVIDERS_ORDERED if _category(p, category_map) == "us-cloud")
        us_pct = us / total * 100 if total else 0
        dom_pct = 100 - us_pct
        rows_data.append((abbr, total, pc, us_pct, dom_pct))

    rows_data.sort(key=lambda r: r[3], reverse=True)

    rows: list[str] = []
    for abbr, total, pc, us_pct, dom_pct in rows_data:
        rows.append(
            f"        {_esc(abbr)} & {_num(total)}"
            f" & {pc.get('microsoft', 0)}"
            f" & {pc.get('google', 0)}"
            f" & {pc.get('aws', 0)}"
            f" & {pc.get('domestic', 0)}"
            f" & {pc.get('foreign', 0)}"
            f" & {pc.get('unknown', 0)}"
            f" & {us_pct:.1f}\\%"
            f" & {dom_pct:.1f}\\% \\\\"
        )

    return (
        f"\\begin{{table}}[t]\n"
        f"    \\centering\n"
        f"    \\caption{{Regional provider breakdown for {country}, sorted by US Cloud share.}}\n"
        f"    \\label{{tab:regional-{country_code}}}\n"
        f"    \\small\n"
        f"    \\renewcommand{{\\arraystretch}}{{0.85}}\n"
        f"    \\begin{{tabularx}}{{\\textwidth}}{{Xrrrrrrrrr}}\n"
        f"        \\toprule\n"
        f"        \\textbf{{Region}} & \\textbf{{Total}} & \\textbf{{MSFT}} & \\textbf{{Goog}}"
        f" & \\textbf{{AWS}} & \\textbf{{Dom}} & \\textbf{{Frgn}} & \\textbf{{Unkn}}"
        f" & \\textbf{{US\\%}} & \\textbf{{Dom\\%}} \\\\\n"
        f"        \\midrule\n" + "\n".join(rows) + "\n"
        "        \\bottomrule\n"
        "    \\end{tabularx}\n"
        "\\end{table}\n"
    )


# ---------------------------------------------------------------------------
# 3. Confidence Distribution
# ---------------------------------------------------------------------------


def latex_confidence(munis: dict[str, Any], country_code: str) -> str:
    country = _COUNTRY_NAMES.get(country_code, country_code.upper())

    confidences = [m["classification_confidence"] for m in munis.values()]
    total = len(confidences)

    buckets = [(90, 100), (80, 90), (70, 80), (60, 70), (50, 60), (0, 50)]
    rows: list[str] = []
    for lo, hi in buckets:
        cnt = sum(1 for c in confidences if lo <= c <= (hi if hi == 100 else hi - 0.01))
        pct = _pct(cnt, total)
        label = f"{lo}--{hi}\\%"
        rows.append(f"        {label} & {_num(cnt)} & {pct}\\% \\\\")

    avg = sum(confidences) / total if total else 0

    # Per-provider stats
    by_prov: dict[str, list[float]] = defaultdict(list)
    for m in munis.values():
        by_prov[m["provider"]].append(m["classification_confidence"])

    prov_rows: list[str] = []
    for prov in _PROVIDERS_ORDERED:
        confs = by_prov.get(prov, [])
        if not confs:
            continue
        avg_p = sum(confs) / len(confs)
        min_p = min(confs)
        low = sum(1 for c in confs if c < 60)
        prov_rows.append(f"        {_esc(prov.title())} & {avg_p:.1f}\\% & {min_p:.1f}\\% & {low} \\\\")

    return (
        f"\\begin{{table}}[t]\n"
        f"    \\centering\n"
        f"    \\caption{{Classification confidence distribution for {country}"
        f" (average: {avg:.1f}\\%).}}\n"
        f"    \\label{{tab:confidence-{country_code}}}\n"
        f"    \\small\n"
        f"    \\begin{{tabular}}{{lrr}}\n"
        f"        \\toprule\n"
        f"        \\textbf{{Range}} & \\textbf{{Count}} & \\textbf{{\\%}} \\\\\n"
        f"        \\midrule\n" + "\n".join(rows) + "\n"
        "        \\bottomrule\n"
        "    \\end{tabular}\n"
        "\n"
        "    \\vspace{0.4cm}\n"
        "\n"
        "    \\begin{tabular}{lrrr}\n"
        "        \\toprule\n"
        "        \\textbf{Provider} & \\textbf{Avg} & \\textbf{Min} & \\textbf{$<$60\\%} \\\\\n"
        "        \\midrule\n" + "\n".join(prov_rows) + "\n"
        "        \\bottomrule\n"
        "    \\end{tabular}\n"
        "\\end{table}\n"
    )


# ---------------------------------------------------------------------------
# 4. Signal Analysis
# ---------------------------------------------------------------------------


def latex_signals(munis: dict[str, Any], country_code: str) -> str:
    country = _COUNTRY_NAMES.get(country_code, country_code.upper())
    total = len(munis)

    signal_counts: Counter[str] = Counter()
    combo_counts: Counter[str] = Counter()

    for m in munis.values():
        signals = m.get("classification_signals", [])
        kinds = sorted({s["kind"] for s in signals})
        for k in kinds:
            signal_counts[k] += 1
        if kinds:
            combo_counts["+".join(kinds)] += 1

    sig_rows: list[str] = []
    for kind, cnt in signal_counts.most_common():
        sig_rows.append(f"        {_esc(kind)} & {_num(cnt)} & {_pct(cnt, total)}\\% \\\\")

    combo_rows: list[str] = []
    for i, (combo, cnt) in enumerate(combo_counts.most_common(15), 1):
        combo_rows.append(f"        {i} & {_esc(combo)} & {_num(cnt)} \\\\")

    return (
        f"\\begin{{table}}[t]\n"
        f"    \\centering\n"
        f"    \\caption{{Signal coverage for {country} ($n={_num(total)}$).}}\n"
        f"    \\label{{tab:signals-{country_code}}}\n"
        f"    \\small\n"
        f"    \\begin{{tabular}}{{lrr}}\n"
        f"        \\toprule\n"
        f"        \\textbf{{Signal}} & \\textbf{{Count}} & \\textbf{{\\%}} \\\\\n"
        f"        \\midrule\n" + "\n".join(sig_rows) + "\n"
        f"        \\bottomrule\n"
        f"    \\end{{tabular}}\n"
        f"\\end{{table}}\n"
        f"\n"
        f"\\begin{{table}}[t]\n"
        f"    \\centering\n"
        f"    \\caption{{Top~15 signal combinations for {country}.}}\n"
        f"    \\label{{tab:signal-combos-{country_code}}}\n"
        f"    \\small\n"
        f"    \\begin{{tabular}}{{rlr}}\n"
        f"        \\toprule\n"
        f"        \\textbf{{\\#}} & \\textbf{{Combination}} & \\textbf{{Count}} \\\\\n"
        f"        \\midrule\n" + "\n".join(combo_rows) + "\n"
        "        \\bottomrule\n"
        "    \\end{tabular}\n"
        "\\end{table}\n"
    )


# ---------------------------------------------------------------------------
# 5. Gateway Report
# ---------------------------------------------------------------------------


def latex_gateways(munis: dict[str, Any], country_code: str) -> str:
    country = _COUNTRY_NAMES.get(country_code, country_code.upper())
    total = len(munis)

    with_gw = {c: m for c, m in munis.items() if m.get("gateway")}
    without_gw = {c: m for c, m in munis.items() if not m.get("gateway")}

    gw_counts: Counter[str] = Counter(m["gateway"] for m in with_gw.values())

    gw_rows: list[str] = []
    for gw, cnt in gw_counts.most_common():
        gw_rows.append(f"        {_esc(gw)} & {_num(cnt)} & {_pct(cnt, total)}\\% \\\\")

    prov_rows: list[str] = []
    for prov in _PROVIDERS_ORDERED:
        cnt_w = sum(1 for m in with_gw.values() if m["provider"] == prov)
        cnt_wo = sum(1 for m in without_gw.values() if m["provider"] == prov)
        prov_rows.append(
            f"        {_esc(prov.title())} & {_num(cnt_w)} & {_pct(cnt_w, len(with_gw))}\\%"
            f" & {_num(cnt_wo)} & {_pct(cnt_wo, len(without_gw))}\\% \\\\"
        )

    return (
        f"\\begin{{table}}[t]\n"
        f"    \\centering\n"
        f"    \\caption{{Security gateway usage for {country}"
        f" ({_num(len(with_gw))}/{_num(total)} municipalities, {_pct(len(with_gw), total)}\\%).}}\n"
        f"    \\label{{tab:gateways-{country_code}}}\n"
        f"    \\small\n"
        f"    \\begin{{tabular}}{{lrr}}\n"
        f"        \\toprule\n"
        f"        \\textbf{{Gateway}} & \\textbf{{Count}} & \\textbf{{\\%}} \\\\\n"
        f"        \\midrule\n" + "\n".join(gw_rows) + "\n"
        "        \\bottomrule\n"
        "    \\end{tabular}\n"
        "\n"
        "    \\vspace{0.4cm}\n"
        "\n"
        "    \\begin{tabular}{lrrrr}\n"
        "        \\toprule\n"
        "        \\textbf{Provider} & \\textbf{With GW} & \\textbf{\\%}"
        " & \\textbf{No GW} & \\textbf{\\%} \\\\\n"
        "        \\midrule\n" + "\n".join(prov_rows) + "\n"
        "        \\bottomrule\n"
        "    \\end{tabular}\n"
        "\\end{table}\n"
    )


# ---------------------------------------------------------------------------
# 6. Domain Sharing
# ---------------------------------------------------------------------------


def latex_domain_sharing(munis: dict[str, Any], country_code: str) -> str:
    country = _COUNTRY_NAMES.get(country_code, country_code.upper())

    by_domain: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for m in munis.values():
        if m.get("domain"):
            by_domain[m["domain"]].append(m)

    shared = {d: ms for d, ms in by_domain.items() if len(ms) > 1}
    shared_sorted = sorted(shared.items(), key=lambda x: len(x[1]), reverse=True)

    rows: list[str] = []
    for domain, ms in shared_sorted[:20]:
        rows.append(f"        \\texttt{{{_esc(domain)}}} & {len(ms)} & {_esc(ms[0]['provider'].title())} \\\\")

    return (
        f"\\begin{{table}}[t]\n"
        f"    \\centering\n"
        f"    \\caption{{Shared email domains in {country}"
        f" ({_num(len(shared))} domains used by multiple municipalities).}}\n"
        f"    \\label{{tab:shared-domains-{country_code}}}\n"
        f"    \\small\n"
        f"    \\begin{{tabularx}}{{\\textwidth}}{{Xrc}}\n"
        f"        \\toprule\n"
        f"        \\textbf{{Domain}} & \\textbf{{Count}} & \\textbf{{Provider}} \\\\\n"
        f"        \\midrule\n" + "\n".join(rows) + "\n"
        "        \\bottomrule\n"
        "    \\end{tabularx}\n"
        "\\end{table}\n"
    )


# ---------------------------------------------------------------------------
# 7. Low-Confidence / Review Candidates
# ---------------------------------------------------------------------------


def latex_low_confidence(
    munis: dict[str, Any],
    region_lookup: dict[str, str],
    country_code: str,
) -> str:
    country = _COUNTRY_NAMES.get(country_code, country_code.upper())

    low = [m for m in munis.values() if m["classification_confidence"] < 60]
    low.sort(key=lambda m: m["classification_confidence"])

    rows: list[str] = []
    for m in low[:30]:
        signals = "+".join(sorted({s["kind"] for s in m.get("classification_signals", [])}))
        region = _region_abbr(m.get("region", ""), region_lookup)
        rows.append(
            f"        {m['code']} & {_esc(m['name'])} & {region}"
            f" & {_esc(m['provider'].title())} & {m['classification_confidence']:.0f}\\%"
            f" & {_esc(signals)} \\\\"
        )

    return (
        f"\\begin{{table}}[t]\n"
        f"    \\centering\n"
        f"    \\caption{{Low-confidence classifications for {country}"
        f" ({_num(len(low))} municipalities below 60\\%).}}\n"
        f"    \\label{{tab:low-confidence-{country_code}}}\n"
        f"    \\small\n"
        f"    \\renewcommand{{\\arraystretch}}{{0.85}}\n"
        f"    \\begin{{tabularx}}{{\\textwidth}}{{lXlcrX}}\n"
        f"        \\toprule\n"
        f"        \\textbf{{Code}} & \\textbf{{Name}} & \\textbf{{Region}}"
        f" & \\textbf{{Provider}} & \\textbf{{Conf.}} & \\textbf{{Signals}} \\\\\n"
        f"        \\midrule\n" + "\n".join(rows) + "\n"
        "        \\bottomrule\n"
        "    \\end{tabularx}\n"
        "\\end{table}\n"
    )


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def export_latex(
    data: dict[str, Any],
    munis: dict[str, Any],
    category_map: dict[str, str],
    region_lookup: dict[str, str],
    country_code: str,
    output_path: Path,
) -> Path:
    """Generate all LaTeX tables and write to *output_path*."""
    generated = data.get("generated", "unknown")
    commit = data.get("commit", "unknown")
    country = _COUNTRY_NAMES.get(country_code, country_code.upper())

    header = (
        f"% Auto-generated LaTeX tables for {country}\n"
        f"% Generated: {generated}\n"
        f"% Commit: {commit}\n"
        f"% Export date: {datetime.now(tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}\n"
        f"% Total municipalities: {len(munis)}\n"
        f"%\n"
        f"% This file is a fragment — include it with \\input{{{output_path.stem}}}\n"
        f"% Required packages: booktabs, tabularx, xcolor[table], threeparttable\n"
    )

    sections = [
        (_divider("1. Overall Summary"), latex_overall_summary(munis, category_map, country_code)),
        (_divider("2. Regional Breakdown"), latex_regional(munis, category_map, region_lookup, country_code)),
        (_divider("3. Confidence Distribution"), latex_confidence(munis, country_code)),
        (_divider("4. Signal Analysis"), latex_signals(munis, country_code)),
        (_divider("5. Gateway Report"), latex_gateways(munis, country_code)),
        (_divider("6. Domain Sharing"), latex_domain_sharing(munis, country_code)),
        (_divider("7. Low-Confidence / Review Candidates"), latex_low_confidence(munis, region_lookup, country_code)),
    ]

    content = header + "\n".join(divider + table for divider, table in sections)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(content, encoding="utf-8")
    return output_path
