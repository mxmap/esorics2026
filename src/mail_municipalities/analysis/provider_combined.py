"""Combined multi-country analysis with pandas DataFrame and LaTeX export."""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from mail_municipalities.provider_classification.analyze import (
    _PROVIDERS_ORDERED,
    load_data,
)

from .helpers import (
    COUNTRIES as _COUNTRIES,
    COUNTRY_NAMES as _COUNTRY_NAMES,
    esc as _esc,
    num as _num,
    region_name as _region_name,
)


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------


def load_all_countries(
    providers_dir: Path = Path("output/providers"),
) -> dict[str, tuple[dict[str, Any], dict[str, Any]]]:
    """Load provider JSONs for all three countries.

    Returns ``{cc: (raw_data, munis_dict)}`` keyed by country code.
    """
    result: dict[str, tuple[dict[str, Any], dict[str, Any]]] = {}
    for cc in _COUNTRIES:
        path = providers_dir / f"providers_{cc}.json"
        data = load_data(path)
        munis = {m["code"]: m for m in data["municipalities"]}
        result[cc] = (data, munis)
    return result


# ---------------------------------------------------------------------------
# DataFrame construction
# ---------------------------------------------------------------------------


def build_combined_dataframe(
    all_data: dict[str, tuple[dict[str, Any], dict[str, Any]]],
) -> pd.DataFrame:
    """Build a summary DataFrame with one row per (country, region).

    Includes per-country subtotal rows and a grand-total row.
    """
    rows: list[dict[str, Any]] = []
    for cc in _COUNTRIES:
        _, munis = all_data[cc]
        for m in munis.values():
            rows.append(
                {
                    "country": cc.upper(),
                    "region": _region_name(m.get("region", "") or "??"),
                    "provider": m["provider"],
                }
            )

    raw = pd.DataFrame(rows)

    # Cross-tabulate: one row per (country, region), columns = providers
    ct = pd.crosstab(
        index=[raw["country"], raw["region"]],
        columns=raw["provider"],
        margins=False,
    )
    # Ensure all provider columns exist (fill zeros for missing)
    for prov in _PROVIDERS_ORDERED:
        if prov not in ct.columns:
            ct[prov] = 0
    ct = ct[list(_PROVIDERS_ORDERED)]  # enforce column order

    summary = ct.reset_index()
    summary.columns.name = None
    summary["total"] = summary[list(_PROVIDERS_ORDERED)].sum(axis=1)
    summary["us_cloud"] = summary["microsoft"] + summary["google"] + summary["aws"]
    summary["us_pct"] = (summary["us_cloud"] / summary["total"] * 100).round(1)
    summary["dom_pct"] = (summary["domestic"] / summary["total"] * 100).round(1)

    # Sort within each country by us_pct descending
    parts: list[pd.DataFrame] = []
    for cc in _COUNTRIES:
        cc_upper = cc.upper()
        part = summary[summary["country"] == cc_upper].sort_values("us_pct", ascending=False)  # pyright: ignore[reportCallIssue]
        parts.append(part)

        # Country subtotal row
        totals = part[list(_PROVIDERS_ORDERED)].sum().to_dict()
        total_n = int(sum(totals.values()))
        us_n = int(totals["microsoft"] + totals["google"] + totals["aws"])
        dom_n = int(totals["domestic"])
        sub = pd.DataFrame(
            [
                {
                    "country": cc_upper,
                    "region": "Total",
                    "total": total_n,
                    "us_cloud": us_n,
                    "us_pct": round(us_n / total_n * 100, 1) if total_n else 0,
                    "dom_pct": round(dom_n / total_n * 100, 1) if total_n else 0,
                    **{p: int(totals[p]) for p in _PROVIDERS_ORDERED},
                }
            ]
        )
        parts.append(sub)

    # Grand total row
    all_provs = summary[list(_PROVIDERS_ORDERED)].sum().to_dict()
    grand_total = int(sum(all_provs.values()))
    grand_us = int(all_provs["microsoft"] + all_provs["google"] + all_provs["aws"])
    grand_dom = int(all_provs["domestic"])
    grand = pd.DataFrame(
        [
            {
                "country": "ALL",
                "region": "Grand Total",
                "total": grand_total,
                "us_cloud": grand_us,
                "us_pct": round(grand_us / grand_total * 100, 1) if grand_total else 0,
                "dom_pct": round(grand_dom / grand_total * 100, 1) if grand_total else 0,
                **{p: int(all_provs[p]) for p in _PROVIDERS_ORDERED},
            }
        ]
    )
    parts.append(grand)

    result = pd.concat(parts, ignore_index=True)

    # Ensure integer types for count columns
    int_cols = ["total", "us_cloud", *list(_PROVIDERS_ORDERED)]
    for col in int_cols:
        result[col] = result[col].astype(int)

    return result


# ---------------------------------------------------------------------------
# LaTeX rendering
# ---------------------------------------------------------------------------

# Color thresholds for US% cells
_COLOR_DEFS = (
    "\\definecolor{ushigh}{RGB}{253,216,213}%   >=70% US\n"
    "\\definecolor{usmid}{RGB}{255,244,199}%    >=50% US\n"
    "\\definecolor{uslow}{RGB}{214,240,216}%    <50% US\n"
)


def _us_cell(pct: float, bold: bool = False) -> str:
    """Format a US% cell with background colour."""
    if pct >= 70:
        color = "ushigh"
    elif pct >= 50:
        color = "usmid"
    else:
        color = "uslow"
    text = f"{pct:.1f}\\%"
    if bold:
        text = f"\\textbf{{{text}}}"
    return f"\\cellcolor{{{color}}}{text}"


def _dom_cell(pct: float, bold: bool = False) -> str:
    """Format a Dom% cell with background colour (inverse of US)."""
    if pct > 50:
        color = "uslow"
    elif pct > 30:
        color = "usmid"
    else:
        color = "ushigh"
    text = f"{pct:.1f}\\%"
    if bold:
        text = f"\\textbf{{{text}}}"
    return f"\\cellcolor{{{color}}}{text}"


def latex_combined_regional(df: pd.DataFrame) -> str:
    """Render the combined DataFrame as an LNCS-formatted LaTeX table."""
    grand_row = df[df["country"] == "ALL"].iloc[0]
    grand_n = int(grand_row["total"])

    lines: list[str] = []
    lines.append("\\begin{table}[t]")
    lines.append("    \\centering")
    lines.append(f"    \\caption{{Provider distribution by region across all countries ($n={_num(grand_n)}$).}}")
    lines.append("    \\label{tab:combined-regional}")
    lines.append("    \\scriptsize")
    lines.append("    \\renewcommand{\\arraystretch}{0.82}")
    lines.append("")
    lines.append("    % Color definitions")
    for cdef in _COLOR_DEFS.strip().splitlines():
        lines.append(f"    {cdef}")
    lines.append("")
    lines.append("    \\begin{tabularx}{\\textwidth}{lXrrrrrrrrr}")
    lines.append("        \\toprule")
    lines.append(
        "        \\textbf{Country} & \\textbf{Region} & \\textbf{Total}"
        " & \\textbf{MSFT} & \\textbf{Goog} & \\textbf{AWS}"
        " & \\textbf{Dom} & \\textbf{Frgn} & \\textbf{Unkn}"
        " & \\textbf{US\\%} & \\textbf{Dom\\%} \\\\"
    )
    lines.append("        \\midrule")

    prev_country: str | None = None

    for _, row_s in df.iterrows():
        row = row_s.to_dict()
        country = str(row["country"])
        region = str(row["region"])
        is_subtotal = region == "Total"
        is_grand = country == "ALL"
        is_bold = is_subtotal or is_grand

        # Separator between country groups
        if prev_country is not None and country != prev_country:
            lines.append("        \\midrule")

        # Country label: show only on first row of group
        if country == prev_country and not is_subtotal:
            country_cell = ""
        else:
            country_cell = f"\\textbf{{{country}}}" if is_bold else country

        # Format cells
        def _fmt(val: Any, bold: bool = False) -> str:
            n = int(val)
            s = _num(n)
            return f"\\textbf{{{s}}}" if bold else s

        region_cell = f"\\textbf{{{_esc(region)}}}" if is_bold else _esc(region)
        total_cell = _fmt(row["total"], is_bold)
        msft = _fmt(row["microsoft"], is_bold)
        goog = _fmt(row["google"], is_bold)
        aws = _fmt(row["aws"], is_bold)
        dom = _fmt(row["domestic"], is_bold)
        frgn = _fmt(row["foreign"], is_bold)
        unkn = _fmt(row["unknown"], is_bold)
        us_pct_cell = _us_cell(float(row["us_pct"]), is_bold)
        dom_pct_cell = _dom_cell(float(row["dom_pct"]), is_bold)

        lines.append(
            f"        {country_cell} & {region_cell} & {total_cell}"
            f" & {msft} & {goog} & {aws}"
            f" & {dom} & {frgn} & {unkn}"
            f" & {us_pct_cell} & {dom_pct_cell} \\\\"
        )

        prev_country = country

    lines.append("        \\bottomrule")
    lines.append("    \\end{tabularx}")
    lines.append("\\end{table}")

    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Country overview table
# ---------------------------------------------------------------------------


def build_country_overview(
    all_data: dict[str, tuple[dict[str, Any], dict[str, Any]]],
) -> pd.DataFrame:
    """Build a compact country-level overview DataFrame."""
    rows: list[dict[str, Any]] = []
    for cc in _COUNTRIES:
        _, munis = all_data[cc]
        total = len(munis)
        provs = pd.Series([m["provider"] for m in munis.values()])
        pc = provs.value_counts().to_dict()
        us = int(pc.get("microsoft", 0) + pc.get("google", 0) + pc.get("aws", 0))
        dom = int(pc.get("domestic", 0))

        confs = [m["classification_confidence"] for m in munis.values()]
        avg_conf = sum(confs) / len(confs) if confs else 0
        low_conf = sum(1 for c in confs if c < 60)

        gw_count = sum(1 for m in munis.values() if m.get("gateway"))

        rows.append(
            {
                "country": _COUNTRY_NAMES[cc],
                "total": total,
                "microsoft": int(pc.get("microsoft", 0)),
                "google": int(pc.get("google", 0)),
                "aws": int(pc.get("aws", 0)),
                "domestic": dom,
                "foreign": int(pc.get("foreign", 0)),
                "unknown": int(pc.get("unknown", 0)),
                "us_pct": round(us / total * 100, 1) if total else 0,
                "dom_pct": round(dom / total * 100, 1) if total else 0,
                "avg_conf": round(avg_conf, 1),
                "low_conf": low_conf,
                "gw_pct": round(gw_count / total * 100, 1) if total else 0,
            }
        )

    # Grand total
    all_total = sum(r["total"] for r in rows)
    all_confs: list[float] = []
    all_gw = 0
    for cc in _COUNTRIES:
        _, munis = all_data[cc]
        all_confs.extend(m["classification_confidence"] for m in munis.values())
        all_gw += sum(1 for m in munis.values() if m.get("gateway"))

    gt = {p: sum(r[p] for r in rows) for p in _PROVIDERS_ORDERED}
    us_total = gt["microsoft"] + gt["google"] + gt["aws"]
    rows.append(
        {
            "country": "Total",
            "total": all_total,
            **gt,
            "us_pct": round(us_total / all_total * 100, 1) if all_total else 0,
            "dom_pct": round(gt["domestic"] / all_total * 100, 1) if all_total else 0,
            "avg_conf": round(sum(all_confs) / len(all_confs), 1) if all_confs else 0,
            "low_conf": sum(1 for c in all_confs if c < 60),
            "gw_pct": round(all_gw / all_total * 100, 1) if all_total else 0,
        }
    )

    return pd.DataFrame(rows)


def latex_country_overview(df: pd.DataFrame) -> str:
    """Render a compact country overview table."""
    grand = df[df["country"] == "Total"].iloc[0]
    grand_n = int(grand["total"])

    lines: list[str] = []
    lines.append("\\begin{table}[t]")
    lines.append("    \\centering")
    lines.append(
        f"    \\caption{{Country-level overview of email provider classification "
        f"($n={_num(grand_n)}$ municipalities).}}"
    )
    lines.append("    \\label{tab:country-overview}")
    lines.append("    \\footnotesize")
    lines.append("    \\begin{threeparttable}")
    lines.append("    \\begin{tabular}{lrrrrrrrrrr}")
    lines.append("        \\toprule")
    lines.append(
        "        \\textbf{Country} & \\textbf{$n$}"
        " & \\textbf{MSFT} & \\textbf{GOOGL} & \\textbf{AMZN}"
        " & \\textbf{Dom} & \\textbf{Frgn} & \\textbf{Unkn}"
        " & \\textbf{US\\%}"
        " & \\textbf{Conf.\\tnote{1}} & \\textbf{GW\\tnote{2}} \\\\"
    )
    lines.append("        \\midrule")

    for _, row_s in df.iterrows():
        row = row_s.to_dict()
        is_total = row["country"] == "Total"

        def _f(val: Any, bold: bool = False) -> str:
            s = _num(int(val))
            return f"\\textbf{{{s}}}" if bold else s

        def _p(val: float, bold: bool = False) -> str:
            s = f"{val:.1f}\\%"
            return f"\\textbf{{{s}}}" if bold else s

        name = f"\\textbf{{{_esc(str(row['country']))}}}" if is_total else _esc(str(row["country"]))

        if is_total:
            lines.append("        \\midrule")

        lines.append(
            f"        {name} & {_f(row['total'], is_total)}"
            f" & {_f(row['microsoft'], is_total)}"
            f" & {_f(row['google'], is_total)}"
            f" & {_f(row['aws'], is_total)}"
            f" & {_f(row['domestic'], is_total)}"
            f" & {_f(row['foreign'], is_total)}"
            f" & {_f(row['unknown'], is_total)}"
            f" & {_p(float(row['us_pct']), is_total)}"
            f" & {_p(float(row['avg_conf']), is_total)}"
            f" & {_p(float(row['gw_pct']), is_total)} \\\\"
        )

    lines.append("        \\bottomrule")
    lines.append("    \\end{tabular}")
    lines.append("    \\begin{tablenotes}\\scriptsize")
    lines.append(
        "        \\item[1] Mean classification confidence; "
        f"{_num(int(grand['low_conf']))} municipalities below 60\\% overall."
    )
    lines.append("        \\item[2] Percentage of municipalities routing mail through a security gateway.")
    lines.append("    \\end{tablenotes}")
    lines.append("    \\end{threeparttable}")
    lines.append("\\end{table}")

    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Export orchestrator
# ---------------------------------------------------------------------------


def export_combined_latex(
    providers_dir: Path = Path("output/providers"),
    output_path: Path | None = None,
) -> Path:
    """Load all countries, build DataFrame, print summary, write LaTeX."""
    all_data = load_all_countries(providers_dir)

    df = build_combined_dataframe(all_data)

    # Print DataFrame for verification
    display_cols = [
        "country",
        "region",
        "total",
        "microsoft",
        "google",
        "aws",
        "domestic",
        "foreign",
        "unknown",
        "us_pct",
        "dom_pct",
    ]
    print("\n  Combined provider summary (pandas DataFrame):\n")
    print(df[display_cols].to_string(index=False))

    # Build country overview
    overview_df = build_country_overview(all_data)
    print("\n  Country overview:\n")
    print(overview_df.to_string(index=False))

    # Render LaTeX
    tex_overview = latex_country_overview(overview_df)
    tex_regional = latex_combined_regional(df)

    # Build output path
    if output_path is None:
        timestamp = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
        output_path = providers_dir / f"tables_combined_{timestamp}.tex"

    # Collect metadata from each country
    commits = []
    for cc in _COUNTRIES:
        data, _ = all_data[cc]
        c = data.get("commit", "?")
        if c not in commits:
            commits.append(c)

    totals = {cc.upper(): len(all_data[cc][1]) for cc in _COUNTRIES}
    total_str = ", ".join(f"{k}={v:,}" for k, v in totals.items())

    header = (
        f"% Auto-generated combined LaTeX table for {', '.join(_COUNTRY_NAMES[cc] for cc in _COUNTRIES)}\n"
        f"% Commits: {', '.join(commits)}\n"
        f"% Export date: {datetime.now(tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}\n"
        f"% Municipalities: {total_str}\n"
        f"%\n"
        f"% This file is a fragment — include it with \\input{{{output_path.stem}}}\n"
        f"% Required packages: booktabs, tabularx, xcolor[table]\n"
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    content = header + "\n" + tex_overview + "\n" + tex_regional
    output_path.write_text(content, encoding="utf-8")

    print(f"\n  LaTeX table written to: {output_path}")
    return output_path


def print_combined_summary(
    providers_dir: Path = Path("output/providers"),
) -> None:
    """Print combined DataFrame summary to stdout (no LaTeX)."""
    all_data = load_all_countries(providers_dir)
    df = build_combined_dataframe(all_data)

    display_cols = [
        "country",
        "region",
        "total",
        "microsoft",
        "google",
        "aws",
        "domestic",
        "foreign",
        "unknown",
        "us_pct",
        "dom_pct",
    ]
    print("\n  Combined provider summary:\n")
    print(df[display_cols].to_string(index=False))
    print(f"\n  Total municipalities: {int(df[df['country'] == 'ALL'].iloc[0]['total']):,}")
    print()
    sys.exit(0)
