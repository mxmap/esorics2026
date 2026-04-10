"""Combined multi-country security analysis with pandas DataFrame and LaTeX export."""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from .helpers import (
    COUNTRIES,
    COUNTRY_NAMES,
    esc,
    num,
    region_name,
)
from .security_latex import _has, _scan_valid, load_security_data

# DKIM intentionally excluded (see feedback_no_dkim.md)

_METRIC_FIELDS = ["spf", "good_spf", "dmarc", "good_dmarc", "dane"]
_DSS_FIELD_MAP = {
    "spf": "has_spf",
    "good_spf": "has_good_spf",
    "dmarc": "has_dmarc",
    "good_dmarc": "has_good_dmarc",
    "dane": "dane_supported",
}

# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------


def load_all_security(
    security_dir: Path = Path("output/security"),
) -> dict[str, dict[str, Any]]:
    """Load security JSONs for all three countries.

    Returns ``{cc: raw_data_dict}`` keyed by country code.
    """
    result: dict[str, dict[str, Any]] = {}
    for cc in COUNTRIES:
        path = security_dir / f"security_{cc}.json"
        result[cc] = load_security_data(path)
    return result


# ---------------------------------------------------------------------------
# DataFrame construction
# ---------------------------------------------------------------------------


def build_security_dataframe(
    all_data: dict[str, dict[str, Any]],
) -> pd.DataFrame:
    """Build a summary DataFrame with one row per (country, region).

    Includes per-country subtotal rows and a grand-total row.
    Sorted by DMARC adoption % descending within each country.
    """
    rows: list[dict[str, Any]] = []
    for cc in COUNTRIES:
        data = all_data[cc]
        valid = _scan_valid(data["municipalities"])
        for m in valid:
            row: dict[str, Any] = {
                "country": cc.upper(),
                "region": region_name(m.get("region", "") or "??"),
            }
            for metric, field in _DSS_FIELD_MAP.items():
                row[metric] = int(_has(m, field))
            rows.append(row)

    raw = pd.DataFrame(rows)

    # Group by (country, region), sum metric columns
    grouped = raw.groupby(["country", "region"], sort=False)[_METRIC_FIELDS].sum()
    summary = grouped.reset_index()  # pyright: ignore[reportAttributeAccessIssue]
    summary["total"] = raw.groupby(["country", "region"], sort=False).size().values

    # Compute percentage columns
    for metric in _METRIC_FIELDS:
        summary[f"{metric}_pct"] = (summary[metric] / summary["total"] * 100).round(1)

    # Sort within each country by dmarc_pct descending
    parts: list[pd.DataFrame] = []
    for cc in COUNTRIES:
        cc_upper = cc.upper()
        part = summary[summary["country"] == cc_upper].sort_values("dmarc_pct", ascending=False)  # pyright: ignore[reportCallIssue]
        parts.append(part)

        # Country subtotal row
        totals = part[_METRIC_FIELDS].sum().to_dict()
        total_n = int(part["total"].sum())  # pyright: ignore[reportArgumentType]
        sub = pd.DataFrame(
            [
                {
                    "country": cc_upper,
                    "region": "Total",
                    "total": total_n,
                    **{m: int(totals[m]) for m in _METRIC_FIELDS},
                    **{f"{m}_pct": round(int(totals[m]) / total_n * 100, 1) if total_n else 0 for m in _METRIC_FIELDS},
                }
            ]
        )
        parts.append(sub)

    # Grand total row
    all_metrics = summary[_METRIC_FIELDS].sum().to_dict()
    grand_total = int(summary["total"].sum())  # pyright: ignore[reportArgumentType]
    grand = pd.DataFrame(
        [
            {
                "country": "ALL",
                "region": "Grand Total",
                "total": grand_total,
                **{m: int(all_metrics[m]) for m in _METRIC_FIELDS},
                **{
                    f"{m}_pct": round(int(all_metrics[m]) / grand_total * 100, 1) if grand_total else 0
                    for m in _METRIC_FIELDS
                },
            }
        ]
    )
    parts.append(grand)

    result = pd.concat(parts, ignore_index=True)

    # Ensure integer types for count columns
    int_cols = ["total", *_METRIC_FIELDS]
    for col in int_cols:
        result[col] = result[col].astype(int)

    return result


# ---------------------------------------------------------------------------
# Country overview
# ---------------------------------------------------------------------------


def build_security_overview(
    all_data: dict[str, dict[str, Any]],
) -> pd.DataFrame:
    """Build a compact country-level overview DataFrame."""
    rows: list[dict[str, Any]] = []
    for cc in COUNTRIES:
        data = all_data[cc]
        valid = _scan_valid(data["municipalities"])
        total = len(valid)
        row: dict[str, Any] = {
            "country": COUNTRY_NAMES[cc],
            "total": total,
        }
        for metric, field in _DSS_FIELD_MAP.items():
            cnt = sum(1 for m in valid if _has(m, field))
            row[metric] = cnt
            row[f"{metric}_pct"] = round(cnt / total * 100, 1) if total else 0
        rows.append(row)

    # Grand total
    all_total = sum(r["total"] for r in rows)
    gt: dict[str, Any] = {
        "country": "Total",
        "total": all_total,
    }
    for metric in _METRIC_FIELDS:
        cnt = sum(r[metric] for r in rows)
        gt[metric] = cnt
        gt[f"{metric}_pct"] = round(cnt / all_total * 100, 1) if all_total else 0
    rows.append(gt)

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# LaTeX rendering
# ---------------------------------------------------------------------------

_SEC_COLOR_DEFS = (
    "\\definecolor{sechigh}{RGB}{214,240,216}%   >=80% adoption (good)\n"
    "\\definecolor{secmid}{RGB}{255,244,199}%    >=50% adoption\n"
    "\\definecolor{seclow}{RGB}{253,216,213}%    <50% adoption (poor)\n"
)


def _sec_cell(pct: float, bold: bool = False) -> str:
    """Format a security percentage cell with background colour."""
    if pct >= 80:
        color = "sechigh"
    elif pct >= 50:
        color = "secmid"
    else:
        color = "seclow"
    text = f"{pct:.1f}\\%"
    if bold:
        text = f"\\textbf{{{text}}}"
    return f"\\cellcolor{{{color}}}{text}"


def latex_security_overview(df: pd.DataFrame) -> str:
    """Render a compact country-level security overview table."""
    grand = df[df["country"] == "Total"].iloc[0]
    grand_n = int(grand["total"])

    lines: list[str] = []
    lines.append("\\begin{table}[t]")
    lines.append("    \\centering")
    lines.append(f"    \\caption{{Country-level email security overview ($n={num(grand_n)}$ municipalities).}}")
    lines.append("    \\label{tab:security-overview}")
    lines.append("    \\footnotesize")
    lines.append("    \\begin{threeparttable}")
    lines.append("    \\begin{tabular}{lrrrrrr}")
    lines.append("        \\toprule")
    lines.append(
        "        \\textbf{Country} & \\textbf{$n$}"
        " & \\textbf{SPF\\%} & \\textbf{gSPF\\%\\tnote{1}}"
        " & \\textbf{DMARC\\%} & \\textbf{gDMARC\\%\\tnote{2}}"
        " & \\textbf{DANE\\%} \\\\"
    )
    lines.append("        \\midrule")

    for _, row_s in df.iterrows():
        row = row_s.to_dict()
        is_total = row["country"] == "Total"

        def _p(val: float, bold: bool = False) -> str:
            s = f"{val:.1f}\\%"
            return f"\\textbf{{{s}}}" if bold else s

        name = f"\\textbf{{{esc(str(row['country']))}}}" if is_total else esc(str(row["country"]))

        if is_total:
            lines.append("        \\midrule")

        lines.append(
            f"        {name} & {num(int(row['total']))}"
            f" & {_p(float(row['spf_pct']), is_total)}"
            f" & {_p(float(row['good_spf_pct']), is_total)}"
            f" & {_p(float(row['dmarc_pct']), is_total)}"
            f" & {_p(float(row['good_dmarc_pct']), is_total)}"
            f" & {_p(float(row['dane_pct']), is_total)} \\\\"
        )

    lines.append("        \\bottomrule")
    lines.append("    \\end{tabular}")
    lines.append("    \\begin{tablenotes}\\scriptsize")
    lines.append("        \\item[1] Well-configured SPF (no overly permissive mechanisms).")
    lines.append("        \\item[2] Well-configured DMARC (enforcement policy).")
    lines.append("    \\end{tablenotes}")
    lines.append("    \\end{threeparttable}")
    lines.append("\\end{table}")

    return "\n".join(lines) + "\n"


_PCT_COLS = ["spf_pct", "good_spf_pct", "dmarc_pct", "good_dmarc_pct", "dane_pct"]
_COL_HEADERS = ["SPF\\%", "gSPF\\%", "DMARC\\%", "gDMARC\\%", "DANE\\%"]


def latex_combined_security(df: pd.DataFrame) -> str:
    """Render the combined regional security DataFrame as a colored LaTeX table."""
    grand_row = df[df["country"] == "ALL"].iloc[0]
    grand_n = int(grand_row["total"])

    lines: list[str] = []
    lines.append("\\begin{table}[t]")
    lines.append("    \\centering")
    lines.append(f"    \\caption{{Email security adoption by region across all countries ($n={num(grand_n)}$).}}")
    lines.append("    \\label{tab:combined-security}")
    lines.append("    \\scriptsize")
    lines.append("    \\renewcommand{\\arraystretch}{0.82}")
    lines.append("")
    lines.append("    % Color definitions")
    for cdef in _SEC_COLOR_DEFS.strip().splitlines():
        lines.append(f"    {cdef}")
    lines.append("")

    col_header = " & ".join(f"\\textbf{{{h}}}" for h in _COL_HEADERS)
    lines.append("    \\begin{tabularx}{\\textwidth}{lXrrrrrr}")
    lines.append("        \\toprule")
    lines.append(f"        \\textbf{{Country}} & \\textbf{{Region}} & \\textbf{{$n$}} & {col_header} \\\\")
    lines.append("        \\midrule")

    prev_country: str | None = None

    for _, row_s in df.iterrows():
        row = row_s.to_dict()
        country = str(row["country"])
        region_val = str(row["region"])
        is_subtotal = region_val == "Total"
        is_grand = country == "ALL"
        is_bold = is_subtotal or is_grand

        if prev_country is not None and country != prev_country:
            lines.append("        \\midrule")

        if country == prev_country and not is_subtotal:
            country_cell = ""
        else:
            country_cell = f"\\textbf{{{country}}}" if is_bold else country

        region_cell = f"\\textbf{{{esc(region_val)}}}" if is_bold else esc(region_val)
        n_str = num(int(row["total"]))
        n_cell = f"\\textbf{{{n_str}}}" if is_bold else n_str

        pct_cells = " & ".join(_sec_cell(float(row[col]), is_bold) for col in _PCT_COLS)

        lines.append(f"        {country_cell} & {region_cell} & {n_cell} & {pct_cells} \\\\")

        prev_country = country

    lines.append("        \\bottomrule")
    lines.append("    \\end{tabularx}")
    lines.append("\\end{table}")

    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Export orchestrator
# ---------------------------------------------------------------------------


def export_combined_security_latex(
    security_dir: Path = Path("output/security"),
    output_path: Path | None = None,
) -> Path:
    """Load all countries, build DataFrame, print summary, write LaTeX."""
    all_data = load_all_security(security_dir)

    df = build_security_dataframe(all_data)

    display_cols = ["country", "region", "total", *[f"{m}_pct" for m in _METRIC_FIELDS]]
    print("\n  Combined security summary (pandas DataFrame):\n")
    print(df[display_cols].to_string(index=False))

    overview_df = build_security_overview(all_data)
    print("\n  Country overview:\n")
    print(overview_df.to_string(index=False))

    tex_overview = latex_security_overview(overview_df)
    tex_regional = latex_combined_security(df)

    if output_path is None:
        timestamp = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
        output_path = security_dir / f"tables_security_combined_{timestamp}.tex"

    commits = []
    for cc in COUNTRIES:
        c = all_data[cc].get("commit", "?")
        if c not in commits:
            commits.append(c)

    totals = {cc.upper(): len(_scan_valid(all_data[cc]["municipalities"])) for cc in COUNTRIES}
    total_str = ", ".join(f"{k}={v:,}" for k, v in totals.items())

    header = (
        f"% Auto-generated combined security LaTeX tables for "
        f"{', '.join(COUNTRY_NAMES[cc] for cc in COUNTRIES)}\n"
        f"% Commits: {', '.join(commits)}\n"
        f"% Export date: {datetime.now(tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}\n"
        f"% Municipalities: {total_str}\n"
        f"%\n"
        f"% This file is a fragment — include it with \\input{{{output_path.stem}}}\n"
        f"% Required packages: booktabs, tabularx, xcolor[table], threeparttable\n"
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    content = header + "\n" + tex_overview + "\n" + tex_regional
    output_path.write_text(content, encoding="utf-8")

    print(f"\n  LaTeX table written to: {output_path}")
    return output_path


def print_combined_security_summary(
    security_dir: Path = Path("output/security"),
) -> None:
    """Print combined DataFrame summary to stdout (no LaTeX)."""
    all_data = load_all_security(security_dir)
    df = build_security_dataframe(all_data)

    display_cols = ["country", "region", "total", *[f"{m}_pct" for m in _METRIC_FIELDS]]
    print("\n  Combined security summary:\n")
    print(df[display_cols].to_string(index=False))
    print(f"\n  Total municipalities: {int(df[df['country'] == 'ALL'].iloc[0]['total']):,}")
    print()
    sys.exit(0)
