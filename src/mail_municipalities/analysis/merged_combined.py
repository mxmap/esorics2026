"""Merged multi-country provider + security table with pandas DataFrame and LaTeX export."""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from mail_municipalities.provider_classification.analyze import _PROVIDERS_ORDERED

from .helpers import (
    COUNTRIES,
    COUNTRY_NAMES,
    esc,
)
from .provider_combined import load_all_countries
from .security_combined import load_all_security
from .security_latex import _has, _scan_valid

# Abbreviated display names matching the paper table style.
_DISPLAY_NAMES: dict[str, str] = {
    # DE
    "Baden-Württemberg": "Bad.-Württ.",
    "Mecklenburg-Vorpommern": "Meckl.-Vorp.",
    "Niedersachsen": "Niedersachs.",
    "Nordrhein-Westfalen": "NRW",
    "Rheinland-Pfalz": "Rheinl.-Pf.",
    "Sachsen-Anhalt": "Sachs.-Anh.",
    "Schleswig-Holstein": "Schles.-Hol.",
    # AT
    "Niederösterreich": "Niederöst.",
    "Oberösterreich": "Oberösterr.",
    # CH
    "Appenzell Ausserrhoden": "Appenzell A.",
    "Appenzell Innerrhoden": "Appenzell I.",
    "Basel-Landschaft": "Basel-Land",
}


def _display_region(raw_region: str) -> str:
    """Return a short display name for a region, matching paper conventions."""
    name = raw_region.removeprefix("Kanton ")
    return _DISPLAY_NAMES.get(name, name)


_SECURITY_FIELDS = ["spf", "good_spf", "dmarc", "good_dmarc", "dane"]
_DSS_FIELD_MAP = {
    "spf": "has_spf",
    "good_spf": "has_good_spf",
    "dmarc": "has_dmarc",
    "good_dmarc": "has_good_dmarc",
    "dane": "dane_supported",
}

# ---------------------------------------------------------------------------
# DataFrame construction
# ---------------------------------------------------------------------------


def build_merged_dataframe(
    provider_data: dict[str, tuple[dict[str, Any], dict[str, Any]]],
    security_data: dict[str, dict[str, Any]],
) -> pd.DataFrame:
    """Build a merged DataFrame with provider counts and security percentages.

    One row per (country, region), plus subtotal and grand-total rows.
    Sorted alphabetically by region within each country.
    """
    # -- provider rows --
    prov_rows: list[dict[str, Any]] = []
    for cc in COUNTRIES:
        _, munis = provider_data[cc]
        for m in munis.values():
            prov_rows.append(
                {
                    "country": cc.upper(),
                    "region": _display_region(m.get("region", "") or "??"),
                    "provider": m["provider"],
                }
            )

    prov_raw = pd.DataFrame(prov_rows)
    ct = pd.crosstab(
        index=[prov_raw["country"], prov_raw["region"]],
        columns=prov_raw["provider"],
        margins=False,
    )
    for prov in _PROVIDERS_ORDERED:
        if prov not in ct.columns:
            ct[prov] = 0
    ct = ct[list(_PROVIDERS_ORDERED)]
    prov_df = ct.reset_index()
    prov_df.columns.name = None
    prov_df["total"] = prov_df[list(_PROVIDERS_ORDERED)].sum(axis=1)

    # -- security rows --
    sec_rows: list[dict[str, Any]] = []
    for cc in COUNTRIES:
        data = security_data[cc]
        valid = _scan_valid(data["municipalities"])
        for m in valid:
            row: dict[str, Any] = {
                "country": cc.upper(),
                "region": _display_region(m.get("region", "") or "??"),
            }
            for metric, field in _DSS_FIELD_MAP.items():
                row[metric] = int(_has(m, field))
            sec_rows.append(row)

    sec_raw = pd.DataFrame(sec_rows)
    sec_grouped = sec_raw.groupby(["country", "region"], sort=False)[_SECURITY_FIELDS].sum()
    sec_df = sec_grouped.reset_index()  # pyright: ignore[reportAttributeAccessIssue]
    sec_df["sec_total"] = sec_raw.groupby(["country", "region"], sort=False).size().values

    # -- merge --
    merged = prov_df.merge(sec_df, on=["country", "region"], how="left")

    # Fill any missing security data with zeros
    for col in [*_SECURITY_FIELDS, "sec_total"]:
        merged[col] = merged[col].fillna(0).astype(int)

    # -- derived percentages --
    merged["us_cloud"] = merged["microsoft"] + merged["google"] + merged["aws"]
    merged["us_pct"] = (merged["us_cloud"] / merged["total"] * 100).round(1)
    merged["dom_pct"] = (merged["domestic"] / merged["total"] * 100).round(1)
    for metric in _SECURITY_FIELDS:
        merged[f"{metric}_pct"] = (merged[metric] / merged["sec_total"].replace(0, 1) * 100).round(1)
        # Zero out pct if sec_total is 0
        merged.loc[merged["sec_total"] == 0, f"{metric}_pct"] = 0.0

    # -- sort alphabetically & assemble with subtotals --
    parts: list[pd.DataFrame] = []
    for cc in COUNTRIES:
        cc_upper = cc.upper()
        part = merged[merged["country"] == cc_upper].sort_values("region")  # pyright: ignore[reportCallIssue]
        parts.append(part)

        # Country subtotal
        prov_totals = part[list(_PROVIDERS_ORDERED)].sum().to_dict()
        total_n = int(sum(prov_totals.values()))
        us_n = int(prov_totals["microsoft"] + prov_totals["google"] + prov_totals["aws"])
        dom_n = int(prov_totals["domestic"])

        sec_totals = part[_SECURITY_FIELDS].sum().to_dict()
        sec_total_n = int(part["sec_total"].sum())  # pyright: ignore[reportArgumentType]

        sub = pd.DataFrame(
            [
                {
                    "country": cc_upper,
                    "region": "Total",
                    "total": total_n,
                    "us_cloud": us_n,
                    "us_pct": round(us_n / total_n * 100, 1) if total_n else 0,
                    "dom_pct": round(dom_n / total_n * 100, 1) if total_n else 0,
                    "sec_total": sec_total_n,
                    **{p: int(prov_totals[p]) for p in _PROVIDERS_ORDERED},
                    **{m: int(sec_totals[m]) for m in _SECURITY_FIELDS},
                    **{
                        f"{m}_pct": round(int(sec_totals[m]) / sec_total_n * 100, 1) if sec_total_n else 0
                        for m in _SECURITY_FIELDS
                    },
                }
            ]
        )
        parts.append(sub)

    # Grand total
    all_provs = merged[list(_PROVIDERS_ORDERED)].sum().to_dict()
    grand_total = int(sum(all_provs.values()))
    grand_us = int(all_provs["microsoft"] + all_provs["google"] + all_provs["aws"])
    grand_dom = int(all_provs["domestic"])
    all_sec = merged[_SECURITY_FIELDS].sum().to_dict()
    grand_sec_total = int(merged["sec_total"].sum())  # pyright: ignore[reportArgumentType]

    grand = pd.DataFrame(
        [
            {
                "country": "ALL",
                "region": "Grand Tot.",
                "total": grand_total,
                "us_cloud": grand_us,
                "us_pct": round(grand_us / grand_total * 100, 1) if grand_total else 0,
                "dom_pct": round(grand_dom / grand_total * 100, 1) if grand_total else 0,
                "sec_total": grand_sec_total,
                **{p: int(all_provs[p]) for p in _PROVIDERS_ORDERED},
                **{m: int(all_sec[m]) for m in _SECURITY_FIELDS},
                **{
                    f"{m}_pct": round(int(all_sec[m]) / grand_sec_total * 100, 1) if grand_sec_total else 0
                    for m in _SECURITY_FIELDS
                },
            }
        ]
    )
    parts.append(grand)

    result = pd.concat(parts, ignore_index=True)

    # Ensure integer types
    int_cols = ["total", "us_cloud", "sec_total", *list(_PROVIDERS_ORDERED), *_SECURITY_FIELDS]
    for col in int_cols:
        result[col] = result[col].astype(int)

    return result


# ---------------------------------------------------------------------------
# Assurance checks
# ---------------------------------------------------------------------------


def validate_merged_dataframe(df: pd.DataFrame) -> None:
    """Validate data integrity. Raises ``AssertionError`` on any failure."""
    prov_count_cols = list(_PROVIDERS_ORDERED)
    sec_count_cols = _SECURITY_FIELDS
    pct_cols = ["us_pct", "dom_pct"] + [f"{m}_pct" for m in _SECURITY_FIELDS]

    # -- percentages in [0, 100] --
    for col in pct_cols:
        vals = df[col]
        assert vals.min() >= 0, f"{col} has value below 0: {vals.min()}"
        assert vals.max() <= 100, f"{col} has value above 100: {vals.max()}"

    # -- security metric counts <= sec_total --
    region_rows = df[~df["region"].isin(["Total", "Grand Tot."])]
    for metric in sec_count_cols:
        over = region_rows[region_rows[metric] > region_rows["sec_total"]]
        assert len(over) == 0, f"{metric} exceeds sec_total in: {list(over['region'])}"

    # -- subtotal integrity --
    for cc in ("DE", "AT", "CH"):
        regions = df[(df["country"] == cc) & (~df["region"].isin(["Total", "Grand Tot."]))]
        subtotal = df[(df["country"] == cc) & (df["region"] == "Total")]
        assert len(subtotal) == 1, f"Expected 1 subtotal row for {cc}, got {len(subtotal)}"
        sub = subtotal.iloc[0]

        for col in prov_count_cols:
            expected = int(regions[col].sum())  # pyright: ignore[reportArgumentType]
            actual = int(sub[col])
            assert actual == expected, f"{cc} subtotal {col}: expected {expected}, got {actual}"

        for col in sec_count_cols:
            expected = int(regions[col].sum())  # pyright: ignore[reportArgumentType]
            actual = int(sub[col])
            assert actual == expected, f"{cc} security subtotal {col}: expected {expected}, got {actual}"

        expected_total = int(regions["total"].sum())  # pyright: ignore[reportArgumentType]
        assert int(sub["total"]) == expected_total, (
            f"{cc} subtotal total: expected {expected_total}, got {int(sub['total'])}"
        )
        expected_sec_total = int(regions["sec_total"].sum())  # pyright: ignore[reportArgumentType]
        assert int(sub["sec_total"]) == expected_sec_total, (
            f"{cc} subtotal sec_total: expected {expected_sec_total}, got {int(sub['sec_total'])}"
        )

    # -- grand total integrity --
    subtotals = df[df["region"] == "Total"]
    grand = df[df["region"] == "Grand Tot."]
    assert len(grand) == 1, f"Expected 1 grand total row, got {len(grand)}"
    g = grand.iloc[0]

    for col in [*prov_count_cols, *sec_count_cols, "total", "sec_total"]:
        expected = int(subtotals[col].sum())  # pyright: ignore[reportArgumentType]
        actual = int(g[col])
        assert actual == expected, f"Grand total {col}: expected {expected}, got {actual}"

    # -- no NaN in key columns --
    key_cols = ["total", "sec_total", *prov_count_cols, *sec_count_cols]
    for col in key_cols:
        assert df[col].isna().sum() == 0, f"NaN found in {col}"


# ---------------------------------------------------------------------------
# LaTeX rendering
# ---------------------------------------------------------------------------

_COLOR_DEFS = """\
    \\definecolor{ushigh}{RGB}{253,216,213}
    \\definecolor{usmid}{RGB}{255,244,199}
    \\definecolor{uslow}{RGB}{214,240,216}
    \\definecolor{sechigh}{RGB}{214,240,216}
    \\definecolor{secmid}{RGB}{255,244,199}
    \\definecolor{seclow}{RGB}{253,216,213}"""


def _us_color(pct: float) -> str:
    if pct >= 50:
        return "ushigh"
    if pct >= 20:
        return "usmid"
    return "uslow"


def _dom_color(pct: float) -> str:
    if pct >= 80:
        return "uslow"
    if pct >= 50:
        return "usmid"
    return "ushigh"


def _sec_color(pct: float) -> str:
    if pct >= 80:
        return "sechigh"
    if pct >= 50:
        return "secmid"
    return "seclow"


def _colored(pct: float, color_fn: Any, bold: bool = False) -> str:
    """Render a percentage cell with background colour, no \\% suffix."""
    color = color_fn(pct)
    text = f"{pct:.1f}"
    if bold:
        text = f"\\textbf{{{text}}}"
    return f"\\cellcolor{{{color}}}{text}"


def _bold_int(val: int, bold: bool = False) -> str:
    s = str(val)
    return f"\\textbf{{{s}}}" if bold else s


def latex_merged_table(df: pd.DataFrame) -> str:
    """Render the merged provider+security DataFrame as a LaTeX table.

    Matches the structure in paper_src/paper.tex ``tab:merged-regional-security-minimal``.
    """
    lines: list[str] = []
    lines.append("\\begin{table}[t]")
    lines.append("    \\centering")
    lines.append("    \\scriptsize")
    lines.append("    \\renewcommand{\\arraystretch}{0.82}")
    lines.append("    \\setlength{\\tabcolsep}{2.2pt}")
    lines.append("")
    lines.append("    % Color definitions")
    lines.append(_COLOR_DEFS)
    lines.append("    \\begin{tabularx}{\\textwidth}{lrrrrrrrrr|rrrrr}")
    lines.append(
        "        & \\multicolumn{9}{c|}{\\textbf{Provider (Absolute \\& \\%)}}"
        " & \\multicolumn{5}{c}{\\textbf{Security (Adoption \\%)}} \\\\"
    )
    lines.append("        \\cmidrule(l{5pt}r{5pt}){2-15}")
    lines.append(
        "        \\textbf{Region}"
        " & \\textbf{\\scalebox{0.8}{$\\sum$}}"
        " & \\textbf{MS}"
        " & \\textbf{G}"
        " & \\textbf{A}"
        " & \\textbf{Dom}"
        " & \\textbf{Frg}"
        " & \\textbf{Unk}"
        " & \\textbf{Dom}"
        " & \\textbf{US}"
        " & \\textbf{S}"
        " & \\textbf{gS}"
        " & \\textbf{DM}"
        " & \\textbf{gDM}"
        " & \\textbf{DN} \\\\"
    )
    lines.append("        \\midrule")

    prev_country: str | None = None

    for _, row_s in df.iterrows():
        row = row_s.to_dict()
        country = str(row["country"])
        region_val = str(row["region"])
        is_subtotal = region_val == "Total"
        is_grand = country == "ALL"
        is_bold = is_subtotal or is_grand

        # Separator between country groups
        if prev_country is not None and country != prev_country:
            lines.append("        \\midrule")

        # Region cell: country code for subtotals, "Grand Tot." for grand
        if is_grand:
            region_cell = "\\textbf{Grand Tot.}"
        elif is_subtotal:
            region_cell = f"\\textbf{{{country}}}"
        else:
            region_cell = esc(region_val)

        # Count cells
        total_cell = _bold_int(int(row["total"]), is_bold)
        ms_cell = _bold_int(int(row["microsoft"]), is_bold)
        g_cell = _bold_int(int(row["google"]), is_bold)
        a_cell = _bold_int(int(row["aws"]), is_bold)
        dom_cell = _bold_int(int(row["domestic"]), is_bold)
        frg_cell = _bold_int(int(row["foreign"]), is_bold)
        unk_cell = _bold_int(int(row["unknown"]), is_bold)

        # Percentage cells
        dom_pct_cell = _colored(float(row["dom_pct"]), _dom_color, is_bold)
        us_pct_cell = _colored(float(row["us_pct"]), _us_color, is_bold)
        spf_cell = _colored(float(row["spf_pct"]), _sec_color, is_bold)
        gspf_cell = _colored(float(row["good_spf_pct"]), _sec_color, is_bold)
        dmarc_cell = _colored(float(row["dmarc_pct"]), _sec_color, is_bold)
        gdmarc_cell = _colored(float(row["good_dmarc_pct"]), _sec_color, is_bold)
        dane_cell = _colored(float(row["dane_pct"]), _sec_color, is_bold)

        lines.append(
            f"        {region_cell}"
            f" & {total_cell}"
            f" & {ms_cell}"
            f" & {g_cell}"
            f" & {a_cell}"
            f" & {dom_cell}"
            f" & {frg_cell}"
            f" & {unk_cell}"
            f" & {dom_pct_cell}"
            f" & {us_pct_cell}"
            f" & {spf_cell}"
            f" & {gspf_cell}"
            f" & {dmarc_cell}"
            f" & {gdmarc_cell}"
            f" & {dane_cell} \\\\"
        )

        prev_country = country

    lines.append("        \\bottomrule")
    lines.append("    \\end{tabularx}")
    lines.append("    \\begin{tablenotes}\\scriptsize")
    lines.append("    \\item[]")
    lines.append("    \\begin{minipage}[t]{\\linewidth}")
    lines.append(
        "    \\textbf{MS}\\,Microsoft,"
        " \\textbf{G}\\,Google,"
        " \\textbf{A}\\,Amazon,"
        " \\textbf{Dom}\\,Domestic,"
        " \\textbf{Frg}\\,Foreign,"
        " \\textbf{Unk}\\,Unknown;"
        " \\textbf{S}\\,SPF,"
        " \\textbf{gS}\\,good SPF,"
        " \\textbf{DM}\\,DMARC,"
        " \\textbf{gDM}\\,good DMARC,"
        " \\textbf{DN}\\,DANE.\\\\[2pt]"
    )
    lines.append(
        "    \\colorbox{uslow}{\\strut\\enspace}\\,$\\geq$80\\%"
        "\\enspace"
        "\\colorbox{usmid}{\\strut\\enspace}\\,50--79\\%"
        "\\enspace"
        "\\colorbox{ushigh}{\\strut\\enspace}\\,$<$50\\%"
        " --- domestic share (provider) resp.\\ adoption (security)."
    )
    lines.append("    \\end{minipage}")
    lines.append("    \\end{tablenotes}")
    lines.append("    \\vspace{7pt}")
    lines.append("    \\caption{Provider Distribution and Email Security Adoption by Region}")
    lines.append("    \\label{tab:merged-regional-security-minimal}")
    lines.append("\\end{table}")

    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Export orchestrator
# ---------------------------------------------------------------------------


def export_merged_latex(
    providers_dir: Path = Path("output/providers"),
    security_dir: Path = Path("output/security"),
    output_path: Path | None = None,
) -> Path:
    """Load all countries, build merged DataFrame, validate, and write LaTeX."""
    provider_data = load_all_countries(providers_dir)
    security_data = load_all_security(security_dir)

    df = build_merged_dataframe(provider_data, security_data)
    validate_merged_dataframe(df)

    # Print for verification
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
        "sec_total",
        "spf_pct",
        "good_spf_pct",
        "dmarc_pct",
        "good_dmarc_pct",
        "dane_pct",
    ]
    print("\n  Merged provider + security summary:\n")
    print(df[display_cols].to_string(index=False))

    tex = latex_merged_table(df)

    if output_path is None:
        timestamp = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
        output_path = providers_dir / f"tables_merged_{timestamp}.tex"

    # Collect metadata
    commits: list[str] = []
    for cc in COUNTRIES:
        for src in (provider_data[cc][0], security_data[cc]):
            c = src.get("commit", "?") if isinstance(src, dict) else src[0].get("commit", "?")
            if c not in commits:
                commits.append(c)

    totals = {cc.upper(): len(provider_data[cc][1]) for cc in COUNTRIES}
    total_str = ", ".join(f"{k}={v:,}" for k, v in totals.items())

    header = (
        f"% Auto-generated merged LaTeX table for "
        f"{', '.join(COUNTRY_NAMES[cc] for cc in COUNTRIES)}\n"
        f"% Commits: {', '.join(commits)}\n"
        f"% Export date: {datetime.now(tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}\n"
        f"% Municipalities: {total_str}\n"
        f"%\n"
        f"% This file is a fragment — include it with \\input{{{output_path.stem}}}\n"
        f"% Required packages: booktabs, tabularx, xcolor[table], graphicx\n"
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    content = header + "\n" + tex
    output_path.write_text(content, encoding="utf-8")

    print(f"\n  LaTeX table written to: {output_path}")
    return output_path


def print_merged_summary(
    providers_dir: Path = Path("output/providers"),
    security_dir: Path = Path("output/security"),
) -> None:
    """Print merged DataFrame summary to stdout (no LaTeX)."""
    provider_data = load_all_countries(providers_dir)
    security_data = load_all_security(security_dir)

    df = build_merged_dataframe(provider_data, security_data)
    validate_merged_dataframe(df)

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
        "sec_total",
        "spf_pct",
        "good_spf_pct",
        "dmarc_pct",
        "good_dmarc_pct",
        "dane_pct",
    ]
    print("\n  Merged provider + security summary:\n")
    print(df[display_cols].to_string(index=False))
    grand = df[df["country"] == "ALL"].iloc[0]
    print(f"\n  Total municipalities: {int(grand['total']):,}")
    print()
    sys.exit(0)
