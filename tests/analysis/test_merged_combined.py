"""Tests for the merged multi-country provider + security analysis module."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from mail_municipalities.analysis.merged_combined import (
    build_merged_dataframe,
    export_merged_latex,
    latex_merged_table,
    validate_merged_dataframe,
)
from mail_municipalities.analysis.provider_combined import load_all_countries
from mail_municipalities.analysis.security_combined import load_all_security

# ---------------------------------------------------------------------------
# Synthetic data — municipalities with both provider and security fields
# ---------------------------------------------------------------------------

_CH_PROVIDER = [
    {
        "code": "1",
        "name": "Zurich",
        "region": "Kanton Zürich",
        "domain": "zurich.ch",
        "provider": "microsoft",
        "category": "us-cloud",
        "classification_confidence": 95.0,
        "classification_signals": [{"kind": "mx", "provider": "microsoft", "weight": 0.2, "detail": "mx"}],
        "mx": [],
        "spf": "",
        "gateway": None,
    },
    {
        "code": "2",
        "name": "Bern",
        "region": "Kanton Bern",
        "domain": "bern.ch",
        "provider": "domestic",
        "category": "ch-based",
        "classification_confidence": 90.0,
        "classification_signals": [{"kind": "mx", "provider": "domestic", "weight": 0.2, "detail": "mx"}],
        "mx": [],
        "spf": "",
        "gateway": None,
    },
    {
        "code": "3",
        "name": "Genf",
        "region": "Kanton Zürich",
        "domain": "genf.ch",
        "provider": "microsoft",
        "category": "us-cloud",
        "classification_confidence": 85.0,
        "classification_signals": [{"kind": "spf", "provider": "microsoft", "weight": 0.2, "detail": "spf"}],
        "mx": [],
        "spf": "",
        "gateway": None,
    },
]

_CH_SECURITY = [
    {
        "code": "1",
        "name": "Zurich",
        "region": "Kanton Zürich",
        "domain": "zurich.ch",
        "mx_records": ["mx.zurich.ch"],
        "dane": {"supported": True, "partial": True},
        "dss": {"has_spf": True, "has_good_spf": True, "has_dmarc": True, "has_good_dmarc": True, "has_dkim": False},
        "scan_valid": True,
    },
    {
        "code": "2",
        "name": "Bern",
        "region": "Kanton Bern",
        "domain": "bern.ch",
        "mx_records": ["mx.bern.ch"],
        "dane": {"supported": False, "partial": False},
        "dss": {"has_spf": True, "has_good_spf": False, "has_dmarc": False, "has_good_dmarc": False, "has_dkim": False},
        "scan_valid": True,
    },
    {
        "code": "3",
        "name": "Genf",
        "region": "Kanton Zürich",
        "domain": "genf.ch",
        "mx_records": ["mx.genf.ch"],
        "dane": {"supported": False, "partial": False},
        "dss": {"has_spf": True, "has_good_spf": True, "has_dmarc": True, "has_good_dmarc": False, "has_dkim": False},
        "scan_valid": True,
    },
]

_DE_PROVIDER = [
    {
        "code": "10",
        "name": "Berlin",
        "region": "Berlin",
        "domain": "berlin.de",
        "provider": "domestic",
        "category": "de-based",
        "classification_confidence": 88.0,
        "classification_signals": [{"kind": "mx", "provider": "domestic", "weight": 0.2, "detail": "mx"}],
        "mx": [],
        "spf": "",
        "gateway": None,
    },
    {
        "code": "11",
        "name": "Munich",
        "region": "Bayern",
        "domain": "munich.de",
        "provider": "google",
        "category": "us-cloud",
        "classification_confidence": 92.0,
        "classification_signals": [{"kind": "mx", "provider": "google", "weight": 0.2, "detail": "mx"}],
        "mx": [],
        "spf": "",
        "gateway": None,
    },
    {
        "code": "12",
        "name": "Stuttgart",
        "region": "Baden-Württemberg",
        "domain": "stuttgart.de",
        "provider": "domestic",
        "category": "de-based",
        "classification_confidence": 90.0,
        "classification_signals": [{"kind": "mx", "provider": "domestic", "weight": 0.2, "detail": "mx"}],
        "mx": [],
        "spf": "",
        "gateway": None,
    },
]

_DE_SECURITY = [
    {
        "code": "10",
        "name": "Berlin",
        "region": "Berlin",
        "domain": "berlin.de",
        "mx_records": ["mx.berlin.de"],
        "dane": {"supported": False, "partial": False},
        "dss": {"has_spf": True, "has_good_spf": True, "has_dmarc": True, "has_good_dmarc": False, "has_dkim": False},
        "scan_valid": True,
    },
    {
        "code": "11",
        "name": "Munich",
        "region": "Bayern",
        "domain": "munich.de",
        "mx_records": ["mx.munich.de"],
        "dane": {"supported": True, "partial": True},
        "dss": {"has_spf": True, "has_good_spf": True, "has_dmarc": True, "has_good_dmarc": True, "has_dkim": False},
        "scan_valid": True,
    },
    {
        "code": "12",
        "name": "Stuttgart",
        "region": "Baden-Württemberg",
        "domain": "stuttgart.de",
        "mx_records": ["mx.stuttgart.de"],
        "dane": {"supported": False, "partial": False},
        "dss": {"has_spf": True, "has_good_spf": False, "has_dmarc": True, "has_good_dmarc": False, "has_dkim": False},
        "scan_valid": True,
    },
]

_AT_PROVIDER = [
    {
        "code": "20",
        "name": "Wien",
        "region": "Wien",
        "domain": "wien.at",
        "provider": "aws",
        "category": "us-cloud",
        "classification_confidence": 80.0,
        "classification_signals": [{"kind": "mx", "provider": "aws", "weight": 0.2, "detail": "mx"}],
        "mx": [],
        "spf": "",
        "gateway": None,
    },
    {
        "code": "21",
        "name": "Graz",
        "region": "Steiermark",
        "domain": "graz.at",
        "provider": "domestic",
        "category": "at-based",
        "classification_confidence": 75.0,
        "classification_signals": [{"kind": "mx", "provider": "domestic", "weight": 0.2, "detail": "mx"}],
        "mx": [],
        "spf": "",
        "gateway": None,
    },
]

_AT_SECURITY = [
    {
        "code": "20",
        "name": "Wien",
        "region": "Wien",
        "domain": "wien.at",
        "mx_records": ["mx.wien.at"],
        "dane": {"supported": False, "partial": False},
        "dss": {
            "has_spf": False,
            "has_good_spf": False,
            "has_dmarc": False,
            "has_good_dmarc": False,
            "has_dkim": False,
        },
        "scan_valid": True,
    },
    {
        "code": "21",
        "name": "Graz",
        "region": "Steiermark",
        "domain": "graz.at",
        "mx_records": ["mx.graz.at"],
        "dane": {"supported": False, "partial": False},
        "dss": {"has_spf": True, "has_good_spf": True, "has_dmarc": True, "has_good_dmarc": True, "has_dkim": False},
        "scan_valid": True,
    },
]


def _make_json(munis: list[dict]) -> dict:
    return {
        "generated": "2026-04-10T00:00:00Z",
        "commit": "abc1234",
        "total": len(munis),
        "counts": {},
        "municipalities": munis,
    }


def _write_files(d: Path) -> None:
    """Write both provider and security JSONs to directory *d*."""
    d.mkdir(parents=True, exist_ok=True)
    prov_dir = d / "providers"
    sec_dir = d / "security"
    prov_dir.mkdir(exist_ok=True)
    sec_dir.mkdir(exist_ok=True)

    for cc, prov, sec in [
        ("ch", _CH_PROVIDER, _CH_SECURITY),
        ("de", _DE_PROVIDER, _DE_SECURITY),
        ("at", _AT_PROVIDER, _AT_SECURITY),
    ]:
        (prov_dir / f"providers_{cc}.json").write_text(json.dumps(_make_json(prov)), encoding="utf-8")
        (sec_dir / f"security_{cc}.json").write_text(json.dumps(_make_json(sec)), encoding="utf-8")


def _load_both(d: Path) -> tuple:
    _write_files(d)
    return load_all_countries(d / "providers"), load_all_security(d / "security")


# ---------------------------------------------------------------------------
# Tests: build_merged_dataframe
# ---------------------------------------------------------------------------


def test_build_merged_dataframe_shape(tmp_path: Path) -> None:
    prov, sec = _load_both(tmp_path)
    df = build_merged_dataframe(prov, sec)

    # DE: 3 regions (Bad.-Württ., Bayern, Berlin) + subtotal = 4
    # AT: 2 regions + subtotal = 3
    # CH: 2 regions + subtotal = 3
    # Grand total = 1
    assert len(df) == 11


def test_build_merged_dataframe_has_all_columns(tmp_path: Path) -> None:
    prov, sec = _load_both(tmp_path)
    df = build_merged_dataframe(prov, sec)

    for col in ["total", "microsoft", "google", "aws", "domestic", "foreign", "unknown"]:
        assert col in df.columns, f"Missing provider column: {col}"
    for col in ["sec_total", "spf_pct", "good_spf_pct", "dmarc_pct", "good_dmarc_pct", "dane_pct"]:
        assert col in df.columns, f"Missing security column: {col}"
    assert "us_pct" in df.columns
    assert "dom_pct" in df.columns


def test_build_merged_dataframe_dach_order(tmp_path: Path) -> None:
    prov, sec = _load_both(tmp_path)
    df = build_merged_dataframe(prov, sec)

    countries = df[~df["country"].isin(["ALL"])]["country"].tolist()
    seen: list[str] = []
    for c in countries:
        if not seen or seen[-1] != c:
            seen.append(c)
    assert seen == ["DE", "AT", "CH"]


def test_build_merged_dataframe_alphabetical_sort(tmp_path: Path) -> None:
    prov, sec = _load_both(tmp_path)
    df = build_merged_dataframe(prov, sec)

    for cc in ("DE", "AT", "CH"):
        regions = df[(df["country"] == cc) & (~df["region"].isin(["Total", "Grand Tot."]))]["region"].tolist()
        assert regions == sorted(regions), f"{cc} regions not alphabetical: {regions}"


def test_build_merged_dataframe_provider_totals(tmp_path: Path) -> None:
    prov, sec = _load_both(tmp_path)
    df = build_merged_dataframe(prov, sec)

    # CH: 3 munis (2 microsoft in Zürich, 1 domestic in Bern)
    ch_sub = df[(df["country"] == "CH") & (df["region"] == "Total")]
    assert int(ch_sub.iloc[0]["total"]) == 3
    assert int(ch_sub.iloc[0]["microsoft"]) == 2
    assert int(ch_sub.iloc[0]["domestic"]) == 1

    # Grand total: 8 municipalities
    grand = df[df["country"] == "ALL"]
    assert int(grand.iloc[0]["total"]) == 8


def test_build_merged_dataframe_security_totals(tmp_path: Path) -> None:
    prov, sec = _load_both(tmp_path)
    df = build_merged_dataframe(prov, sec)

    # CH: 3 valid scans, all have SPF, 2 have DMARC (Zurich+Genf), 1 has DANE (Zurich)
    ch_sub = df[(df["country"] == "CH") & (df["region"] == "Total")]
    assert int(ch_sub.iloc[0]["sec_total"]) == 3
    assert float(ch_sub.iloc[0]["spf_pct"]) == 100.0
    assert float(ch_sub.iloc[0]["dane_pct"]) == pytest.approx(33.3, abs=0.1)

    # Grand total: 8 valid scans, 7 have SPF
    grand = df[df["country"] == "ALL"]
    assert int(grand.iloc[0]["sec_total"]) == 8
    assert float(grand.iloc[0]["spf_pct"]) == pytest.approx(87.5, abs=0.1)


def test_build_merged_dataframe_display_names(tmp_path: Path) -> None:
    prov, sec = _load_both(tmp_path)
    df = build_merged_dataframe(prov, sec)

    # "Baden-Württemberg" should be abbreviated to "Bad.-Württ."
    regions = df[(df["country"] == "DE") & (~df["region"].isin(["Total", "Grand Tot."]))]["region"].tolist()
    assert "Bad.-Württ." in regions
    assert "Baden-Württemberg" not in regions


def test_build_merged_dataframe_known_percentages(tmp_path: Path) -> None:
    prov, sec = _load_both(tmp_path)
    df = build_merged_dataframe(prov, sec)

    # CH Zürich: 2 microsoft out of 2 → us_pct=100
    zh = df[(df["country"] == "CH") & (df["region"] == "Zürich")]
    assert float(zh.iloc[0]["us_pct"]) == 100.0
    assert float(zh.iloc[0]["dom_pct"]) == 0.0

    # CH Bern: 1 domestic out of 1 → us_pct=0, dom_pct=100
    be = df[(df["country"] == "CH") & (df["region"] == "Bern")]
    assert float(be.iloc[0]["us_pct"]) == 0.0
    assert float(be.iloc[0]["dom_pct"]) == 100.0


# ---------------------------------------------------------------------------
# Tests: validate_merged_dataframe
# ---------------------------------------------------------------------------


def test_validate_passes_on_valid_data(tmp_path: Path) -> None:
    prov, sec = _load_both(tmp_path)
    df = build_merged_dataframe(prov, sec)
    validate_merged_dataframe(df)  # should not raise


def test_validate_detects_bad_subtotal(tmp_path: Path) -> None:
    prov, sec = _load_both(tmp_path)
    df = build_merged_dataframe(prov, sec)

    # Corrupt DE subtotal
    idx = df[(df["country"] == "DE") & (df["region"] == "Total")].index[0]
    df.at[idx, "microsoft"] = 999

    with pytest.raises(AssertionError, match="DE subtotal microsoft"):
        validate_merged_dataframe(df)


def test_validate_detects_pct_out_of_range(tmp_path: Path) -> None:
    prov, sec = _load_both(tmp_path)
    df = build_merged_dataframe(prov, sec)

    idx = df.index[0]
    df.at[idx, "us_pct"] = 101.0

    with pytest.raises(AssertionError, match="us_pct has value above 100"):
        validate_merged_dataframe(df)


def test_validate_detects_metric_exceeds_total(tmp_path: Path) -> None:
    prov, sec = _load_both(tmp_path)
    df = build_merged_dataframe(prov, sec)

    # Find a region row (not subtotal/grand)
    region_rows = df[~df["region"].isin(["Total", "Grand Tot."])]
    idx = region_rows.index[0]
    df.at[idx, "spf"] = int(df.at[idx, "sec_total"]) + 5

    with pytest.raises(AssertionError, match="spf exceeds sec_total"):
        validate_merged_dataframe(df)


def test_validate_detects_bad_grand_total(tmp_path: Path) -> None:
    prov, sec = _load_both(tmp_path)
    df = build_merged_dataframe(prov, sec)

    idx = df[df["region"] == "Grand Tot."].index[0]
    df.at[idx, "total"] = 999

    with pytest.raises(AssertionError, match="Grand total total"):
        validate_merged_dataframe(df)


# ---------------------------------------------------------------------------
# Tests: latex_merged_table
# ---------------------------------------------------------------------------


def test_latex_structure(tmp_path: Path) -> None:
    prov, sec = _load_both(tmp_path)
    df = build_merged_dataframe(prov, sec)
    tex = latex_merged_table(df)

    assert "\\begin{table}" in tex
    assert "\\end{table}" in tex
    assert "\\begin{tabularx}" in tex
    assert "\\end{tabularx}" in tex
    assert "\\begin{tablenotes}" in tex
    assert "\\end{tablenotes}" in tex
    assert "\\midrule" in tex
    assert "\\bottomrule" in tex
    assert "\\caption{" in tex
    assert "tab:merged-regional-security-minimal" in tex


def test_latex_no_toprule(tmp_path: Path) -> None:
    prov, sec = _load_both(tmp_path)
    df = build_merged_dataframe(prov, sec)
    tex = latex_merged_table(df)

    assert "\\toprule" not in tex


def test_latex_column_spec(tmp_path: Path) -> None:
    prov, sec = _load_both(tmp_path)
    df = build_merged_dataframe(prov, sec)
    tex = latex_merged_table(df)

    assert "lrrrrrrrrr|rrrrr" in tex


def test_latex_multicolumn_headers(tmp_path: Path) -> None:
    prov, sec = _load_both(tmp_path)
    df = build_merged_dataframe(prov, sec)
    tex = latex_merged_table(df)

    assert "\\multicolumn{9}{c|}" in tex
    assert "\\multicolumn{5}{c}" in tex
    assert "Provider (Absolute" in tex
    assert "Security (Adoption" in tex


def test_latex_cmidrule(tmp_path: Path) -> None:
    prov, sec = _load_both(tmp_path)
    df = build_merged_dataframe(prov, sec)
    tex = latex_merged_table(df)

    assert "\\cmidrule(l{5pt}r{5pt}){2-15}" in tex


def test_latex_has_all_colors(tmp_path: Path) -> None:
    prov, sec = _load_both(tmp_path)
    df = build_merged_dataframe(prov, sec)
    tex = latex_merged_table(df)

    for color in ("ushigh", "usmid", "uslow", "sechigh", "secmid", "seclow"):
        assert f"\\definecolor{{{color}}}" in tex
    assert "\\cellcolor{" in tex


def test_latex_no_percent_suffix_in_data(tmp_path: Path) -> None:
    prov, sec = _load_both(tmp_path)
    df = build_merged_dataframe(prov, sec)
    tex = latex_merged_table(df)

    # Data rows should NOT have \% (only headers/tablenotes use \%)
    for line in tex.splitlines():
        # Skip header lines, tablenotes, and caption
        if any(kw in line for kw in ["textbf{Region}", "multicolumn", "tablenotes", "caption", "item", "colorbox"]):
            continue
        if "cellcolor" in line:
            # These are data percentage cells — should have no \%
            assert "\\%" not in line, f"Found \\% in data line: {line.strip()}"


def test_latex_no_thousands_separator(tmp_path: Path) -> None:
    prov, sec = _load_both(tmp_path)
    df = build_merged_dataframe(prov, sec)
    tex = latex_merged_table(df)

    # No {,} patterns in data lines
    for line in tex.splitlines():
        if "cellcolor" in line and "definecolor" not in line:
            assert "{,}" not in line, f"Found thousands separator in: {line.strip()}"


def test_latex_bold_subtotals(tmp_path: Path) -> None:
    prov, sec = _load_both(tmp_path)
    df = build_merged_dataframe(prov, sec)
    tex = latex_merged_table(df)

    assert "\\textbf{DE}" in tex
    assert "\\textbf{AT}" in tex
    assert "\\textbf{CH}" in tex
    assert "\\textbf{Grand Tot.}" in tex


def test_latex_color_legend_in_tablenotes(tmp_path: Path) -> None:
    prov, sec = _load_both(tmp_path)
    df = build_merged_dataframe(prov, sec)
    tex = latex_merged_table(df)

    assert "\\colorbox{uslow}" in tex
    assert "\\colorbox{usmid}" in tex
    assert "\\colorbox{ushigh}" in tex
    assert "80\\%" in tex
    assert "50--79\\%" in tex
    assert "minipage" in tex
    assert "provider" in tex
    assert "security" in tex


def test_latex_tablenotes_before_caption(tmp_path: Path) -> None:
    prov, sec = _load_both(tmp_path)
    df = build_merged_dataframe(prov, sec)
    tex = latex_merged_table(df)

    tn_pos = tex.index("\\begin{tablenotes}")
    cap_pos = tex.index("\\caption{")
    assert tn_pos < cap_pos


def test_latex_country_order(tmp_path: Path) -> None:
    prov, sec = _load_both(tmp_path)
    df = build_merged_dataframe(prov, sec)
    tex = latex_merged_table(df)

    de_pos = tex.index("\\textbf{DE}")
    at_pos = tex.index("\\textbf{AT}")
    ch_pos = tex.index("\\textbf{CH}")
    assert de_pos < at_pos < ch_pos


# ---------------------------------------------------------------------------
# Tests: export_merged_latex
# ---------------------------------------------------------------------------


def test_export_merged_latex_writes_file(tmp_path: Path) -> None:
    _write_files(tmp_path)
    output = tmp_path / "merged.tex"
    result = export_merged_latex(
        providers_dir=tmp_path / "providers",
        security_dir=tmp_path / "security",
        output_path=output,
    )

    assert result == output
    assert output.exists()
    content = output.read_text(encoding="utf-8")

    assert "Auto-generated" in content
    assert "tab:merged-regional-security-minimal" in content
    assert "\\begin{table}" in content
    assert "\\end{table}" in content
    assert content.count("\\begin{table}") == 1
    assert content.count("\\end{table}") == 1
