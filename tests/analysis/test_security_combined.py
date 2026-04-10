"""Tests for combined multi-country security analysis module."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from mail_municipalities.analysis.security_combined import (
    build_security_dataframe,
    build_security_overview,
    export_combined_security_latex,
    latex_combined_security,
    latex_security_overview,
    load_all_security,
)

# ---------------------------------------------------------------------------
# Synthetic data — 2–3 municipalities per country with known security flags
# ---------------------------------------------------------------------------

_CH_MUNIS = [
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
]

_DE_MUNIS = [
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
]

_AT_MUNIS = [
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


def _write_security_files(d: Path) -> None:
    """Write synthetic security JSONs to directory *d*."""
    d.mkdir(parents=True, exist_ok=True)
    for cc, munis in [("ch", _CH_MUNIS), ("de", _DE_MUNIS), ("at", _AT_MUNIS)]:
        (d / f"security_{cc}.json").write_text(json.dumps(_make_json(munis)), encoding="utf-8")


def _build_test_all_data(d: Path) -> dict:
    _write_security_files(d)
    return load_all_security(d)


# ---------------------------------------------------------------------------
# Tests: load_all_security
# ---------------------------------------------------------------------------


def test_load_all_security(tmp_path: Path) -> None:
    _write_security_files(tmp_path)
    data = load_all_security(tmp_path)
    assert set(data.keys()) == {"ch", "de", "at"}
    assert len(data["ch"]["municipalities"]) == 2
    assert len(data["de"]["municipalities"]) == 2
    assert len(data["at"]["municipalities"]) == 2


def test_load_all_security_missing_file(tmp_path: Path) -> None:
    (tmp_path).mkdir(parents=True, exist_ok=True)
    (tmp_path / "security_ch.json").write_text(json.dumps(_make_json(_CH_MUNIS)), encoding="utf-8")
    with pytest.raises(SystemExit):
        load_all_security(tmp_path)


# ---------------------------------------------------------------------------
# Tests: build_security_dataframe
# ---------------------------------------------------------------------------


def test_build_security_dataframe_shape(tmp_path: Path) -> None:
    all_data = _build_test_all_data(tmp_path)
    df = build_security_dataframe(all_data)

    # DE: 2 regions (Berlin, Bayern) + subtotal = 3
    # AT: 2 regions (Wien, Steiermark) + subtotal = 3
    # CH: 2 regions (Zürich, Bern) + subtotal = 3
    # Grand total = 1
    # Total rows = 10
    assert len(df) == 10
    assert "country" in df.columns
    assert "region" in df.columns
    assert "dmarc_pct" in df.columns


def test_build_security_dataframe_dach_order(tmp_path: Path) -> None:
    all_data = _build_test_all_data(tmp_path)
    df = build_security_dataframe(all_data)

    countries = df[~df["country"].isin(["ALL"])]["country"].tolist()
    # Remove consecutive duplicates to get the country group order
    seen: list[str] = []
    for c in countries:
        if not seen or seen[-1] != c:
            seen.append(c)
    assert seen == ["DE", "AT", "CH"]


def test_build_security_dataframe_totals(tmp_path: Path) -> None:
    all_data = _build_test_all_data(tmp_path)
    df = build_security_dataframe(all_data)

    # CH subtotal: 2 municipalities (both have SPF, 1 with DMARC)
    ch_total = df[(df["country"] == "CH") & (df["region"] == "Total")]
    assert len(ch_total) == 1
    assert int(ch_total.iloc[0]["total"]) == 2
    assert int(ch_total.iloc[0]["spf"]) == 2
    assert int(ch_total.iloc[0]["dmarc"]) == 1

    # Grand total: 6 municipalities
    grand = df[df["country"] == "ALL"]
    assert int(grand.iloc[0]["total"]) == 6


# ---------------------------------------------------------------------------
# Tests: latex_combined_security
# ---------------------------------------------------------------------------


def test_latex_combined_security_structure(tmp_path: Path) -> None:
    all_data = _build_test_all_data(tmp_path)
    df = build_security_dataframe(all_data)
    tex = latex_combined_security(df)

    assert "\\begin{table}" in tex
    assert "\\end{table}" in tex
    assert "\\toprule" in tex
    assert "\\bottomrule" in tex
    assert "\\caption{" in tex
    assert "tab:combined-security" in tex


def test_latex_combined_security_contains_all_countries(tmp_path: Path) -> None:
    all_data = _build_test_all_data(tmp_path)
    df = build_security_dataframe(all_data)
    tex = latex_combined_security(df)

    assert "CH" in tex
    assert "DE" in tex
    assert "AT" in tex
    assert "ALL" in tex


def test_latex_combined_security_has_colors(tmp_path: Path) -> None:
    all_data = _build_test_all_data(tmp_path)
    df = build_security_dataframe(all_data)
    tex = latex_combined_security(df)

    assert "\\definecolor{sechigh}" in tex
    assert "\\definecolor{secmid}" in tex
    assert "\\definecolor{seclow}" in tex
    assert "\\cellcolor{" in tex


# ---------------------------------------------------------------------------
# Tests: latex_security_overview
# ---------------------------------------------------------------------------


def test_latex_security_overview_structure(tmp_path: Path) -> None:
    all_data = _build_test_all_data(tmp_path)
    df = build_security_overview(all_data)
    tex = latex_security_overview(df)

    assert "\\begin{table}" in tex
    assert "\\end{table}" in tex
    assert "tab:security-overview" in tex
    assert "Germany" in tex
    assert "Austria" in tex
    assert "Switzerland" in tex
    assert "threeparttable" in tex
    assert "tablenotes" in tex
    assert "\\cellcolor" not in tex


def test_build_security_overview_dach_order(tmp_path: Path) -> None:
    all_data = _build_test_all_data(tmp_path)
    df = build_security_overview(all_data)
    countries = list(df["country"])
    assert countries == ["Germany", "Austria", "Switzerland", "Total"]


# ---------------------------------------------------------------------------
# Tests: export_combined_security_latex
# ---------------------------------------------------------------------------


def test_export_combined_security_latex_writes_file(tmp_path: Path) -> None:
    _write_security_files(tmp_path)
    output = tmp_path / "combined.tex"
    result = export_combined_security_latex(security_dir=tmp_path, output_path=output)

    assert result == output
    assert output.exists()
    content = output.read_text(encoding="utf-8")

    assert "Auto-generated" in content
    assert "tab:security-overview" in content
    assert "tab:combined-security" in content
    assert content.count("\\begin{table}") == 2
    assert content.count("\\end{table}") == 2
