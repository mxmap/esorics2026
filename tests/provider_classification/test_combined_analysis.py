"""Tests for the combined multi-country analysis module."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from mail_municipalities.analysis.provider_combined import (
    build_combined_dataframe,
    build_country_overview,
    export_combined_latex,
    latex_combined_regional,
    latex_country_overview,
    load_all_countries,
)

# ---------------------------------------------------------------------------
# Synthetic data — 2–3 municipalities per country with known providers
# ---------------------------------------------------------------------------

_CH_MUNIS = [
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

_DE_MUNIS = [
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
]

_AT_MUNIS = [
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


def _make_json(munis: list[dict]) -> dict:
    return {
        "generated": "2026-04-07T00:00:00Z",
        "commit": "abc1234",
        "total": len(munis),
        "counts": {},
        "municipalities": munis,
    }


def _write_provider_files(d: Path) -> None:
    """Write synthetic provider JSONs to directory *d*."""
    d.mkdir(parents=True, exist_ok=True)
    for cc, munis in [("ch", _CH_MUNIS), ("de", _DE_MUNIS), ("at", _AT_MUNIS)]:
        (d / f"providers_{cc}.json").write_text(json.dumps(_make_json(munis)), encoding="utf-8")


def _build_test_all_data(d: Path) -> dict:
    _write_provider_files(d)
    return load_all_countries(d)


# ---------------------------------------------------------------------------
# Tests: load_all_countries
# ---------------------------------------------------------------------------


def test_load_all_countries(tmp_path: Path) -> None:
    _write_provider_files(tmp_path)
    data = load_all_countries(tmp_path)
    assert set(data.keys()) == {"ch", "de", "at"}
    assert len(data["ch"][1]) == 3
    assert len(data["de"][1]) == 2
    assert len(data["at"][1]) == 2


def test_load_all_countries_missing_file(tmp_path: Path) -> None:
    # Only write CH
    (tmp_path).mkdir(parents=True, exist_ok=True)
    (tmp_path / "providers_ch.json").write_text(json.dumps(_make_json(_CH_MUNIS)), encoding="utf-8")
    with pytest.raises(SystemExit):
        load_all_countries(tmp_path)


# ---------------------------------------------------------------------------
# Tests: build_combined_dataframe
# ---------------------------------------------------------------------------


def test_build_combined_dataframe_shape(tmp_path: Path) -> None:
    all_data = _build_test_all_data(tmp_path)
    df = build_combined_dataframe(all_data)

    # AT: 2 regions (Wien, Steiermark) + subtotal = 3
    # CH: 2 regions (Zürich, Bern) + subtotal = 3
    # DE: 2 regions (Berlin, Bayern) + subtotal = 3
    # Grand total = 1
    # Total rows = 10
    assert len(df) == 10
    assert "country" in df.columns
    assert "region" in df.columns
    assert "us_pct" in df.columns
    assert "dom_pct" in df.columns


def test_build_combined_dataframe_totals(tmp_path: Path) -> None:
    all_data = _build_test_all_data(tmp_path)
    df = build_combined_dataframe(all_data)

    # CH subtotal: 3 municipalities (2 microsoft in ZH, 1 domestic in BE)
    ch_total = df[(df["country"] == "CH") & (df["region"] == "Total")]
    assert len(ch_total) == 1
    assert int(ch_total.iloc[0]["total"]) == 3
    assert int(ch_total.iloc[0]["microsoft"]) == 2
    assert int(ch_total.iloc[0]["domestic"]) == 1

    # DE subtotal: 2 municipalities (1 domestic in Berlin, 1 google in Bayern)
    de_total = df[(df["country"] == "DE") & (df["region"] == "Total")]
    assert int(de_total.iloc[0]["total"]) == 2
    assert int(de_total.iloc[0]["google"]) == 1
    assert int(de_total.iloc[0]["domestic"]) == 1

    # AT subtotal: 2 municipalities (1 aws in Wien, 1 domestic in Steiermark)
    at_total = df[(df["country"] == "AT") & (df["region"] == "Total")]
    assert int(at_total.iloc[0]["total"]) == 2
    assert int(at_total.iloc[0]["aws"]) == 1
    assert int(at_total.iloc[0]["domestic"]) == 1

    # Grand total: 7 municipalities
    grand = df[df["country"] == "ALL"]
    assert int(grand.iloc[0]["total"]) == 7


def test_build_combined_dataframe_us_pct(tmp_path: Path) -> None:
    all_data = _build_test_all_data(tmp_path)
    df = build_combined_dataframe(all_data)

    # CH Zürich: 2 microsoft out of 2 → 100%
    zh = df[(df["country"] == "CH") & (df["region"] == "Zürich")]
    assert float(zh.iloc[0]["us_pct"]) == 100.0

    # CH Bern: 1 domestic out of 1 → 0%
    be = df[(df["country"] == "CH") & (df["region"] == "Bern")]
    assert float(be.iloc[0]["us_pct"]) == 0.0

    # Grand total: 4 US cloud (2 msft + 1 google + 1 aws) out of 7
    grand = df[df["country"] == "ALL"]
    expected_us_pct = round(4 / 7 * 100, 1)
    assert float(grand.iloc[0]["us_pct"]) == expected_us_pct


def test_build_combined_dataframe_sorted_by_us_pct(tmp_path: Path) -> None:
    all_data = _build_test_all_data(tmp_path)
    df = build_combined_dataframe(all_data)

    # Within CH: Zürich (100%) should come before Bern (0%)
    ch_regions = df[(df["country"] == "CH") & (df["region"] != "Total")]
    us_pcts = list(ch_regions["us_pct"])
    assert us_pcts == sorted(us_pcts, reverse=True)


# ---------------------------------------------------------------------------
# Tests: latex_combined_regional
# ---------------------------------------------------------------------------


def test_latex_combined_regional_structure(tmp_path: Path) -> None:
    all_data = _build_test_all_data(tmp_path)
    df = build_combined_dataframe(all_data)
    tex = latex_combined_regional(df)

    assert "\\begin{table}" in tex
    assert "\\end{table}" in tex
    assert "\\toprule" in tex
    assert "\\bottomrule" in tex
    assert "\\caption{" in tex
    assert "tab:combined-regional" in tex


def test_latex_combined_regional_contains_all_countries(tmp_path: Path) -> None:
    all_data = _build_test_all_data(tmp_path)
    df = build_combined_dataframe(all_data)
    tex = latex_combined_regional(df)

    assert "CH" in tex
    assert "DE" in tex
    assert "AT" in tex
    assert "ALL" in tex


def test_latex_combined_regional_has_colors(tmp_path: Path) -> None:
    all_data = _build_test_all_data(tmp_path)
    df = build_combined_dataframe(all_data)
    tex = latex_combined_regional(df)

    assert "\\definecolor{ushigh}" in tex
    assert "\\definecolor{usmid}" in tex
    assert "\\definecolor{uslow}" in tex
    assert "\\cellcolor{" in tex


def test_latex_combined_numbers_match_dataframe(tmp_path: Path) -> None:
    all_data = _build_test_all_data(tmp_path)
    df = build_combined_dataframe(all_data)
    tex = latex_combined_regional(df)

    # Grand total row should show total=7
    grand = df[df["country"] == "ALL"].iloc[0]
    assert str(int(grand["total"])) in tex

    # CH Zürich has 2 municipalities — should appear in tex
    assert "rich" in tex  # "Zürich" rendered in LaTeX


# ---------------------------------------------------------------------------
# Tests: build_country_overview / latex_country_overview
# ---------------------------------------------------------------------------


def test_build_country_overview(tmp_path: Path) -> None:
    all_data = _build_test_all_data(tmp_path)
    df = build_country_overview(all_data)

    # 3 countries + 1 total row = 4
    assert len(df) == 4
    assert list(df["country"]) == ["Germany", "Austria", "Switzerland", "Total"]

    # AT: 2 munis (1 aws, 1 domestic)
    at = df[df["country"] == "Austria"].iloc[0]
    assert int(at["total"]) == 2
    assert int(at["aws"]) == 1
    assert int(at["domestic"]) == 1

    # Total row
    total = df[df["country"] == "Total"].iloc[0]
    assert int(total["total"]) == 7


def test_build_country_overview_confidence(tmp_path: Path) -> None:
    all_data = _build_test_all_data(tmp_path)
    df = build_country_overview(all_data)

    # CH has confidences 95, 90, 85 → avg 90.0
    ch = df[df["country"] == "Switzerland"].iloc[0]
    assert float(ch["avg_conf"]) == 90.0
    assert int(ch["low_conf"]) == 0


def test_latex_country_overview_structure(tmp_path: Path) -> None:
    all_data = _build_test_all_data(tmp_path)
    df = build_country_overview(all_data)
    tex = latex_country_overview(df)

    assert "\\begin{table}" in tex
    assert "\\end{table}" in tex
    assert "tab:country-overview" in tex
    assert "Austria" in tex
    assert "Switzerland" in tex
    assert "Germany" in tex
    assert "Conf." in tex
    assert "GW" in tex
    assert "MS" in tex
    assert "GOOG" in tex
    assert "AWS" in tex
    assert "threeparttable" in tex
    assert "tablenotes" in tex
    # No cell colors in the overview table
    assert "\\cellcolor" not in tex


# ---------------------------------------------------------------------------
# Tests: export_combined_latex
# ---------------------------------------------------------------------------


def test_export_combined_latex_writes_file(tmp_path: Path) -> None:
    _write_provider_files(tmp_path)
    output = tmp_path / "combined.tex"
    result = export_combined_latex(providers_dir=tmp_path, output_path=output)

    assert result == output
    assert output.exists()
    content = output.read_text(encoding="utf-8")

    assert "Auto-generated" in content
    assert "tab:combined-regional" in content
    assert "\\begin{table}" in content
    assert "\\end{table}" in content
    assert "tab:country-overview" in content
    assert "tab:combined-regional" in content
    assert content.count("\\begin{table}") == 2
    assert content.count("\\end{table}") == 2
