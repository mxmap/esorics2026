"""Tests for the LaTeX export module."""

from __future__ import annotations

from pathlib import Path

from mail_municipalities.analysis.provider_latex import (
    export_latex,
    latex_confidence,
    latex_domain_sharing,
    latex_gateways,
    latex_low_confidence,
    latex_overall_summary,
    latex_regional,
    latex_signals,
)

# ---------------------------------------------------------------------------
# Reuse synthetic data from test_analyze
# ---------------------------------------------------------------------------

_CATEGORY_MAP = {
    "microsoft": "us-cloud",
    "google": "us-cloud",
    "aws": "us-cloud",
    "domestic": "ch-based",
    "foreign": "foreign",
    "unknown": "unknown",
}
_REGION_LOOKUP = {
    "Kanton Zürich": "ZH",
    "Kanton Bern": "BE",
    "Kanton Genf": "GE",
}
_CC = "ch"

_MUNIS = {
    "1": {
        "code": "1",
        "name": "Zurich Town",
        "region": "Kanton Zürich",
        "domain": "zurich.ch",
        "provider": "microsoft",
        "category": "us-cloud",
        "classification_confidence": 95.0,
        "classification_signals": [
            {"kind": "mx", "provider": "microsoft", "weight": 0.2, "detail": "mx match"},
            {"kind": "spf", "provider": "microsoft", "weight": 0.2, "detail": "spf match"},
            {"kind": "autodiscover", "provider": "microsoft", "weight": 0.08, "detail": "ad match"},
        ],
        "mx": ["mail.protection.outlook.com"],
        "spf": "v=spf1 include:spf.protection.outlook.com -all",
        "gateway": None,
    },
    "2": {
        "code": "2",
        "name": "Bern Village",
        "region": "Kanton Bern",
        "domain": "bern.ch",
        "provider": "unknown",
        "category": "unknown",
        "classification_confidence": 90.0,
        "classification_signals": [
            {"kind": "mx", "provider": "unknown", "weight": 0.2, "detail": "mx match"},
            {"kind": "spf", "provider": "unknown", "weight": 0.2, "detail": "spf match"},
        ],
        "mx": ["mail.bern.ch"],
        "spf": "v=spf1 a mx -all",
        "gateway": None,
    },
    "3": {
        "code": "3",
        "name": "Genf City",
        "region": "Kanton Genf",
        "domain": "shared.ch",
        "provider": "domestic",
        "category": "ch-based",
        "classification_confidence": 50.0,
        "classification_signals": [
            {"kind": "spf", "provider": "domestic", "weight": 0.2, "detail": "spf match"},
        ],
        "mx": ["mx.localmail.ch"],
        "spf": "v=spf1 include:spf.localmail.ch -all",
        "gateway": "seppmail",
    },
    "4": {
        "code": "4",
        "name": "Genf Town",
        "region": "Kanton Genf",
        "domain": "shared.ch",
        "provider": "domestic",
        "category": "ch-based",
        "classification_confidence": 55.0,
        "classification_signals": [
            {"kind": "spf", "provider": "domestic", "weight": 0.2, "detail": "spf match"},
            {"kind": "mx", "provider": "microsoft", "weight": 0.2, "detail": "mx conflict"},
        ],
        "mx": ["mx.localmail.ch"],
        "spf": "v=spf1 include:spf.localmail.ch -all",
        "gateway": "seppmail",
    },
    "5": {
        "code": "5",
        "name": "No Signal Town",
        "region": "",
        "domain": "nosignal.ch",
        "provider": "unknown",
        "category": "unknown",
        "classification_confidence": 60.0,
        "classification_signals": [],
        "mx": [],
        "spf": "",
        "gateway": None,
    },
}

_DATA = {
    "generated": "2026-03-24T00:00:00Z",
    "commit": "abc1234",
    "total": 5,
    "counts": {"microsoft": 1, "unknown": 2, "domestic": 2},
    "municipalities": list(_MUNIS.values()),
}


# ---------------------------------------------------------------------------
# Helper to assert valid LaTeX table structure
# ---------------------------------------------------------------------------


def _assert_valid_table(tex: str) -> None:
    assert "\\begin{table}" in tex
    assert "\\end{table}" in tex
    assert "\\toprule" in tex
    assert "\\bottomrule" in tex
    assert "\\caption{" in tex
    assert "\\label{" in tex


# ---------------------------------------------------------------------------
# Individual table tests
# ---------------------------------------------------------------------------


def test_latex_overall_summary() -> None:
    tex = latex_overall_summary(_MUNIS, _CATEGORY_MAP, _CC)
    _assert_valid_table(tex)
    assert "tab:overall-ch" in tex
    assert "Switzerland" in tex
    assert "Microsoft" in tex
    assert "US Cloud" in tex


def test_latex_regional() -> None:
    tex = latex_regional(_MUNIS, _CATEGORY_MAP, _REGION_LOOKUP, _CC)
    _assert_valid_table(tex)
    assert "tab:regional-ch" in tex
    assert "ZH" in tex
    assert "BE" in tex
    assert "GE" in tex


def test_latex_confidence() -> None:
    tex = latex_confidence(_MUNIS, _CC)
    _assert_valid_table(tex)
    assert "tab:confidence-ch" in tex
    assert "90--100" in tex
    assert "Provider" in tex


def test_latex_signals() -> None:
    tex = latex_signals(_MUNIS, _CC)
    assert "tab:signals-ch" in tex
    assert "tab:signal-combos-ch" in tex
    assert tex.count("\\begin{table}") == 2
    assert "spf" in tex
    assert "mx" in tex


def test_latex_gateways() -> None:
    tex = latex_gateways(_MUNIS, _CC)
    _assert_valid_table(tex)
    assert "tab:gateways-ch" in tex
    assert "seppmail" in tex
    assert "With GW" in tex


def test_latex_domain_sharing() -> None:
    tex = latex_domain_sharing(_MUNIS, _CC)
    _assert_valid_table(tex)
    assert "tab:shared-domains-ch" in tex
    assert "shared.ch" in tex


def test_latex_low_confidence() -> None:
    tex = latex_low_confidence(_MUNIS, _REGION_LOOKUP, _CC)
    _assert_valid_table(tex)
    assert "tab:low-confidence-ch" in tex
    assert "Genf City" in tex
    assert "Genf Town" in tex


# ---------------------------------------------------------------------------
# export_latex writes a file with all tables
# ---------------------------------------------------------------------------


def test_export_latex_writes_file(tmp_path: Path) -> None:
    tex_path = tmp_path / "tables_ch_test.tex"
    result = export_latex(_DATA, _MUNIS, _CATEGORY_MAP, _REGION_LOOKUP, _CC, tex_path)

    assert result == tex_path
    assert tex_path.exists()

    content = tex_path.read_text(encoding="utf-8")

    # Header comments
    assert "Auto-generated" in content
    assert "abc1234" in content

    # All 7 table labels present
    for label in [
        "tab:overall-ch",
        "tab:regional-ch",
        "tab:confidence-ch",
        "tab:signals-ch",
        "tab:signal-combos-ch",
        "tab:gateways-ch",
        "tab:shared-domains-ch",
        "tab:low-confidence-ch",
    ]:
        assert label in content

    # Should have 8 tables total (signals produces 2)
    assert content.count("\\begin{table}") == 8
    assert content.count("\\end{table}") == 8


def test_export_latex_creates_parent_dirs(tmp_path: Path) -> None:
    tex_path = tmp_path / "sub" / "dir" / "tables.tex"
    export_latex(_DATA, _MUNIS, _CATEGORY_MAP, _REGION_LOOKUP, _CC, tex_path)
    assert tex_path.exists()
