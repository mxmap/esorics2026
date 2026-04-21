"""Tests for single-country security LaTeX export."""

from __future__ import annotations

from pathlib import Path

from mail_municipalities.analysis.security_latex import (
    export_security_latex,
    latex_security_regional,
    latex_security_summary,
)

# ---------------------------------------------------------------------------
# Synthetic data
# ---------------------------------------------------------------------------

_REGION_LOOKUP = {
    "Kanton Zürich": "ZH",
    "Kanton Bern": "BE",
    "Kanton Genf": "GE",
}

_MUNIS = [
    {
        "code": "1",
        "name": "Zurich Town",
        "region": "Kanton Zürich",
        "domain": "zurich.ch",
        "mx_records": ["mx1.zurich.ch"],
        "dane": {"supported": True, "partial": True},
        "dss": {
            "has_spf": True,
            "has_good_spf": True,
            "has_dmarc": True,
            "has_good_dmarc": True,
            "has_dkim": False,
        },
        "scan_valid": True,
    },
    {
        "code": "2",
        "name": "Bern Village",
        "region": "Kanton Bern",
        "domain": "bern.ch",
        "mx_records": ["mx1.bern.ch"],
        "dane": {"supported": False, "partial": False},
        "dss": {
            "has_spf": True,
            "has_good_spf": False,
            "has_dmarc": True,
            "has_good_dmarc": False,
            "has_dkim": False,
        },
        "scan_valid": True,
    },
    {
        "code": "3",
        "name": "Genf City",
        "region": "Kanton Genf",
        "domain": "genf.ch",
        "mx_records": ["mx1.genf.ch"],
        "dane": {"supported": False, "partial": False},
        "dss": {
            "has_spf": True,
            "has_good_spf": True,
            "has_dmarc": False,
            "has_good_dmarc": False,
            "has_dkim": False,
        },
        "scan_valid": True,
    },
    {
        "code": "4",
        "name": "Genf Town",
        "region": "Kanton Genf",
        "domain": "genf2.ch",
        "mx_records": [],
        "dane": None,
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
        "code": "5",
        "name": "Invalid Town",
        "region": "Kanton Zürich",
        "domain": "invalid.ch",
        "mx_records": [],
        "dane": None,
        "dss": None,
        "scan_valid": False,
    },
]

_DATA = {
    "generated": "2026-04-10T00:00:00Z",
    "commit": "abc1234",
    "total": 5,
    "counts": {"scanned": 4, "spf": 3, "good_spf": 2, "dmarc": 2, "good_dmarc": 1, "dane_supported": 1},
    "municipalities": _MUNIS,
}


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def _assert_valid_table(tex: str) -> None:
    assert "\\begin{table}" in tex
    assert "\\end{table}" in tex
    assert "\\toprule" in tex
    assert "\\bottomrule" in tex
    assert "\\caption{" in tex
    assert "\\label{" in tex


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_latex_security_summary() -> None:
    tex = latex_security_summary(_MUNIS, "ch")
    _assert_valid_table(tex)
    assert "tab:security-ch" in tex
    assert "Switzerland" in tex
    assert "SPF" in tex
    assert "DMARC" in tex
    assert "DANE" in tex
    # DKIM must not appear
    assert "DKIM" not in tex
    # Only scan_valid=True municipalities counted (4, not 5)
    assert "$n=4$" in tex


def test_latex_security_regional() -> None:
    tex = latex_security_regional(_MUNIS, _REGION_LOOKUP, "ch")
    _assert_valid_table(tex)
    assert "tab:security-regional-ch" in tex
    assert "ZH" in tex
    assert "BE" in tex
    assert "GE" in tex
    assert "sorted by DMARC" in tex


def test_latex_security_regional_sorted_by_dmarc() -> None:
    tex = latex_security_regional(_MUNIS, _REGION_LOOKUP, "ch")
    lines = tex.split("\n")
    region_lines = [line for line in lines if any(r in line for r in ("ZH", "BE", "GE"))]
    # ZH and BE have 100% DMARC, GE has 0% — GE should be last
    assert len(region_lines) == 3
    assert "GE" in region_lines[-1]


def test_export_security_latex_writes_file(tmp_path: Path) -> None:
    tex_path = tmp_path / "tables_security_ch_test.tex"
    result = export_security_latex(_DATA, "ch", tex_path)

    assert result == tex_path
    assert tex_path.exists()

    content = tex_path.read_text(encoding="utf-8")

    assert "Auto-generated" in content
    assert "abc1234" in content
    assert "tab:security-ch" in content
    assert "tab:security-regional-ch" in content
    assert content.count("\\begin{table}") == 2
    assert content.count("\\end{table}") == 2


def test_export_security_latex_creates_parent_dirs(tmp_path: Path) -> None:
    tex_path = tmp_path / "sub" / "dir" / "tables.tex"
    export_security_latex(_DATA, "ch", tex_path)
    assert tex_path.exists()
