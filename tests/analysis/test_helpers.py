"""Tests for shared LaTeX formatting helpers."""

from mail_municipalities.analysis.helpers import esc, make_region_lookup, num, pct, region_name


def test_num_below_thousand() -> None:
    assert num(0) == "0"
    assert num(999) == "999"


def test_num_thousands_separator() -> None:
    assert num(1_000) == "1{,}000"
    assert num(15_331) == "15{,}331"


def test_pct_normal() -> None:
    assert pct(1, 4) == "25.0"
    assert pct(1, 3) == "33.3"


def test_pct_zero_denominator() -> None:
    assert pct(0, 0) == "0.0"
    assert pct(5, 0) == "0.0"


def test_esc_special_chars() -> None:
    assert esc("a & b") == "a \\& b"
    assert esc("100%") == "100\\%"
    assert esc("foo_bar") == "foo\\_bar"
    assert esc("sec#1") == "sec\\#1"


def test_esc_no_special() -> None:
    assert esc("hello") == "hello"


def test_region_name_strips_kanton() -> None:
    assert region_name("Kanton Zürich") == "Zürich"


def test_region_name_truncates_long() -> None:
    result = region_name("Mecklenburg-Vorpommern")
    assert len(result) <= 18
    assert result.endswith(".")


def test_region_name_short_passthrough() -> None:
    assert region_name("Bayern") == "Bayern"


def test_make_region_lookup_ch() -> None:
    lookup = make_region_lookup("ch")
    assert lookup["Kanton Zürich"] == "ZH"
    assert lookup["Kanton Bern"] == "BE"


def test_make_region_lookup_unknown() -> None:
    lookup = make_region_lookup("xx")
    assert lookup == {}
