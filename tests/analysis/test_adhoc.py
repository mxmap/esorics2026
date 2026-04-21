"""Tests for adhoc analysis helpers."""

import pandas as pd

from mail_municipalities.analysis.adhoc import _classify_mx_infra, _pct, _security_row


class TestPct:
    def test_normal(self) -> None:
        s = pd.Series([True, False, True, True])
        assert _pct(s) == 75.0

    def test_all_true(self) -> None:
        assert _pct(pd.Series([True, True])) == 100.0

    def test_all_false(self) -> None:
        assert _pct(pd.Series([False, False])) == 0.0

    def test_empty_series(self) -> None:
        assert _pct(pd.Series([], dtype=bool)) == 0.0


class TestSecurityRow:
    def test_basic(self) -> None:
        df = pd.DataFrame(
            {
                "has_spf": [True, False],
                "has_good_spf": [True, True],
                "has_dmarc": [False, False],
                "has_good_dmarc": [False, False],
                "dane_supported": [True, False],
            }
        )
        row = _security_row("test", df)
        assert row["group"] == "test"
        assert row["n"] == 2
        assert row["has_spf"] == 50.0
        assert row["has_good_spf"] == 100.0
        assert row["has_dmarc"] == 0.0
        assert row["dane_supported"] == 50.0

    def test_empty(self) -> None:
        cols = ["has_spf", "has_good_spf", "has_dmarc", "has_good_dmarc", "dane_supported"]
        df = pd.DataFrame({c: pd.Series([], dtype=bool) for c in cols})
        row = _security_row("empty", df)
        assert row["n"] == 0
        assert all(row[c] == 0.0 for c in cols)


class TestClassifyMxInfra:
    def test_known_providers(self) -> None:
        assert _classify_mx_infra("mail.rzone.de.") == "Strato"
        assert _classify_mx_infra("mx.infomaniak.ch") == "Infomaniak"
        assert _classify_mx_infra("mx00.ionos.de") == "IONOS"
        assert _classify_mx_infra("mx00.ionos.com") == "IONOS"
        assert _classify_mx_infra("kasserver.com") == "All-Inkl"
        assert _classify_mx_infra("mx.hostpoint.ch") == "Hostpoint"
        assert _classify_mx_infra("gw.seppmail.ch") == "SEPPmail"
        assert _classify_mx_infra("mx.sophos.com") == "Sophos"

    def test_unknown(self) -> None:
        assert _classify_mx_infra("mail.example.com") == "other"

    def test_empty_and_nan(self) -> None:
        assert _classify_mx_infra("") == "other"
        assert _classify_mx_infra(float("nan")) == "other"
        assert _classify_mx_infra("  ") == "other"

    def test_case_insensitive(self) -> None:
        assert _classify_mx_infra("MAIL.RZONE.DE") == "Strato"

    def test_first_match_wins(self) -> None:
        # seppmail appears before sophos in rules
        assert _classify_mx_infra("seppmail.sophos.example") == "SEPPmail"
