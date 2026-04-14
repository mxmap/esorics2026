"""Tests for chi-square independence tests in charts module."""

import pandas as pd
import pytest

from mail_municipalities.analysis.charts import (
    compute_chi_square_tests,
    compute_gateway_chi_square,
)


def _make_df(
    categories: list[str],
    metric_vals: list[bool],
    gateways: list[str] | None = None,
) -> pd.DataFrame:
    """Build a minimal DataFrame matching the expected export schema."""
    n = len(categories)
    assert len(metric_vals) == n
    return pd.DataFrame({
        "scan_valid": [True] * n,
        "category": categories,
        "gateway": gateways if gateways is not None else [""] * n,
        "has_spf": metric_vals,
        "has_good_spf": metric_vals,
        "has_dmarc": metric_vals,
        "has_good_dmarc": metric_vals,
        "dane_supported": metric_vals,
    })


class TestChiSquare:
    def test_significant_difference(self) -> None:
        """US Cloud all True, Domestic all False → large chi2, tiny p."""
        cats = ["us-cloud"] * 50 + ["de-based"] * 50
        vals = [True] * 50 + [False] * 50
        results = compute_chi_square_tests(_make_df(cats, vals))

        assert len(results) == 5
        for r in results:
            assert r["chi2"] > 50
            assert r["p"] < 0.001
            assert r["dof"] == 1
            assert r["n"] == 100

    def test_no_difference(self) -> None:
        """Identical distributions → chi2 near 0, p near 1."""
        cats = ["us-cloud"] * 50 + ["de-based"] * 50
        vals = [True, False] * 25 + [True, False] * 25
        results = compute_chi_square_tests(_make_df(cats, vals))

        for r in results:
            assert r["chi2"] < 1
            assert r["p"] > 0.3

    def test_excludes_other_category(self) -> None:
        """Rows with 'foreign' or 'unknown' are excluded from n."""
        cats = ["us-cloud"] * 30 + ["de-based"] * 30 + ["foreign"] * 20
        vals = [True] * 30 + [False] * 30 + [True] * 20
        results = compute_chi_square_tests(_make_df(cats, vals))

        assert results[0]["n"] == 60  # 'foreign' excluded

    def test_excludes_invalid_scans(self) -> None:
        """Rows with scan_valid=False are excluded."""
        df = _make_df(["us-cloud"] * 40 + ["de-based"] * 40, [True] * 40 + [False] * 40)
        df.loc[70:, "scan_valid"] = False  # mark last 10 as invalid
        results = compute_chi_square_tests(df)

        assert results[0]["n"] == 70

    def test_all_same_metric_value(self) -> None:
        """When metric is True everywhere, chi2 should be 0."""
        cats = ["us-cloud"] * 30 + ["ch-based"] * 30
        vals = [True] * 60
        results = compute_chi_square_tests(_make_df(cats, vals))

        for r in results:
            assert r["chi2"] == pytest.approx(0.0, abs=0.01)
            assert r["p"] > 0.9


class TestGatewayChiSquare:
    def test_gateway_effect(self) -> None:
        """Gateway municipalities all True, non-gateway all False."""
        cats = ["de-based"] * 100
        vals = [True] * 40 + [False] * 60
        gws = ["SEPPmail"] * 40 + [""] * 60
        results = compute_gateway_chi_square(_make_df(cats, vals, gateways=gws))

        assert len(results) == 4  # no DANE in gateway metrics
        for r in results:
            assert r["chi2"] > 50
            assert r["p"] < 0.001
            assert r["delta_pp"] > 0

    def test_no_gateway_effect(self) -> None:
        """Same metric distribution regardless of gateway."""
        cats = ["de-based"] * 100
        vals = [True, False] * 50
        gws = ["SEPPmail"] * 50 + [""] * 50
        results = compute_gateway_chi_square(_make_df(cats, vals, gateways=gws))

        for r in results:
            assert r["chi2"] < 1
            assert r["p"] > 0.3
            assert abs(r["delta_pp"]) < 5

    def test_negative_delta(self) -> None:
        """Gateway reduces metric adoption → negative delta."""
        cats = ["de-based"] * 100
        vals = [False] * 40 + [True] * 60
        gws = ["Sophos"] * 40 + [""] * 60
        results = compute_gateway_chi_square(_make_df(cats, vals, gateways=gws))

        for r in results:
            assert r["delta_pp"] < 0
