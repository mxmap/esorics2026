"""Unit tests for provider classification validation with synthetic data."""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any

import pytest

from mail_municipalities.provider_classification.models import SignalKind
from mail_municipalities.provider_classification.probes import WEIGHTS
from mail_municipalities.provider_classification.runner import _build_category_map
from mail_municipalities.provider_classification.validate import (
    ValidationResult,
    _check_entry,
    _check_metadata,
    _check_signal,
    _infer_country,
    _normalize_baseline,
    _valid_categories,
    validate_regression,
    validate_structure,
    run_validation,
)

# ── Factory helpers ──────────────────────────────────────────────────


def _make_signal(**overrides: Any) -> dict[str, Any]:
    sig: dict[str, Any] = {
        "kind": "mx",
        "provider": "microsoft",
        "weight": WEIGHTS[SignalKind.MX],
        "detail": "mx1.example.com",
    }
    sig.update(overrides)
    return sig


def _make_entry(code: str = "1", **overrides: Any) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "code": code,
        "name": "Test Municipality",
        "region": "ZH",
        "domain": "example.ch",
        "mx": ["mx1.example.com"],
        "spf": "v=spf1 include:spf.protection.outlook.com -all",
        "provider": "microsoft",
        "category": "us-cloud",
        "classification_confidence": 80.0,
        "classification_signals": [_make_signal()],
    }
    entry.update(overrides)
    return entry


def _make_output(entries: list[dict[str, Any]] | None = None, **overrides: Any) -> dict[str, Any]:
    if entries is None:
        entries = [_make_entry(str(i + 1)) for i in range(3)]
    counts: dict[str, int] = Counter()
    for e in entries:
        counts[e.get("provider", "?")] += 1
    data: dict[str, Any] = {
        "generated": "2026-01-01T00:00:00",
        "total": len(entries),
        "counts": dict(sorted(counts.items())),
        "municipalities": entries,
    }
    data.update(overrides)
    return data


# ── TestValidationResult ─────────────────────────────────────────────


class TestValidationResult:
    def test_initial_state(self):
        r = ValidationResult()
        assert r.errors == []
        assert r.warnings == []
        assert r.passed == 0

    def test_ok_increments_passed(self):
        r = ValidationResult()
        r.ok("check 1")
        r.ok()
        assert r.passed == 2

    def test_error_appends(self):
        r = ValidationResult()
        r.error("something broke")
        assert r.errors == ["something broke"]

    def test_warn_appends(self):
        r = ValidationResult()
        r.warn("heads up")
        assert r.warnings == ["heads up"]

    def test_success_true_when_no_errors(self):
        r = ValidationResult()
        r.warn("just a warning")
        assert r.success is True

    def test_success_false_when_errors(self):
        r = ValidationResult()
        r.error("bad")
        assert r.success is False

    def test_summary_markup_passed_only(self):
        r = ValidationResult()
        r.ok()
        markup = r.summary_markup()
        assert "1 passed" in markup
        assert "warning" not in markup
        assert "error" not in markup

    def test_summary_markup_with_warnings(self):
        r = ValidationResult()
        r.ok()
        r.warn("w")
        markup = r.summary_markup()
        assert "1 warnings" in markup

    def test_summary_markup_with_errors(self):
        r = ValidationResult()
        r.ok()
        r.error("e")
        markup = r.summary_markup()
        assert "1 errors" in markup

    def test_summary_markup_all_three(self):
        r = ValidationResult()
        r.ok()
        r.warn("w")
        r.error("e")
        markup = r.summary_markup()
        assert "passed" in markup
        assert "warning" in markup
        assert "error" in markup


# ── TestCheckMetadata ────────────────────────────────────────────────


class TestCheckMetadata:
    def test_valid_metadata(self):
        data = _make_output()
        r = ValidationResult()
        _check_metadata(data, r)
        assert r.success
        assert r.passed >= 3

    @pytest.mark.parametrize("field", ["generated", "total", "counts", "municipalities"])
    def test_missing_required_field(self, field):
        data = _make_output()
        del data[field]
        r = ValidationResult()
        _check_metadata(data, r)
        assert not r.success
        assert any(field in e for e in r.errors)

    def test_total_not_int(self):
        data = _make_output(total="abc")
        r = ValidationResult()
        _check_metadata(data, r)
        assert not r.success
        assert any("total" in e for e in r.errors)

    def test_total_zero(self):
        data = _make_output(total=0)
        r = ValidationResult()
        _check_metadata(data, r)
        assert not r.success

    def test_total_negative(self):
        data = _make_output(total=-1)
        r = ValidationResult()
        _check_metadata(data, r)
        assert not r.success

    def test_municipalities_not_list(self):
        data = _make_output(municipalities={})
        r = ValidationResult()
        _check_metadata(data, r)
        assert not r.success
        assert any("list" in e for e in r.errors)

    def test_total_mismatch(self):
        data = _make_output()
        data["total"] = 999
        # Also fix counts sum to match total so we isolate the mismatch check
        r = ValidationResult()
        _check_metadata(data, r)
        assert any("total=" in e for e in r.errors)

    def test_counts_sum_mismatch(self):
        data = _make_output()
        data["counts"]["microsoft"] = 999
        r = ValidationResult()
        _check_metadata(data, r)
        assert any("counts sum" in e for e in r.errors)

    def test_counts_distribution_mismatch(self):
        data = _make_output()
        # Add a fake provider count that doesn't match any entries
        data["counts"]["google"] = 0
        r = ValidationResult()
        _check_metadata(data, r)
        assert any("distribution" in e for e in r.errors)

    def test_counts_keys_not_sorted(self):
        entries = [
            _make_entry("1", provider="microsoft"),
            _make_entry("2", provider="google", category="us-cloud"),
        ]
        data = _make_output(entries=entries)
        # Force unsorted keys
        data["counts"] = {"microsoft": 1, "google": 1}
        r = ValidationResult()
        _check_metadata(data, r)
        assert any("sorted" in w for w in r.warnings)


# ── TestCheckEntry ───────────────────────────────────────────────────


class TestCheckEntry:
    @pytest.fixture()
    def ctx(self):
        cat_map = _build_category_map("ch")
        valid_cats = _valid_categories("ch")
        return cat_map, valid_cats

    def test_valid_entry(self, ctx):
        cat_map, valid_cats = ctx
        entry = _make_entry()
        r = ValidationResult()
        _check_entry(entry, r, cat_map, valid_cats)
        assert r.success

    @pytest.mark.parametrize(
        "field",
        [
            "code",
            "name",
            "region",
            "domain",
            "mx",
            "spf",
            "provider",
            "category",
            "classification_confidence",
            "classification_signals",
        ],
    )
    def test_missing_required_field(self, ctx, field):
        cat_map, valid_cats = ctx
        entry = _make_entry()
        del entry[field]
        r = ValidationResult()
        _check_entry(entry, r, cat_map, valid_cats)
        assert not r.success
        assert any("missing field" in e for e in r.errors)

    def test_unknown_provider_value(self, ctx):
        cat_map, valid_cats = ctx
        entry = _make_entry(provider="bogus", category="unknown")
        r = ValidationResult()
        _check_entry(entry, r, cat_map, valid_cats)
        assert any("unknown provider" in e for e in r.errors)

    def test_unknown_category_value(self, ctx):
        cat_map, valid_cats = ctx
        entry = _make_entry(category="bogus")
        r = ValidationResult()
        _check_entry(entry, r, cat_map, valid_cats)
        assert any("unknown category" in e for e in r.errors)

    def test_category_mismatch(self, ctx):
        cat_map, valid_cats = ctx
        entry = _make_entry(provider="microsoft", category="foreign")
        r = ValidationResult()
        _check_entry(entry, r, cat_map, valid_cats)
        assert any("should map to" in e for e in r.errors)

    def test_confidence_negative(self, ctx):
        cat_map, valid_cats = ctx
        entry = _make_entry(classification_confidence=-1)
        r = ValidationResult()
        _check_entry(entry, r, cat_map, valid_cats)
        assert any("out of" in e for e in r.errors)

    def test_confidence_above_100(self, ctx):
        cat_map, valid_cats = ctx
        entry = _make_entry(classification_confidence=101)
        r = ValidationResult()
        _check_entry(entry, r, cat_map, valid_cats)
        assert any("out of" in e for e in r.errors)

    def test_confidence_not_number(self, ctx):
        cat_map, valid_cats = ctx
        entry = _make_entry(classification_confidence="high")
        r = ValidationResult()
        _check_entry(entry, r, cat_map, valid_cats)
        assert any("out of" in e for e in r.errors)

    def test_unknown_provider_nonzero_confidence(self, ctx):
        cat_map, valid_cats = ctx
        entry = _make_entry(
            provider="unknown",
            category="unknown",
            classification_confidence=50,
            classification_signals=[],
        )
        r = ValidationResult()
        _check_entry(entry, r, cat_map, valid_cats)
        assert any("expected 0" in e for e in r.errors)

    def test_unknown_provider_with_signals(self, ctx):
        cat_map, valid_cats = ctx
        entry = _make_entry(
            provider="unknown",
            category="unknown",
            classification_confidence=0,
            classification_signals=[_make_signal()],
        )
        r = ValidationResult()
        _check_entry(entry, r, cat_map, valid_cats)
        assert any("unknown provider with signals" in w for w in r.warnings)

    def test_domain_with_at_sign(self, ctx):
        cat_map, valid_cats = ctx
        entry = _make_entry(domain="user@example.ch")
        r = ValidationResult()
        _check_entry(entry, r, cat_map, valid_cats)
        assert any("email" in e for e in r.errors)

    def test_empty_domain_non_unknown_provider(self, ctx):
        cat_map, valid_cats = ctx
        entry = _make_entry(domain="")
        r = ValidationResult()
        _check_entry(entry, r, cat_map, valid_cats)
        assert any("empty domain" in w for w in r.warnings)

    def test_empty_gateway_field(self, ctx):
        cat_map, valid_cats = ctx
        entry = _make_entry(gateway="")
        r = ValidationResult()
        _check_entry(entry, r, cat_map, valid_cats)
        assert any("gateway" in w for w in r.warnings)


# ── TestCheckSignal ──────────────────────────────────────────────────


class TestCheckSignal:
    def test_valid_signal(self):
        r = ValidationResult()
        _check_signal("1", _make_signal(), r)
        assert r.success

    @pytest.mark.parametrize("field", ["kind", "provider", "weight", "detail"])
    def test_missing_field(self, field):
        sig = _make_signal()
        del sig[field]
        r = ValidationResult()
        _check_signal("1", sig, r)
        assert any("missing" in e for e in r.errors)

    def test_unknown_signal_kind(self):
        # Use out-of-range weight to avoid SignalKind() crash in weight-matching branch
        sig = _make_signal(kind="bogus", weight=1.5)
        r = ValidationResult()
        _check_signal("1", sig, r)
        assert any("unknown signal kind" in e for e in r.errors)

    def test_unknown_signal_provider(self):
        sig = _make_signal(provider="bogus")
        r = ValidationResult()
        _check_signal("1", sig, r)
        assert any("unknown signal provider" in e for e in r.errors)

    def test_weight_negative(self):
        sig = _make_signal(weight=-0.1)
        r = ValidationResult()
        _check_signal("1", sig, r)
        assert any("weight" in e and "out of" in e for e in r.errors)

    def test_weight_above_one(self):
        sig = _make_signal(weight=1.5)
        r = ValidationResult()
        _check_signal("1", sig, r)
        assert any("weight" in e and "out of" in e for e in r.errors)

    def test_weight_mismatch(self):
        sig = _make_signal(kind="mx", weight=0.50)
        r = ValidationResult()
        _check_signal("1", sig, r)
        assert any("expected" in e for e in r.errors)

    def test_empty_detail(self):
        sig = _make_signal(detail="")
        r = ValidationResult()
        _check_signal("1", sig, r)
        assert any("detail empty" in w for w in r.warnings)


# ── TestValidateStructure ────────────────────────────────────────────


class TestValidateStructure:
    def test_valid_output_passes(self):
        data = _make_output()
        r = validate_structure(data)
        assert r.success

    def test_early_return_on_metadata_failure(self):
        data = _make_output()
        del data["total"]
        r = validate_structure(data)
        assert not r.success
        # Only metadata error, no entry-level errors
        assert len(r.errors) == 1

    def test_unsorted_codes_warning(self):
        entries = [_make_entry("3"), _make_entry("1"), _make_entry("2")]
        data = _make_output(entries=entries)
        r = validate_structure(data)
        assert any("sorted" in w for w in r.warnings)

    def test_sorted_codes_ok(self):
        entries = [_make_entry("1"), _make_entry("2"), _make_entry("3")]
        data = _make_output(entries=entries)
        r = validate_structure(data)
        assert not any("sorted" in w for w in r.warnings)

    def test_low_confidence_share_warning(self):
        # All entries have confidence < 50
        entries = [_make_entry(str(i + 1), classification_confidence=10.0) for i in range(10)]
        data = _make_output(entries=entries)
        r = validate_structure(data)
        assert any("confidence >= 50" in w for w in r.warnings)

    def test_high_zero_confidence_warning(self):
        # >5% entries have confidence == 0 -> need unknown provider for those
        entries = []
        for i in range(10):
            entries.append(_make_entry(str(i + 1)))
        # Make 2 out of 12 (>5%) unknown with confidence 0
        entries.append(
            _make_entry(
                "11", provider="unknown", category="unknown", classification_confidence=0, classification_signals=[]
            )
        )
        entries.append(
            _make_entry(
                "12", provider="unknown", category="unknown", classification_confidence=0, classification_signals=[]
            )
        )
        data = _make_output(entries=entries)
        r = validate_structure(data)
        assert any("0 confidence" in w for w in r.warnings)


# ── TestNormalizeBaseline ────────────────────────────────────────────


class TestNormalizeBaseline:
    def test_list_format_with_code(self):
        data = {"municipalities": [{"code": "1", "name": "A"}, {"code": "2", "name": "B"}]}
        result = _normalize_baseline(data)
        assert result == {"1": {"code": "1", "name": "A"}, "2": {"code": "2", "name": "B"}}

    def test_list_format_with_bfs_fallback(self):
        data = {"municipalities": [{"bfs": "10", "name": "A"}]}
        result = _normalize_baseline(data)
        assert "10" in result

    def test_dict_format_with_code(self):
        data = {"municipalities": {"k1": {"code": "1", "name": "A"}}}
        result = _normalize_baseline(data)
        assert "1" in result

    def test_dict_format_with_bfs_fallback(self):
        data = {"municipalities": {"k1": {"bfs": "10", "name": "A"}}}
        result = _normalize_baseline(data)
        assert "10" in result

    def test_dict_format_key_fallback(self):
        data = {"municipalities": {"99": {"name": "A"}}}
        result = _normalize_baseline(data)
        assert "99" in result

    def test_empty_municipalities(self):
        data = {"municipalities": {}}
        result = _normalize_baseline(data)
        assert result == {}

    def test_missing_municipalities(self):
        data = {}
        result = _normalize_baseline(data)
        assert result == {}


# ── TestValidateRegression ───────────────────────────────────────────


def _make_regression_data(
    n: int = 10,
    *,
    provider: str = "microsoft",
    confidence: float = 80.0,
) -> dict[str, Any]:
    entries = [_make_entry(str(i + 1), provider=provider, classification_confidence=confidence) for i in range(n)]
    return _make_output(entries=entries)


class TestValidateRegression:
    def test_identical_outputs_pass(self):
        data = _make_regression_data()
        r = validate_regression(data, data)
        assert r.success
        assert hasattr(r, "_regression")

    def test_only_baseline_municipalities_warning(self):
        current = _make_regression_data(n=5)
        baseline = _make_regression_data(n=10)
        r = validate_regression(current, baseline)
        assert any("in baseline but not in current" in w for w in r.warnings)

    def test_only_current_municipalities_warning(self):
        current = _make_regression_data(n=10)
        baseline = _make_regression_data(n=5)
        r = validate_regression(current, baseline)
        assert any("in current but not in baseline" in w for w in r.warnings)

    def test_no_common_municipalities_error(self):
        cur_entries = [_make_entry(str(i + 100)) for i in range(3)]
        base_entries = [_make_entry(str(i + 200)) for i in range(3)]
        current = _make_output(entries=cur_entries)
        baseline = _make_output(entries=base_entries)
        r = validate_regression(current, baseline)
        assert not r.success
        assert any("no common" in e for e in r.errors)

    def test_no_provider_changes_ok(self):
        data = _make_regression_data()
        r = validate_regression(data, data)
        assert r.success
        assert r._regression["provider_changes"] == []  # type: ignore[attr-defined]

    def test_provider_changes_warning(self):
        # 1 change out of 10 = 10% but let's do 1/100 to stay under threshold
        base_entries = [_make_entry(str(i + 1)) for i in range(100)]
        cur_entries = [_make_entry(str(i + 1)) for i in range(100)]
        cur_entries[0] = _make_entry("1", provider="google", category="us-cloud")
        current = _make_output(entries=cur_entries)
        baseline = _make_output(entries=base_entries)
        r = validate_regression(current, baseline)
        assert r.success  # under 5% threshold = warning not error
        assert any("provider change" in w for w in r.warnings)

    def test_provider_changes_exceed_threshold_error(self):
        # 10 changes out of 20 = 50% >> 5%
        base_entries = [_make_entry(str(i + 1)) for i in range(20)]
        cur_entries = [_make_entry(str(i + 1)) for i in range(20)]
        for i in range(10):
            cur_entries[i] = _make_entry(str(i + 1), provider="google", category="us-cloud")
        current = _make_output(entries=cur_entries)
        baseline = _make_output(entries=base_entries)
        r = validate_regression(current, baseline)
        assert not r.success
        assert any("exceeds" in e for e in r.errors)

    def test_confidence_deltas_computed(self):
        base_entries = [_make_entry(str(i + 1), classification_confidence=80.0) for i in range(5)]
        cur_entries = [_make_entry(str(i + 1), classification_confidence=90.0) for i in range(5)]
        current = _make_output(entries=cur_entries)
        baseline = _make_output(entries=base_entries)
        r = validate_regression(current, baseline)
        assert all(d == pytest.approx(10.0) for d in r._regression["deltas"])  # type: ignore[attr-defined]

    def test_big_confidence_drop_warning(self):
        base_entries = [_make_entry(str(i + 1), classification_confidence=90.0) for i in range(5)]
        cur_entries = [_make_entry(str(i + 1), classification_confidence=50.0) for i in range(5)]
        current = _make_output(entries=cur_entries)
        baseline = _make_output(entries=base_entries)
        r = validate_regression(current, baseline)
        assert any("dropped confidence" in w for w in r.warnings)

    def test_no_big_drops_ok(self):
        base_entries = [_make_entry(str(i + 1), classification_confidence=80.0) for i in range(5)]
        cur_entries = [_make_entry(str(i + 1), classification_confidence=75.0) for i in range(5)]
        current = _make_output(entries=cur_entries)
        baseline = _make_output(entries=base_entries)
        r = validate_regression(current, baseline)
        # No big-drop warning
        assert not any("dropped confidence" in w for w in r.warnings)

    def test_new_unknowns_warning(self):
        base_entries = [_make_entry(str(i + 1)) for i in range(20)]
        cur_entries = [_make_entry(str(i + 1)) for i in range(20)]
        cur_entries[0] = _make_entry(
            "1",
            provider="unknown",
            category="unknown",
            classification_confidence=0,
            classification_signals=[],
        )
        current = _make_output(entries=cur_entries)
        baseline = _make_output(entries=base_entries)
        r = validate_regression(current, baseline)
        assert any("became 'unknown'" in w for w in r.warnings)

    def test_regression_data_stashed(self):
        data = _make_regression_data()
        r = validate_regression(data, data)
        reg = r._regression  # type: ignore[attr-defined]
        assert "common" in reg
        assert "provider_changes" in reg
        assert "base_counts" in reg
        assert "cur_counts" in reg
        assert "deltas" in reg
        assert "big_drops" in reg
        assert "new_unknowns" in reg


# ── TestInferCountry ─────────────────────────────────────────────────


class TestInferCountry:
    def test_providers_ch(self):
        assert _infer_country(Path("providers_ch.json")) == "ch"

    def test_providers_de(self):
        assert _infer_country(Path("providers_de.json")) == "de"

    def test_no_match_defaults_ch(self):
        assert _infer_country(Path("output.json")) == "ch"


# ── TestRunValidation ────────────────────────────────────────────────


class TestRunValidation:
    def test_output_file_not_found(self, tmp_path):
        assert run_validation(tmp_path / "nonexistent.json") is False

    def test_valid_file_passes(self, tmp_path):
        data = _make_output()
        path = tmp_path / "providers_ch.json"
        path.write_text(json.dumps(data))
        assert run_validation(path) is True

    def test_baseline_file_not_found(self, tmp_path):
        data = _make_output()
        out = tmp_path / "providers_ch.json"
        out.write_text(json.dumps(data))
        assert run_validation(out, baseline_path=tmp_path / "missing.json") is False

    def test_with_valid_baseline(self, tmp_path):
        data = _make_output()
        out = tmp_path / "providers_ch.json"
        base = tmp_path / "baseline.json"
        out.write_text(json.dumps(data))
        base.write_text(json.dumps(data))
        assert run_validation(out, baseline_path=base) is True

    def test_structural_failure_returns_false(self, tmp_path):
        data = _make_output()
        del data["total"]
        path = tmp_path / "providers_ch.json"
        path.write_text(json.dumps(data))
        assert run_validation(path) is False

    def test_infer_country_from_filename(self, tmp_path):
        data = _make_output()
        path = tmp_path / "providers_de.json"
        path.write_text(json.dumps(data))
        # Should pass — _build_category_map("de") uses "de-based" for domestic
        # Our entries use "us-cloud" category which is the same for all countries
        assert run_validation(path) is True
