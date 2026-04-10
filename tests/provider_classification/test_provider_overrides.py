"""Tests for provider classification override mechanism."""

import json
from unittest.mock import patch

import pytest

from mail_municipalities.provider_classification.runner import (
    _apply_provider_overrides,
    _build_category_map,
    _load_provider_overrides,
    run,
)
from mail_municipalities.provider_classification.models import (
    ClassificationResult,
    Provider,
)


# ── _load_provider_overrides ─────────────────────────────────────────


class TestLoadProviderOverrides:
    def test_missing_file_returns_empty(self):
        result = _load_provider_overrides("xx")
        assert result == {}

    def test_valid_file(self, tmp_path, monkeypatch):
        (tmp_path / "data" / "at").mkdir(parents=True)
        data = {
            "20302": {"provider": "domestic", "operator": "gsz", "source": "test"},
        }
        (tmp_path / "data" / "at" / "provider_overrides.json").write_text(json.dumps(data), encoding="utf-8")
        monkeypatch.chdir(tmp_path)
        result = _load_provider_overrides("at")
        assert "20302" in result
        assert result["20302"]["provider"] == "domestic"
        assert result["20302"]["operator"] == "gsz"

    def test_skips_entry_missing_fields(self, tmp_path, monkeypatch):
        (tmp_path / "data" / "at").mkdir(parents=True)
        data = {
            "111": {"provider": "domestic"},  # missing operator, source
            "222": {"provider": "domestic", "operator": "gsz", "source": "ok"},
        }
        (tmp_path / "data" / "at" / "provider_overrides.json").write_text(json.dumps(data), encoding="utf-8")
        monkeypatch.chdir(tmp_path)
        result = _load_provider_overrides("at")
        assert "111" not in result
        assert "222" in result

    def test_skips_invalid_provider(self, tmp_path, monkeypatch):
        (tmp_path / "data" / "at").mkdir(parents=True)
        data = {
            "111": {"provider": "azure", "operator": "x", "source": "x"},
        }
        (tmp_path / "data" / "at" / "provider_overrides.json").write_text(json.dumps(data), encoding="utf-8")
        monkeypatch.chdir(tmp_path)
        result = _load_provider_overrides("at")
        assert result == {}

    def test_empty_file(self, tmp_path, monkeypatch):
        (tmp_path / "data" / "at").mkdir(parents=True)
        (tmp_path / "data" / "at" / "provider_overrides.json").write_text("{}", encoding="utf-8")
        monkeypatch.chdir(tmp_path)
        result = _load_provider_overrides("at")
        assert result == {}


# ── _apply_provider_overrides ────────────────────────────────────────


class TestApplyProviderOverrides:
    def _make_result(self, code, name, provider="unknown", confidence=0.0):
        return {
            "code": code,
            "name": name,
            "provider": provider,
            "category": "unknown" if provider == "unknown" else "us-cloud",
            "classification_confidence": confidence,
            "classification_signals": [{"kind": "tenant", "detail": "test"}],
            "mx": ["mx1.example.com"],
            "spf": "v=spf1 ~all",
            "gateway": "cisco",
        }

    def test_applies_to_unknown(self):
        category_map = _build_category_map("at")
        results = {"20302": self._make_result("20302", "Dellach")}
        overrides = {"20302": {"provider": "domestic", "operator": "gsz", "source": "test"}}

        applied = _apply_provider_overrides(results, overrides, category_map)

        assert applied == 1
        entry = results["20302"]
        assert entry["provider"] == "domestic"
        assert entry["category"] == "at-based"
        assert entry["classification_confidence"] == 100.0
        assert entry["override"] == {"operator": "gsz", "source": "test"}
        # Original fields preserved
        assert entry["mx"] == ["mx1.example.com"]
        assert entry["gateway"] == "cisco"
        assert entry["classification_signals"] == [{"kind": "tenant", "detail": "test"}]

    def test_skips_non_unknown(self):
        category_map = _build_category_map("at")
        results = {"20923": self._make_result("20923", "Wolfsberg", "microsoft", 100.0)}
        overrides = {"20923": {"provider": "domestic", "operator": "gsz", "source": "test"}}

        applied = _apply_provider_overrides(results, overrides, category_map)

        assert applied == 0
        assert results["20923"]["provider"] == "microsoft"

    def test_warns_on_missing_code(self):
        category_map = _build_category_map("at")
        results = {}
        overrides = {"99999": {"provider": "domestic", "operator": "gsz", "source": "test"}}

        applied = _apply_provider_overrides(results, overrides, category_map)
        assert applied == 0

    def test_mixed_apply_and_skip(self):
        category_map = _build_category_map("at")
        results = {
            "001": self._make_result("001", "A"),  # unknown → override
            "002": self._make_result("002", "B", "microsoft", 100.0),  # skip
            "003": self._make_result("003", "C"),  # unknown → override
        }
        overrides = {
            "001": {"provider": "domestic", "operator": "gsz", "source": "test"},
            "002": {"provider": "domestic", "operator": "gsz", "source": "test"},
            "003": {"provider": "domestic", "operator": "gsz", "source": "test"},
        }

        applied = _apply_provider_overrides(results, overrides, category_map)

        assert applied == 2
        assert results["001"]["provider"] == "domestic"
        assert results["002"]["provider"] == "microsoft"
        assert results["003"]["provider"] == "domestic"


# ── Integration: run() with overrides ────────────────────────────────


def _make_resolver_output(municipalities):
    return {"municipalities": municipalities}


class TestRunWithOverrides:
    @pytest.fixture
    def domains_json(self, tmp_path):
        data = _make_resolver_output(
            [
                {"code": "20302", "name": "Dellach", "region": "Kärnten", "emails": ["ktn.gde.at"]},
                {"code": "20923", "name": "Wolfsberg", "region": "Kärnten", "emails": ["wolfsberg.at"]},
            ]
        )
        path = tmp_path / "domains_at_detailed.json"
        path.write_text(json.dumps(data), encoding="utf-8")
        return path

    @pytest.fixture
    def overrides_dir(self, tmp_path, monkeypatch):
        (tmp_path / "data" / "at").mkdir(parents=True)
        overrides = {
            "20302": {"provider": "domestic", "operator": "gsz", "source": "test"},
            "20923": {"provider": "domestic", "operator": "gsz", "source": "test"},
        }
        (tmp_path / "data" / "at" / "provider_overrides.json").write_text(json.dumps(overrides), encoding="utf-8")
        monkeypatch.chdir(tmp_path)

    async def test_run_applies_overrides(self, domains_json, overrides_dir, tmp_path):
        unknown_result = ClassificationResult(
            provider=Provider.UNKNOWN, confidence=0.0, evidence=[], gateway="cisco", mx_hosts=[]
        )
        ms_result = ClassificationResult(
            provider=Provider.MS365, confidence=0.92, evidence=[], mx_hosts=["wolfsberg-at.mail.protection.outlook.com"]
        )

        results_map = {"ktn.gde.at": unknown_result, "wolfsberg.at": ms_result}

        async def fake_classify_many(domains, max_concurrency=20, *, country_code=None):
            for d in domains:
                yield d, results_map[d]

        output_path = tmp_path / "providers_at.json"
        with patch(
            "mail_municipalities.provider_classification.runner.classify_many",
            side_effect=fake_classify_many,
        ):
            await run(domains_json, output_path, country_code="at")

        data = json.loads(output_path.read_text())
        munis = {m["code"]: m for m in data["municipalities"]}

        # Dellach: was unknown, override applied
        assert munis["20302"]["provider"] == "domestic"
        assert munis["20302"]["category"] == "at-based"
        assert munis["20302"]["classification_confidence"] == 100.0
        assert munis["20302"]["override"] == {"operator": "gsz", "source": "test"}

        # Wolfsberg: was microsoft, override NOT applied
        assert munis["20923"]["provider"] == "microsoft"
        assert "override" not in munis["20923"]

    async def test_run_counts_reflect_overrides(self, domains_json, overrides_dir, tmp_path):
        unknown_result = ClassificationResult(
            provider=Provider.UNKNOWN, confidence=0.0, evidence=[], gateway="cisco", mx_hosts=[]
        )
        ms_result = ClassificationResult(provider=Provider.MS365, confidence=0.92, evidence=[], mx_hosts=[])

        results_map = {"ktn.gde.at": unknown_result, "wolfsberg.at": ms_result}

        async def fake_classify_many(domains, max_concurrency=20, *, country_code=None):
            for d in domains:
                yield d, results_map[d]

        output_path = tmp_path / "providers_at.json"
        with patch(
            "mail_municipalities.provider_classification.runner.classify_many",
            side_effect=fake_classify_many,
        ):
            await run(domains_json, output_path, country_code="at")

        data = json.loads(output_path.read_text())
        assert data["counts"]["domestic"] == 1
        assert data["counts"]["microsoft"] == 1
        assert "unknown" not in data["counts"]

    async def test_run_minified_includes_override(self, domains_json, overrides_dir, tmp_path):
        unknown_result = ClassificationResult(
            provider=Provider.UNKNOWN, confidence=0.0, evidence=[], gateway="cisco", mx_hosts=[]
        )

        async def fake_classify_many(domains, max_concurrency=20, *, country_code=None):
            for d in domains:
                yield d, unknown_result

        output_path = tmp_path / "providers_at.json"
        with patch(
            "mail_municipalities.provider_classification.runner.classify_many",
            side_effect=fake_classify_many,
        ):
            await run(domains_json, output_path, country_code="at")

        mini = json.loads((tmp_path / "providers_at.min.json").read_text())
        overridden = [m for m in mini["municipalities"] if "override" in m]
        assert len(overridden) >= 1
        assert overridden[0]["override"]["operator"] == "gsz"

    async def test_run_without_overrides_unchanged(self, domains_json, tmp_path, monkeypatch):
        """No override file → behavior identical to before."""
        monkeypatch.chdir(tmp_path)

        ms_result = ClassificationResult(provider=Provider.MS365, confidence=0.92, evidence=[], mx_hosts=[])

        async def fake_classify_many(domains, max_concurrency=20, *, country_code=None):
            for d in domains:
                yield d, ms_result

        output_path = tmp_path / "providers_at.json"
        with patch(
            "mail_municipalities.provider_classification.runner.classify_many",
            side_effect=fake_classify_many,
        ):
            await run(domains_json, output_path, country_code="at")

        data = json.loads(output_path.read_text())
        for m in data["municipalities"]:
            assert "override" not in m
