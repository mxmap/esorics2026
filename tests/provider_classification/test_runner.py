"""Tests for the classification pipeline runner."""

import json
from unittest.mock import patch

import pytest

from mail_municipalities.provider_classification.runner import (
    PROVIDER_OUTPUT_NAMES,
    _build_category_map,
    _load_resolver_output,
    _minify_for_frontend,
    _output_provider,
    _serialize_result,
    run,
)
from mail_municipalities.provider_classification.models import (
    ClassificationResult,
    Evidence,
    Provider,
    SignalKind,
)
from mail_municipalities.provider_classification.probes import WEIGHTS


class TestProviderOutputNames:
    def test_ms365_mapped(self):
        assert PROVIDER_OUTPUT_NAMES["ms365"] == "microsoft"

    def test_output_provider_ms365(self):
        assert _output_provider(Provider.MS365) == "microsoft"

    def test_output_provider_google(self):
        assert _output_provider(Provider.GOOGLE) == "google"

    def test_output_provider_unknown(self):
        assert _output_provider(Provider.UNKNOWN) == "unknown"


class TestLoadResolverOutput:
    def test_loads_array_format(self, tmp_path):
        data = {
            "municipalities": [
                {"code": "1", "name": "A", "region": "R", "emails": ["a.ch"]},
                {"code": "2", "name": "B", "region": "R", "emails": []},
            ]
        }
        path = tmp_path / "test.json"
        path.write_text(json.dumps(data), encoding="utf-8")
        entries = _load_resolver_output(path)
        assert entries["1"]["_domain"] == "a.ch"
        assert entries["2"]["_domain"] == ""

    def test_preserves_extra_fields(self, tmp_path):
        data = {
            "municipalities": [
                {
                    "code": "1",
                    "name": "A",
                    "region": "R",
                    "emails": ["a.ch"],
                    "sources_detail": {"scrape": ["a.ch"]},
                    "flags": ["test"],
                },
            ]
        }
        path = tmp_path / "test.json"
        path.write_text(json.dumps(data), encoding="utf-8")
        entries = _load_resolver_output(path)
        assert entries["1"]["sources_detail"] == {"scrape": ["a.ch"]}
        assert entries["1"]["flags"] == ["test"]


class TestSerializeResult:
    def test_basic_serialization(self):
        category_map = _build_category_map("ch")
        result = ClassificationResult(
            provider=Provider.MS365,
            confidence=0.4,
            evidence=[
                Evidence(
                    kind=SignalKind.MX,
                    provider=Provider.MS365,
                    weight=WEIGHTS[SignalKind.MX],
                    detail="MX match",
                    raw="example.mail.protection.outlook.com",
                ),
                Evidence(
                    kind=SignalKind.SPF,
                    provider=Provider.MS365,
                    weight=WEIGHTS[SignalKind.SPF],
                    detail="SPF match",
                    raw="v=spf1 include:spf.protection.outlook.com -all",
                ),
            ],
            mx_hosts=["example.mail.protection.outlook.com"],
            spf_raw="v=spf1 include:spf.protection.outlook.com -all",
        )
        entry = {
            "code": "351",
            "name": "Bern",
            "region": "Kanton Bern",
            "_domain": "bern.ch",
        }
        out = _serialize_result(entry, result, category_map)

        assert out["code"] == "351"
        assert out["provider"] == "microsoft"
        assert out["category"] == "us-cloud"
        assert out["classification_confidence"] == 40.0
        assert out["mx"] == ["example.mail.protection.outlook.com"]
        assert out["spf"] == "v=spf1 include:spf.protection.outlook.com -all"
        assert len(out["classification_signals"]) == 2
        assert out["classification_signals"][0]["kind"] == "mx"
        assert out["classification_signals"][0]["provider"] == "microsoft"

    def test_gateway_included(self):
        category_map = _build_category_map("ch")
        result = ClassificationResult(
            provider=Provider.MS365,
            confidence=0.4,
            evidence=[],
            gateway="seppmail",
            mx_hosts=[],
        )
        entry = {"code": "1", "name": "Test", "_domain": "test.ch"}
        out = _serialize_result(entry, result, category_map)
        assert out["gateway"] == "seppmail"

    def test_no_gateway_omitted(self):
        category_map = _build_category_map("ch")
        result = ClassificationResult(
            provider=Provider.UNKNOWN,
            confidence=0.0,
            evidence=[],
            mx_hosts=[],
        )
        entry = {"code": "1", "name": "Test", "_domain": "test.ch"}
        out = _serialize_result(entry, result, category_map)
        assert "gateway" not in out

    def test_resolve_fields_passthrough(self):
        category_map = _build_category_map("ch")
        result = ClassificationResult(
            provider=Provider.UNKNOWN,
            confidence=0.0,
            evidence=[],
            mx_hosts=[],
        )
        entry = {
            "code": "1",
            "name": "Test",
            "_domain": "test.ch",
            "sources_detail": {"scrape": ["test.ch"]},
            "flags": ["test_flag"],
        }
        out = _serialize_result(entry, result, category_map)
        assert out["sources_detail"] == {"scrape": ["test.ch"]}
        assert out["resolve_flags"] == ["test_flag"]


def _make_resolver_output(municipalities):
    """Helper to create domain resolver output format."""
    return {"municipalities": municipalities}


class TestPipelineRun:
    @pytest.fixture
    def domains_json(self, tmp_path):
        data = _make_resolver_output(
            [
                {
                    "code": "351",
                    "name": "Bern",
                    "region": "Kanton Bern",
                    "emails": ["bern.ch"],
                },
                {
                    "code": "9999",
                    "name": "Testingen",
                    "region": "Testland",
                    "emails": [],
                },
            ]
        )
        path = tmp_path / "domains_ch_detailed.json"
        path.write_text(json.dumps(data), encoding="utf-8")
        return path

    async def test_run_writes_output(self, domains_json, tmp_path):
        ms_result = ClassificationResult(
            provider=Provider.MS365,
            confidence=0.4,
            evidence=[
                Evidence(
                    kind=SignalKind.MX,
                    provider=Provider.MS365,
                    weight=WEIGHTS[SignalKind.MX],
                    detail="MX match",
                    raw="bern-ch.mail.protection.outlook.com",
                ),
            ],
            mx_hosts=["bern-ch.mail.protection.outlook.com"],
        )

        async def fake_classify_many(domains, max_concurrency=20, *, country_code=None):
            for d in domains:
                yield d, ms_result

        output_path = tmp_path / "providers_ch.json"
        with patch(
            "mail_municipalities.provider_classification.runner.classify_many",
            side_effect=fake_classify_many,
        ):
            await run(domains_json, output_path, country_code="ch")

        assert output_path.exists()
        data = json.loads(output_path.read_text())
        assert data["total"] == 2
        munis = {m["code"]: m for m in data["municipalities"]}
        assert "351" in munis
        assert "9999" in munis
        assert munis["351"]["provider"] == "microsoft"
        assert munis["351"]["category"] == "us-cloud"
        assert munis["9999"]["provider"] == "unknown"
        assert munis["9999"]["category"] == "unknown"
        assert munis["9999"]["classification_confidence"] == 0.0

    async def test_run_no_domain_entry(self, domains_json, tmp_path):
        ms_result = ClassificationResult(
            provider=Provider.MS365,
            confidence=0.4,
            evidence=[],
            mx_hosts=[],
        )

        async def fake_classify_many(domains, max_concurrency=20, *, country_code=None):
            for d in domains:
                yield d, ms_result

        output_path = tmp_path / "providers_ch.json"
        with patch(
            "mail_municipalities.provider_classification.runner.classify_many",
            side_effect=fake_classify_many,
        ):
            await run(domains_json, output_path, country_code="ch")

        data = json.loads(output_path.read_text())
        munis = {m["code"]: m for m in data["municipalities"]}
        no_domain = munis["9999"]
        assert no_domain["domain"] == ""
        assert no_domain["mx"] == []

    async def test_run_passthrough_fields(self, tmp_path):
        data = _make_resolver_output(
            [
                {
                    "code": "100",
                    "name": "Town",
                    "region": "Kanton Zürich",
                    "emails": ["town.ch"],
                    "sources_detail": {"scrape": ["town.ch"]},
                    "flags": ["test_flag"],
                },
            ]
        )
        path = tmp_path / "domains.json"
        path.write_text(json.dumps(data), encoding="utf-8")

        result = ClassificationResult(
            provider=Provider.GOOGLE,
            confidence=0.4,
            evidence=[],
            mx_hosts=["mx.google.com"],
        )

        async def fake_classify_many(domains, max_concurrency=20, *, country_code=None):
            for d in domains:
                yield d, result

        output_path = tmp_path / "providers.json"
        with patch(
            "mail_municipalities.provider_classification.runner.classify_many",
            side_effect=fake_classify_many,
        ):
            await run(path, output_path, country_code="ch")

        out = json.loads(output_path.read_text())
        munis = {m["code"]: m for m in out["municipalities"]}
        entry = munis["100"]
        assert entry["sources_detail"] == {"scrape": ["town.ch"]}
        assert entry["resolve_flags"] == ["test_flag"]

    async def test_run_counts_in_output(self, domains_json, tmp_path):
        result = ClassificationResult(
            provider=Provider.MS365,
            confidence=0.4,
            evidence=[],
            mx_hosts=[],
        )

        async def fake_classify_many(domains, max_concurrency=20, *, country_code=None):
            for d in domains:
                yield d, result

        output_path = tmp_path / "providers.json"
        with patch(
            "mail_municipalities.provider_classification.runner.classify_many",
            side_effect=fake_classify_many,
        ):
            await run(domains_json, output_path, country_code="ch")

        data = json.loads(output_path.read_text())
        assert "counts" in data
        assert data["counts"]["microsoft"] == 1
        assert data["counts"]["unknown"] == 1

    async def test_run_writes_minified_output(self, domains_json, tmp_path):
        ms_result = ClassificationResult(
            provider=Provider.MS365,
            confidence=0.4,
            evidence=[
                Evidence(
                    kind=SignalKind.MX,
                    provider=Provider.MS365,
                    weight=WEIGHTS[SignalKind.MX],
                    detail="MX match",
                    raw="bern-ch.mail.protection.outlook.com",
                ),
            ],
            mx_hosts=["bern-ch.mail.protection.outlook.com"],
        )

        async def fake_classify_many(domains, max_concurrency=20, *, country_code=None):
            for d in domains:
                yield d, ms_result

        output_path = tmp_path / "providers.json"
        with patch(
            "mail_municipalities.provider_classification.runner.classify_many",
            side_effect=fake_classify_many,
        ):
            await run(domains_json, output_path, country_code="ch")

        mini_path = tmp_path / "providers.min.json"
        assert mini_path.exists()

        raw = mini_path.read_text(encoding="utf-8")
        assert "\n" not in raw

        mini = json.loads(raw)
        assert "generated" in mini
        assert "municipalities" in mini
        assert "total" not in mini
        assert "counts" not in mini


class TestMinifyForFrontend:
    def _make_full_output(self):
        return {
            "generated": "2026-01-01T00:00:00Z",
            "total": 1,
            "counts": {"microsoft": 1},
            "municipalities": [
                {
                    "code": "351",
                    "name": "Bern",
                    "region": "Kanton Bern",
                    "domain": "bern.ch",
                    "mx": ["bern-ch.mail.protection.outlook.com"],
                    "spf": "v=spf1 include:spf.protection.outlook.com -all",
                    "provider": "microsoft",
                    "category": "us-cloud",
                    "classification_confidence": 40.0,
                    "classification_signals": [
                        {
                            "kind": "mx",
                            "provider": "microsoft",
                            "weight": 0.4,
                            "detail": "MX match",
                        },
                    ],
                    "gateway": "seppmail",
                    "sources_detail": {"scrape": ["bern.ch"]},
                    "resolve_flags": ["test_flag"],
                }
            ],
        }

    def test_minify_strips_unused_fields(self):
        full = self._make_full_output()
        mini = _minify_for_frontend(full)

        entry = mini["municipalities"][0]
        assert "sources_detail" not in entry
        assert "resolve_flags" not in entry

        sig = entry["classification_signals"][0]
        assert "provider" not in sig
        assert "weight" not in sig

        assert "total" not in mini
        assert "counts" not in mini

    def test_minify_preserves_frontend_fields(self):
        full = self._make_full_output()
        mini = _minify_for_frontend(full)

        assert mini["generated"] == "2026-01-01T00:00:00Z"
        entry = mini["municipalities"][0]
        assert entry["name"] == "Bern"
        assert entry["domain"] == "bern.ch"
        assert entry["mx"] == ["bern-ch.mail.protection.outlook.com"]
        assert entry["spf"] == "v=spf1 include:spf.protection.outlook.com -all"
        assert entry["provider"] == "microsoft"
        assert entry["category"] == "us-cloud"
        assert entry["classification_confidence"] == 40.0
        assert entry["gateway"] == "seppmail"

        sig = entry["classification_signals"][0]
        assert sig["kind"] == "mx"
        assert sig["detail"] == "MX match"


class TestPipelineLogging:
    @pytest.fixture
    def domains_json(self, tmp_path):
        data = _make_resolver_output(
            [
                {
                    "code": "351",
                    "name": "Bern",
                    "region": "Kanton Bern",
                    "emails": ["bern.ch"],
                },
            ]
        )
        path = tmp_path / "domains.json"
        path.write_text(json.dumps(data), encoding="utf-8")
        return path

    async def test_logs_progress_messages(self, domains_json, tmp_path, caplog):
        ms_result = ClassificationResult(
            provider=Provider.MS365,
            confidence=0.4,
            evidence=[],
            mx_hosts=[],
        )

        async def fake_classify_many(domains, max_concurrency=20, *, country_code=None):
            for d in domains:
                yield d, ms_result

        output_path = tmp_path / "providers.json"
        with patch(
            "mail_municipalities.provider_classification.runner.classify_many",
            side_effect=fake_classify_many,
        ):
            await run(domains_json, output_path, country_code="ch")

        assert any("Classifying" in msg for msg in caplog.messages)
        assert any("Wrote" in msg for msg in caplog.messages)
