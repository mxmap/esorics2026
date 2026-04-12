"""Tests for security scan override mechanism."""

import json

import pytest

from mail_municipalities.security_analysis.models import (
    DaneSummary,
    DssSummary,
    MunicipalitySecurity,
)
from mail_municipalities.security_analysis.runner import (
    _apply_security_overrides,
    _load_security_overrides,
    build_output,
)


# ── _load_security_overrides ─────────────────────────────────────────


class TestLoadSecurityOverrides:
    def test_missing_file_returns_empty(self):
        result = _load_security_overrides("xx")
        assert result == {}

    def test_valid_file(self, tmp_path, monkeypatch):
        (tmp_path / "data" / "de").mkdir(parents=True)
        data = {
            "02000000": {
                "dss": {"has_spf": True, "has_good_spf": True},
                "source": "dss bug: large TXT response",
            },
        }
        (tmp_path / "data" / "de" / "security_overrides.json").write_text(json.dumps(data), encoding="utf-8")
        monkeypatch.chdir(tmp_path)
        result = _load_security_overrides("de")
        assert "02000000" in result
        assert result["02000000"]["dss"]["has_spf"] is True
        assert result["02000000"]["source"] == "dss bug: large TXT response"

    def test_skips_entry_missing_source(self, tmp_path, monkeypatch):
        (tmp_path / "data" / "de").mkdir(parents=True)
        data = {
            "111": {"dss": {"has_spf": True}},  # missing source
            "222": {"dss": {"has_spf": True}, "source": "ok"},
        }
        (tmp_path / "data" / "de" / "security_overrides.json").write_text(json.dumps(data), encoding="utf-8")
        monkeypatch.chdir(tmp_path)
        result = _load_security_overrides("de")
        assert "111" not in result
        assert "222" in result

    def test_empty_file(self, tmp_path, monkeypatch):
        (tmp_path / "data" / "de").mkdir(parents=True)
        (tmp_path / "data" / "de" / "security_overrides.json").write_text("{}", encoding="utf-8")
        monkeypatch.chdir(tmp_path)
        result = _load_security_overrides("de")
        assert result == {}


# ── _apply_security_overrides ────────────────────────────────────────


class TestApplySecurityOverrides:
    def _make_muni(self, code="001", name="Test", has_spf=False, has_good_spf=False, scan_valid=True):
        return MunicipalitySecurity(
            code=code,
            name=name,
            region="TestRegion",
            domain="test.de",
            dane=DaneSummary(supported=False, partial=False),
            dss=DssSummary(has_spf=has_spf, has_good_spf=has_good_spf),
            scan_valid=scan_valid,
        )

    def test_overrides_dss_fields(self):
        muni = self._make_muni(code="02000000", has_spf=False, has_good_spf=False)
        overrides = {
            "02000000": {
                "dss": {"has_spf": True, "has_good_spf": True},
                "source": "dss bug",
            },
        }
        applied = _apply_security_overrides([muni], overrides)
        assert applied == 1
        assert muni.dss is not None
        assert muni.dss.has_spf is True
        assert muni.dss.has_good_spf is True
        assert muni.scan_valid is True
        assert muni.override == {"source": "dss bug"}

    def test_overrides_dane_fields(self):
        muni = self._make_muni(code="001")
        overrides = {
            "001": {
                "dane": {"supported": True, "partial": True},
                "source": "dane correction",
            },
        }
        applied = _apply_security_overrides([muni], overrides)
        assert applied == 1
        assert muni.dane is not None
        assert muni.dane.supported is True
        assert muni.dane.partial is True

    def test_non_overridden_unchanged(self):
        muni = self._make_muni(code="999", has_spf=False)
        overrides = {
            "001": {
                "dss": {"has_spf": True},
                "source": "test",
            },
        }
        _apply_security_overrides([muni], overrides)
        assert muni.dss is not None
        assert muni.dss.has_spf is False
        assert muni.override is None

    def test_warns_on_missing_code(self):
        overrides = {
            "99999": {
                "dss": {"has_spf": True},
                "source": "test",
            },
        }
        applied = _apply_security_overrides([], overrides)
        assert applied == 0

    def test_sets_scan_valid(self):
        muni = self._make_muni(code="001", scan_valid=False)
        overrides = {
            "001": {
                "dss": {"has_spf": True},
                "source": "fix",
            },
        }
        _apply_security_overrides([muni], overrides)
        assert muni.scan_valid is True

    def test_partial_dss_override_preserves_other_fields(self):
        muni = self._make_muni(code="001", has_spf=False)
        assert muni.dss is not None
        muni.dss.has_dmarc = True
        overrides = {
            "001": {
                "dss": {"has_spf": True},
                "source": "partial fix",
            },
        }
        _apply_security_overrides([muni], overrides)
        assert muni.dss is not None
        assert muni.dss.has_spf is True
        assert muni.dss.has_dmarc is True  # untouched


# ── Integration: build_output with overrides ─────────────────────────


class TestBuildOutputWithOverrides:
    @pytest.fixture
    def domains_json(self, tmp_path):
        data = {
            "municipalities": [
                {"code": "02000000", "name": "Hamburg", "region": "Hamburg", "emails": ["hamburg.de"]},
                {"code": "08119008", "name": "Backnang", "region": "BW", "emails": ["backnang.de"]},
            ]
        }
        path = tmp_path / "domains_de.json"
        path.write_text(json.dumps(data), encoding="utf-8")
        return path

    @pytest.fixture
    def domain_security(self):
        return {
            "hamburg.de": {
                "mx_records": ["mx.hamburg.de"],
                "dane": DaneSummary(supported=False, partial=False),
                "dss": DssSummary(has_spf=False, has_good_spf=False, has_dmarc=True, has_good_dmarc=True),
                "scan_valid": True,
            },
            "backnang.de": {
                "mx_records": ["mx.backnang.de"],
                "dane": DaneSummary(supported=False, partial=False),
                "dss": DssSummary(has_spf=False, has_good_spf=False),
                "scan_valid": True,
            },
        }

    def test_overrides_applied_in_output(self, domains_json, domain_security, tmp_path, monkeypatch):
        (tmp_path / "data" / "de").mkdir(parents=True)
        overrides = {
            "02000000": {
                "dss": {"has_spf": True, "has_good_spf": True},
                "source": "dss bug: large TXT response",
            },
        }
        (tmp_path / "data" / "de" / "security_overrides.json").write_text(json.dumps(overrides), encoding="utf-8")
        monkeypatch.chdir(tmp_path)

        output = build_output(domains_json, domain_security, "de")
        by_code = {m.code: m for m in output.municipalities}

        # Hamburg: overridden
        hamburg = by_code["02000000"]
        assert hamburg.dss is not None
        assert hamburg.dss.has_spf is True
        assert hamburg.dss.has_good_spf is True
        assert hamburg.dss.has_dmarc is True  # preserved from scan
        assert hamburg.override == {"source": "dss bug: large TXT response"}

        # Backnang: not overridden
        backnang = by_code["08119008"]
        assert backnang.dss is not None
        assert backnang.dss.has_spf is False
        assert backnang.override is None

    def test_counts_reflect_overrides(self, domains_json, domain_security, tmp_path, monkeypatch):
        (tmp_path / "data" / "de").mkdir(parents=True)
        overrides = {
            "02000000": {
                "dss": {"has_spf": True, "has_good_spf": True},
                "source": "fix",
            },
        }
        (tmp_path / "data" / "de" / "security_overrides.json").write_text(json.dumps(overrides), encoding="utf-8")
        monkeypatch.chdir(tmp_path)

        output = build_output(domains_json, domain_security, "de")

        # 1 from override + 0 from scan
        assert output.counts["spf"] == 1
        assert output.counts["good_spf"] == 1

    def test_no_overrides_file_unchanged(self, domains_json, domain_security, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        output = build_output(domains_json, domain_security, "de")
        for m in output.municipalities:
            assert m.override is None
