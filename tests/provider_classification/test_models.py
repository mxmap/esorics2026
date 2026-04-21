"""Tests for models: enums, Pydantic models, validation."""

import pytest
from pydantic import ValidationError

from mail_municipalities.provider_classification.models import (
    ClassificationResult,
    CymruResult,
    Evidence,
    Provider,
    SignalKind,
)


class TestProvider:
    def test_values(self):
        assert Provider.MS365 == "ms365"
        assert Provider.GOOGLE == "google"
        assert Provider.AWS == "aws"
        assert Provider.DOMESTIC == "domestic"
        assert Provider.FOREIGN == "foreign"
        assert Provider.UNKNOWN == "unknown"

    def test_str_serialization(self):
        assert str(Provider.MS365) == "Provider.MS365"
        assert Provider.MS365.value == "ms365"

    def test_all_members(self):
        assert set(Provider) == {
            Provider.MS365,
            Provider.GOOGLE,
            Provider.AWS,
            Provider.DOMESTIC,
            Provider.FOREIGN,
            Provider.UNKNOWN,
        }


class TestSignalKind:
    def test_values(self):
        assert SignalKind.MX == "mx"
        assert SignalKind.SPF == "spf"
        assert SignalKind.DKIM == "dkim"
        assert SignalKind.DMARC == "dmarc"
        assert SignalKind.AUTODISCOVER == "autodiscover"
        assert SignalKind.CNAME_CHAIN == "cname_chain"
        assert SignalKind.SMTP == "smtp"
        assert SignalKind.TENANT == "tenant"
        assert SignalKind.ASN == "asn"
        assert SignalKind.TXT_VERIFICATION == "txt_verification"
        assert SignalKind.SPF_IP == "spf_ip"

    def test_all_members(self):
        assert len(SignalKind) == 11


class TestEvidence:
    def test_construction(self):
        e = Evidence(
            kind=SignalKind.MX,
            provider=Provider.MS365,
            weight=0.30,
            detail="test detail",
            raw="test.outlook.com",
        )
        assert e.kind == SignalKind.MX
        assert e.provider == Provider.MS365
        assert e.weight == 0.30
        assert e.detail == "test detail"
        assert e.raw == "test.outlook.com"

    def test_default_raw(self):
        e = Evidence(kind=SignalKind.SPF, provider=Provider.GOOGLE, weight=0.25, detail="test")
        assert e.raw == ""

    def test_immutability(self):
        e = Evidence(kind=SignalKind.MX, provider=Provider.MS365, weight=0.30, detail="test")
        with pytest.raises(ValidationError):
            e.weight = 0.5

    def test_weight_too_high(self):
        with pytest.raises(ValidationError):
            Evidence(kind=SignalKind.MX, provider=Provider.MS365, weight=1.5, detail="test")

    def test_weight_too_low(self):
        with pytest.raises(ValidationError):
            Evidence(kind=SignalKind.MX, provider=Provider.MS365, weight=-0.1, detail="test")

    def test_json_roundtrip(self):
        e = Evidence(kind=SignalKind.MX, provider=Provider.MS365, weight=0.30, detail="test")
        data = e.model_dump()
        assert data["kind"] == "mx"
        assert data["provider"] == "ms365"
        e2 = Evidence.model_validate(data)
        assert e == e2

    def test_new_signal_kinds(self):
        for kind in (
            SignalKind.SMTP,
            SignalKind.TENANT,
            SignalKind.ASN,
            SignalKind.TXT_VERIFICATION,
            SignalKind.SPF_IP,
        ):
            e = Evidence(kind=kind, provider=Provider.MS365, weight=0.10, detail="test")
            assert e.kind == kind


class TestClassificationResult:
    def test_construction(self):
        r = ClassificationResult(provider=Provider.UNKNOWN, confidence=0.0, evidence=[])
        assert r.provider == Provider.UNKNOWN
        assert r.confidence == 0.0
        assert r.evidence == []
        assert r.gateway is None

    def test_gateway_field(self):
        r = ClassificationResult(provider=Provider.MS365, confidence=0.5, evidence=[], gateway="seppmail")
        assert r.gateway == "seppmail"

    def test_gateway_default_none(self):
        r = ClassificationResult(provider=Provider.MS365, confidence=0.5, evidence=[])
        assert r.gateway is None

    def test_immutability(self):
        r = ClassificationResult(provider=Provider.MS365, confidence=0.5, evidence=[])
        with pytest.raises(ValidationError):
            r.confidence = 0.9

    def test_confidence_bounds(self):
        with pytest.raises(ValidationError):
            ClassificationResult(provider=Provider.MS365, confidence=1.5, evidence=[])
        with pytest.raises(ValidationError):
            ClassificationResult(provider=Provider.MS365, confidence=-0.1, evidence=[])

    def test_with_evidence(self):
        e = Evidence(kind=SignalKind.MX, provider=Provider.MS365, weight=0.30, detail="test")
        r = ClassificationResult(provider=Provider.MS365, confidence=0.30, evidence=[e])
        assert len(r.evidence) == 1
        assert r.evidence[0].kind == SignalKind.MX

    def test_mx_hosts_field(self):
        r = ClassificationResult(
            provider=Provider.MS365,
            confidence=0.5,
            evidence=[],
            mx_hosts=["mx1.example.com", "mx2.example.com"],
        )
        assert r.mx_hosts == ["mx1.example.com", "mx2.example.com"]

    def test_mx_hosts_default_empty(self):
        r = ClassificationResult(provider=Provider.MS365, confidence=0.5, evidence=[])
        assert r.mx_hosts == []

    def test_spf_raw_field(self):
        r = ClassificationResult(
            provider=Provider.MS365,
            confidence=0.5,
            evidence=[],
            spf_raw="v=spf1 include:spf.protection.outlook.com ~all",
        )
        assert r.spf_raw == "v=spf1 include:spf.protection.outlook.com ~all"

    def test_spf_raw_default_empty(self):
        r = ClassificationResult(provider=Provider.MS365, confidence=0.5, evidence=[])
        assert r.spf_raw == ""


class TestCymruResult:
    def test_valid_parse(self):
        result = CymruResult.from_txt("3303 | 193.5.224.0/20 | CH | ripencc | 1997-05-26")
        assert result is not None
        assert result.asn == 3303
        assert result.country_code == "ch"

    def test_too_few_parts(self):
        result = CymruResult.from_txt("3303 | 195.186.0.0/16")
        assert result is None

    def test_invalid_asn(self):
        result = CymruResult.from_txt("abc | 193.5.224.0/20 | CH | ripencc | 1997-05-26")
        assert result is None

    def test_empty_string(self):
        result = CymruResult.from_txt("")
        assert result is None
