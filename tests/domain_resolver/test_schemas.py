"""Tests for Pydantic schemas."""

from datetime import datetime, timezone

from mail_municipalities.domain_resolver.schemas import (
    Confidence,
    Country,
    DomainCandidate,
    MunicipalityDetailedOutput,
    MunicipalityOutput,
    MunicipalityRecord,
    PipelineOutput,
    Source,
)


class TestEnums:
    def test_country_values(self):
        assert Country.CH == "ch"
        assert Country.DE == "de"
        assert Country.AT == "at"
        assert len(Country) == 3

    def test_confidence_values(self):
        assert Confidence.HIGH == "high"
        assert Confidence.MEDIUM == "medium"
        assert Confidence.LOW == "low"
        assert Confidence.NONE == "none"

    def test_source_values(self):
        assert Source.OVERRIDE == "override"
        assert Source.SCRAPE == "scrape"
        assert Source.REDIRECT == "redirect"
        assert Source.WIKIDATA == "wikidata"
        assert Source.STATIC == "static"
        assert Source.GUESS == "guess"
        assert Source.NONE == "none"


class TestDomainCandidate:
    def test_basic(self):
        c = DomainCandidate(domain="example.ch", source="wikidata")
        assert c.domain == "example.ch"
        assert c.source == "wikidata"
        assert c.is_email_domain is False

    def test_email_domain(self):
        c = DomainCandidate(domain="example.at", source="bresu_email", is_email_domain=True)
        assert c.is_email_domain is True


class TestMunicipalityRecord:
    def test_defaults(self):
        r = MunicipalityRecord(code="261", name="Zürich", region="Kanton Zürich", country=Country.CH)
        assert r.candidates == []
        assert r.override_domain is None
        assert r.scraped_emails == {}
        assert r.redirects == {}
        assert r.accessible == {}
        assert r.mx_valid == {}
        assert r.emails == []
        assert r.source == Source.NONE
        assert r.confidence == Confidence.NONE
        assert r.sources_detail == {}
        assert r.flags == []

    def test_with_candidates(self):
        r = MunicipalityRecord(
            code="10101",
            name="Eisenstadt",
            region="Burgenland",
            country=Country.AT,
            candidates=[
                DomainCandidate(domain="eisenstadt.gv.at", source="bresu"),
                DomainCandidate(domain="eisenstadt.at", source="guess"),
            ],
        )
        assert len(r.candidates) == 2
        assert r.candidates[0].domain == "eisenstadt.gv.at"

    def test_serialization_roundtrip(self):
        r = MunicipalityRecord(
            code="01001000",
            name="Flensburg",
            region="Schleswig-Holstein",
            country=Country.DE,
            emails=["flensburg.de"],
            source=Source.SCRAPE,
            confidence=Confidence.HIGH,
        )
        data = r.model_dump()
        r2 = MunicipalityRecord.model_validate(data)
        assert r2.code == "01001000"
        assert r2.emails == ["flensburg.de"]
        assert r2.source == Source.SCRAPE

    def test_mutable_fields_independent(self):
        r1 = MunicipalityRecord(code="1", name="A", region="R", country=Country.CH)
        r2 = MunicipalityRecord(code="2", name="B", region="R", country=Country.CH)
        r1.emails.append("a.ch")
        assert r2.emails == []


class TestMunicipalityOutput:
    def test_minimal(self):
        o = MunicipalityOutput(code="261", name="Zürich", region="Kanton Zürich")
        assert o.website == ""
        assert o.emails == []

    def test_with_data(self):
        o = MunicipalityOutput(
            code="261",
            name="Zürich",
            region="Kanton Zürich",
            website="zuerich.ch",
            emails=["zuerich.ch", "stadt-zuerich.ch"],
        )
        assert len(o.emails) == 2


class TestMunicipalityDetailedOutput:
    def test_inherits_output(self):
        d = MunicipalityDetailedOutput(
            code="10101",
            name="Eisenstadt",
            region="Burgenland",
            website="eisenstadt.gv.at",
            emails=["eisenstadt.gv.at"],
            source="scrape",
            confidence="high",
            sources_detail={"scrape": ["eisenstadt.gv.at"]},
            flags=[],
        )
        assert d.source == "scrape"
        assert d.confidence == "high"
        assert isinstance(d, MunicipalityOutput)


class TestPipelineOutput:
    def test_structure(self):
        now = datetime.now(tz=timezone.utc)
        p = PipelineOutput(
            generated=now,
            total=2,
            municipalities=[
                MunicipalityOutput(code="1", name="A", region="R"),
                MunicipalityOutput(code="2", name="B", region="R"),
            ],
        )
        assert p.total == 2
        assert len(p.municipalities) == 2

    def test_json_roundtrip(self):
        now = datetime.now(tz=timezone.utc)
        p = PipelineOutput(
            generated=now,
            total=1,
            municipalities=[
                MunicipalityDetailedOutput(
                    code="261",
                    name="Zürich",
                    region="Kanton Zürich",
                    source="scrape",
                    confidence="high",
                )
            ],
        )
        json_str = p.model_dump_json()
        p2 = PipelineOutput.model_validate_json(json_str)
        assert p2.total == 1
