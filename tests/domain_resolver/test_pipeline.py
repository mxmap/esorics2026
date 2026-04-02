"""Tests for pipeline orchestrator."""

from mail_municipalities.domain_resolver.countries.austria import AustriaConfig
from mail_municipalities.domain_resolver.countries.germany import GermanyConfig
from mail_municipalities.domain_resolver.pipeline import _decide_one, phase_export
from mail_municipalities.domain_resolver.schemas import (
    Confidence,
    Country,
    DomainCandidate,
    MunicipalityRecord,
    Source,
)


def _make_record(**kwargs) -> MunicipalityRecord:
    defaults = dict(code="001", name="Test", region="Region", country=Country.DE)
    defaults.update(kwargs)
    return MunicipalityRecord(**defaults)  # type: ignore[arg-type]


class TestDecideOne:
    def setup_method(self):
        self.config = GermanyConfig()
        self.empty_validation: dict[str, tuple[bool, str | None, bool]] = {}

    def test_override_with_mx(self):
        rec = _make_record(override_domain="test.de")
        mx_valid = {"test.de": True}
        _decide_one(rec, self.config, mx_valid, self.empty_validation)
        assert rec.emails == ["test.de"]
        assert rec.confidence == Confidence.HIGH
        assert rec.source == Source.OVERRIDE

    def test_override_without_mx(self):
        rec = _make_record(override_domain="test.de")
        mx_valid = {"test.de": False}
        _decide_one(rec, self.config, mx_valid, self.empty_validation)
        assert rec.emails == ["test.de"]
        assert rec.confidence == Confidence.MEDIUM
        assert "no_mx" in rec.flags

    def test_override_empty_domain(self):
        rec = _make_record(override_domain="")
        _decide_one(rec, self.config, {}, self.empty_validation)
        assert rec.confidence == Confidence.NONE
        assert rec.source == Source.OVERRIDE

    def test_scraped_emails(self):
        rec = _make_record(
            candidates=[DomainCandidate(domain="test.de", source="livenson")],
            scraped_emails={"test.de": ["email.de"]},
        )
        mx_valid = {"email.de": True, "test.de": True}
        _decide_one(rec, self.config, mx_valid, self.empty_validation)
        assert "email.de" in rec.emails
        assert rec.confidence == Confidence.HIGH
        assert rec.source == Source.SCRAPE

    def test_scraped_beats_static(self):
        rec = _make_record(
            candidates=[
                DomainCandidate(domain="static.de", source="livenson"),
                DomainCandidate(domain="other.de", source="wikidata"),
            ],
            scraped_emails={"static.de": ["scraped.de"]},
        )
        mx_valid = {"scraped.de": True, "static.de": True, "other.de": True}
        _decide_one(rec, self.config, mx_valid, self.empty_validation)
        assert rec.confidence == Confidence.HIGH
        assert rec.source == Source.SCRAPE

    def test_static_unconfirmed(self):
        rec = _make_record(
            candidates=[DomainCandidate(domain="static.de", source="livenson")],
        )
        mx_valid = {"static.de": True}
        _decide_one(rec, self.config, mx_valid, self.empty_validation)
        assert rec.emails == ["static.de"]
        assert rec.confidence == Confidence.MEDIUM
        assert "unverified" in rec.flags

    def test_static_name_match_verified(self):
        rec = _make_record(
            name="Ebikon",
            candidates=[DomainCandidate(domain="ebikon.de", source="wikidata")],
        )
        mx_valid = {"ebikon.de": True}
        _decide_one(rec, self.config, mx_valid, self.empty_validation)
        assert rec.emails == ["ebikon.de"]
        assert rec.confidence == Confidence.HIGH
        assert "unverified" not in rec.flags

    def test_static_single_source_stays_medium(self):
        """Static candidate from one source without name match stays MEDIUM."""
        rec = _make_record(
            candidates=[DomainCandidate(domain="amt-eider.de", source="livenson", is_email_domain=True)],
        )
        mx_valid = {"amt-eider.de": True}
        _decide_one(rec, self.config, mx_valid, self.empty_validation)
        assert rec.emails == ["amt-eider.de"]
        assert rec.confidence == Confidence.MEDIUM
        assert "unverified" in rec.flags

    def test_static_multi_source_gets_high(self):
        """Static candidate confirmed by multiple sources gets HIGH confidence."""
        rec = _make_record(
            candidates=[
                DomainCandidate(domain="amt-eider.de", source="livenson", is_email_domain=True),
                DomainCandidate(domain="amt-eider.de", source="b42labs", is_email_domain=True),
            ],
        )
        mx_valid = {"amt-eider.de": True}
        _decide_one(rec, self.config, mx_valid, self.empty_validation)
        assert rec.emails == ["amt-eider.de"]
        assert rec.confidence == Confidence.HIGH
        assert "unverified" not in rec.flags

    def test_guess_only(self):
        rec = _make_record(
            candidates=[DomainCandidate(domain="guess.de", source="guess")],
        )
        mx_valid = {"guess.de": True}
        _decide_one(rec, self.config, mx_valid, self.empty_validation)
        assert rec.emails == ["guess.de"]
        assert rec.confidence == Confidence.LOW
        assert "guess_only" in rec.flags

    def test_nothing_found(self):
        rec = _make_record()
        _decide_one(rec, self.config, {}, self.empty_validation)
        assert rec.emails == []
        assert rec.confidence == Confidence.NONE
        assert rec.source == Source.NONE

    def test_parked_domain_excluded_from_static_pool(self):
        rec = _make_record(
            candidates=[DomainCandidate(domain="parked.de", source="livenson")],
            content_flags={"parked.de": ["parked"]},
        )
        mx_valid = {"parked.de": True}
        _decide_one(rec, self.config, mx_valid, self.empty_validation)
        assert rec.emails == []
        assert rec.confidence == Confidence.NONE

    def test_parked_domain_excluded_from_guess_pool(self):
        rec = _make_record(
            candidates=[DomainCandidate(domain="parked.de", source="guess")],
            content_flags={"parked.de": ["parked"]},
        )
        mx_valid = {"parked.de": True}
        _decide_one(rec, self.config, mx_valid, self.empty_validation)
        assert rec.emails == []
        assert rec.confidence == Confidence.NONE

    def test_multiple_scraped_emails(self):
        rec = _make_record(
            name="Flensburg",
            candidates=[DomainCandidate(domain="flensburg.de", source="livenson")],
            scraped_emails={"flensburg.de": ["flensburg.de", "alt-flensburg.de"]},
        )
        mx_valid = {"flensburg.de": True, "alt-flensburg.de": True}
        _decide_one(rec, self.config, mx_valid, self.empty_validation)
        assert len(rec.emails) == 2
        assert rec.confidence == Confidence.HIGH


def _make_at_record(**kwargs) -> MunicipalityRecord:
    defaults = dict(code="20604", name="Dellach im Drautal", region="Kärnten", country=Country.AT)
    defaults.update(kwargs)
    return MunicipalityRecord(**defaults)  # type: ignore[arg-type]


class TestDecideOneAustria:
    def setup_method(self):
        self.config = AustriaConfig()
        self.empty_validation: dict[str, tuple[bool, str | None, bool]] = {}

    def test_regional_domain_gets_high(self):
        """Regional email domain (ktn.gde.at) for matching region → HIGH."""
        rec = _make_at_record(
            candidates=[DomainCandidate(domain="ktn.gde.at", source="bresu_email")],
        )
        mx_valid = {"ktn.gde.at": True}
        _decide_one(rec, self.config, mx_valid, self.empty_validation)
        assert rec.confidence == Confidence.HIGH
        assert "unverified" not in rec.flags

    def test_name_match_on_secondary_email_gets_high(self):
        """Gov domain first + name-matching domain second → HIGH."""
        rec = _make_at_record(
            candidates=[
                DomainCandidate(domain="ktn.gde.at", source="bresu_email"),
                DomainCandidate(domain="dellach-drau.at", source="wikidata"),
            ],
        )
        mx_valid = {"ktn.gde.at": True, "dellach-drau.at": True}
        _decide_one(rec, self.config, mx_valid, self.empty_validation)
        assert rec.confidence == Confidence.HIGH
        assert "unverified" not in rec.flags

    def test_multi_source_on_secondary_email_gets_high(self):
        """Best email single-source, but secondary has 2+ sources → HIGH."""
        rec = _make_at_record(
            candidates=[
                DomainCandidate(domain="ktn.gde.at", source="bresu_email"),
                DomainCandidate(domain="dellach-drau.at", source="bresu"),
                DomainCandidate(domain="dellach-drau.at", source="wikidata"),
            ],
        )
        mx_valid = {"ktn.gde.at": True, "dellach-drau.at": True}
        _decide_one(rec, self.config, mx_valid, self.empty_validation)
        assert rec.confidence == Confidence.HIGH
        assert "unverified" not in rec.flags


class TestPhaseExport:
    def test_creates_three_files(self, tmp_path):
        records = [
            _make_record(
                code="001",
                emails=["test.de"],
                source=Source.SCRAPE,
                confidence=Confidence.HIGH,
            ),
            _make_record(
                code="002",
                emails=[],
                source=Source.NONE,
                confidence=Confidence.NONE,
            ),
        ]
        phase_export(records, tmp_path, "de")

        assert (tmp_path / "domains_de.json").exists()
        assert (tmp_path / "domains_de_detailed.json").exists()
        assert (tmp_path / "domains_de_review.json").exists()

    def test_output_format(self, tmp_path):
        import json

        records = [
            _make_record(
                code="001",
                name="Flensburg",
                region="SH",
                emails=["flensburg.de"],
                source=Source.SCRAPE,
                confidence=Confidence.HIGH,
                website_domain="flensburg.de",
            ),
        ]
        phase_export(records, tmp_path, "de")

        data = json.loads((tmp_path / "domains_de.json").read_text())
        assert data["total"] == 1
        m = data["municipalities"][0]
        assert m["code"] == "001"
        assert m["name"] == "Flensburg"
        assert m["emails"] == ["flensburg.de"]

    def test_review_filters_correctly(self, tmp_path):
        import json

        records = [
            _make_record(code="001", confidence=Confidence.HIGH),
            _make_record(code="002", confidence=Confidence.LOW, flags=["guess_only"]),
            _make_record(code="003", confidence=Confidence.NONE),
        ]
        phase_export(records, tmp_path, "de")

        review = json.loads((tmp_path / "domains_de_review.json").read_text())
        assert review["total"] == 2
        codes = [m["code"] for m in review["municipalities"]]
        assert "001" not in codes
        assert "002" in codes
        assert "003" in codes
