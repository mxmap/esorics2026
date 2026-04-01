"""Cross-attribute validation tests for pipeline output.

Each test builds a realistic set of MunicipalityRecords for a country,
runs the decide phase, then asserts that the output is self-consistent.
Also validates real output files in domains/ when they exist.
"""

import json
from pathlib import Path

import pytest

from municipality_email.countries.austria import AustriaConfig
from municipality_email.countries.germany import GermanyConfig
from municipality_email.countries.switzerland import SwitzerlandConfig
from municipality_email.pipeline import _decide_one, phase_export
from municipality_email.schemas import (
    Confidence,
    Country,
    DomainCandidate,
    MunicipalityRecord,
    Source,
)

DOMAINS_DIR = Path(__file__).resolve().parent.parent / "domains"

# ── Helpers ────────────────────────────────────────────────────────────

VALID_SOURCE_VALUES = {s.value for s in Source}
VALID_CONFIDENCE_VALUES = {c.value for c in Confidence}
VALID_SOURCES = {
    Source.OVERRIDE,
    Source.SCRAPE,
    Source.REDIRECT,
    Source.WIKIDATA,
    Source.STATIC,
    Source.GUESS,
}
VALID_CONFIDENCES = {Confidence.HIGH, Confidence.MEDIUM, Confidence.LOW, Confidence.NONE}
VALID_FLAGS = {"no_mx", "unverified", "guess_only", "website_mismatch", "no_municipality_keywords"}

CH_CANTONS = {
    "Kanton Zürich",
    "Kanton Bern",
    "Kanton Luzern",
    "Kanton Uri",
    "Kanton Schwyz",
    "Kanton Obwalden",
    "Kanton Nidwalden",
    "Kanton Glarus",
    "Kanton Zug",
    "Kanton Freiburg",
    "Kanton Solothurn",
    "Kanton Basel-Stadt",
    "Kanton Basel-Landschaft",
    "Kanton Schaffhausen",
    "Kanton Appenzell Ausserrhoden",
    "Kanton Appenzell Innerrhoden",
    "Kanton St. Gallen",
    "Kanton Graubünden",
    "Kanton Aargau",
    "Kanton Thurgau",
    "Kanton Tessin",
    "Kanton Waadt",
    "Kanton Wallis",
    "Kanton Neuenburg",
    "Kanton Genf",
    "Kanton Jura",
}

DE_BUNDESLAENDER = {
    "Schleswig-Holstein",
    "Hamburg",
    "Niedersachsen",
    "Bremen",
    "Nordrhein-Westfalen",
    "Hessen",
    "Rheinland-Pfalz",
    "Baden-Württemberg",
    "Bayern",
    "Saarland",
    "Berlin",
    "Brandenburg",
    "Mecklenburg-Vorpommern",
    "Sachsen",
    "Sachsen-Anhalt",
    "Thüringen",
}

AT_BUNDESLAENDER = {
    "Burgenland",
    "Kärnten",
    "Niederösterreich",
    "Oberösterreich",
    "Salzburg",
    "Steiermark",
    "Tirol",
    "Vorarlberg",
    "Wien",
}


def assert_minimal_entry(m: dict, *, label: str = "") -> None:
    """Validate a single minimal-tier municipality entry."""
    ctx = f"{m.get('code', '?')} {m.get('name', '?')}"
    assert m.get("code"), f"{label}{ctx}: code missing"
    assert m.get("name"), f"{label}{ctx}: name missing"
    assert "region" in m, f"{label}{ctx}: region key missing"
    assert "website" in m, f"{label}{ctx}: website key missing"
    assert isinstance(m["website"], str)
    assert "emails" in m, f"{label}{ctx}: emails key missing"
    assert isinstance(m["emails"], list)

    # Email domains should look valid
    for email in m["emails"]:
        assert isinstance(email, str)
        assert "." in email, f"{label}{ctx}: email '{email}' has no TLD"
        assert " " not in email, f"{label}{ctx}: email '{email}' has spaces"
        assert "@" not in email, f"{label}{ctx}: email '{email}' has @ (should be domain)"

    # Website should look valid if non-empty
    if m["website"]:
        assert "." in m["website"], f"{label}{ctx}: website '{m['website']}' has no TLD"


def assert_detailed_entry(m: dict, *, label: str = "") -> None:
    """Validate a single detailed-tier municipality entry (superset of minimal)."""
    assert_minimal_entry(m, label=label)
    ctx = f"{m.get('code', '?')} {m.get('name', '?')}"

    assert m["source"] in VALID_SOURCE_VALUES, f"{label}{ctx}: bad source '{m['source']}'"
    assert m["confidence"] in VALID_CONFIDENCE_VALUES, (
        f"{label}{ctx}: bad confidence '{m['confidence']}'"
    )
    assert isinstance(m["flags"], list)
    assert isinstance(m["sources_detail"], dict)

    # Cross-attribute invariants
    if m["emails"]:
        assert m["confidence"] != "none", f"{label}{ctx}: has emails but confidence=none"
        assert m["source"] != "none", f"{label}{ctx}: has emails but source=none"
    if m["confidence"] == "none":
        assert m["emails"] == [], f"{label}{ctx}: confidence=none but has emails"
    if m["source"] == "none":
        assert m["confidence"] == "none", (
            f"{label}{ctx}: source=none but confidence={m['confidence']}"
        )
        assert m["emails"] == [], f"{label}{ctx}: source=none but has emails"

    # Flag ↔ source/confidence rules
    if "no_mx" in m["flags"]:
        assert m["source"] == "override", f"{label}{ctx}: no_mx but source={m['source']}"
    if "guess_only" in m["flags"]:
        assert m["source"] == "guess", f"{label}{ctx}: guess_only but source={m['source']}"
        assert m["confidence"] == "low", (
            f"{label}{ctx}: guess_only but confidence={m['confidence']}"
        )
    if "unverified" in m["flags"]:
        assert m["source"] not in ("scrape", "override", "guess", "none"), (
            f"{label}{ctx}: unverified but source={m['source']}"
        )
        assert m["confidence"] == "medium", (
            f"{label}{ctx}: unverified but confidence={m['confidence']}"
        )
    if m["source"] == "scrape":
        assert m["confidence"] == "high", f"{label}{ctx}: scrape but confidence={m['confidence']}"

    for flag in m["flags"]:
        assert flag in VALID_FLAGS, f"{label}{ctx}: unknown flag '{flag}'"


def assert_record_invariants(rec: MunicipalityRecord) -> None:
    """Validate cross-attribute invariants on a decided record."""
    # Required fields always present
    assert rec.code, f"{rec.name}: code must be non-empty"
    assert rec.name, f"{rec.code}: name must be non-empty"
    assert rec.region, f"{rec.code}: region must be non-empty"
    assert rec.confidence in VALID_CONFIDENCES
    assert rec.source in VALID_SOURCES | {Source.NONE}

    # Emails ↔ confidence consistency
    if rec.emails:
        assert rec.confidence != Confidence.NONE, (
            f"{rec.code} {rec.name}: has emails {rec.emails} but confidence=NONE"
        )
        assert rec.source != Source.NONE, f"{rec.code} {rec.name}: has emails but source=NONE"
    if rec.confidence == Confidence.NONE:
        assert rec.emails == [], f"{rec.code} {rec.name}: confidence=NONE but emails={rec.emails}"

    # Source ↔ confidence consistency
    if rec.source == Source.NONE:
        assert rec.confidence == Confidence.NONE, (
            f"{rec.code} {rec.name}: source=NONE but confidence={rec.confidence}"
        )
        assert rec.emails == [], f"{rec.code} {rec.name}: source=NONE but emails={rec.emails}"

    # Flag ↔ source/confidence consistency
    if "no_mx" in rec.flags:
        assert rec.source == Source.OVERRIDE, (
            f"{rec.code} {rec.name}: no_mx flag but source={rec.source}"
        )
    if "guess_only" in rec.flags:
        assert rec.source == Source.GUESS, (
            f"{rec.code} {rec.name}: guess_only flag but source={rec.source}"
        )
        assert rec.confidence == Confidence.LOW, (
            f"{rec.code} {rec.name}: guess_only flag but confidence={rec.confidence}"
        )
    if "unverified" in rec.flags:
        assert rec.source not in (Source.SCRAPE, Source.OVERRIDE, Source.GUESS, Source.NONE), (
            f"{rec.code} {rec.name}: unverified flag but source={rec.source}"
        )
        assert rec.confidence == Confidence.MEDIUM, (
            f"{rec.code} {rec.name}: unverified flag but confidence={rec.confidence}"
        )

    # Scrape source → high confidence
    if rec.source == Source.SCRAPE:
        assert rec.confidence == Confidence.HIGH, (
            f"{rec.code} {rec.name}: scrape source but confidence={rec.confidence}"
        )

    # Email domains should look like domains
    for email in rec.emails:
        assert "." in email, f"{rec.code} {rec.name}: email '{email}' missing TLD"
        assert " " not in email, f"{rec.code} {rec.name}: email '{email}' contains space"
        assert "@" not in email, (
            f"{rec.code} {rec.name}: email '{email}' contains @ (should be domain only)"
        )

    # Website domain should look like a domain if set
    if rec.website_domain:
        assert "." in rec.website_domain, (
            f"{rec.code} {rec.name}: website '{rec.website_domain}' missing TLD"
        )

    # All flags should be known
    for flag in rec.flags:
        assert flag in VALID_FLAGS, f"{rec.code} {rec.name}: unknown flag '{flag}'"


def _make_ch(**kwargs) -> MunicipalityRecord:
    defaults = dict(code="0261", name="Zürich", region="Kanton Zürich", country=Country.CH)
    defaults.update(kwargs)
    return MunicipalityRecord(**defaults)  # type: ignore[arg-type]


def _make_de(**kwargs) -> MunicipalityRecord:
    defaults = dict(
        code="01001000", name="Flensburg", region="Schleswig-Holstein", country=Country.DE
    )
    defaults.update(kwargs)
    return MunicipalityRecord(**defaults)  # type: ignore[arg-type]


def _make_at(**kwargs) -> MunicipalityRecord:
    defaults = dict(code="10101", name="Eisenstadt", region="Burgenland", country=Country.AT)
    defaults.update(kwargs)
    return MunicipalityRecord(**defaults)  # type: ignore[arg-type]


# ── Switzerland ────────────────────────────────────────────────────────


class TestSwitzerlandOutput:
    def setup_method(self):
        self.config = SwitzerlandConfig()
        self.empty_validation: dict[str, tuple[bool, str | None, bool]] = {}

    def _decide(self, rec, mx_valid=None, validation=None):
        _decide_one(rec, self.config, mx_valid or {}, validation or self.empty_validation)
        assert_record_invariants(rec)
        return rec

    def test_scraped_ch_domain(self):
        rec = _make_ch(
            candidates=[DomainCandidate(domain="zuerich.ch", source="wikidata")],
            scraped_emails={"zuerich.ch": ["zuerich.ch"]},
        )
        self._decide(rec, mx_valid={"zuerich.ch": True})
        assert rec.emails == ["zuerich.ch"]
        assert rec.confidence == Confidence.HIGH
        assert rec.source == Source.SCRAPE

    def test_override_with_mx(self):
        rec = _make_ch(override_domain="stadt-zuerich.ch")
        self._decide(rec, mx_valid={"stadt-zuerich.ch": True})
        assert rec.emails == ["stadt-zuerich.ch"]
        assert rec.source == Source.OVERRIDE
        assert rec.confidence == Confidence.HIGH

    def test_override_without_mx(self):
        rec = _make_ch(override_domain="stadt-zuerich.ch")
        self._decide(rec, mx_valid={"stadt-zuerich.ch": False})
        assert "no_mx" in rec.flags
        assert rec.confidence == Confidence.MEDIUM

    def test_override_empty_suppresses_email(self):
        rec = _make_ch(override_domain="")
        self._decide(rec)
        assert rec.emails == []
        assert rec.confidence == Confidence.NONE
        assert rec.source == Source.OVERRIDE

    def test_static_with_name_match(self):
        rec = _make_ch(
            name="Ebikon",
            candidates=[DomainCandidate(domain="ebikon.ch", source="wikidata")],
        )
        self._decide(rec, mx_valid={"ebikon.ch": True})
        assert rec.confidence == Confidence.HIGH
        assert "unverified" not in rec.flags

    def test_static_without_name_match(self):
        rec = _make_ch(
            candidates=[DomainCandidate(domain="unrelated.ch", source="livenson")],
        )
        self._decide(rec, mx_valid={"unrelated.ch": True})
        assert rec.confidence == Confidence.MEDIUM
        assert "unverified" in rec.flags

    def test_guess_only(self):
        rec = _make_ch(
            name="Ebikon",
            candidates=[DomainCandidate(domain="ebikon.ch", source="guess")],
        )
        self._decide(rec, mx_valid={"ebikon.ch": True})
        assert rec.confidence == Confidence.LOW
        assert "guess_only" in rec.flags
        assert rec.source == Source.GUESS

    def test_nothing_found(self):
        rec = _make_ch(candidates=[])
        self._decide(rec)
        assert rec.emails == []
        assert rec.confidence == Confidence.NONE
        assert rec.source == Source.NONE

    def test_parked_domain_excluded(self):
        rec = _make_ch(
            candidates=[DomainCandidate(domain="parked.ch", source="wikidata")],
            content_flags={"parked.ch": ["parked"]},
        )
        self._decide(rec, mx_valid={"parked.ch": True})
        assert rec.emails == []
        assert rec.confidence == Confidence.NONE

    def test_multiple_scraped_emails_returned(self):
        rec = _make_ch(
            name="Zürich",
            candidates=[DomainCandidate(domain="zuerich.ch", source="wikidata")],
            scraped_emails={"zuerich.ch": ["zuerich.ch", "verwaltung.ch"]},
        )
        self._decide(rec, mx_valid={"zuerich.ch": True, "verwaltung.ch": True})
        assert len(rec.emails) == 2
        assert "zuerich.ch" in rec.emails
        assert "verwaltung.ch" in rec.emails


# ── Germany ────────────────────────────────────────────────────────────


class TestGermanyOutput:
    def setup_method(self):
        self.config = GermanyConfig()
        self.empty_validation: dict[str, tuple[bool, str | None, bool]] = {}

    def _decide(self, rec, mx_valid=None, validation=None):
        _decide_one(rec, self.config, mx_valid or {}, validation or self.empty_validation)
        assert_record_invariants(rec)
        return rec

    def test_scraped_de_domain(self):
        rec = _make_de(
            candidates=[DomainCandidate(domain="flensburg.de", source="livenson")],
            scraped_emails={"flensburg.de": ["flensburg.de"]},
        )
        self._decide(rec, mx_valid={"flensburg.de": True})
        assert rec.emails == ["flensburg.de"]
        assert rec.confidence == Confidence.HIGH

    def test_override_beats_scrape(self):
        rec = _make_de(
            override_domain="official.de",
            candidates=[DomainCandidate(domain="flensburg.de", source="livenson")],
            scraped_emails={"flensburg.de": ["flensburg.de"]},
        )
        self._decide(rec, mx_valid={"official.de": True, "flensburg.de": True})
        assert rec.emails == ["official.de"]
        assert rec.source == Source.OVERRIDE

    def test_static_wikidata_source(self):
        rec = _make_de(
            candidates=[DomainCandidate(domain="flensburg.de", source="wikidata")],
        )
        self._decide(rec, mx_valid={"flensburg.de": True})
        assert rec.source == Source.WIKIDATA
        assert rec.confidence == Confidence.HIGH  # name matches domain

    def test_no_mx_for_any_candidate(self):
        rec = _make_de(
            candidates=[
                DomainCandidate(domain="a.de", source="livenson"),
                DomainCandidate(domain="b.de", source="wikidata"),
            ],
        )
        self._decide(rec, mx_valid={"a.de": False, "b.de": False})
        assert rec.emails == []
        assert rec.confidence == Confidence.NONE

    def test_guess_with_gemeinde_prefix(self):
        rec = _make_de(
            name="Neustadt",
            candidates=[DomainCandidate(domain="gemeinde-neustadt.de", source="guess")],
        )
        self._decide(rec, mx_valid={"gemeinde-neustadt.de": True})
        assert rec.confidence == Confidence.LOW
        assert rec.source == Source.GUESS

    def test_multiple_sources_scrape_wins(self):
        rec = _make_de(
            name="Flensburg",
            candidates=[
                DomainCandidate(domain="flensburg.de", source="livenson"),
                DomainCandidate(domain="stadt-flensburg.de", source="wikidata"),
            ],
            scraped_emails={"flensburg.de": ["flensburg.de"]},
        )
        self._decide(rec, mx_valid={"flensburg.de": True, "stadt-flensburg.de": True})
        assert rec.source == Source.SCRAPE
        assert "flensburg.de" in rec.emails


# ── Austria ────────────────────────────────────────────────────────────


class TestAustriaOutput:
    def setup_method(self):
        self.config = AustriaConfig()
        self.empty_validation: dict[str, tuple[bool, str | None, bool]] = {}

    def _decide(self, rec, mx_valid=None, validation=None):
        _decide_one(rec, self.config, mx_valid or {}, validation or self.empty_validation)
        assert_record_invariants(rec)
        return rec

    def test_gv_at_domain_preferred(self):
        """Austria prefers .gv.at government domains."""
        rec = _make_at(
            candidates=[
                DomainCandidate(domain="eisenstadt.gv.at", source="wikidata"),
                DomainCandidate(domain="eisenstadt.at", source="bresu"),
            ],
            scraped_emails={"eisenstadt.gv.at": ["eisenstadt.gv.at", "eisenstadt.at"]},
        )
        self._decide(rec, mx_valid={"eisenstadt.gv.at": True, "eisenstadt.at": True})
        # .gv.at should come first due to Austria's pick_best_email
        assert rec.emails[0] == "eisenstadt.gv.at"

    def test_gde_at_domain(self):
        """.gde.at is also a government domain in Austria."""
        rec = _make_at(
            name="Klagenfurt",
            code="20101",
            region="Kärnten",
            candidates=[DomainCandidate(domain="klagenfurt.ktn.gde.at", source="bresu")],
            scraped_emails={"klagenfurt.ktn.gde.at": ["klagenfurt.ktn.gde.at"]},
        )
        self._decide(rec, mx_valid={"klagenfurt.ktn.gde.at": True})
        assert rec.emails == ["klagenfurt.ktn.gde.at"]
        assert rec.confidence == Confidence.HIGH

    def test_override_with_gv_at(self):
        rec = _make_at(override_domain="eisenstadt.gv.at")
        self._decide(rec, mx_valid={"eisenstadt.gv.at": True})
        assert rec.source == Source.OVERRIDE
        assert rec.confidence == Confidence.HIGH

    def test_static_bresu_source(self):
        rec = _make_at(
            candidates=[DomainCandidate(domain="eisenstadt.gv.at", source="bresu")],
        )
        self._decide(rec, mx_valid={"eisenstadt.gv.at": True})
        assert rec.source == Source.STATIC
        assert rec.confidence == Confidence.HIGH  # name match via .gv.at

    def test_nothing_found_at(self):
        rec = _make_at(candidates=[])
        self._decide(rec)
        assert rec.emails == []
        assert rec.confidence == Confidence.NONE
        assert rec.source == Source.NONE

    def test_parked_at_domain_excluded(self):
        rec = _make_at(
            candidates=[DomainCandidate(domain="parked.at", source="bresu")],
            content_flags={"parked.at": ["parked"]},
        )
        self._decide(rec, mx_valid={"parked.at": True})
        assert rec.emails == []


# ── Export validation ──────────────────────────────────────────────────


class TestExportOutputValidation:
    """Validate that exported JSON files have consistent structure."""

    @pytest.fixture()
    def ch_records(self):
        config = SwitzerlandConfig()
        validation: dict[str, tuple[bool, str | None, bool]] = {
            "zuerich.ch": (True, None, False),
        }
        records = [
            _make_ch(
                code="0261",
                name="Zürich",
                candidates=[DomainCandidate(domain="zuerich.ch", source="wikidata")],
                scraped_emails={"zuerich.ch": ["zuerich.ch"]},
            ),
            _make_ch(code="0351", name="Bern", region="Kanton Bern"),
            _make_ch(
                code="1061",
                name="Ebikon",
                region="Kanton Luzern",
                candidates=[DomainCandidate(domain="ebikon.ch", source="guess")],
            ),
        ]
        mx = {"zuerich.ch": True, "ebikon.ch": True}
        for rec in records:
            _decide_one(rec, config, mx, validation)
        return records

    def test_minimal_output_structure(self, ch_records, tmp_path):
        phase_export(ch_records, tmp_path, "ch")
        data = json.loads((tmp_path / "ch.json").read_text())

        assert "generated" in data
        assert data["total"] == len(ch_records)
        assert len(data["municipalities"]) == data["total"]

        for m in data["municipalities"]:
            assert_minimal_entry(m)
            # Minimal should NOT have detailed fields
            assert "source" not in m
            assert "confidence" not in m

    def test_detailed_output_structure(self, ch_records, tmp_path):
        phase_export(ch_records, tmp_path, "ch")
        data = json.loads((tmp_path / "ch_detailed.json").read_text())

        assert data["total"] == len(ch_records)
        for m in data["municipalities"]:
            assert_detailed_entry(m)

    def test_review_only_contains_flagged_or_low_confidence(self, ch_records, tmp_path):
        phase_export(ch_records, tmp_path, "ch")
        data = json.loads((tmp_path / "ch_review.json").read_text())

        for m in data["municipalities"]:
            assert_detailed_entry(m)
            needs_review = m["confidence"] in ("low", "none") or len(m["flags"]) > 0
            assert needs_review

    def test_codes_are_unique(self, ch_records, tmp_path):
        phase_export(ch_records, tmp_path, "ch")
        data = json.loads((tmp_path / "ch.json").read_text())
        codes = [m["code"] for m in data["municipalities"]]
        assert len(codes) == len(set(codes)), "Duplicate codes in output"

    def test_sorted_by_code(self, ch_records, tmp_path):
        phase_export(ch_records, tmp_path, "ch")
        data = json.loads((tmp_path / "ch.json").read_text())
        codes = [m["code"] for m in data["municipalities"]]
        assert codes == sorted(codes), "Output not sorted by code"


class TestExportMultiCountry:
    """Run decide + export for each country with varied scenarios and validate output."""

    @pytest.mark.parametrize(
        "cc,config_cls,make_fn,tld",
        [
            ("ch", SwitzerlandConfig, _make_ch, ".ch"),
            ("de", GermanyConfig, _make_de, ".de"),
            ("at", AustriaConfig, _make_at, ".at"),
        ],
        ids=["switzerland", "germany", "austria"],
    )
    def test_varied_scenarios_consistent(self, cc, config_cls, make_fn, tld, tmp_path):
        config = config_cls()
        domain = f"test{tld}"
        guess_domain = f"guess{tld}"
        static_domain = f"static{tld}"

        records = [
            # Scraped with MX
            make_fn(
                code="001",
                name="ScrapedTown",
                candidates=[DomainCandidate(domain=domain, source="wikidata")],
                scraped_emails={domain: [domain]},
            ),
            # Override
            make_fn(code="002", name="OverrideTown", override_domain=f"override{tld}"),
            # Override empty (suppressed)
            make_fn(code="003", name="SuppressedTown", override_domain=""),
            # Static unverified
            make_fn(
                code="004",
                name="StaticTown",
                candidates=[DomainCandidate(domain=static_domain, source="livenson")],
            ),
            # Guess only
            make_fn(
                code="005",
                name="GuessTown",
                candidates=[DomainCandidate(domain=guess_domain, source="guess")],
            ),
            # Nothing
            make_fn(code="006", name="EmptyTown"),
            # Parked
            make_fn(
                code="007",
                name="ParkedTown",
                candidates=[DomainCandidate(domain=f"parked{tld}", source="wikidata")],
                content_flags={f"parked{tld}": ["parked"]},
            ),
        ]

        mx = {
            domain: True,
            f"override{tld}": True,
            static_domain: True,
            guess_domain: True,
            f"parked{tld}": True,
        }
        validation: dict[str, tuple[bool, str | None, bool]] = {
            domain: (True, None, False),
        }

        for rec in records:
            _decide_one(rec, config, mx, validation)
            assert_record_invariants(rec)

        # Verify expected outcomes
        by_code = {r.code: r for r in records}
        assert by_code["001"].confidence == Confidence.HIGH
        assert by_code["001"].source == Source.SCRAPE
        assert by_code["002"].source == Source.OVERRIDE
        assert by_code["003"].emails == []
        assert by_code["003"].source == Source.OVERRIDE
        assert by_code["004"].confidence in (Confidence.HIGH, Confidence.MEDIUM)
        assert "unverified" in by_code["004"].flags or config.domain_matches_name(
            "StaticTown", static_domain
        )
        assert by_code["005"].confidence == Confidence.LOW
        assert "guess_only" in by_code["005"].flags
        assert by_code["006"].confidence == Confidence.NONE
        assert by_code["007"].emails == []

        # Export and validate JSON structure
        phase_export(records, tmp_path, cc)

        for suffix in [f"{cc}.json", f"{cc}_detailed.json", f"{cc}_review.json"]:
            data = json.loads((tmp_path / suffix).read_text())
            assert data["total"] >= 0
            assert len(data["municipalities"]) == data["total"]

        detailed = json.loads((tmp_path / f"{cc}_detailed.json").read_text())
        for m in detailed["municipalities"]:
            assert_detailed_entry(m)


# ── Real output file validation ───────────────────────────────────────


def _load_output(cc: str, suffix: str = "") -> dict | None:
    """Load a real output file, returning None if it doesn't exist."""
    filename = f"{cc}{suffix}.json"
    path = DOMAINS_DIR / filename
    if not path.exists():
        return None
    return json.loads(path.read_text())


class TestRealOutputFiles:
    """Validate actual pipeline output files in domains/.

    Tests are skipped when the corresponding file does not exist.
    """

    @pytest.mark.parametrize("cc", ["ch", "de", "at"])
    def test_minimal_structure(self, cc):
        data = _load_output(cc)
        if data is None:
            pytest.skip(f"domains/{cc}.json not found")

        assert "generated" in data
        assert "total" in data
        assert isinstance(data["total"], int)
        assert data["total"] > 0, "Output has zero municipalities"
        assert len(data["municipalities"]) == data["total"]

        for m in data["municipalities"]:
            assert_minimal_entry(m, label=f"[{cc}] ")

    @pytest.mark.parametrize("cc", ["ch", "de", "at"])
    def test_detailed_structure_and_invariants(self, cc):
        data = _load_output(cc, "_detailed")
        if data is None:
            pytest.skip(f"domains/{cc}_detailed.json not found")

        assert data["total"] > 0
        assert len(data["municipalities"]) == data["total"]

        for m in data["municipalities"]:
            assert_detailed_entry(m, label=f"[{cc}] ")

    @pytest.mark.parametrize("cc", ["ch", "de", "at"])
    def test_review_only_flagged_or_low(self, cc):
        data = _load_output(cc, "_review")
        if data is None:
            pytest.skip(f"domains/{cc}_review.json not found")

        assert len(data["municipalities"]) == data["total"]

        for m in data["municipalities"]:
            assert_detailed_entry(m, label=f"[{cc} review] ")
            needs_review = m["confidence"] in ("low", "none") or len(m["flags"]) > 0
            assert needs_review, (
                f"[{cc}] {m['code']} {m['name']}: in review but confidence={m['confidence']} flags={m['flags']}"
            )

    @pytest.mark.parametrize("cc", ["ch", "de", "at"])
    def test_codes_unique_and_sorted(self, cc):
        data = _load_output(cc)
        if data is None:
            pytest.skip(f"domains/{cc}.json not found")

        codes = [m["code"] for m in data["municipalities"]]
        assert len(codes) == len(set(codes)), f"[{cc}] Duplicate codes"
        assert codes == sorted(codes), f"[{cc}] Not sorted by code"

    @pytest.mark.parametrize("cc", ["ch", "de", "at"])
    def test_names_non_empty(self, cc):
        data = _load_output(cc)
        if data is None:
            pytest.skip(f"domains/{cc}.json not found")

        for m in data["municipalities"]:
            assert m["name"].strip(), f"[{cc}] {m['code']}: empty name"

    @pytest.mark.parametrize(
        "cc,expected_regions",
        [
            ("ch", CH_CANTONS),
            ("de", DE_BUNDESLAENDER),
            ("at", AT_BUNDESLAENDER),
        ],
    )
    def test_regions_are_valid(self, cc, expected_regions):
        data = _load_output(cc)
        if data is None:
            pytest.skip(f"domains/{cc}.json not found")

        for m in data["municipalities"]:
            assert m["region"], f"[{cc}] {m['code']} {m['name']}: region is empty"
            assert m["region"] in expected_regions, (
                f"[{cc}] {m['code']} {m['name']}: unknown region '{m['region']}'"
            )

    @pytest.mark.parametrize("cc", ["ch", "de", "at"])
    def test_review_is_subset_of_detailed(self, cc):
        detailed = _load_output(cc, "_detailed")
        review = _load_output(cc, "_review")
        if detailed is None or review is None:
            pytest.skip(f"domains/{cc}_detailed.json or {cc}_review.json not found")

        detailed_codes = {m["code"] for m in detailed["municipalities"]}
        review_codes = {m["code"] for m in review["municipalities"]}
        assert review_codes <= detailed_codes, (
            f"[{cc}] Review contains codes not in detailed: {review_codes - detailed_codes}"
        )

    @pytest.mark.parametrize("cc", ["ch", "de", "at"])
    def test_minimal_and_detailed_agree(self, cc):
        minimal = _load_output(cc)
        detailed = _load_output(cc, "_detailed")
        if minimal is None or detailed is None:
            pytest.skip(f"domains/{cc}.json or {cc}_detailed.json not found")

        assert minimal["total"] == detailed["total"]

        min_by_code = {m["code"]: m for m in minimal["municipalities"]}
        det_by_code = {m["code"]: m for m in detailed["municipalities"]}

        assert set(min_by_code.keys()) == set(det_by_code.keys()), (
            f"[{cc}] Code sets differ between minimal and detailed"
        )

        for code in min_by_code:
            mi = min_by_code[code]
            de = det_by_code[code]
            assert mi["name"] == de["name"], f"[{cc}] {code}: name mismatch"
            assert mi["region"] == de["region"], f"[{cc}] {code}: region mismatch"
            assert mi["website"] == de["website"], f"[{cc}] {code}: website mismatch"
            assert mi["emails"] == de["emails"], f"[{cc}] {code}: emails mismatch"
