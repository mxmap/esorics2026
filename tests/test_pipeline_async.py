"""Tests for async pipeline phases."""

from pathlib import Path

import respx

from municipality_email.countries.germany import GermanyConfig
from municipality_email.pipeline import (
    _print_dry_run,
    _set_website,
    _update_records_from_scrape,
    phase_collect,
    phase_decide,
    phase_mx,
    phase_scrape,
    phase_validate,
)
from municipality_email.schemas import (
    Confidence,
    Country,
    DomainCandidate,
    MunicipalityRecord,
)


def _make_record(**kwargs) -> MunicipalityRecord:
    defaults = dict(code="001", name="Test", region="Region", country=Country.DE)
    defaults.update(kwargs)
    return MunicipalityRecord(**defaults)


class TestPhaseCollect:
    async def test_adds_guess_for_no_candidates(self):
        config = GermanyConfig()

        async def mock_collect(data_dir):
            return [
                _make_record(code="001", name="Teststadt"),
                _make_record(
                    code="002",
                    name="Flensburg",
                    candidates=[DomainCandidate(domain="flensburg.de", source="livenson")],
                ),
            ]

        config.collect_candidates = mock_collect
        records = await phase_collect(config, Path("data/de"))

        # Record 001 should have guess domains
        r001 = next(r for r in records if r.code == "001")
        assert any(c.source == "guess" for c in r001.candidates)

        # Record 002 should NOT get guess domains (has real candidate)
        r002 = next(r for r in records if r.code == "002")
        assert not any(c.source == "guess" for c in r002.candidates)

    async def test_skip_override_from_guessing(self):
        config = GermanyConfig()

        async def mock_collect(data_dir):
            return [_make_record(code="001", name="Test", override_domain="test.de")]

        config.collect_candidates = mock_collect
        records = await phase_collect(config, Path("data/de"))
        r001 = records[0]
        assert not any(c.source == "guess" for c in r001.candidates)


class TestPhaseValidate:
    async def test_marks_accessible(self):
        records = [
            _make_record(
                candidates=[
                    DomainCandidate(domain="good.de", source="livenson"),
                    DomainCandidate(domain="bad.de", source="wikidata"),
                ]
            )
        ]
        config = GermanyConfig()

        with respx.mock:
            respx.head("https://www.good.de").respond(200)
            respx.head("https://www.bad.de").respond(500)
            respx.head("https://bad.de").respond(500)

            validation = await phase_validate(records, config)

        assert validation["good.de"][0] is True  # accessible
        assert validation["bad.de"][0] is False


class TestPhaseScrape:
    async def test_scrapes_accessible_only(self):
        records = [
            _make_record(
                candidates=[
                    DomainCandidate(domain="good.de", source="livenson"),
                    DomainCandidate(domain="bad.de", source="wikidata"),
                ]
            )
        ]
        config = GermanyConfig()
        validation = {
            "good.de": (True, None, False),
            "bad.de": (False, None, False),
        }

        with respx.mock:
            respx.get("https://www.good.de/").respond(200, html="<p>info@good.de</p>")
            # Scrape subpages too
            for subpage in config.subpages:
                respx.get(f"https://www.good.de{subpage}").respond(200, html="")
            respx.get("https://good.de/").respond(200, html="")
            for subpage in config.subpages:
                respx.get(f"https://good.de{subpage}").respond(200, html="")

            results = await phase_scrape(records, config, validation)

        assert "good.de" in results
        assert "bad.de" not in results

    async def test_uses_cache(self, tmp_path):
        import json
        from datetime import datetime, timezone

        cache_path = tmp_path / "scrape_cache.json"
        cache_data = {
            "cached.de": {
                "emails": ["cached.de"],
                "redirect": None,
                "accessible": True,
                "timestamp": datetime.now(tz=timezone.utc).isoformat(),
            }
        }
        cache_path.write_text(json.dumps(cache_data))

        records = [
            _make_record(candidates=[DomainCandidate(domain="cached.de", source="livenson")])
        ]
        config = GermanyConfig()
        validation = {"cached.de": (True, None, False)}

        results = await phase_scrape(records, config, validation, cache_path)
        assert "cached.de" in results
        assert results["cached.de"][0] == {"cached.de"}


class TestPhaseMx:
    async def test_validates_domains(self, mock_dns):
        mock_dns["good.de"] = ["mx.good.de"]
        mock_dns["bad.de"] = []

        records = [
            _make_record(
                candidates=[
                    DomainCandidate(domain="good.de", source="livenson"),
                    DomainCandidate(domain="bad.de", source="wikidata"),
                ]
            )
        ]
        scrape_results = {"good.de": ({"email.de"}, None, True)}
        mock_dns["email.de"] = ["mx.email.de"]
        config = GermanyConfig()

        mx_valid = await phase_mx(records, scrape_results, config)
        assert mx_valid["good.de"] is True
        assert mx_valid["bad.de"] is False
        assert mx_valid["email.de"] is True


class TestPhaseDecide:
    def test_decides_for_all_records(self, mock_dns):
        records = [
            _make_record(
                code="001",
                candidates=[DomainCandidate(domain="a.de", source="livenson")],
                scraped_emails={"a.de": ["a.de"]},
            ),
            _make_record(code="002"),
        ]
        mx_valid = {"a.de": True}
        config = GermanyConfig()
        validation: dict = {}

        phase_decide(records, config, mx_valid, validation)

        assert records[0].confidence == Confidence.HIGH
        assert records[1].confidence == Confidence.NONE


class TestUpdateRecordsFromScrape:
    def test_updates_scraped_emails(self):
        records = [_make_record(candidates=[DomainCandidate(domain="a.de", source="livenson")])]
        scrape_results = {"a.de": ({"email.de"}, "redirect.de", True)}
        _update_records_from_scrape(records, scrape_results)

        assert records[0].scraped_emails["a.de"] == ["email.de"]
        assert records[0].redirects["a.de"] == "redirect.de"
        assert records[0].accessible["a.de"] is True


class TestSetWebsite:
    def test_follows_redirect(self):
        rec = _make_record(website_domain="old.de")
        validation = {"old.de": (True, "new.de", False)}
        _set_website(rec, validation)
        assert rec.website_domain == "new.de"

    def test_clears_inaccessible(self):
        rec = _make_record(
            website_domain="dead.de",
            candidates=[DomainCandidate(domain="alive.de", source="guess")],
        )
        validation = {"dead.de": (False, None, False), "alive.de": (True, None, False)}
        _set_website(rec, validation)
        assert rec.website_domain == "alive.de"

    def test_no_accessible(self):
        rec = _make_record(website_domain="dead.de")
        validation = {"dead.de": (False, None, False)}
        _set_website(rec, validation)
        assert rec.website_domain is None


class TestPrintDryRun:
    def test_no_error(self, capsys):
        records = [
            _make_record(candidates=[DomainCandidate(domain="a.de", source="livenson")]),
            _make_record(candidates=[DomainCandidate(domain="b.de", source="guess")]),
            _make_record(override_domain="c.de"),
        ]
        config = GermanyConfig()
        _print_dry_run(records, config)
        out = capsys.readouterr().out
        assert "DRY RUN" in out
        assert "3" in out  # total municipalities
