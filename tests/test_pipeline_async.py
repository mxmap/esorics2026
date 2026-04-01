"""Tests for async pipeline phases."""

from pathlib import Path

import respx

from municipality_email.cache import CacheDB
from municipality_email.countries.germany import GermanyConfig
from unittest.mock import AsyncMock, patch

from municipality_email.pipeline import (
    _print_dry_run,
    _set_website,
    _update_records_from_scrape,
    phase_collect,
    phase_content_validate,
    phase_decide,
    phase_dns_prefilter,
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
    return MunicipalityRecord(**defaults)  # type: ignore[arg-type]


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

        r001 = next(r for r in records if r.code == "001")
        assert any(c.source == "guess" for c in r001.candidates)

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


class TestPhaseDnsPrefilter:
    async def test_filters_nonresolving_domains(self):
        records = [
            _make_record(
                candidates=[
                    DomainCandidate(domain="good.de", source="livenson"),
                    DomainCandidate(domain="bad.de", source="guess"),
                ]
            )
        ]

        async def _lookup_a(domain):
            return domain == "good.de"

        with patch("municipality_email.pipeline.lookup_a", side_effect=_lookup_a):
            await phase_dns_prefilter(records)

        domains = [c.domain for c in records[0].candidates]
        assert "good.de" in domains
        assert "bad.de" not in domains

    async def test_keeps_all_resolving(self):
        records = [
            _make_record(
                candidates=[
                    DomainCandidate(domain="a.de", source="livenson"),
                    DomainCandidate(domain="b.de", source="guess"),
                ]
            )
        ]

        with patch(
            "municipality_email.pipeline.lookup_a", new_callable=AsyncMock, return_value=True
        ):
            await phase_dns_prefilter(records)

        assert len(records[0].candidates) == 2

    async def test_uses_dns_cache(self, tmp_path):
        records = [
            _make_record(candidates=[DomainCandidate(domain="cached.de", source="livenson")])
        ]

        async with CacheDB(tmp_path / "cache.db") as cache:
            await cache.put_dns_many({"cached.de": True})
            # No DNS mock needed — should be fully cached
            result = await phase_dns_prefilter(records, cache)

        assert result["cached.de"] is True
        assert len(records[0].candidates) == 1

    async def test_persists_to_dns_cache(self, tmp_path):
        records = [_make_record(candidates=[DomainCandidate(domain="new.de", source="livenson")])]

        async with CacheDB(tmp_path / "cache.db") as cache:
            with patch(
                "municipality_email.pipeline.lookup_a",
                new_callable=AsyncMock,
                return_value=True,
            ):
                await phase_dns_prefilter(records, cache)

            cached = await cache.get_dns_many({"new.de"})
            assert "new.de" in cached
            assert cached["new.de"] is True

    async def test_www_fallback_resolves(self):
        """Domain that only resolves via www. prefix should pass the filter."""
        records = [
            _make_record(
                candidates=[DomainCandidate(domain="www-only.ch", source="wikidata")]
            )
        ]

        async def _lookup_a(domain):
            return domain == "www.www-only.ch"

        with patch("municipality_email.pipeline.lookup_a", side_effect=_lookup_a):
            result = await phase_dns_prefilter(records)

        assert result["www-only.ch"] is True
        assert len(records[0].candidates) == 1
        assert records[0].candidates[0].domain == "www-only.ch"

    async def test_www_fallback_both_fail(self):
        """When neither bare nor www resolves, domain should be eliminated."""
        records = [
            _make_record(
                candidates=[DomainCandidate(domain="dead.ch", source="wikidata")]
            )
        ]

        with patch(
            "municipality_email.pipeline.lookup_a", new_callable=AsyncMock, return_value=False
        ):
            result = await phase_dns_prefilter(records)

        assert result["dead.ch"] is False
        assert len(records[0].candidates) == 0


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

        assert validation["good.de"][0] is True
        assert validation["bad.de"][0] is False

    async def test_uses_head_cache(self, tmp_path):
        records = [
            _make_record(candidates=[DomainCandidate(domain="cached.de", source="livenson")])
        ]
        config = GermanyConfig()

        async with CacheDB(tmp_path / "cache.db") as cache:
            await cache.put_head_many({"cached.de": (True, "redir.de", False)})
            # No HTTP mock needed — should be fully cached
            validation = await phase_validate(records, config, cache)

        assert validation["cached.de"] == (True, "redir.de", False)


class TestPhaseContentValidate:
    async def test_classifies_parked_domain(self):
        records = [
            _make_record(candidates=[DomainCandidate(domain="parked.de", source="livenson")])
        ]
        config = GermanyConfig()
        validation: dict[str, tuple[bool, str | None, bool]] = {"parked.de": (True, None, False)}

        with respx.mock:
            respx.get("https://www.parked.de/").respond(
                200, html="<html>This domain is parked</html>"
            )
            flags = await phase_content_validate(records, config, validation)

        assert flags["parked.de"] == ["parked"]
        assert records[0].content_flags["parked.de"] == ["parked"]

    async def test_classifies_municipality_page(self):
        records = [
            _make_record(candidates=[DomainCandidate(domain="gemeinde.de", source="livenson")])
        ]
        config = GermanyConfig()
        validation: dict[str, tuple[bool, str | None, bool]] = {"gemeinde.de": (True, None, False)}

        with respx.mock:
            respx.get("https://www.gemeinde.de/").respond(
                200, html="<html><h1>Gemeinde Musterstadt</h1></html>"
            )
            flags = await phase_content_validate(records, config, validation)

        assert flags["gemeinde.de"] == ["has_municipality_keywords"]

    async def test_skips_inaccessible_domains(self):
        records = [_make_record(candidates=[DomainCandidate(domain="down.de", source="livenson")])]
        config = GermanyConfig()
        validation: dict[str, tuple[bool, str | None, bool]] = {"down.de": (False, None, False)}

        flags = await phase_content_validate(records, config, validation)
        assert "down.de" not in flags

    async def test_uses_content_cache(self, tmp_path):
        records = [
            _make_record(candidates=[DomainCandidate(domain="cached.de", source="livenson")])
        ]
        config = GermanyConfig()
        validation: dict[str, tuple[bool, str | None, bool]] = {"cached.de": (True, None, False)}

        async with CacheDB(tmp_path / "cache.db") as cache:
            await cache.put_content_many({"cached.de": ["has_municipality_keywords"]})
            # No HTTP mock needed — should be fully cached
            flags = await phase_content_validate(records, config, validation, cache)

        assert flags["cached.de"] == ["has_municipality_keywords"]

    async def test_persists_to_content_cache(self, tmp_path):
        records = [_make_record(candidates=[DomainCandidate(domain="new.de", source="livenson")])]
        config = GermanyConfig()
        validation: dict[str, tuple[bool, str | None, bool]] = {"new.de": (True, None, False)}

        async with CacheDB(tmp_path / "cache.db") as cache:
            with respx.mock:
                respx.get("https://www.new.de/").respond(200, html="<html>Rathaus Info</html>")
                await phase_content_validate(records, config, validation, cache)

            cached = await cache.get_content_many({"new.de"})
            assert "new.de" in cached
            assert cached["new.de"] == ["has_municipality_keywords"]


class TestPhaseScrapeSkipsParked:
    async def test_skips_parked_domains(self):
        records = [
            _make_record(
                candidates=[
                    DomainCandidate(domain="parked.de", source="livenson"),
                    DomainCandidate(domain="good.de", source="wikidata"),
                ]
            )
        ]
        config = GermanyConfig()
        validation: dict[str, tuple[bool, str | None, bool]] = {
            "parked.de": (True, None, False),
            "good.de": (True, None, False),
        }
        content_flags = {"parked.de": ["parked"]}

        with respx.mock:
            respx.get("https://www.good.de/").respond(200, html="<p>info@good.de</p>")
            for subpage in config.subpages:
                respx.get(f"https://www.good.de{subpage}").respond(200, html="")
            # No mock for parked.de — it should not be requested

            results = await phase_scrape(records, config, validation, content_flags=content_flags)

        assert "good.de" in results
        assert "parked.de" not in results


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
        validation: dict[str, tuple[bool, str | None, bool]] = {
            "good.de": (True, None, False),
            "bad.de": (False, None, False),
        }

        with respx.mock:
            respx.get("https://www.good.de/").respond(200, html="<p>info@good.de</p>")
            for subpage in config.subpages:
                respx.get(f"https://www.good.de{subpage}").respond(200, html="")

            results = await phase_scrape(records, config, validation)

        assert "good.de" in results
        assert "bad.de" not in results

    async def test_uses_cache(self, tmp_path):
        records = [
            _make_record(candidates=[DomainCandidate(domain="cached.de", source="livenson")])
        ]
        config = GermanyConfig()
        validation: dict[str, tuple[bool, str | None, bool]] = {"cached.de": (True, None, False)}

        async with CacheDB(tmp_path / "cache.db") as cache:
            await cache.put_scrape("cached.de", {"cached.de"}, None, True)
            results = await phase_scrape(records, config, validation, cache)

        assert "cached.de" in results
        assert results["cached.de"][0] == {"cached.de"}

    async def test_persists_to_cache(self, tmp_path):
        records = [_make_record(candidates=[DomainCandidate(domain="new.de", source="livenson")])]
        config = GermanyConfig()
        validation: dict[str, tuple[bool, str | None, bool]] = {"new.de": (True, None, False)}

        async with CacheDB(tmp_path / "cache.db") as cache:
            with respx.mock:
                respx.get("https://www.new.de/").respond(200, html="<p>info@new.de</p>")
                for subpage in config.subpages:
                    respx.get(f"https://www.new.de{subpage}").respond(200, html="")
                await phase_scrape(records, config, validation, cache)

            # Verify it was persisted
            cached = await cache.get_scrape_many({"new.de"})
            assert "new.de" in cached


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
        scrape_results: dict[str, tuple[set[str], str | None, bool]] = {"good.de": ({"email.de"}, None, True)}
        mock_dns["email.de"] = ["mx.email.de"]
        config = GermanyConfig()

        mx_valid = await phase_mx(records, scrape_results, config)
        assert mx_valid["good.de"] is True
        assert mx_valid["bad.de"] is False
        assert mx_valid["email.de"] is True

    async def test_uses_mx_cache(self, tmp_path, mock_dns):
        records = [
            _make_record(candidates=[DomainCandidate(domain="cached.de", source="livenson")])
        ]
        config = GermanyConfig()

        async with CacheDB(tmp_path / "cache.db") as cache:
            await cache.put_mx_many({"cached.de": True})
            # mock_dns has no entry for cached.de — would return [] without cache
            mx_valid = await phase_mx(records, {}, config, cache)

        assert mx_valid["cached.de"] is True


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
        scrape_results: dict[str, tuple[set[str], str | None, bool]] = {"a.de": ({"email.de"}, "redirect.de", True)}
        _update_records_from_scrape(records, scrape_results)

        assert records[0].scraped_emails["a.de"] == ["email.de"]
        assert records[0].redirects["a.de"] == "redirect.de"
        assert records[0].accessible["a.de"] is True


class TestSetWebsite:
    def test_follows_redirect(self):
        rec = _make_record(website_domain="old.de")
        validation: dict[str, tuple[bool, str | None, bool]] = {"old.de": (True, "new.de", False)}
        _set_website(rec, validation)
        assert rec.website_domain == "new.de"

    def test_clears_inaccessible(self):
        rec = _make_record(
            website_domain="dead.de",
            candidates=[DomainCandidate(domain="alive.de", source="guess")],
        )
        validation: dict[str, tuple[bool, str | None, bool]] = {"dead.de": (False, None, False), "alive.de": (True, None, False)}
        _set_website(rec, validation)
        assert rec.website_domain == "alive.de"

    def test_no_accessible(self):
        rec = _make_record(website_domain="dead.de")
        validation: dict[str, tuple[bool, str | None, bool]] = {"dead.de": (False, None, False)}
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
        assert "3" in out
