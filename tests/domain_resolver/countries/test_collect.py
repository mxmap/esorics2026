"""Tests for collect_candidates methods with mocked API clients."""

import json
from pathlib import Path
from unittest.mock import AsyncMock, patch


from mail_municipalities.domain_resolver.countries.austria import AustriaConfig
from mail_municipalities.domain_resolver.countries.germany import GermanyConfig
from mail_municipalities.domain_resolver.countries.switzerland import SwitzerlandConfig


def _make_ch_data(tmp_path: Path):
    """Create minimal CH data directory."""
    overrides = {"261": {"email_domain": "zuerich.ch"}}
    (tmp_path / "overrides.json").write_text(json.dumps(overrides))
    return tmp_path


def _make_de_data(tmp_path: Path):
    """Create minimal DE data directory."""
    livenson = [
        {
            "id": "DE-01001000",
            "name": "Flensburg",
            "country": "DE",
            "region": "Schleswig-Holstein",
            "domain": "flensburg.de",
            "osm_relation_id": 27020,
        }
    ]
    (tmp_path / "municipalities_de_livenson.json").write_text(json.dumps(livenson))

    b42labs = {
        "generated": "2026-01-01",
        "total": 1,
        "municipalities": {"01001000": {"bfs": "01001000", "name": "Flensburg", "domain": "flensburg.de"}},
    }
    (tmp_path / "data_b42labs_de.json").write_text(json.dumps(b42labs))

    csv_content = "\ufeffKommune,E-Mail,Bundesland,Einwohner\nFlensburg,info@flensburg.de,Schleswig-Holstein,90000\n"
    (tmp_path / "E-Mail-Liste_alex.csv").write_text(csv_content, encoding="utf-8-sig")

    (tmp_path / "overrides.json").write_text("{}")
    return tmp_path


def _make_at_data(tmp_path: Path):
    """Create minimal AT data directory."""
    bresu = [
        {
            "Gemeindekennziffer": 10101,
            "Gemeindename": "Eisenstadt",
            "PLZ": 7000,
            "Website": "http://www.eisenstadt.gv.at",
            "Mail": "rathaus@eisenstadt.at",
        }
    ]
    (tmp_path / "bresu_oe_gemeinden.json").write_text(json.dumps(bresu))
    (tmp_path / "overrides.json").write_text("{}")
    return tmp_path


class TestSwitzerlandCollect:
    async def test_basic(self, tmp_path):
        data_dir = _make_ch_data(tmp_path)
        config = SwitzerlandConfig()

        bfs_data = {
            "261": {"bfs": "261", "name": "Zürich", "canton": "Kanton Zürich"},
            "351": {"bfs": "351", "name": "Bern", "canton": "Kanton Bern"},
        }
        wikidata_data = {
            "261": {
                "code": "261",
                "name": "Zürich",
                "website": "https://www.zuerich.ch",
            }
        }

        with (
            patch(
                "mail_municipalities.domain_resolver.countries.switzerland.fetch_bfs_municipalities",
                new_callable=AsyncMock,
                return_value=bfs_data,
            ),
            patch(
                "mail_municipalities.domain_resolver.countries.switzerland.fetch_openplz_ch_municipalities",
                new_callable=AsyncMock,
                return_value=bfs_data,
            ),
            patch(
                "mail_municipalities.domain_resolver.countries.switzerland.fetch_wikidata",
                new_callable=AsyncMock,
                return_value=wikidata_data,
            ),
        ):
            records = await config.collect_candidates(data_dir)

        assert len(records) >= 2
        zurich = next(r for r in records if r.code == "261")
        assert zurich.override_domain == "zuerich.ch"
        assert zurich.region == "Kanton Zürich"

        bern = next(r for r in records if r.code == "351")
        assert bern.name == "Bern"
        assert bern.override_domain is None

    async def test_override_only_municipality(self, tmp_path):
        overrides = {"9999": {"email_domain": "test.ch", "name": "Ghost Town", "canton": "Kanton Bern"}}
        (tmp_path / "overrides.json").write_text(json.dumps(overrides))
        config = SwitzerlandConfig()

        with (
            patch(
                "mail_municipalities.domain_resolver.countries.switzerland.fetch_bfs_municipalities",
                new_callable=AsyncMock,
                return_value={},
            ),
            patch(
                "mail_municipalities.domain_resolver.countries.switzerland.fetch_openplz_ch_municipalities",
                new_callable=AsyncMock,
                return_value={},
            ),
            patch(
                "mail_municipalities.domain_resolver.countries.switzerland.fetch_wikidata",
                new_callable=AsyncMock,
                return_value={},
            ),
        ):
            records = await config.collect_candidates(tmp_path)

        assert len(records) == 1
        assert records[0].code == "9999"
        assert records[0].name == "Ghost Town"

    async def test_wikidata_canton_fallback(self, tmp_path):
        """When BFS has no canton, Wikidata cantonLabel is used."""
        data_dir = _make_ch_data(tmp_path)
        config = SwitzerlandConfig()

        bfs_data = {
            "1001": {"bfs": "1001", "name": "Doppleschwand", "canton": ""},
        }
        wikidata_data = {
            "1001": {
                "code": "1001",
                "name": "Doppleschwand",
                "website": "",
                "cantonLabel": "Kanton Luzern",
            }
        }

        with (
            patch(
                "mail_municipalities.domain_resolver.countries.switzerland.fetch_bfs_municipalities",
                new_callable=AsyncMock,
                return_value=bfs_data,
            ),
            patch(
                "mail_municipalities.domain_resolver.countries.switzerland.fetch_openplz_ch_municipalities",
                new_callable=AsyncMock,
                return_value=bfs_data,
            ),
            patch(
                "mail_municipalities.domain_resolver.countries.switzerland.fetch_wikidata",
                new_callable=AsyncMock,
                return_value=wikidata_data,
            ),
        ):
            records = await config.collect_candidates(data_dir)

        doppleschwand = next(r for r in records if r.code == "1001")
        assert doppleschwand.region == "Kanton Luzern"

    async def test_openplz_extras_excluded(self, tmp_path):
        """Municipalities in OpenPLZ but not BFS should be excluded."""
        data_dir = _make_ch_data(tmp_path)
        config = SwitzerlandConfig()

        bfs_data = {
            "261": {"bfs": "261", "name": "Zürich", "canton": "Kanton Zürich"},
        }
        openplz_data = {
            "261": {"bfs": "261", "name": "Zürich", "canton": "Kanton Zürich"},
            "9998": {"bfs": "9998", "name": "Dissolved Town", "canton": "Kanton Bern"},
        }

        with (
            patch(
                "mail_municipalities.domain_resolver.countries.switzerland.fetch_bfs_municipalities",
                new_callable=AsyncMock,
                return_value=bfs_data,
            ),
            patch(
                "mail_municipalities.domain_resolver.countries.switzerland.fetch_openplz_ch_municipalities",
                new_callable=AsyncMock,
                return_value=openplz_data,
            ),
            patch(
                "mail_municipalities.domain_resolver.countries.switzerland.fetch_wikidata",
                new_callable=AsyncMock,
                return_value={},
            ),
        ):
            records = await config.collect_candidates(data_dir)

        codes = {r.code for r in records}
        assert "261" in codes
        assert "9998" not in codes


class TestGermanyCollect:
    async def test_basic(self, tmp_path):
        data_dir = _make_de_data(tmp_path)
        config = GermanyConfig()

        wikidata_data = {
            "01001000": {
                "code": "01001000",
                "name": "Flensburg",
                "website": "https://www.flensburg.de",
            }
        }

        with patch(
            "mail_municipalities.domain_resolver.countries.germany.fetch_wikidata",
            new_callable=AsyncMock,
            return_value=wikidata_data,
        ):
            records = await config.collect_candidates(data_dir)

        assert len(records) >= 1
        fl = next(r for r in records if r.code == "01001000")
        assert fl.name == "Flensburg"
        assert fl.region == "Schleswig-Holstein"
        # Should have candidates from livenson, b42labs, wikidata, csv
        sources = {c.source for c in fl.candidates}
        assert "livenson" in sources
        assert "wikidata" in sources

        # livenson and b42labs should be marked as email domain sources
        liv_cands = [c for c in fl.candidates if c.source == "livenson"]
        assert all(c.is_email_domain for c in liv_cands)
        b42_cands = [c for c in fl.candidates if c.source == "b42labs"]
        assert all(c.is_email_domain for c in b42_cands)

    async def test_csv_bundesland_mismatch(self, tmp_path):
        """CSV entry should be skipped if Bundesland doesn't match."""
        livenson = [
            {
                "id": "DE-08001000",
                "name": "Aalen",
                "country": "DE",
                "region": "Baden-Württemberg",
                "domain": "aalen.de",
            }
        ]
        (tmp_path / "municipalities_de_livenson.json").write_text(json.dumps(livenson))
        (tmp_path / "data_b42labs_de.json").write_text(json.dumps({"municipalities": {}}))
        # CSV says Bayern, but municipality is in BW
        csv_content = "\ufeffKommune,E-Mail,Bundesland,Einwohner\nAalen,info@aalen.de,Bayern,67000\n"
        (tmp_path / "E-Mail-Liste_alex.csv").write_text(csv_content, encoding="utf-8-sig")
        (tmp_path / "overrides.json").write_text("{}")

        config = GermanyConfig()
        with patch(
            "mail_municipalities.domain_resolver.countries.germany.fetch_wikidata",
            new_callable=AsyncMock,
            return_value={},
        ):
            records = await config.collect_candidates(tmp_path)

        aalen = next(r for r in records if r.code == "08001000")
        csv_candidates = [c for c in aalen.candidates if c.source == "csv_email"]
        assert len(csv_candidates) == 0  # Bundesland mismatch


class TestAustriaCollect:
    async def test_basic(self, tmp_path):
        data_dir = _make_at_data(tmp_path)
        config = AustriaConfig()

        openplz_data = {
            "10101": {
                "name": "Eisenstadt",
                "status": "Statutarstadt",
                "bundesland": "Burgenland",
            }
        }
        wikidata_data = {
            "10101": {
                "code": "10101",
                "name": "Eisenstadt",
                "website": "https://www.eisenstadt.gv.at",
            }
        }

        with (
            patch(
                "mail_municipalities.domain_resolver.countries.austria.fetch_openplz_municipalities",
                new_callable=AsyncMock,
                return_value=openplz_data,
            ),
            patch(
                "mail_municipalities.domain_resolver.countries.austria.fetch_wikidata",
                new_callable=AsyncMock,
                return_value=wikidata_data,
            ),
        ):
            records = await config.collect_candidates(data_dir)

        assert len(records) >= 1
        eis = next(r for r in records if r.code == "10101")
        assert eis.name == "Eisenstadt"
        assert eis.region == "Burgenland"

        # Should have bresu + wikidata + auto .gv.at candidates
        sources = {c.source for c in eis.candidates}
        assert "bresu" in sources or "bresu_email" in sources

    async def test_auto_gv_at_domains(self, tmp_path):
        (tmp_path / "bresu_oe_gemeinden.json").write_text("[]")
        (tmp_path / "overrides.json").write_text("{}")

        config = AustriaConfig()
        openplz_data = {"10101": {"name": "Eisenstadt", "bundesland": "Burgenland"}}

        with (
            patch(
                "mail_municipalities.domain_resolver.countries.austria.fetch_openplz_municipalities",
                new_callable=AsyncMock,
                return_value=openplz_data,
            ),
            patch(
                "mail_municipalities.domain_resolver.countries.austria.fetch_wikidata",
                new_callable=AsyncMock,
                return_value={},
            ),
        ):
            records = await config.collect_candidates(tmp_path)

        eis = records[0]
        gv_domains = [c.domain for c in eis.candidates if c.domain.endswith(".gv.at")]
        assert len(gv_domains) > 0
        assert "eisenstadt.gv.at" in gv_domains

    async def test_override_skips_gv_at(self, tmp_path):
        (tmp_path / "bresu_oe_gemeinden.json").write_text("[]")
        overrides = {"10101": {"email_domain": "custom.at"}}
        (tmp_path / "overrides.json").write_text(json.dumps(overrides))

        config = AustriaConfig()
        openplz_data = {"10101": {"name": "Eisenstadt", "bundesland": "Burgenland"}}

        with (
            patch(
                "mail_municipalities.domain_resolver.countries.austria.fetch_openplz_municipalities",
                new_callable=AsyncMock,
                return_value=openplz_data,
            ),
            patch(
                "mail_municipalities.domain_resolver.countries.austria.fetch_wikidata",
                new_callable=AsyncMock,
                return_value={},
            ),
        ):
            records = await config.collect_candidates(tmp_path)

        eis = records[0]
        assert eis.override_domain == "custom.at"
        # Should NOT have auto-added .gv.at (override skips it)
        gv_guesses = [c for c in eis.candidates if c.source == "guess"]
        assert len(gv_guesses) == 0
