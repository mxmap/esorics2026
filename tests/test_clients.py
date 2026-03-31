"""Tests for API clients and static loaders."""

import json

from municipality_email.clients.bfs import CANTON_SHORT_TO_FULL, _parse_csv_response
from municipality_email.clients.static import (
    load_bresu,
    load_destatis,
    load_livenson,
    normalize_csv_name,
)


class TestBfsApi:
    def test_parse_csv_response(self):
        csv_text = (
            "HistoricalCode,BfsCode,Level,Parent,Name,ShortName\n"
            "100,1,1,,Kanton Zürich,ZH\n"
            "200,10,2,100,Bezirk Zürich,BZH\n"
            "300,261,3,200,Zürich,ZH\n"
        )
        entries = _parse_csv_response(csv_text)
        assert len(entries) == 3
        assert entries[2]["bfsCode"] == 261
        assert entries[2]["name"] == "Zürich"
        assert entries[2]["level"] == 3
        assert entries[2]["parent"] == 200

    def test_canton_mapping_complete(self):
        assert len(CANTON_SHORT_TO_FULL) == 26
        assert CANTON_SHORT_TO_FULL["zh"] == "Kanton Zürich"
        assert CANTON_SHORT_TO_FULL["ti"] == "Kanton Tessin"


class TestNormalizeCsvName:
    def test_basic(self):
        assert normalize_csv_name("Zürich") == "zuerich"

    def test_umlauts(self):
        assert normalize_csv_name("Münsingen") == "muensingen"

    def test_eszett(self):
        assert normalize_csv_name("Straßberg") == "strassberg"

    def test_whitespace(self):
        assert normalize_csv_name("  Bad  Hindelang  ") == "bad hindelang"


class TestLoadLivenson:
    def test_basic(self, tmp_path):
        data = [
            {
                "id": "DE-01001000",
                "name": "Flensburg",
                "country": "DE",
                "region": "Schleswig-Holstein",
                "domain": "flensburg.de",
                "osm_relation_id": 27020,
            }
        ]
        path = tmp_path / "municipalities_de_livenson.json"
        path.write_text(json.dumps(data))
        result = load_livenson(path)
        assert "01001000" in result
        assert result["01001000"]["name"] == "Flensburg"
        assert result["01001000"]["domain"] == "flensburg.de"


class TestLoadBresu:
    def test_basic(self, tmp_path):
        data = [
            {
                "Gemeindekennziffer": 10101,
                "Gemeindename": "Eisenstadt",
                "PLZ": 7000,
                "Website": "http://www.eisenstadt.gv.at",
                "Mail": "rathaus@eisenstadt.at",
            }
        ]
        path = tmp_path / "bresu_oe_gemeinden.json"
        path.write_text(json.dumps(data))
        result = load_bresu(path)
        assert "10101" in result
        assert result["10101"]["name"] == "Eisenstadt"
        assert result["10101"]["website_domain"] == "eisenstadt.gv.at"
        assert result["10101"]["email_domain"] == "eisenstadt.at"


class TestLoadDestatis:
    def test_basic(self, tmp_path):
        data = {"01001000": {"name": "Flensburg, Stadt"}}
        path = tmp_path / "municipalities_destatis.json"
        path.write_text(json.dumps(data))
        result = load_destatis(path)
        assert result["01001000"] == "Flensburg, Stadt"
