"""Tests for remaining static file loaders."""

import json

from municipality_email.clients.static import load_b42labs, load_csv_alex


class TestLoadB42labs:
    def test_basic(self, tmp_path):
        data = {
            "generated": "2026-03-18T00:00:00Z",
            "total": 1,
            "municipalities": {
                "01001000": {
                    "bfs": "01001000",
                    "name": "Flensburg",
                    "domain": "flensburg.de",
                }
            },
        }
        path = tmp_path / "data_b42labs_de.json"
        path.write_text(json.dumps(data))
        result = load_b42labs(path)
        assert "01001000" in result
        assert result["01001000"]["name"] == "Flensburg"


class TestLoadCsvAlex:
    def test_basic(self, tmp_path):
        csv_content = (
            "\ufeffKommune,E-Mail,Bundesland,Einwohner\n"
            "Aalen,presseamt@aalen.de,Baden-Württemberg,67621\n"
            "Achern,stadtverwaltung@achern.de,Baden-Württemberg,26733\n"
        )
        path = tmp_path / "E-Mail-Liste_alex.csv"
        path.write_text(csv_content, encoding="utf-8-sig")
        result = load_csv_alex(path, set())
        assert "aalen" in result
        assert result["aalen"][0][0] == "aalen.de"
        assert result["aalen"][0][1] == "Baden-Württemberg"

    def test_filters_skip_domains(self, tmp_path):
        csv_content = (
            "\ufeffKommune,E-Mail,Bundesland,Einwohner\nTest,info@gmail.com,Bayern,1000\n"
        )
        path = tmp_path / "E-Mail-Liste_alex.csv"
        path.write_text(csv_content, encoding="utf-8-sig")
        result = load_csv_alex(path, {"gmail.com"})
        assert len(result) == 0

    def test_empty_rows(self, tmp_path):
        csv_content = (
            "\ufeffKommune,E-Mail,Bundesland,Einwohner\n"
            "Valid,info@valid.de,Bayern,1000\n"
            ",,,,\n"
            "NoEmail,invalid,Bayern,500\n"
        )
        path = tmp_path / "E-Mail-Liste_alex.csv"
        path.write_text(csv_content, encoding="utf-8-sig")
        result = load_csv_alex(path, set())
        assert "valid" in result
        assert len(result) == 1
