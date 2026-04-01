"""Tests for async API clients (OpenPLZ, Wikidata)."""

import httpx
import respx

from municipality_email.clients.openplz import (
    OPENPLZ_BASE_AT,
    OPENPLZ_BASE_CH,
    fetch_openplz_ch_municipalities,
    fetch_openplz_municipalities,
)
from municipality_email.clients.wikidata import SPARQL_URL, fetch_wikidata


class TestFetchOpenplzChMunicipalities:
    async def test_fetch_and_parse(self):
        with respx.mock:
            respx.get(f"{OPENPLZ_BASE_CH}/1/Communes?page=1&pageSize=50").respond(
                200,
                json=[
                    {
                        "key": "261",
                        "name": "Zürich",
                        "shortName": "Zürich",
                        "canton": {"key": "1", "name": "Zürich", "shortName": "ZH"},
                        "district": {"key": "112", "name": "Zürich", "shortName": "Zürich"},
                    },
                    {
                        "key": "230",
                        "name": "Winterthur",
                        "shortName": "Winterthur",
                        "canton": {"key": "1", "name": "Zürich", "shortName": "ZH"},
                        "district": {
                            "key": "110",
                            "name": "Winterthur",
                            "shortName": "Winterthur",
                        },
                    },
                ],
            )
            for canton_key in range(2, 27):
                respx.get(f"{OPENPLZ_BASE_CH}/{canton_key}/Communes?page=1&pageSize=50").respond(
                    200, json=[]
                )

            result = await fetch_openplz_ch_municipalities()

        assert "261" in result
        assert result["261"]["name"] == "Zürich"
        assert result["261"]["canton"] == "Kanton Zürich"
        assert "230" in result

    async def test_canton_resolved_correctly(self):
        """Sursee should be in Kanton Luzern, not Kanton Aargau."""
        with respx.mock:
            respx.get(f"{OPENPLZ_BASE_CH}/3/Communes?page=1&pageSize=50").respond(
                200,
                json=[
                    {
                        "key": "1103",
                        "name": "Sursee",
                        "shortName": "Sursee",
                        "canton": {"key": "3", "name": "Luzern", "shortName": "LU"},
                        "district": {
                            "key": "314",
                            "name": "Wahlkreis Sursee",
                            "shortName": "Sursee",
                        },
                    },
                ],
            )
            for canton_key in [*range(1, 3), *range(4, 27)]:
                respx.get(f"{OPENPLZ_BASE_CH}/{canton_key}/Communes?page=1&pageSize=50").respond(
                    200, json=[]
                )

            result = await fetch_openplz_ch_municipalities()

        assert "1103" in result
        assert result["1103"]["name"] == "Sursee"
        assert result["1103"]["canton"] == "Kanton Luzern"

    async def test_pagination(self):
        """Should paginate when a canton has more than 50 municipalities."""
        page1 = [
            {
                "key": str(i),
                "name": f"Gemeinde {i}",
                "shortName": f"G{i}",
                "canton": {"key": "1", "name": "Zürich", "shortName": "ZH"},
                "district": {"key": "100", "name": "Bezirk", "shortName": "B"},
            }
            for i in range(1, 51)
        ]
        page2 = [
            {
                "key": "51",
                "name": "Gemeinde 51",
                "shortName": "G51",
                "canton": {"key": "1", "name": "Zürich", "shortName": "ZH"},
                "district": {"key": "100", "name": "Bezirk", "shortName": "B"},
            }
        ]
        with respx.mock:
            respx.get(f"{OPENPLZ_BASE_CH}/1/Communes?page=1&pageSize=50").respond(200, json=page1)
            respx.get(f"{OPENPLZ_BASE_CH}/1/Communes?page=2&pageSize=50").respond(200, json=page2)
            for canton_key in range(2, 27):
                respx.get(f"{OPENPLZ_BASE_CH}/{canton_key}/Communes?page=1&pageSize=50").respond(
                    200, json=[]
                )

            result = await fetch_openplz_ch_municipalities()

        assert len(result) == 51


class TestFetchOpenplzMunicipalities:
    async def test_fetch_paginated(self):
        with respx.mock:
            # Bundesland 1 has 2 municipalities in 1 page
            respx.get(f"{OPENPLZ_BASE_AT}/1/Municipalities?page=1&pageSize=50").respond(
                200,
                json=[
                    {
                        "key": "10101",
                        "name": "Eisenstadt",
                        "status": "Statutarstadt",
                        "district": {"name": "Eisenstadt (Stadt)"},
                        "federalProvince": {"name": "Burgenland"},
                    },
                    {
                        "key": "10102",
                        "name": "Rust",
                        "district": {"name": "Eisenstadt (Stadt)"},
                        "federalProvince": {"name": "Burgenland"},
                    },
                ],
            )
            # Bundesländer 2-9 empty
            for bl in range(2, 10):
                respx.get(f"{OPENPLZ_BASE_AT}/{bl}/Municipalities?page=1&pageSize=50").respond(
                    200, json=[]
                )

            result = await fetch_openplz_municipalities()

        assert "10101" in result
        assert result["10101"]["name"] == "Eisenstadt"
        assert "10102" in result

    async def test_with_provided_client(self):
        with respx.mock:
            for bl in range(1, 10):
                respx.get(f"{OPENPLZ_BASE_AT}/{bl}/Municipalities?page=1&pageSize=50").respond(
                    200, json=[]
                )

            async with httpx.AsyncClient() as client:
                result = await fetch_openplz_municipalities(client)

        assert result == {}


class TestFetchWikidata:
    async def test_basic_query(self):
        sparql_response = {
            "results": {
                "bindings": [
                    {
                        "bfs": {"value": "261"},
                        "itemLabel": {"value": "Zürich"},
                        "website": {"value": "https://www.zuerich.ch"},
                    },
                    {
                        "bfs": {"value": "351"},
                        "itemLabel": {"value": "Bern"},
                        "website": {"value": ""},
                    },
                ]
            }
        }
        with respx.mock:
            respx.post(SPARQL_URL).respond(200, json=sparql_response)
            result = await fetch_wikidata("SELECT ...", code_field="bfs")

        assert "261" in result
        assert result["261"]["name"] == "Zürich"
        assert result["261"]["website"] == "https://www.zuerich.ch"
        assert "351" in result

    async def test_deduplicates_by_code(self):
        sparql_response = {
            "results": {
                "bindings": [
                    {"bfs": {"value": "1"}, "itemLabel": {"value": "A"}, "website": {"value": ""}},
                    {
                        "bfs": {"value": "1"},
                        "itemLabel": {"value": "A"},
                        "website": {"value": "https://a.ch"},
                    },
                ]
            }
        }
        with respx.mock:
            respx.post(SPARQL_URL).respond(200, json=sparql_response)
            result = await fetch_wikidata("SELECT ...", code_field="bfs")

        assert len(result) == 1
        assert result["1"]["website"] == "https://a.ch"

    async def test_skips_empty_code(self):
        sparql_response = {
            "results": {
                "bindings": [
                    {"bfs": {"value": ""}, "itemLabel": {"value": "X"}, "website": {"value": ""}},
                ]
            }
        }
        with respx.mock:
            respx.post(SPARQL_URL).respond(200, json=sparql_response)
            result = await fetch_wikidata("SELECT ...", code_field="bfs")

        assert len(result) == 0

    async def test_preserves_extra_bindings(self):
        sparql_response = {
            "results": {
                "bindings": [
                    {
                        "bfs": {"value": "261"},
                        "itemLabel": {"value": "Zürich"},
                        "website": {"value": ""},
                        "cantonLabel": {"value": "Kanton Zürich"},
                    },
                ]
            }
        }
        with respx.mock:
            respx.post(SPARQL_URL).respond(200, json=sparql_response)
            result = await fetch_wikidata("SELECT ...", code_field="bfs")

        assert result["261"]["cantonLabel"] == "Kanton Zürich"
