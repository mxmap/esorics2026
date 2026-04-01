"""Tests for async API clients (BFS, OpenPLZ, Wikidata)."""

import httpx
import respx

from municipality_email.clients.bfs import fetch_bfs_municipalities
from municipality_email.clients.openplz import OPENPLZ_BASE_AT, fetch_openplz_municipalities
from municipality_email.clients.wikidata import SPARQL_URL, fetch_wikidata


class TestFetchBfsMunicipalities:
    async def test_fetch_and_parse(self):
        csv_text = (
            "HistoricalCode,BfsCode,Level,Parent,Name,ShortName\n"
            "100,1,1,,Kanton Zürich,ZH\n"
            "200,10,2,100,Bezirk Zürich,BZH\n"
            "300,261,3,200,Zürich,ZH\n"
            "301,262,3,200,Winterthur,WI\n"
        )
        with respx.mock:
            respx.get("https://www.agvchapp.bfs.admin.ch/api/communes/snapshot").respond(
                200, text=csv_text
            )
            result = await fetch_bfs_municipalities(date="01-01-2026")

        assert "261" in result
        assert result["261"]["name"] == "Zürich"
        assert result["261"]["canton"] == "Kanton Zürich"
        assert "262" in result

    async def test_direct_canton_parent(self):
        # When commune parent is directly a canton (level 1)
        csv_text = (
            "HistoricalCode,BfsCode,Level,Parent,Name,ShortName\n"
            "100,1,1,,Basel-Stadt,BS\n"
            "300,2701,3,100,Basel,BS\n"
        )
        with respx.mock:
            respx.get("https://www.agvchapp.bfs.admin.ch/api/communes/snapshot").respond(
                200, text=csv_text
            )
            result = await fetch_bfs_municipalities(date="01-01-2026")

        assert "2701" in result
        assert result["2701"]["canton"] == "Kanton Basel-Stadt"

    async def test_multi_level_hierarchy(self):
        """Walk up through multiple intermediate levels to reach canton."""
        csv_text = (
            "HistoricalCode,BfsCode,Level,Parent,Name,ShortName\n"
            "100,1,1,,Kanton Graubünden,GR\n"
            "200,10,2,100,Region Prättigau/Davos,RPD\n"
            "250,15,2,200,Kreis Klosters,KKL\n"
            "300,3871,3,250,Klosters,KL\n"
        )
        with respx.mock:
            respx.get("https://www.agvchapp.bfs.admin.ch/api/communes/snapshot").respond(
                200, text=csv_text
            )
            result = await fetch_bfs_municipalities(date="01-01-2026")

        assert "3871" in result
        assert result["3871"]["canton"] == "Kanton Graubünden"


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
