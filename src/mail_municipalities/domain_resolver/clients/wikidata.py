"""Wikidata SPARQL client for municipality websites."""

from __future__ import annotations

import httpx
import stamina
from loguru import logger

SPARQL_URL = "https://query.wikidata.org/sparql"


@stamina.retry(
    on=(httpx.HTTPStatusError, httpx.ConnectError, httpx.TimeoutException),
    attempts=10,
    wait_initial=5.0,
    wait_max=120.0,
    wait_jitter=5.0,
)
async def fetch_sparql(client: httpx.AsyncClient, url: str, data: dict, headers: dict) -> httpx.Response:
    r = await client.post(url, data=data, headers=headers)
    r.raise_for_status()
    return r


async def fetch_wikidata(
    sparql_query: str,
    code_field: str,
    *,
    name_field: str = "itemLabel",
    website_field: str = "website",
) -> dict[str, dict[str, str]]:
    """Query Wikidata SPARQL for municipalities with websites.

    Args:
        sparql_query: Country-specific SPARQL query string.
        code_field: The binding variable for the municipality code (e.g. "bfs", "ags", "gkz").
        name_field: The binding variable for the name.
        website_field: The binding variable for the website URL.

    Returns:
        Dict mapping code -> {"code", "name", "website", ...extra bindings}.
    """
    logger.info("Fetching municipalities from Wikidata (code_field={})...", code_field)
    headers = {
        "Accept": "application/sparql-results+json",
        "User-Agent": "MunicipalityEmail/1.0 usage=academic repo=https://github.com/davidhuser",
    }
    async with httpx.AsyncClient(timeout=120, http2=True) as client:
        r = await fetch_sparql(client, SPARQL_URL, {"query": sparql_query}, headers)
        data = r.json()

    municipalities: dict[str, dict[str, str]] = {}
    for row in data["results"]["bindings"]:
        code = row.get(code_field, {}).get("value", "")
        if not code:
            continue
        name = row.get(name_field, {}).get("value", "")
        website = row.get(website_field, {}).get("value", "")

        if code in municipalities:
            if not municipalities[code].get("website") and website:
                municipalities[code]["website"] = website
        else:
            entry: dict[str, str] = {
                "code": code,
                "name": name,
                "website": website,
            }
            # Preserve extra bindings (e.g. cantonLabel for CH)
            for key, val in row.items():
                if key not in (code_field, name_field, website_field, "item"):
                    entry[key] = val.get("value", "") if isinstance(val, dict) else str(val)
            municipalities[code] = entry

    logger.info(
        "Wikidata: {} municipalities, {} with websites",
        len(municipalities),
        sum(1 for m in municipalities.values() if m.get("website")),
    )
    return municipalities
