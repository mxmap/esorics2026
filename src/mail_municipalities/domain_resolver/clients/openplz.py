"""OpenPLZ API clients for Austria and Switzerland."""

from __future__ import annotations

import httpx
import stamina
from loguru import logger

from mail_municipalities.domain_resolver.clients.bfs import CANTON_SHORT_TO_FULL

OPENPLZ_BASE_AT = "https://openplzapi.org/at/FederalProvinces"
OPENPLZ_BASE_CH = "https://openplzapi.org/ch/Cantons"


async def fetch_openplz_municipalities(
    client: httpx.AsyncClient | None = None,
) -> dict[str, dict]:
    """Fetch all Austrian municipalities from the OpenPLZ API.

    Iterates all 9 Bundesländer and paginates to get the full list.
    Returns dict keyed by 5-digit GKZ string.
    """
    own_client = client is None
    if own_client:
        client = httpx.AsyncClient(timeout=30, http2=True)

    try:
        municipalities: dict[str, dict] = {}

        for bl_key in range(1, 10):
            page = 1
            while True:
                url = f"{OPENPLZ_BASE_AT}/{bl_key}/Municipalities?page={page}&pageSize=50"
                r = await client.get(url)
                r.raise_for_status()
                data = r.json()

                if not data:
                    break

                for entry in data:
                    gkz = str(entry["key"]).zfill(5)
                    municipalities[gkz] = {
                        "name": entry["name"],
                        "status": entry.get("status", ""),
                        "postalCode": entry.get("postalCode", ""),
                        "district": entry.get("district", {}).get("name", ""),
                        "bundesland": entry.get("federalProvince", {}).get("name", ""),
                    }

                if len(data) < 50:
                    break
                page += 1

        logger.info("OpenPLZ AT: {} municipalities loaded", len(municipalities))
        return municipalities
    finally:
        if own_client:
            await client.aclose()


@stamina.retry(
    on=(httpx.HTTPStatusError, httpx.ConnectError, httpx.TimeoutException),
    attempts=3,
    wait_initial=2.0,
)
async def _fetch_ch_page(client: httpx.AsyncClient, url: str) -> list[dict]:
    r = await client.get(url)
    r.raise_for_status()
    return r.json()


async def fetch_openplz_ch_municipalities(
    client: httpx.AsyncClient | None = None,
) -> dict[str, dict]:
    """Fetch all Swiss municipalities from the OpenPLZ API.

    Iterates all 26 cantons and paginates to get the full list.
    Returns dict keyed by BFS code string -> {"bfs", "name", "canton"}.
    """
    own_client = client is None
    if own_client:
        client = httpx.AsyncClient(timeout=30, http2=True)

    try:
        municipalities: dict[str, dict] = {}

        for canton_key in range(1, 27):
            page = 1
            while True:
                url = f"{OPENPLZ_BASE_CH}/{canton_key}/Communes?page={page}&pageSize=50"
                data = await _fetch_ch_page(client, url)

                if not data:
                    break

                for entry in data:
                    bfs_code = str(entry["key"])
                    canton_short = entry.get("canton", {}).get("shortName", "").lower()
                    canton = CANTON_SHORT_TO_FULL.get(canton_short, "")

                    municipalities[bfs_code] = {
                        "bfs": bfs_code,
                        "name": entry["name"],
                        "canton": canton,
                    }

                if len(data) < 50:
                    break
                page += 1

        logger.info("OpenPLZ CH: {} municipalities loaded", len(municipalities))
        return municipalities
    finally:
        if own_client:
            await client.aclose()
