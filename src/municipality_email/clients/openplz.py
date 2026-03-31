"""Austrian OpenPLZ API client."""

from __future__ import annotations

import httpx
from loguru import logger

OPENPLZ_BASE_AT = "https://openplzapi.org/at/FederalProvinces"


async def fetch_openplz_municipalities(
    client: httpx.AsyncClient | None = None,
) -> dict[str, dict]:
    """Fetch all Austrian municipalities from the OpenPLZ API.

    Iterates all 9 Bundesländer and paginates to get the full list.
    Returns dict keyed by 5-digit GKZ string.
    """
    own_client = client is None
    if own_client:
        client = httpx.AsyncClient(timeout=30)

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

        logger.info("OpenPLZ: {} municipalities loaded", len(municipalities))
        return municipalities
    finally:
        if own_client:
            await client.aclose()
