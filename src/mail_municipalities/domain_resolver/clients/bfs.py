"""Swiss BFS (Federal Statistical Office) API client."""

from __future__ import annotations

import csv
import io
import time

import httpx
import stamina
from loguru import logger

BFS_API_URL = "https://www.agvchapp.bfs.admin.ch/api/communes/snapshot"

CANTON_SHORT_TO_FULL = {
    "zh": "Kanton Zürich",
    "be": "Kanton Bern",
    "lu": "Kanton Luzern",
    "ur": "Kanton Uri",
    "sz": "Kanton Schwyz",
    "ow": "Kanton Obwalden",
    "nw": "Kanton Nidwalden",
    "gl": "Kanton Glarus",
    "zg": "Kanton Zug",
    "fr": "Kanton Freiburg",
    "so": "Kanton Solothurn",
    "bs": "Kanton Basel-Stadt",
    "bl": "Kanton Basel-Landschaft",
    "sh": "Kanton Schaffhausen",
    "ar": "Kanton Appenzell Ausserrhoden",
    "ai": "Kanton Appenzell Innerrhoden",
    "sg": "Kanton St. Gallen",
    "gr": "Kanton Graubünden",
    "ag": "Kanton Aargau",
    "tg": "Kanton Thurgau",
    "ti": "Kanton Tessin",
    "vd": "Kanton Waadt",
    "vs": "Kanton Wallis",
    "ne": "Kanton Neuenburg",
    "ge": "Kanton Genf",
    "ju": "Kanton Jura",
}


@stamina.retry(
    on=(httpx.HTTPStatusError, httpx.ConnectError, httpx.TimeoutException),
    attempts=3,
    wait_initial=2.0,
)
async def _fetch(client: httpx.AsyncClient, url: str, params: dict) -> httpx.Response:
    r = await client.get(url, params=params)
    r.raise_for_status()
    return r


def _parse_csv_response(text: str) -> list[dict]:
    """Parse the BFS API CSV response into a list of dicts."""
    reader = csv.DictReader(io.StringIO(text))
    entries = []
    for row in reader:
        entries.append(
            {
                "historicalCode": int(row["HistoricalCode"]),
                "bfsCode": int(row["BfsCode"]),
                "level": int(row["Level"]),
                "parent": int(row["Parent"]) if row.get("Parent") else None,
                "name": row["Name"],
                "shortName": row["ShortName"],
            }
        )
    return entries


async def fetch_bfs_municipalities(date: str | None = None) -> dict[str, dict]:
    """Fetch municipality list from BFS REST API.

    Args:
        date: Optional date in DD-MM-YYYY format. Defaults to today.

    Returns:
        Dict mapping BFS code (str) to {"bfs", "name", "canton"}.
    """
    if date is None:
        date = time.strftime("%d-%m-%Y")

    logger.info("Fetching municipalities from BFS (date={})...", date)

    async with httpx.AsyncClient(timeout=60, http2=True) as client:
        t0 = time.monotonic()
        r = await _fetch(client, BFS_API_URL, {"date": date})
        logger.debug("BFS API response: {} bytes in {:.1f}s", len(r.text), time.monotonic() - t0)
        entries = _parse_csv_response(r.text)

    # Build lookup by HistoricalCode for parent resolution
    by_hist_code: dict[int, dict] = {}
    for entry in entries:
        by_hist_code[entry["historicalCode"]] = entry

    # Filter to Level 3 (communes) and resolve cantons
    municipalities: dict[str, dict] = {}
    for entry in entries:
        if entry["level"] != 3:
            continue

        bfs_code = str(entry["bfsCode"])
        name = entry["name"]

        # Resolve canton: walk up hierarchy until Level 1 (canton)
        canton = ""
        current = entry
        for _ in range(5):  # max depth guard
            parent_code = current.get("parent")
            if parent_code is None:
                break
            parent = by_hist_code.get(parent_code)
            if parent is None:
                break
            if parent["level"] == 1:
                canton_short = parent.get("shortName", "").lower()
                canton = CANTON_SHORT_TO_FULL.get(canton_short, "")
                break
            current = parent

        if not canton:
            logger.warning("BFS: no canton resolved for {} ({})", bfs_code, name)

        municipalities[bfs_code] = {
            "bfs": bfs_code,
            "name": name,
            "canton": canton,
        }

    logger.info("BFS API: {} municipalities", len(municipalities))
    return municipalities
