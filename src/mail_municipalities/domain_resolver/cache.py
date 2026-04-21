"""SQLite-backed cache for HEAD validation, scraping, and MX results."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

import aiosqlite

_CHUNK_SIZE = 900  # SQLite variable limit is 999


def _chunked(items: list, size: int) -> Iterator[list]:
    """Yield successive chunks of `size` from `items`."""
    for i in range(0, len(items), size):
        yield items[i : i + size]


def _now_utc() -> str:
    """UTC timestamp in SQLite-compatible format."""
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


_SCHEMA = """
CREATE TABLE IF NOT EXISTS head_cache (
    domain TEXT PRIMARY KEY,
    accessible INTEGER NOT NULL,
    redirect TEXT,
    ssl_failed INTEGER NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS scrape_cache (
    domain TEXT PRIMARY KEY,
    emails TEXT NOT NULL,
    redirect TEXT,
    accessible INTEGER NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS mx_cache (
    domain TEXT PRIMARY KEY,
    has_mx INTEGER NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dns_cache (
    domain TEXT PRIMARY KEY,
    resolves INTEGER NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS content_cache (
    domain TEXT PRIMARY KEY,
    flags TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
"""


class CacheDB:
    """Async SQLite cache for pipeline network phases."""

    def __init__(
        self,
        path: Path,
        *,
        head_ttl_days: int = 1,
        scrape_ttl_days: int = 7,
        mx_ttl_days: int = 1,
        dns_ttl_days: int = 1,
        content_ttl_days: int = 7,
    ) -> None:
        self._path = path
        self._head_ttl = head_ttl_days
        self._scrape_ttl = scrape_ttl_days
        self._mx_ttl = mx_ttl_days
        self._dns_ttl = dns_ttl_days
        self._content_ttl = content_ttl_days
        self._db: aiosqlite.Connection | None = None

    async def __aenter__(self) -> CacheDB:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._db = await aiosqlite.connect(self._path)
        await self._db.execute("PRAGMA journal_mode=WAL")
        await self._db.execute("PRAGMA synchronous=NORMAL")
        await self._db.executescript(_SCHEMA)
        await self._db.commit()
        return self

    async def __aexit__(self, *exc) -> None:
        if self._db is not None:
            await self._db.close()
            self._db = None

    # ── HEAD cache ──────────────────────────────────────────────────

    async def get_head_many(self, domains: set[str]) -> dict[str, tuple[bool, str | None, bool]]:
        """Load cached HEAD results, filtering expired entries."""
        assert self._db is not None
        result: dict[str, tuple[bool, str | None, bool]] = {}
        for chunk in _chunked(sorted(domains), _CHUNK_SIZE):
            placeholders = ",".join("?" * len(chunk))
            sql = (
                f"SELECT domain, accessible, redirect, ssl_failed FROM head_cache "
                f"WHERE domain IN ({placeholders}) "
                f"AND updated_at > datetime('now', '-{self._head_ttl} days')"
            )
            async with self._db.execute(sql, chunk) as cur:
                async for row in cur:
                    result[row[0]] = (bool(row[1]), row[2], bool(row[3]))
        return result

    async def put_head_many(self, entries: dict[str, tuple[bool, str | None, bool]]) -> None:
        """Store HEAD results (upsert).

        Only caches accessible results — inaccessible domains are re-checked
        every run since they may come back online.
        """
        assert self._db is not None
        now = _now_utc()
        rows = [
            (domain, int(accessible), redirect, int(ssl_failed), now)
            for domain, (accessible, redirect, ssl_failed) in entries.items()
            if accessible
        ]
        await self._db.executemany(
            "INSERT OR REPLACE INTO head_cache "
            "(domain, accessible, redirect, ssl_failed, updated_at) "
            "VALUES (?, ?, ?, ?, ?)",
            rows,
        )
        await self._db.commit()

    # ── Scrape cache ────────────────────────────────────────────────

    async def get_scrape_many(self, domains: set[str]) -> dict[str, tuple[set[str], str | None, bool]]:
        """Load cached scrape results, filtering expired entries."""
        assert self._db is not None
        result: dict[str, tuple[set[str], str | None, bool]] = {}
        for chunk in _chunked(sorted(domains), _CHUNK_SIZE):
            placeholders = ",".join("?" * len(chunk))
            sql = (
                f"SELECT domain, emails, redirect, accessible FROM scrape_cache "
                f"WHERE domain IN ({placeholders}) "
                f"AND updated_at > datetime('now', '-{self._scrape_ttl} days')"
            )
            async with self._db.execute(sql, chunk) as cur:
                async for row in cur:
                    result[row[0]] = (set(json.loads(row[1])), row[2], bool(row[3]))
        return result

    async def put_scrape(
        self,
        domain: str,
        emails: set[str],
        redirect: str | None,
        accessible: bool,
    ) -> None:
        """Store a single scrape result (upsert + commit)."""
        assert self._db is not None
        await self._db.execute(
            "INSERT OR REPLACE INTO scrape_cache "
            "(domain, emails, redirect, accessible, updated_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (domain, json.dumps(sorted(emails)), redirect, int(accessible), _now_utc()),
        )
        await self._db.commit()

    # ── MX cache ────────────────────────────────────────────────────

    async def get_mx_many(self, domains: set[str]) -> dict[str, bool]:
        """Load cached MX results, filtering expired entries."""
        assert self._db is not None
        result: dict[str, bool] = {}
        for chunk in _chunked(sorted(domains), _CHUNK_SIZE):
            placeholders = ",".join("?" * len(chunk))
            sql = (
                f"SELECT domain, has_mx FROM mx_cache "
                f"WHERE domain IN ({placeholders}) "
                f"AND updated_at > datetime('now', '-{self._mx_ttl} days')"
            )
            async with self._db.execute(sql, chunk) as cur:
                async for row in cur:
                    result[row[0]] = bool(row[1])
        return result

    async def put_mx_many(self, entries: dict[str, bool]) -> None:
        """Store MX results (upsert)."""
        assert self._db is not None
        now = _now_utc()
        rows = [(domain, int(has_mx), now) for domain, has_mx in entries.items()]
        await self._db.executemany(
            "INSERT OR REPLACE INTO mx_cache (domain, has_mx, updated_at) VALUES (?, ?, ?)",
            rows,
        )
        await self._db.commit()

    # ── DNS cache ──────────────────────────────────────────────────

    async def get_dns_many(self, domains: set[str]) -> dict[str, bool]:
        """Load cached DNS resolution results, filtering expired entries."""
        assert self._db is not None
        result: dict[str, bool] = {}
        for chunk in _chunked(sorted(domains), _CHUNK_SIZE):
            placeholders = ",".join("?" * len(chunk))
            sql = (
                f"SELECT domain, resolves FROM dns_cache "
                f"WHERE domain IN ({placeholders}) "
                f"AND updated_at > datetime('now', '-{self._dns_ttl} days')"
            )
            async with self._db.execute(sql, chunk) as cur:
                async for row in cur:
                    result[row[0]] = bool(row[1])
        return result

    async def put_dns_many(self, entries: dict[str, bool]) -> None:
        """Store DNS resolution results (upsert)."""
        assert self._db is not None
        now = _now_utc()
        rows = [(domain, int(resolves), now) for domain, resolves in entries.items()]
        await self._db.executemany(
            "INSERT OR REPLACE INTO dns_cache (domain, resolves, updated_at) VALUES (?, ?, ?)",
            rows,
        )
        await self._db.commit()

    # ── Content cache ──────────────────────────────────────────────

    async def get_content_many(self, domains: set[str]) -> dict[str, list[str]]:
        """Load cached content validation results, filtering expired entries."""
        assert self._db is not None
        result: dict[str, list[str]] = {}
        for chunk in _chunked(sorted(domains), _CHUNK_SIZE):
            placeholders = ",".join("?" * len(chunk))
            sql = (
                f"SELECT domain, flags FROM content_cache "
                f"WHERE domain IN ({placeholders}) "
                f"AND updated_at > datetime('now', '-{self._content_ttl} days')"
            )
            async with self._db.execute(sql, chunk) as cur:
                async for row in cur:
                    result[row[0]] = json.loads(row[1])
        return result

    async def put_content_many(self, entries: dict[str, list[str]]) -> None:
        """Store content validation results (upsert)."""
        assert self._db is not None
        now = _now_utc()
        rows = [(domain, json.dumps(flags), now) for domain, flags in entries.items()]
        await self._db.executemany(
            "INSERT OR REPLACE INTO content_cache (domain, flags, updated_at) VALUES (?, ?, ?)",
            rows,
        )
        await self._db.commit()
