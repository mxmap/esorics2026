"""Tests for SQLite cache module."""

import asyncio


from mail_municipalities.domain_resolver.cache import CacheDB


class TestCacheDBLifecycle:
    async def test_creates_tables(self, tmp_path):
        async with CacheDB(tmp_path / "cache.db") as cache:
            assert cache._db is not None
            async with cache._db.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name") as cur:
                tables = [row[0] async for row in cur]
        assert "head_cache" in tables
        assert "scrape_cache" in tables
        assert "mx_cache" in tables
        assert "dns_cache" in tables
        assert "content_cache" in tables

    async def test_wal_mode(self, tmp_path):
        async with CacheDB(tmp_path / "cache.db") as cache:
            assert cache._db is not None
            async with cache._db.execute("PRAGMA journal_mode") as cur:
                row = await cur.fetchone()
            assert row is not None
            assert row[0] == "wal"

    async def test_context_manager_closes(self, tmp_path):
        cache = CacheDB(tmp_path / "cache.db")
        async with cache:
            assert cache._db is not None
        assert cache._db is None

    async def test_creates_parent_dirs(self, tmp_path):
        path = tmp_path / "sub" / "dir" / "cache.db"
        async with CacheDB(path):
            pass
        assert path.exists()


class TestHeadCache:
    async def test_put_get_roundtrip(self, tmp_path):
        async with CacheDB(tmp_path / "cache.db") as cache:
            await cache.put_head_many(
                {
                    "example.ch": (True, None, False),
                    "redirect.ch": (True, "target.ch", False),
                    "ssl.ch": (True, None, True),
                }
            )
            result = await cache.get_head_many({"example.ch", "redirect.ch", "ssl.ch"})

        assert result["example.ch"] == (True, None, False)
        assert result["redirect.ch"] == (True, "target.ch", False)
        assert result["ssl.ch"] == (True, None, True)

    async def test_ttl_expiry(self, tmp_path):
        async with CacheDB(tmp_path / "cache.db", head_ttl_days=1) as cache:
            await cache.put_head_many({"old.ch": (True, None, False)})
            # Backdate the entry to 2 days ago
            assert cache._db is not None
            await cache._db.execute(
                "UPDATE head_cache SET updated_at = datetime('now', '-2 days') WHERE domain = 'old.ch'"
            )
            await cache._db.commit()
            result = await cache.get_head_many({"old.ch"})
        assert result == {}

    async def test_partial_hits(self, tmp_path):
        async with CacheDB(tmp_path / "cache.db") as cache:
            await cache.put_head_many({"hit.ch": (True, None, False)})
            result = await cache.get_head_many({"hit.ch", "miss.ch"})
        assert "hit.ch" in result
        assert "miss.ch" not in result

    async def test_empty_db(self, tmp_path):
        async with CacheDB(tmp_path / "cache.db") as cache:
            result = await cache.get_head_many({"anything.ch"})
        assert result == {}


class TestScrapeCache:
    async def test_put_get_roundtrip(self, tmp_path):
        async with CacheDB(tmp_path / "cache.db") as cache:
            await cache.put_scrape("example.ch", {"example.ch", "alt.ch"}, "redir.ch", True)
            result = await cache.get_scrape_many({"example.ch"})

        assert "example.ch" in result
        emails, redirect, accessible = result["example.ch"]
        assert emails == {"example.ch", "alt.ch"}
        assert redirect == "redir.ch"
        assert accessible is True

    async def test_none_redirect(self, tmp_path):
        async with CacheDB(tmp_path / "cache.db") as cache:
            await cache.put_scrape("test.ch", set(), None, False)
            result = await cache.get_scrape_many({"test.ch"})
        assert result["test.ch"] == (set(), None, False)

    async def test_upsert_overwrites(self, tmp_path):
        async with CacheDB(tmp_path / "cache.db") as cache:
            await cache.put_scrape("test.ch", {"old.ch"}, None, True)
            await cache.put_scrape("test.ch", {"new.ch"}, "redir.ch", True)
            result = await cache.get_scrape_many({"test.ch"})
        assert result["test.ch"][0] == {"new.ch"}
        assert result["test.ch"][1] == "redir.ch"

    async def test_ttl_expiry(self, tmp_path):
        async with CacheDB(tmp_path / "cache.db", scrape_ttl_days=7) as cache:
            await cache.put_scrape("old.ch", {"old.ch"}, None, True)
            assert cache._db is not None
            await cache._db.execute(
                "UPDATE scrape_cache SET updated_at = datetime('now', '-10 days') WHERE domain = 'old.ch'"
            )
            await cache._db.commit()
            result = await cache.get_scrape_many({"old.ch"})
        assert result == {}

    async def test_empty_emails(self, tmp_path):
        async with CacheDB(tmp_path / "cache.db") as cache:
            await cache.put_scrape("empty.ch", set(), None, False)
            result = await cache.get_scrape_many({"empty.ch"})
        assert result["empty.ch"][0] == set()


class TestMxCache:
    async def test_put_get_roundtrip(self, tmp_path):
        async with CacheDB(tmp_path / "cache.db") as cache:
            await cache.put_mx_many({"good.ch": True, "bad.ch": False})
            result = await cache.get_mx_many({"good.ch", "bad.ch"})
        assert result["good.ch"] is True
        assert result["bad.ch"] is False

    async def test_ttl_expiry(self, tmp_path):
        async with CacheDB(tmp_path / "cache.db", mx_ttl_days=1) as cache:
            await cache.put_mx_many({"old.ch": True})
            assert cache._db is not None
            await cache._db.execute(
                "UPDATE mx_cache SET updated_at = datetime('now', '-2 days') WHERE domain = 'old.ch'"
            )
            await cache._db.commit()
            result = await cache.get_mx_many({"old.ch"})
        assert result == {}

    async def test_partial_hits(self, tmp_path):
        async with CacheDB(tmp_path / "cache.db") as cache:
            await cache.put_mx_many({"hit.ch": True})
            result = await cache.get_mx_many({"hit.ch", "miss.ch"})
        assert "hit.ch" in result
        assert "miss.ch" not in result

    async def test_empty_db(self, tmp_path):
        async with CacheDB(tmp_path / "cache.db") as cache:
            result = await cache.get_mx_many({"anything.ch"})
        assert result == {}


class TestDnsCache:
    async def test_put_get_roundtrip(self, tmp_path):
        async with CacheDB(tmp_path / "cache.db") as cache:
            await cache.put_dns_many({"good.de": True, "bad.de": False})
            result = await cache.get_dns_many({"good.de", "bad.de"})
        assert result["good.de"] is True
        assert result["bad.de"] is False

    async def test_ttl_expiry(self, tmp_path):
        async with CacheDB(tmp_path / "cache.db", dns_ttl_days=1) as cache:
            await cache.put_dns_many({"old.de": True})
            assert cache._db is not None
            await cache._db.execute(
                "UPDATE dns_cache SET updated_at = datetime('now', '-2 days') WHERE domain = 'old.de'"
            )
            await cache._db.commit()
            result = await cache.get_dns_many({"old.de"})
        assert result == {}

    async def test_partial_hits(self, tmp_path):
        async with CacheDB(tmp_path / "cache.db") as cache:
            await cache.put_dns_many({"hit.de": True})
            result = await cache.get_dns_many({"hit.de", "miss.de"})
        assert "hit.de" in result
        assert "miss.de" not in result

    async def test_empty_db(self, tmp_path):
        async with CacheDB(tmp_path / "cache.db") as cache:
            result = await cache.get_dns_many({"anything.de"})
        assert result == {}


class TestContentCache:
    async def test_put_get_roundtrip(self, tmp_path):
        async with CacheDB(tmp_path / "cache.db") as cache:
            await cache.put_content_many({"parked.de": ["parked"], "good.de": ["has_municipality_keywords"]})
            result = await cache.get_content_many({"parked.de", "good.de"})
        assert result["parked.de"] == ["parked"]
        assert result["good.de"] == ["has_municipality_keywords"]

    async def test_ttl_expiry(self, tmp_path):
        async with CacheDB(tmp_path / "cache.db", content_ttl_days=7) as cache:
            await cache.put_content_many({"old.de": ["parked"]})
            assert cache._db is not None
            await cache._db.execute(
                "UPDATE content_cache SET updated_at = datetime('now', '-10 days') WHERE domain = 'old.de'"
            )
            await cache._db.commit()
            result = await cache.get_content_many({"old.de"})
        assert result == {}

    async def test_partial_hits(self, tmp_path):
        async with CacheDB(tmp_path / "cache.db") as cache:
            await cache.put_content_many({"hit.de": ["parked"]})
            result = await cache.get_content_many({"hit.de", "miss.de"})
        assert "hit.de" in result
        assert "miss.de" not in result

    async def test_empty_db(self, tmp_path):
        async with CacheDB(tmp_path / "cache.db") as cache:
            result = await cache.get_content_many({"anything.de"})
        assert result == {}


class TestEdgeCases:
    async def test_large_batch_chunking(self, tmp_path):
        """Ensure batches >900 domains are chunked correctly."""
        domains = {f"d{i}.ch": True for i in range(2000)}
        async with CacheDB(tmp_path / "cache.db") as cache:
            await cache.put_mx_many(domains)
            result = await cache.get_mx_many(set(domains))
        assert len(result) == 2000

    async def test_concurrent_writes(self, tmp_path):
        """Multiple concurrent put_scrape calls should not error."""
        async with CacheDB(tmp_path / "cache.db") as cache:
            tasks = [cache.put_scrape(f"d{i}.ch", {f"d{i}.ch"}, None, True) for i in range(50)]
            await asyncio.gather(*tasks)
            result = await cache.get_scrape_many({f"d{i}.ch" for i in range(50)})
        assert len(result) == 50

    async def test_empty_set_query(self, tmp_path):
        async with CacheDB(tmp_path / "cache.db") as cache:
            result = await cache.get_head_many(set())
        assert result == {}

    async def test_put_empty_dict(self, tmp_path):
        async with CacheDB(tmp_path / "cache.db") as cache:
            await cache.put_head_many({})
            await cache.put_mx_many({})
