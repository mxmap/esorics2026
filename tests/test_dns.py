"""Tests for DNS module."""

from unittest.mock import AsyncMock, MagicMock, patch

import dns.exception
import dns.resolver
import pytest

from municipality_email.dns import (
    lookup_a,
    lookup_mx,
    make_resolvers,
    reset_resolvers,
    resolve_robust,
)


@pytest.fixture(autouse=True)
def _reset():
    reset_resolvers()
    yield
    reset_resolvers()


class TestMakeResolvers:
    def test_creates_three_resolvers(self):
        resolvers = make_resolvers()
        assert len(resolvers) == 3

    def test_shared_cache(self):
        resolvers = make_resolvers()
        assert resolvers[0].cache is resolvers[1].cache
        assert resolvers[1].cache is resolvers[2].cache

    def test_timeouts(self):
        resolvers = make_resolvers()
        for r in resolvers:
            assert r.timeout == 5
            assert r.lifetime == 8


class TestResolveRobust:
    async def test_success_first_resolver(self):
        mock_answer = MagicMock()
        with patch("municipality_email.dns.get_resolvers") as mock_get:
            r1 = MagicMock()
            r1.resolve = AsyncMock(return_value=mock_answer)
            mock_get.return_value = [r1]
            result = await resolve_robust("example.com", "MX")
        assert result is mock_answer

    async def test_nxdomain_terminal(self):
        with patch("municipality_email.dns.get_resolvers") as mock_get:
            r1 = MagicMock()
            r1.resolve = AsyncMock(side_effect=dns.resolver.NXDOMAIN())
            r2 = MagicMock()
            r2.resolve = AsyncMock(return_value=MagicMock())
            mock_get.return_value = [r1, r2]
            result = await resolve_robust("nonexistent.example", "MX")
        assert result is None
        r2.resolve.assert_not_called()

    async def test_timeout_retries_next(self):
        mock_answer = MagicMock()
        with patch("municipality_email.dns.get_resolvers") as mock_get:
            r1 = MagicMock()
            r1.resolve = AsyncMock(side_effect=dns.exception.Timeout())
            r2 = MagicMock()
            r2.resolve = AsyncMock(return_value=mock_answer)
            mock_get.return_value = [r1, r2]
            result = await resolve_robust("example.com", "A")
        assert result is mock_answer

    async def test_no_answer_retries_next(self):
        mock_answer = MagicMock()
        with patch("municipality_email.dns.get_resolvers") as mock_get:
            r1 = MagicMock()
            r1.resolve = AsyncMock(side_effect=dns.resolver.NoAnswer())
            r2 = MagicMock()
            r2.resolve = AsyncMock(return_value=mock_answer)
            mock_get.return_value = [r1, r2]
            result = await resolve_robust("example.com", "MX")
        assert result is mock_answer

    async def test_all_fail_returns_none(self):
        with patch("municipality_email.dns.get_resolvers") as mock_get:
            r1 = MagicMock()
            r1.resolve = AsyncMock(side_effect=dns.exception.Timeout())
            r2 = MagicMock()
            r2.resolve = AsyncMock(side_effect=dns.exception.Timeout())
            mock_get.return_value = [r1, r2]
            result = await resolve_robust("example.com", "MX")
        assert result is None


class TestLookupA:
    async def test_a_record_resolves(self):
        with patch(
            "municipality_email.dns.resolve_robust", new_callable=AsyncMock
        ) as mock_resolve:
            mock_resolve.return_value = MagicMock()
            result = await lookup_a("example.com")
        assert result is True
        mock_resolve.assert_called_once_with("example.com", "A")

    async def test_nxdomain_returns_false(self):
        with patch(
            "municipality_email.dns.resolve_robust", new_callable=AsyncMock
        ) as mock_resolve:
            mock_resolve.return_value = None
            result = await lookup_a("nonexistent.example")
        assert result is False
        assert mock_resolve.call_count == 2

    async def test_no_a_but_aaaa_resolves(self):
        with patch(
            "municipality_email.dns.resolve_robust", new_callable=AsyncMock
        ) as mock_resolve:
            mock_resolve.side_effect = [None, MagicMock()]
            result = await lookup_a("ipv6only.example.com")
        assert result is True
        assert mock_resolve.call_args_list[0].args == ("ipv6only.example.com", "A")
        assert mock_resolve.call_args_list[1].args == ("ipv6only.example.com", "AAAA")


class TestLookupMx:
    async def test_returns_sorted_lowercase(self):
        mock_records = [MagicMock(), MagicMock()]
        mock_records[0].exchange = "MX2.Example.COM."
        mock_records[1].exchange = "MX1.Example.COM."
        mock_answer = MagicMock()
        mock_answer.__iter__ = lambda self: iter(mock_records)

        with patch(
            "municipality_email.dns.resolve_robust", new_callable=AsyncMock
        ) as mock_resolve:
            mock_resolve.return_value = mock_answer
            result = await lookup_mx("example.com")

        assert result == ["mx1.example.com", "mx2.example.com"]

    async def test_no_mx_returns_empty(self):
        with patch(
            "municipality_email.dns.resolve_robust", new_callable=AsyncMock
        ) as mock_resolve:
            mock_resolve.return_value = None
            result = await lookup_mx("no-mx.example.com")

        assert result == []
