"""Shared test fixtures."""

import pytest


@pytest.fixture
def mock_dns(monkeypatch):
    """Patch DNS lookup_mx to return configurable results."""

    results: dict[str, list[str]] = {}

    async def _lookup_mx(domain: str) -> list[str]:
        return results.get(domain, [])

    monkeypatch.setattr("municipality_email.dns.lookup_mx", _lookup_mx)
    monkeypatch.setattr("municipality_email.pipeline.lookup_mx", _lookup_mx)
    return results
