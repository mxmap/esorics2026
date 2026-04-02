"""Shared test fixtures."""

import pytest
import stamina
from loguru import logger


@pytest.fixture(autouse=True)
def _disable_stamina_wait():
    stamina.set_testing(True)
    yield
    stamina.set_testing(False)


@pytest.fixture
def caplog(caplog):
    handler_id = logger.add(
        caplog.handler,
        format="{message}",
        level=0,
        enqueue=False,
    )
    yield caplog
    logger.remove(handler_id)


@pytest.fixture
def mock_dns(monkeypatch):
    """Patch DNS lookup_mx to return configurable results."""

    results: dict[str, list[str]] = {}

    async def _lookup_mx(domain: str) -> list[str]:
        return results.get(domain, [])

    monkeypatch.setattr("mail_municipalities.core.dns.lookup_mx", _lookup_mx)
    monkeypatch.setattr("mail_municipalities.domain_resolver.pipeline.lookup_mx", _lookup_mx)
    return results
