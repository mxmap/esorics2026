"""Tests for async scraping functions."""

import httpx
import respx

from municipality_email.scraping import (
    _is_ssl_error,
    scrape_email_domains,
    validate_domain_accessibility,
)


class TestValidateDomainAccessibility:
    async def test_accessible(self):
        with respx.mock:
            respx.head("https://www.example.ch").respond(200)
            async with httpx.AsyncClient() as client:
                accessible, redirect, ssl_failed = await validate_domain_accessibility(
                    client, "example.ch"
                )
        assert accessible is True
        assert redirect is None
        assert ssl_failed is False

    async def test_redirect(self):
        with respx.mock:
            respx.head("https://www.old.ch").respond(301, headers={"Location": "https://new.ch/"})
            respx.head("https://new.ch/").respond(200)
            async with httpx.AsyncClient(follow_redirects=True) as client:
                accessible, redirect, ssl_failed = await validate_domain_accessibility(
                    client, "old.ch"
                )
        assert accessible is True

    async def test_not_accessible(self):
        with respx.mock:
            respx.head("https://www.dead.ch").respond(500)
            respx.head("https://dead.ch").respond(500)
            async with httpx.AsyncClient() as client:
                accessible, redirect, ssl_failed = await validate_domain_accessibility(
                    client, "dead.ch"
                )
        assert accessible is False
        assert ssl_failed is False

    async def test_connection_error(self):
        with respx.mock:
            respx.head("https://www.nohost.ch").mock(side_effect=httpx.ConnectError("refused"))
            respx.head("https://nohost.ch").mock(side_effect=httpx.ConnectError("refused"))
            async with httpx.AsyncClient() as client:
                accessible, redirect, ssl_failed = await validate_domain_accessibility(
                    client, "nohost.ch"
                )
        assert accessible is False


class TestScrapeEmailDomains:
    async def test_finds_emails(self):
        with respx.mock:
            respx.get("https://www.example.ch/").respond(200, html="<p>info@example.ch</p>")
            respx.get("https://www.example.ch/kontakt").respond(
                200, html="<p>admin@example.ch</p>"
            )
            respx.get("https://example.ch/").respond(200, html="")
            respx.get("https://example.ch/kontakt").respond(200, html="")

            async with httpx.AsyncClient() as client:
                emails, redirect, accessible = await scrape_email_domains(
                    client, "example.ch", ["/kontakt"], set()
                )

        assert "example.ch" in emails
        assert accessible is True

    async def test_respects_skip_domains(self):
        with respx.mock:
            respx.get("https://www.example.ch/").respond(
                200, html="<p>info@example.ch user@gmail.com</p>"
            )
            respx.get("https://example.ch/").respond(200, html="")

            async with httpx.AsyncClient() as client:
                emails, _, _ = await scrape_email_domains(client, "example.ch", [], {"gmail.com"})

        assert "example.ch" in emails
        assert "gmail.com" not in emails

    async def test_empty_domain(self):
        async with httpx.AsyncClient() as client:
            emails, redirect, accessible = await scrape_email_domains(client, "", [], set())
        assert emails == set()
        assert accessible is False

    async def test_consecutive_failure_bailout(self):
        with respx.mock:
            # All URLs fail
            respx.get("https://www.failing.ch/").mock(side_effect=httpx.ConnectError("refused"))
            respx.get("https://www.failing.ch/kontakt").mock(
                side_effect=httpx.ConnectError("refused")
            )
            respx.get("https://www.failing.ch/impressum").mock(
                side_effect=httpx.ConnectError("refused")
            )
            # After 3 consecutive failures, should bail
            respx.get("https://failing.ch/").mock(side_effect=httpx.ConnectError("refused"))

            async with httpx.AsyncClient() as client:
                emails, redirect, accessible = await scrape_email_domains(
                    client, "failing.ch", ["/kontakt", "/impressum"], set()
                )

        assert emails == set()
        assert accessible is False

    async def test_non_exhaustive_stops_early(self):
        with respx.mock:
            respx.get("https://www.example.ch/").respond(200, html="<p>info@example.ch</p>")

            async with httpx.AsyncClient() as client:
                emails, _, accessible = await scrape_email_domains(
                    client,
                    "example.ch",
                    ["/kontakt", "/impressum"],
                    set(),
                    exhaustive=False,
                )

        assert "example.ch" in emails
        assert accessible is True

    async def test_detects_redirect(self):
        with respx.mock:
            # Redirect from old to new domain
            respx.get("https://www.old.ch/").respond(
                200,
                html="<p>info@old.ch</p>",
                headers={"Location": "https://new.ch/"},
            )
            respx.get("https://old.ch/").respond(200, html="")

            async with httpx.AsyncClient() as client:
                emails, redirect, accessible = await scrape_email_domains(
                    client, "old.ch", [], set()
                )

        assert accessible is True


class TestIsSslError:
    def test_ssl_verification_error(self):
        import ssl

        exc = httpx.ConnectError("SSL error")
        exc.__cause__ = ssl.SSLCertVerificationError()
        assert _is_ssl_error(exc) is True

    def test_not_ssl(self):
        exc = httpx.ConnectError("Connection refused")
        assert _is_ssl_error(exc) is False

    def test_string_match(self):
        exc = httpx.ConnectError("CERTIFICATE_VERIFY_FAILED")
        assert _is_ssl_error(exc) is True
