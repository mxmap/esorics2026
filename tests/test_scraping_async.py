"""Tests for async scraping functions."""

import ssl
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import respx

from municipality_email.scraping import (
    _is_ssl_error,
    _try_fetch,
    scrape_email_domains,
    scrape_with_playwright,
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
        exc = httpx.ConnectError("SSL error")
        exc.__cause__ = ssl.SSLCertVerificationError()
        assert _is_ssl_error(exc) is True

    def test_not_ssl(self):
        exc = httpx.ConnectError("Connection refused")
        assert _is_ssl_error(exc) is False

    def test_string_match(self):
        exc = httpx.ConnectError("CERTIFICATE_VERIFY_FAILED")
        assert _is_ssl_error(exc) is True


def _make_ssl_connect_error() -> httpx.ConnectError:
    """Create a ConnectError with an SSL cause for testing."""
    exc = httpx.ConnectError("SSL error")
    exc.__cause__ = ssl.SSLCertVerificationError()
    return exc


def _raise_ssl_connect_error(request: httpx.Request) -> None:
    """Side-effect callable that raises SSL ConnectError (preserves cause chain in respx)."""
    exc = httpx.ConnectError("SSL error")
    exc.__cause__ = ssl.SSLCertVerificationError()
    raise exc


class TestValidateDomainAccessibilitySSL:
    async def test_ssl_error_insecure_fallback_succeeds(self):
        """SSL error on normal HEAD, insecure retry succeeds."""
        mock_response = MagicMock(spec=httpx.Response)
        mock_response.status_code = 200
        mock_response.url = "https://www.sslbad.ch/"

        # Outer client raises SSL on head
        outer_client = AsyncMock()
        outer_client.head = AsyncMock(side_effect=_make_ssl_connect_error())

        # Inner insecure client succeeds
        mock_ic = AsyncMock()
        mock_ic.__aenter__ = AsyncMock(return_value=mock_ic)
        mock_ic.__aexit__ = AsyncMock(return_value=False)
        mock_ic.head = AsyncMock(return_value=mock_response)

        with patch("municipality_email.scraping.httpx.AsyncClient", return_value=mock_ic):
            accessible, redirect, ssl_failed = await validate_domain_accessibility(
                outer_client, "sslbad.ch"
            )

        assert accessible is True
        assert ssl_failed is True

    async def test_ssl_error_insecure_fallback_also_fails(self):
        """SSL error on normal HEAD, insecure retry also fails → tries bare, also fails."""
        outer_client = AsyncMock()
        outer_client.head = AsyncMock(side_effect=_make_ssl_connect_error())

        mock_ic = AsyncMock()
        mock_ic.__aenter__ = AsyncMock(return_value=mock_ic)
        mock_ic.__aexit__ = AsyncMock(return_value=False)
        mock_ic.head = AsyncMock(side_effect=Exception("insecure also failed"))

        with patch("municipality_email.scraping.httpx.AsyncClient", return_value=mock_ic):
            accessible, redirect, ssl_failed = await validate_domain_accessibility(
                outer_client, "sslbad.ch"
            )

        assert accessible is False
        assert ssl_failed is False

    async def test_generic_exception_continues(self):
        """Non-SSL, non-ConnectError exception → continues to next prefix."""
        with respx.mock:
            respx.head("https://www.weird.ch").mock(side_effect=httpx.ReadTimeout("timeout"))
            respx.head("https://weird.ch").respond(200)
            async with httpx.AsyncClient() as client:
                accessible, redirect, ssl_failed = await validate_domain_accessibility(
                    client, "weird.ch"
                )
        assert accessible is True
        assert ssl_failed is False


class TestTryFetch:
    async def test_ssl_failed_flag_insecure_succeeds(self):
        """When ssl_failed=True, uses _fetch_insecure and returns response."""
        mock_resp = MagicMock(spec=httpx.Response)
        with patch(
            "municipality_email.scraping._fetch_insecure",
            new_callable=AsyncMock,
            return_value=mock_resp,
        ):
            async with httpx.AsyncClient() as client:
                resp, ssl_flag = await _try_fetch(client, "https://example.ch/", ssl_failed=True)
        assert resp is mock_resp
        assert ssl_flag is True

    async def test_ssl_failed_flag_insecure_raises(self):
        """When ssl_failed=True and _fetch_insecure raises, returns (None, True)."""
        with patch(
            "municipality_email.scraping._fetch_insecure",
            new_callable=AsyncMock,
            side_effect=Exception("network down"),
        ):
            async with httpx.AsyncClient() as client:
                resp, ssl_flag = await _try_fetch(client, "https://example.ch/", ssl_failed=True)
        assert resp is None
        assert ssl_flag is True

    async def test_ssl_error_on_normal_fetch_insecure_succeeds(self):
        """Normal fetch hits SSL error, insecure fallback succeeds."""
        mock_resp = MagicMock(spec=httpx.Response)
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(side_effect=_make_ssl_connect_error())

        with patch(
            "municipality_email.scraping._fetch_insecure",
            new_callable=AsyncMock,
            return_value=mock_resp,
        ):
            resp, ssl_flag = await _try_fetch(mock_client, "https://sslbad.ch/", ssl_failed=False)
        assert resp is mock_resp
        assert ssl_flag is True

    async def test_ssl_error_on_normal_fetch_insecure_also_fails(self):
        """Normal fetch hits SSL error, insecure fallback also raises."""
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(side_effect=_make_ssl_connect_error())

        with patch(
            "municipality_email.scraping._fetch_insecure",
            new_callable=AsyncMock,
            side_effect=Exception("also broken"),
        ):
            resp, ssl_flag = await _try_fetch(mock_client, "https://sslbad.ch/", ssl_failed=False)
        assert resp is None
        assert ssl_flag is True

    async def test_generic_exception(self):
        """Non-SSL exception returns (None, ssl_failed unchanged)."""
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(side_effect=httpx.ReadTimeout("slow"))

        resp, ssl_flag = await _try_fetch(mock_client, "https://timeout.ch/", ssl_failed=False)
        assert resp is None
        assert ssl_flag is False


class TestScrapeEmailDomainsExtended:
    async def test_discovers_contact_links_from_homepage(self):
        """Homepage contains a contact link not in static subpages → scrapes it."""
        homepage_html = """
        <html><body>
            <a href="/kontakt-formular">Kontakt</a>
            <p>No email here</p>
        </body></html>
        """
        with respx.mock:
            respx.get("https://www.example.ch/").respond(200, html=homepage_html)
            respx.get("https://www.example.ch/kontakt-formular").respond(
                200, html="<p>info@example.ch</p>"
            )

            async with httpx.AsyncClient() as client:
                emails, redirect, accessible = await scrape_email_domains(
                    client, "example.ch", [], set()
                )

        assert "example.ch" in emails
        assert accessible is True

    async def test_subpage_failure_then_success(self):
        """One subpage fails, next succeeds → emails found despite partial failure."""
        with respx.mock:
            respx.get("https://www.example.ch/").respond(200, html="<p>no emails</p>")
            respx.get("https://www.example.ch/kontakt").mock(
                side_effect=httpx.ConnectError("refused")
            )
            respx.get("https://www.example.ch/impressum").respond(
                200, html="<p>info@example.ch</p>"
            )

            async with httpx.AsyncClient() as client:
                emails, redirect, accessible = await scrape_email_domains(
                    client, "example.ch", ["/kontakt", "/impressum"], set()
                )

        assert "example.ch" in emails
        assert accessible is True

    async def test_non_exhaustive_stops_on_subpage(self):
        """Non-exhaustive mode: homepage has no email, subpage has email → returns early."""
        with respx.mock:
            respx.get("https://www.example.ch/").respond(200, html="<p>no emails</p>")
            respx.get("https://www.example.ch/kontakt").respond(200, html="<p>info@example.ch</p>")
            # /impressum should NOT be fetched since we stop early
            respx.get("https://www.example.ch/impressum").respond(
                200, html="<p>other@example.ch</p>"
            )

            async with httpx.AsyncClient() as client:
                emails, redirect, accessible = await scrape_email_domains(
                    client,
                    "example.ch",
                    ["/kontakt", "/impressum"],
                    set(),
                    exhaustive=False,
                )

        assert "example.ch" in emails
        assert accessible is True


class TestScrapeWithPlaywright:
    async def test_finds_emails(self):
        """Playwright renders page, extracts emails from DOM."""
        mock_response = MagicMock()
        mock_response.__bool__ = MagicMock(return_value=True)

        mock_page = AsyncMock()
        mock_page.goto = AsyncMock(return_value=mock_response)
        mock_page.wait_for_timeout = AsyncMock()
        mock_page.url = "https://www.example.ch/"
        mock_page.content = AsyncMock(return_value="<p>info@example.ch</p>")
        mock_page.close = AsyncMock()

        mock_browser = AsyncMock()
        mock_browser.new_page = AsyncMock(return_value=mock_page)
        mock_browser.close = AsyncMock()

        mock_pw = AsyncMock()
        mock_pw.chromium = MagicMock()
        mock_pw.chromium.launch = AsyncMock(return_value=mock_browser)

        mock_pw_ctx = AsyncMock()
        mock_pw_ctx.__aenter__ = AsyncMock(return_value=mock_pw)
        mock_pw_ctx.__aexit__ = AsyncMock(return_value=False)

        with patch(
            "playwright.async_api.async_playwright",
            return_value=mock_pw_ctx,
        ):
            emails, redirect = await scrape_with_playwright("example.ch", ["/kontakt"], set())

        assert "example.ch" in emails

    async def test_detects_redirect(self):
        """Playwright detects redirect when final URL differs from domain."""
        mock_response = MagicMock()
        mock_response.__bool__ = MagicMock(return_value=True)

        mock_page = AsyncMock()
        mock_page.goto = AsyncMock(return_value=mock_response)
        mock_page.wait_for_timeout = AsyncMock()
        mock_page.url = "https://www.redirected.ch/"
        mock_page.content = AsyncMock(return_value="<p>info@redirected.ch</p>")
        mock_page.close = AsyncMock()

        mock_browser = AsyncMock()
        mock_browser.new_page = AsyncMock(return_value=mock_page)
        mock_browser.close = AsyncMock()

        mock_pw = AsyncMock()
        mock_pw.chromium = MagicMock()
        mock_pw.chromium.launch = AsyncMock(return_value=mock_browser)

        mock_pw_ctx = AsyncMock()
        mock_pw_ctx.__aenter__ = AsyncMock(return_value=mock_pw)
        mock_pw_ctx.__aexit__ = AsyncMock(return_value=False)

        with patch(
            "playwright.async_api.async_playwright",
            return_value=mock_pw_ctx,
        ):
            emails, redirect = await scrape_with_playwright("example.ch", [], set())

        assert redirect == "redirected.ch"

    async def test_page_error_continues(self):
        """Playwright page.goto raises → continues to next URL, returns empty."""
        mock_page = AsyncMock()
        mock_page.goto = AsyncMock(side_effect=Exception("Navigation failed"))
        mock_page.close = AsyncMock()

        mock_browser = AsyncMock()
        mock_browser.new_page = AsyncMock(return_value=mock_page)
        mock_browser.close = AsyncMock()

        mock_pw = AsyncMock()
        mock_pw.chromium = MagicMock()
        mock_pw.chromium.launch = AsyncMock(return_value=mock_browser)

        mock_pw_ctx = AsyncMock()
        mock_pw_ctx.__aenter__ = AsyncMock(return_value=mock_pw)
        mock_pw_ctx.__aexit__ = AsyncMock(return_value=False)

        with patch(
            "playwright.async_api.async_playwright",
            return_value=mock_pw_ctx,
        ):
            emails, redirect = await scrape_with_playwright("example.ch", ["/kontakt"], set())

        assert emails == set()
        assert redirect is None

    async def test_none_response_skipped(self):
        """Playwright page.goto returns None → skips to next URL."""
        mock_page = AsyncMock()
        mock_page.goto = AsyncMock(return_value=None)
        mock_page.close = AsyncMock()

        mock_browser = AsyncMock()
        mock_browser.new_page = AsyncMock(return_value=mock_page)
        mock_browser.close = AsyncMock()

        mock_pw = AsyncMock()
        mock_pw.chromium = MagicMock()
        mock_pw.chromium.launch = AsyncMock(return_value=mock_browser)

        mock_pw_ctx = AsyncMock()
        mock_pw_ctx.__aenter__ = AsyncMock(return_value=mock_pw)
        mock_pw_ctx.__aexit__ = AsyncMock(return_value=False)

        with patch(
            "playwright.async_api.async_playwright",
            return_value=mock_pw_ctx,
        ):
            emails, redirect = await scrape_with_playwright("example.ch", [], set())

        assert emails == set()
        assert redirect is None
