"""Tests for scraping module."""

import httpx
import json
from datetime import datetime, timedelta, timezone

import ssl

from municipality_email.scraping import (
    _is_ssl_error,
    _process_scrape_response,
    discover_contact_links,
    _is_valid_email,
    _slugify_name,
    build_urls,
    build_urls_single_base,
    decrypt_cloudflare_email,
    decrypt_typo3,
    detect_website_mismatch,
    extract_email_domains,
    load_overrides,
    load_scrape_cache,
    save_scrape_cache,
    url_to_domain,
)


class TestUrlToDomain:
    def test_basic(self):
        assert url_to_domain("https://www.example.ch") == "example.ch"

    def test_without_scheme(self):
        assert url_to_domain("example.ch") == "example.ch"

    def test_with_www(self):
        assert url_to_domain("https://www.eisenstadt.gv.at/kontakt") == "eisenstadt.gv.at"

    def test_without_www(self):
        assert url_to_domain("https://flensburg.de") == "flensburg.de"

    def test_empty(self):
        assert url_to_domain("") is None
        assert url_to_domain(None) is None

    def test_http(self):
        assert url_to_domain("http://example.de/path") == "example.de"


class TestIsValidEmail:
    def test_valid(self):
        assert _is_valid_email("info@example.ch") is True

    def test_invalid(self):
        assert _is_valid_email("notanemail") is False

    def test_asset_extension_rejected(self):
        assert _is_valid_email("logo@2x.png") is False
        assert _is_valid_email("style@media.css") is False
        assert _is_valid_email("script@bundle.js") is False

    def test_real_tld_accepted(self):
        assert _is_valid_email("info@example.de") is True
        assert _is_valid_email("info@example.at") is True


class TestDecryptTypo3:
    def test_default_offset(self):
        # "ockn,vyzconi,lf" with offset 2 -> "mail@example.ch"
        encrypted = "ockn,vyzconi,lf"
        result = decrypt_typo3(encrypted, 2)
        # Verify the decryption produces valid email-like output
        assert "@" in result or "." in result

    def test_known_pair(self):
        # Encrypt "a" with offset -2 gives chr(0x61 + (-2) % 26) = chr(0x61 + 24) = chr(0x79) = 'y'
        # So decrypt "y" with offset 2 should give "a"
        assert decrypt_typo3("y", 2) == "a"

    def test_non_range_passthrough(self):
        # Characters outside the 3 ranges pass through unchanged
        assert decrypt_typo3(" ", 2) == " "
        assert decrypt_typo3("!", 2) == "!"


class TestDecryptCloudflareEmail:
    def test_known_example(self):
        # data-cfemail from bad-hindelang.de -> info@feratel.at
        result = decrypt_cloudflare_email("7f161119103f191a0d1e0b1a13511e0b")
        assert result == "info@feratel.at"

    def test_empty_after_key(self):
        result = decrypt_cloudflare_email("ff")
        assert result == ""


class TestExtractEmailDomains:
    def test_plain_email(self):
        html = "<p>Contact: info@example.ch</p>"
        domains = extract_email_domains(html, set())
        assert "example.ch" in domains

    def test_mailto_link(self):
        html = '<a href="mailto:contact@example.de">Email</a>'
        domains = extract_email_domains(html, set())
        assert "example.de" in domains

    def test_typo3_obfuscated(self):
        # Build a TYPO3-encrypted mailto link
        html = """<script>linkTo_UnCryptMailto('ocknvq,kphqBgzcorng0ej')</script>"""
        domains = extract_email_domains(html, set())
        assert isinstance(domains, set)

    def test_typo3_url_encoded(self):
        # boudry.ch pattern: %2C is URL-encoded comma, %27 is quote delimiter
        html = (
            '<a href="javascript:linkTo_UnCryptMailto('
            '%27kygjrm8amkkslc%2CzmsbpwYlc%2Caf%27);">Contact</a>'
        )
        domains = extract_email_domains(html, set())
        assert "ne.ch" in domains

    def test_at_obfuscation_round(self):
        html = "<p>info(at)example.ch</p>"
        domains = extract_email_domains(html, set())
        assert "example.ch" in domains

    def test_at_obfuscation_square(self):
        html = "<p>info[At]example.de</p>"
        domains = extract_email_domains(html, set())
        assert "example.de" in domains

    def test_skip_domains(self):
        html = "<p>info@example.ch user@gmail.com</p>"
        domains = extract_email_domains(html, {"gmail.com"})
        assert "example.ch" in domains
        assert "gmail.com" not in domains

    def test_asset_extension_filtered(self):
        html = '<img srcset="logo@2x.png 2x">'
        domains = extract_email_domains(html, set())
        assert len(domains) == 0

    def test_multiple_emails(self):
        html = "<p>a@one.ch b@two.ch c@one.ch</p>"
        domains = extract_email_domains(html, set())
        assert domains == {"one.ch", "two.ch"}

    def test_buildmail_js(self):
        html = '<span><script>buildMail("buildM_abc", "info", "chalais.ch","", "" , "");</script></span>'
        domains = extract_email_domains(html, set())
        assert "chalais.ch" in domains

    def test_html_entity_mailto(self):
        html = '<a href="mailto:&#105;n&#102;&#111;&#64;ard&#111;&#110;&#46;c&#104;">Contact</a>'
        domains = extract_email_domains(html, set())
        assert "ardon.ch" in domains

    def test_html_entity_plain_email(self):
        html = "<p>&#105;&#110;&#102;&#111;&#64;&#116;&#101;&#115;&#116;&#46;&#99;&#104;</p>"
        domains = extract_email_domains(html, set())
        assert "test.ch" in domains

    def test_data_email_link_base64(self):
        html = (
            '<a data-email-link="bWFpbHRvJTNBaW5mbyU0MDM3MTUlMkVjaA==" rel="nofollow">Contact</a>'
        )
        domains = extract_email_domains(html, set())
        assert "3715.ch" in domains

    def test_null_span_injection(self):
        html = (
            '<a href="#">info<span class="none" data-nosnippet="" '
            'aria-hidden="true">NULL</span>@example.ch</a>'
        )
        domains = extract_email_domains(html, set())
        assert "example.ch" in domains

    def test_nanmail_span_injection(self):
        html = (
            '<a class="nanmail">mail@<span class="d2024-02-14" '
            'style="display: none;">@@null</span>belmont.ch</a>'
        )
        domains = extract_email_domains(html, set())
        assert "belmont.ch" in domains

    def test_rot13_email(self):
        html = "<a href='#terssr$onibvf.pu' class='email'>greffe(at)bavois.ch</a>"
        domains = extract_email_domains(html, set())
        assert "bavois.ch" in domains

    def test_reserved_domain_filtered(self):
        html = "<p>info@yourcompany.example.com admin@test.example.net</p>"
        domains = extract_email_domains(html, set())
        assert "yourcompany.example.com" not in domains
        assert "test.example.net" not in domains

    def test_reserved_domain_exact_match(self):
        html = "<p>info@example.com admin@localhost</p>"
        domains = extract_email_domains(html, set())
        assert "example.com" not in domains

    def test_non_reserved_domain_kept(self):
        html = "<p>info@example.ch admin@myexample.com</p>"
        domains = extract_email_domains(html, set())
        assert "example.ch" in domains
        assert "myexample.com" in domains

    def test_invalid_tld_filtered(self):
        html = "<p>user@something.invalidtld123</p>"
        domains = extract_email_domains(html, set())
        assert len(domains) == 0

    def test_bad_base64_data_email_link(self):
        html = '<a data-email-link="not-valid-base64!!!">Contact</a>'
        domains = extract_email_domains(html, set())
        # Should not crash, just skip the bad data
        assert isinstance(domains, set)

    def test_cloudflare_email_protection(self):
        html = (
            '<a href="/cdn-cgi/l/email-protection" class="__cf_email__" '
            'data-cfemail="7f161119103f191a0d1e0b1a13511e0b">'
            "[email&#160;protected]</a>"
        )
        domains = extract_email_domains(html, set())
        assert "feratel.at" in domains

    def test_cloudflare_bad_hex(self):
        html = '<span data-cfemail="ZZZZ_not_hex"></span>'
        domains = extract_email_domains(html, set())
        assert isinstance(domains, set)

    def test_joomla_sp_form_id(self):
        import base64 as b64

        inner_email = b64.b64encode(b"gemeinde@t-online.de").decode()
        payload = json.dumps({"recipient_email": inner_email, "from": inner_email})
        b64_payload = b64.b64encode(payload.encode()).decode()
        form_value = b64_payload + ":abc123hmac"
        html = f'<input type="hidden" name="form_id" value="{form_value}">'
        domains = extract_email_domains(html, set())
        assert "t-online.de" in domains

    def test_joomla_sp_form_id_bad_data(self):
        html = '<input type="hidden" name="form_id" value="not-base64!!!:hmac">'
        domains = extract_email_domains(html, set())
        assert isinstance(domains, set)


class TestDiscoverContactLinks:
    def test_finds_contact_links(self):
        html = """
        <a href="/services/impressum.html/145">Impressum</a>
        <a href="/de/verwaltung/kontakt.html">Kontakt</a>
        <a href="/other/page">Other</a>
        <a href="https://www.example.ch/fr/contact">Contact FR</a>
        """
        paths = discover_contact_links(html, "example.ch")
        assert "/services/impressum.html/145" in paths
        assert "/de/verwaltung/kontakt.html" in paths
        assert "/fr/contact" in paths
        assert "/other/page" not in paths

    def test_skips_external_links(self):
        html = '<a href="https://other-site.ch/kontakt">External</a>'
        paths = discover_contact_links(html, "example.ch")
        assert paths == []

    def test_skips_asset_urls(self):
        html = '<a href="/kontakt/flyer.pdf">PDF</a>'
        paths = discover_contact_links(html, "example.ch")
        assert paths == []

    def test_skips_non_contact_links(self):
        html = '<a href="/news/2026/article">News</a> <a href="/events">Events</a>'
        paths = discover_contact_links(html, "example.ch")
        assert paths == []

    def test_relative_links(self):
        html = '<a href="/gemeinde/verwaltung/kontakt">Kontakt</a>'
        paths = discover_contact_links(html, "example.ch")
        assert "/gemeinde/verwaltung/kontakt" in paths

    def test_relative_no_leading_slash(self):
        html = '<a href="IT/Il-Comune-75442900">Comune</a>'
        paths = discover_contact_links(html, "ascona.ch")
        assert "/IT/Il-Comune-75442900" in paths

    def test_deduplicates(self):
        html = """
        <a href="/kontakt">Kontakt</a>
        <a href="/kontakt">Kontakt again</a>
        """
        paths = discover_contact_links(html, "example.ch")
        assert paths.count("/kontakt") == 1

    def test_skips_javascript_mailto_tel(self):
        html = """
        <a href="javascript:void(0)">JS</a>
        <a href="mailto:info@example.ch">Email</a>
        <a href="tel:+41000">Phone</a>
        <a href="#">Hash</a>
        """
        paths = discover_contact_links(html, "example.ch")
        assert paths == []

    def test_skips_non_http_scheme(self):
        html = '<a href="ftp://example.ch/kontakt">FTP Kontakt</a>'
        paths = discover_contact_links(html, "example.ch")
        assert paths == []

    def test_skips_root_path(self):
        html = '<a href="/">Home</a>'
        paths = discover_contact_links(html, "example.ch")
        assert paths == []

    def test_skips_redirect_email_link(self):
        html = '<a href="/redirectEmailLink/abc123">Email Redirect</a>'
        paths = discover_contact_links(html, "example.ch")
        assert paths == []

    def test_skips_percent_encoded_paths(self):
        html = '<a href="/kontakt/N%C3%A4her">Kontakt</a>'
        paths = discover_contact_links(html, "example.ch")
        assert paths == []


class TestBuildUrls:
    def test_basic(self):
        urls = build_urls("example.ch", ["/kontakt", "/impressum"])
        assert urls[0] == "https://www.example.ch/"
        assert "https://www.example.ch/kontakt" in urls
        assert "https://example.ch/" in urls
        assert "https://example.ch/impressum" in urls

    def test_strips_www(self):
        urls = build_urls("www.example.ch", [])
        assert urls[0] == "https://www.example.ch/"
        assert urls[1] == "https://example.ch/"

    def test_with_scheme(self):
        urls = build_urls("https://example.de", [])
        assert "https://www.example.de/" in urls


class TestBuildUrlsSingleBase:
    def test_use_www(self):
        urls = build_urls_single_base("example.ch", ["/kontakt", "/impressum"], use_www=True)
        assert urls == [
            "https://www.example.ch/",
            "https://www.example.ch/kontakt",
            "https://www.example.ch/impressum",
        ]

    def test_bare(self):
        urls = build_urls_single_base("example.ch", ["/kontakt"], use_www=False)
        assert urls == [
            "https://example.ch/",
            "https://example.ch/kontakt",
        ]

    def test_strips_www_prefix(self):
        urls = build_urls_single_base("www.example.ch", [], use_www=True)
        assert urls == ["https://www.example.ch/"]

    def test_empty_subpages(self):
        urls = build_urls_single_base("example.ch", [], use_www=False)
        assert urls == ["https://example.ch/"]


class TestDetectWebsiteMismatch:
    def test_match(self):
        assert detect_website_mismatch("Zürich", "zuerich.ch") is False

    def test_mismatch(self):
        assert detect_website_mismatch("Zürich", "totallyunrelated.ch") is True

    def test_prefix_match(self):
        assert detect_website_mismatch("Grindelwald", "gemeinde-grindelwald.ch") is False

    def test_empty(self):
        assert detect_website_mismatch("", "example.ch") is False
        assert detect_website_mismatch("Test", "") is False

    def test_canton_subdomain(self):
        assert detect_website_mismatch("Teufen", "teufen.ar.ch") is False

    def test_french_accents(self):
        assert detect_website_mismatch("Genève", "geneve.ch") is False

    def test_word_match(self):
        assert detect_website_mismatch("Bad Hindelang", "bad-hindelang.de") is False

    def test_word_match_reversed_order(self):
        # Slug "berngraben" not in "graben-bern.ch", but word "bern" is
        assert detect_website_mismatch("Bern Graben", "graben-bern.ch") is False

    def test_stripped_domain_match(self):
        # After stripping "gemeinde-", slug matches the stripped domain
        assert detect_website_mismatch("Test", "gemeinde-test.ch") is False

    def test_domain_base_first_match(self):
        # slug matches domain_base_first after canton subdomain split
        assert detect_website_mismatch("Teufen", "teufen.ar.ch") is False


class TestSlugifyName:
    def test_basic(self):
        slugs = _slugify_name("Zürich")
        assert "zuerich" in slugs
        assert "zürich" not in slugs  # should be slugified

    def test_french(self):
        slugs = _slugify_name("Genève")
        assert "geneve" in slugs

    def test_parenthetical_stripped(self):
        slugs = _slugify_name("Neustadt (Hessen)")
        assert any("neustadt" in s for s in slugs)
        assert not any("hessen" in s for s in slugs)


class TestLoadOverrides:
    def test_missing_file(self, tmp_path):
        result = load_overrides(tmp_path / "nonexistent.json")
        assert result == {}

    def test_valid_file(self, tmp_path):
        data = {"261": {"domain": "zuerich.ch", "reason": "test"}}
        path = tmp_path / "overrides.json"
        path.write_text(json.dumps(data))
        result = load_overrides(path)
        assert result["261"]["domain"] == "zuerich.ch"


class TestScrapeCache:
    def test_roundtrip(self, tmp_path):
        path = tmp_path / "cache.json"
        data = {
            "example.ch": ({"example.ch", "alt.ch"}, "redirect.ch", True),
            "other.de": (set(), None, False),
        }
        save_scrape_cache(path, data)
        loaded = load_scrape_cache(path, ttl_days=7)
        assert loaded["example.ch"][0] == {"example.ch", "alt.ch"}
        assert loaded["example.ch"][1] == "redirect.ch"
        assert loaded["example.ch"][2] is True
        assert loaded["other.de"][0] == set()

    def test_ttl_expiry(self, tmp_path):
        path = tmp_path / "cache.json"
        old_ts = (datetime.now(tz=timezone.utc) - timedelta(days=10)).isoformat()
        fresh_ts = datetime.now(tz=timezone.utc).isoformat()
        raw = {
            "old.ch": {"emails": [], "redirect": None, "accessible": True, "timestamp": old_ts},
            "fresh.ch": {
                "emails": ["a.ch"],
                "redirect": None,
                "accessible": True,
                "timestamp": fresh_ts,
            },
        }
        path.write_text(json.dumps(raw))
        loaded = load_scrape_cache(path, ttl_days=7)
        assert "old.ch" not in loaded
        assert "fresh.ch" in loaded

    def test_bad_timestamp(self, tmp_path):
        path = tmp_path / "cache.json"
        raw = {
            "bad.ch": {
                "emails": ["bad.ch"],
                "redirect": None,
                "accessible": True,
                "timestamp": "not-a-date",
            },
        }
        path.write_text(json.dumps(raw))
        loaded = load_scrape_cache(path, ttl_days=7)
        assert "bad.ch" in loaded  # bad timestamp is skipped, entry kept

    def test_missing_file(self, tmp_path):
        loaded = load_scrape_cache(tmp_path / "nope.json")
        assert loaded == {}

    def test_corrupt_file(self, tmp_path):
        path = tmp_path / "cache.json"
        path.write_text("not json{{{")
        loaded = load_scrape_cache(path)
        assert loaded == {}


class TestProcessScrapeResponse:
    def _make_response(self, url: str, text: str, status_code: int = 200) -> httpx.Response:
        return httpx.Response(
            status_code=status_code,
            text=text,
            headers={"content-type": "text/html"},
            request=httpx.Request("GET", url),
        )

    def test_redirect_blocklist_skips_email_extraction(self):
        r = self._make_response(
            "https://www.immoscout24.ch/search",
            "<p>info@immoscout24.ch</p>",
        )
        domains, redirect = _process_scrape_response(r, "cornol.ch", set(), None, set())
        assert redirect == "immoscout24.ch"
        assert "immoscout24.ch" not in domains

    def test_normal_redirect_extracts_emails(self):
        r = self._make_response(
            "https://www.labaroche.ch/",
            "<p>info@labaroche.ch</p>",
        )
        domains, redirect = _process_scrape_response(r, "baroche.ch", set(), None, set())
        assert redirect == "labaroche.ch"
        assert "labaroche.ch" in domains

    def test_no_redirect_extracts_emails(self):
        r = self._make_response(
            "https://www.example.ch/",
            "<p>info@example.ch</p>",
        )
        domains, redirect = _process_scrape_response(r, "example.ch", set(), None, set())
        assert redirect is None
        assert "example.ch" in domains

    def test_error_status_non_html_skips(self):
        r = httpx.Response(
            status_code=404,
            text="Not Found",
            headers={"content-type": "text/plain"},
            request=httpx.Request("GET", "https://www.test.ch/kontakt"),
        )
        domains, redirect = _process_scrape_response(r, "test.ch", set(), None, set())
        assert len(domains) == 0

    def test_error_status_html_extracts(self):
        r = self._make_response(
            "https://www.test.ch/kontakt",
            "<p>info@test.ch</p>",
            status_code=404,
        )
        domains, redirect = _process_scrape_response(r, "test.ch", set(), None, set())
        assert "test.ch" in domains

    def test_preserves_existing_redirect(self):
        r = self._make_response(
            "https://www.other.ch/",
            "<p>info@other.ch</p>",
        )
        domains, redirect = _process_scrape_response(r, "test.ch", set(), "other.ch", set())
        assert redirect == "other.ch"
        assert "other.ch" in domains


class TestIsSSLError:
    def test_ssl_cert_error(self):
        exc = ssl.SSLCertVerificationError("certificate verify failed")
        assert _is_ssl_error(exc) is True

    def test_chained_ssl_error(self):
        inner = ssl.SSLCertVerificationError("certificate verify failed")
        outer = httpx.ConnectError("connect failed")
        outer.__cause__ = inner
        assert _is_ssl_error(outer) is True

    def test_certificate_verify_failed_string(self):
        exc = Exception("CERTIFICATE_VERIFY_FAILED")
        assert _is_ssl_error(exc) is True

    def test_non_ssl_error(self):
        exc = ConnectionError("timeout")
        assert _is_ssl_error(exc) is False

    def test_no_cause_chain(self):
        exc = ValueError("unrelated")
        assert _is_ssl_error(exc) is False
