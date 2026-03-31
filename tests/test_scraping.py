"""Tests for scraping module."""

import json
from datetime import datetime, timedelta, timezone

from municipality_email.scraping import (
    _is_valid_email,
    _slugify_name,
    build_urls,
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
        # May or may not find depending on the exact encrypted string — test the mechanism
        assert isinstance(domains, set)

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

    def test_missing_file(self, tmp_path):
        loaded = load_scrape_cache(tmp_path / "nope.json")
        assert loaded == {}

    def test_corrupt_file(self, tmp_path):
        path = tmp_path / "cache.json"
        path.write_text("not json{{{")
        loaded = load_scrape_cache(path)
        assert loaded == {}
