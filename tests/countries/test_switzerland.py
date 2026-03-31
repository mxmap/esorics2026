"""Tests for Switzerland country configuration."""

from municipality_email.countries.switzerland import (
    CANTON_ABBREVIATIONS,
    SKIP_DOMAINS_CH,
    SwitzerlandConfig,
)


class TestSwitzerlandConfig:
    def setup_method(self):
        self.config = SwitzerlandConfig()

    def test_country(self):
        assert self.config.country == "ch"
        assert self.config.concurrency == 30

    def test_guess_domains_basic(self):
        domains = self.config.guess_domains("Zürich", "Kanton Zürich")
        assert "zuerich.ch" in domains
        assert "gemeinde-zuerich.ch" in domains
        assert "stadt-zuerich.ch" in domains
        assert "zuerich.zh.ch" in domains

    def test_guess_domains_french(self):
        domains = self.config.guess_domains("Genève", "Kanton Genf")
        assert "geneve.ch" in domains
        assert "geneve.ge.ch" in domains

    def test_guess_domains_italian(self):
        domains = self.config.guess_domains("Lugano", "Kanton Tessin")
        assert "lugano.ch" in domains
        assert "comune-di-lugano.ch" in domains
        assert "lugano.ti.ch" in domains

    def test_guess_domains_compound(self):
        domains = self.config.guess_domains("Rüti bei Lyssach", "Kanton Bern")
        # Should include joined variant
        assert any("ruetibeilyssach" in d for d in domains)

    def test_guess_domains_slash(self):
        domains = self.config.guess_domains("Köniz/Koenigsberg", "Kanton Bern")
        assert any("koeniz" in d for d in domains)
        assert any("koenigsberg" in d for d in domains)

    def test_slugify_name_umlauts(self):
        slugs = self.config.slugify_name("Zürich")
        assert "zuerich" in slugs

    def test_slugify_name_french(self):
        slugs = self.config.slugify_name("Genève")
        assert "geneve" in slugs

    def test_slugify_name_parenthetical(self):
        slugs = self.config.slugify_name("Wald (ZH)")
        assert any("wald" in s for s in slugs)
        assert not any("zh" in s for s in slugs)

    def test_domain_matches_name_basic(self):
        assert self.config.domain_matches_name("Zürich", "zuerich.ch") is True
        assert self.config.domain_matches_name("Zürich", "totallyother.ch") is False

    def test_domain_matches_name_prefix(self):
        assert self.config.domain_matches_name("Grindelwald", "gemeinde-grindelwald.ch") is True

    def test_domain_matches_name_canton_subdomain(self):
        assert self.config.domain_matches_name("Teufen", "teufen.ar.ch") is True


class TestConstants:
    def test_canton_abbreviations_complete(self):
        assert len(CANTON_ABBREVIATIONS) == 26

    def test_skip_domains_base(self):
        assert "gmail.com" in SKIP_DOMAINS_CH
        assert "bluewin.ch" in SKIP_DOMAINS_CH
        assert "gmx.ch" in SKIP_DOMAINS_CH
