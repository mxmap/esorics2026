"""Tests for Germany country configuration."""

from municipality_email.countries.germany import (
    BUNDESLAND_BY_PREFIX,
    SKIP_DOMAINS_DE,
    GermanyConfig,
    ags_to_bundesland,
)


class TestGermanyConfig:
    def setup_method(self):
        self.config = GermanyConfig()

    def test_country(self):
        assert self.config.country == "de"
        assert self.config.concurrency == 50

    def test_guess_domains_basic(self):
        domains = self.config.guess_domains("Flensburg", "Schleswig-Holstein")
        assert "flensburg.de" in domains
        assert "stadt-flensburg.de" in domains
        assert "gemeinde-flensburg.de" in domains
        assert "markt-flensburg.de" in domains
        assert "vg-flensburg.de" in domains
        assert "samtgemeinde-flensburg.de" in domains
        assert "amt-flensburg.de" in domains
        assert "flensburg-online.de" in domains
        assert "flensburg-info.de" in domains

    def test_guess_domains_umlaut(self):
        domains = self.config.guess_domains("Münsingen", "Baden-Württemberg")
        assert "muensingen.de" in domains

    def test_guess_domains_eszett(self):
        domains = self.config.guess_domains("Straßberg", "Sachsen-Anhalt")
        assert "strassberg.de" in domains

    def test_guess_domains_compound(self):
        domains = self.config.guess_domains("Bad Hindelang", "Bayern")
        assert "bad-hindelang.de" in domains
        assert "badhindelang.de" in domains

    def test_guess_domains_parenthetical(self):
        domains = self.config.guess_domains("Neustadt (Hessen)", "Hessen")
        assert "neustadt.de" in domains

    def test_slugify_name(self):
        slugs = self.config.slugify_name("Neustadt (Hessen)")
        assert "neustadt" in slugs

    def test_slugify_name_eszett(self):
        slugs = self.config.slugify_name("Großbeeren")
        assert "grossbeeren" in slugs

    def test_domain_matches_name(self):
        assert self.config.domain_matches_name("Flensburg", "flensburg.de") is True
        assert self.config.domain_matches_name("Flensburg", "stadt-flensburg.de") is True
        assert self.config.domain_matches_name("Flensburg", "vg-flensburg.de") is True
        assert self.config.domain_matches_name("Flensburg", "samtgemeinde-flensburg.de") is True
        assert self.config.domain_matches_name("Flensburg", "amt-flensburg.de") is True
        assert self.config.domain_matches_name("Flensburg", "random.de") is False

    def test_domain_matches_empty(self):
        assert self.config.domain_matches_name("", "flensburg.de") is False
        assert self.config.domain_matches_name("Flensburg", "") is False


class TestConstants:
    def test_bundesland_mapping(self):
        assert len(BUNDESLAND_BY_PREFIX) == 16
        assert ags_to_bundesland("01001000") == "Schleswig-Holstein"
        assert ags_to_bundesland("09000000") == "Bayern"

    def test_skip_domains(self):
        assert "web.de" in SKIP_DOMAINS_DE
        assert "gmx.de" in SKIP_DOMAINS_DE
        assert "t-online.de" in SKIP_DOMAINS_DE
        assert "hirsch-woelfl.de" in SKIP_DOMAINS_DE
