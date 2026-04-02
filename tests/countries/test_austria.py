"""Tests for Austria country configuration."""

from municipality_email.countries.austria import (
    BUNDESLAND_BY_PREFIX_AT,
    REGIONAL_DOMAIN_SUFFIXES_AT,
    SKIP_DOMAINS_AT,
    AustriaConfig,
    gkz_to_bundesland,
)


class TestAustriaConfig:
    def setup_method(self):
        self.config = AustriaConfig()

    def test_country(self):
        assert self.config.country == "at"
        assert self.config.concurrency == 50

    def test_guess_domains_basic(self):
        domains = self.config.guess_domains("Eisenstadt", "Burgenland")
        assert "eisenstadt.at" in domains
        assert "eisenstadt.gv.at" in domains
        assert "gemeinde-eisenstadt.at" in domains
        assert "stadt-eisenstadt.at" in domains
        assert "marktgemeinde-eisenstadt.at" in domains
        assert "stadtgemeinde-eisenstadt.at" in domains
        assert "eisenstadt.or.at" in domains
        assert "eisenstadt-online.at" in domains
        assert "eisenstadt-info.at" in domains

    def test_guess_domains_regional(self):
        domains = self.config.guess_domains("Eisenstadt", "Burgenland")
        assert "eisenstadt.bgld.gv.at" in domains

    def test_guess_domains_kaernten(self):
        domains = self.config.guess_domains("Klagenfurt", "Kärnten")
        assert "klagenfurt.ktn.gde.at" in domains

    def test_guess_domains_regional_noe(self):
        domains = self.config.guess_domains("Korneuburg", "Niederösterreich")
        assert "korneuburg.noe.gv.at" in domains

    def test_guess_domains_regional_stmk(self):
        domains = self.config.guess_domains("Graz", "Steiermark")
        assert "graz.stmk.gv.at" in domains

    def test_guess_domains_umlaut(self):
        domains = self.config.guess_domains("Wörgl", "Tirol")
        assert "woergl.at" in domains
        assert "woergl.gv.at" in domains

    def test_slugify_name(self):
        slugs = self.config.slugify_name("Eisenstadt")
        assert "eisenstadt" in slugs

    def test_slugify_name_umlaut(self):
        slugs = self.config.slugify_name("Wörgl")
        assert "woergl" in slugs

    def test_domain_matches_name_basic(self):
        assert self.config.domain_matches_name("Eisenstadt", "eisenstadt.gv.at") is True
        assert self.config.domain_matches_name("Eisenstadt", "eisenstadt.at") is True
        assert self.config.domain_matches_name("Eisenstadt", "random.at") is False

    def test_domain_matches_name_prefix(self):
        assert self.config.domain_matches_name("Eisenstadt", "stadt-eisenstadt.at") is True
        assert self.config.domain_matches_name("Hallein", "marktgemeinde-hallein.at") is True

    def test_domain_matches_name_compound_tld(self):
        assert self.config.domain_matches_name("Klagenfurt", "klagenfurt.gv.at") is True
        assert self.config.domain_matches_name("Villach", "villach.gde.at") is True

    def test_slugify_strips_prepositions(self):
        slugs = self.config.slugify_name("Neufeld an der Leitha")
        assert "neufeld-leitha" in slugs
        assert "neufeld" in slugs

        slugs = self.config.slugify_name("Neuberg im Burgenland")
        assert "neuberg-burgenland" in slugs
        assert "neuberg" in slugs

        slugs = self.config.slugify_name("Neustift bei Güssing")
        assert "neustift-guessing" in slugs

    def test_slugify_sankt_abbreviation(self):
        slugs = self.config.slugify_name("Sankt Michael im Burgenland")
        assert "st-michael" in slugs
        assert "st-michael-burgenland" in slugs

        slugs = self.config.slugify_name("Groß Sankt Florian")
        assert "gross-st-florian" in slugs

    def test_domain_matches_name_prepositions(self):
        assert self.config.domain_matches_name(
            "Neufeld an der Leitha", "neufeld-leitha.bgld.gv.at"
        )
        assert self.config.domain_matches_name("Neuberg im Burgenland", "neuberg.bgld.gv.at")
        assert self.config.domain_matches_name(
            "Oggau am Neusiedler See", "oggau-neusiedler-see.bgld.gv.at"
        )
        assert self.config.domain_matches_name("Weiden bei Rechnitz", "weiden-rechnitz.at")

    def test_pick_best_email_gov_preference(self):
        emails = {"eisenstadt.at", "eisenstadt.gv.at", "other.at"}
        result = self.config.pick_best_email(emails, "Eisenstadt", set())
        assert result[0] == "eisenstadt.gv.at"

    def test_pick_best_email_gov_name_first(self):
        emails = {"random.gv.at", "eisenstadt.gv.at"}
        result = self.config.pick_best_email(emails, "Eisenstadt", set())
        assert result[0] == "eisenstadt.gv.at"

    def test_regional_suffixes(self):
        assert "bgld.gv.at" in self.config.regional_suffixes("Burgenland")
        assert "ktn.gde.at" in self.config.regional_suffixes("Kärnten")
        assert self.config.regional_suffixes("Unknown") == []


class TestConstants:
    def test_bundesland_mapping(self):
        assert len(BUNDESLAND_BY_PREFIX_AT) == 9
        assert gkz_to_bundesland("10101") == "Burgenland"
        assert gkz_to_bundesland("90001") == "Wien"

    def test_regional_suffixes(self):
        assert "bgld.gv.at" in REGIONAL_DOMAIN_SUFFIXES_AT["1"]
        assert "ktn.gde.at" in REGIONAL_DOMAIN_SUFFIXES_AT["2"]
        assert "noe.gv.at" in REGIONAL_DOMAIN_SUFFIXES_AT["3"]
        assert "ooe.gv.at" in REGIONAL_DOMAIN_SUFFIXES_AT["4"]
        assert "salzburg.gv.at" in REGIONAL_DOMAIN_SUFFIXES_AT["5"]
        assert "stmk.gv.at" in REGIONAL_DOMAIN_SUFFIXES_AT["6"]
        assert "tirol.gv.at" in REGIONAL_DOMAIN_SUFFIXES_AT["7"]
        assert "vlbg.gv.at" in REGIONAL_DOMAIN_SUFFIXES_AT["8"]
        assert "wien.gv.at" in REGIONAL_DOMAIN_SUFFIXES_AT["9"]
        assert len(REGIONAL_DOMAIN_SUFFIXES_AT) == 9

    def test_skip_domains(self):
        assert "aon.at" in SKIP_DOMAINS_AT
        assert "riskommunal.net" in SKIP_DOMAINS_AT
        assert "gem2go.at" in SKIP_DOMAINS_AT
