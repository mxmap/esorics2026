"""Tests for email domain filtering layers."""

from municipality_email.countries.austria import AustriaConfig
from municipality_email.countries.switzerland import SwitzerlandConfig
from municipality_email.filtering import (
    _is_municipality_domain,
    build_frequency_blocklist,
    filter_scraped_pool,
    is_valid_tld,
    score_domain_relevance,
)
from municipality_email.schemas import Country, MunicipalityRecord


def _make_record(**kwargs) -> MunicipalityRecord:
    defaults = dict(code="001", name="Test", region="", country=Country.CH)
    defaults.update(kwargs)
    return MunicipalityRecord(**defaults)  # type: ignore[arg-type]


# ── Layer 1: TLD Validation ────────────────────────────────────────


class TestIsValidTld:
    def test_valid_ch(self):
        assert is_valid_tld("example.ch") is True

    def test_valid_de(self):
        assert is_valid_tld("gemeinde.de") is True

    def test_valid_gv_at(self):
        assert is_valid_tld("herisau.gv.at") is True

    def test_valid_cantonal(self):
        assert is_valid_tld("herisau.ar.ch") is True

    def test_garbled_no_tld(self):
        assert is_valid_tld("8fth2pcv7bpaxijuchoaqn.zmdjvyleierm") is False

    def test_garbled_short(self):
        assert is_valid_tld("2.wq") is False

    def test_garbled_random(self):
        assert is_valid_tld("c3ku.cuxw") is False

    def test_garbled_multi_dot(self):
        assert is_valid_tld("ss3ht.k.lmb") is False


# ── Layer 2: Frequency Blocklist ───────────────────────────────────


class TestBuildFrequencyBlocklist:
    def test_frequent_domain_blocked(self):
        records = [
            _make_record(
                code=str(i),
                name=f"Muni{i}",
                scraped_emails={f"muni{i}.ch": ["common-service.ch"]},
            )
            for i in range(10)
        ]
        blocklist = build_frequency_blocklist(records, threshold_floor=5)
        assert "common-service.ch" in blocklist

    def test_rare_domain_not_blocked(self):
        records = [
            _make_record(
                code="1",
                name="Aarberg",
                scraped_emails={"aarberg.ch": ["aarberg.ch", "rare-local.ch"]},
            ),
            _make_record(
                code="2",
                name="Bern",
                scraped_emails={"bern.ch": ["bern.ch"]},
            ),
        ]
        blocklist = build_frequency_blocklist(records, threshold_floor=5)
        assert "rare-local.ch" not in blocklist
        assert "aarberg.ch" not in blocklist

    def test_empty_records(self):
        blocklist = build_frequency_blocklist([])
        assert blocklist == set()

    def test_threshold_scales_with_count(self):
        records = [
            _make_record(
                code=str(i),
                name=f"Muni{i}",
                scraped_emails={f"muni{i}.ch": ["shared.ch"]},
            )
            for i in range(1000)
        ]
        blocklist = build_frequency_blocklist(records, threshold_pct=0.005, threshold_floor=5)
        assert "shared.ch" in blocklist


# ── Layer 3: Strict Municipality Domain Check ──────────────────────


class TestIsMunicipalityDomain:
    def setup_method(self):
        self.ch = SwitzerlandConfig()
        self.at = AustriaConfig()

    def test_direct_match(self):
        assert _is_municipality_domain("baden.ch", "Baden", self.ch) is True

    def test_gemeinde_prefix(self):
        assert _is_municipality_domain("gemeinde-baden.ch", "Baden", self.ch) is True

    def test_stadt_prefix(self):
        assert _is_municipality_domain("stadt-baden.ch", "Baden", self.ch) is True

    def test_cantonal_subdomain(self):
        assert _is_municipality_domain("herisau.ar.ch", "Herisau", self.ch) is True

    def test_cantonal_subdomain_baden(self):
        assert _is_municipality_domain("baden.ag.ch", "Baden", self.ch) is True

    def test_austrian_gv_at(self):
        assert _is_municipality_domain("eisenstadt.gv.at", "Eisenstadt", self.at) is True

    def test_rejects_feuerwehr(self):
        assert _is_municipality_domain("feuerwehr-baden.ch", "Baden", self.ch) is False

    def test_rejects_schule(self):
        assert _is_municipality_domain("schule-baden.ch", "Baden", self.ch) is False

    def test_rejects_unrelated(self):
        assert _is_municipality_domain("apothekedrkunz.ch", "Baden", self.ch) is False

    def test_rejects_no_match(self):
        assert _is_municipality_domain("reichlinzuegeln.ch", "Sisikon", self.ch) is False

    def test_stadt_prefix_unhyphenated(self):
        assert _is_municipality_domain("stadtsursee.ch", "Sursee", self.ch) is True

    def test_gemeinde_prefix_unhyphenated(self):
        assert _is_municipality_domain("gemeindesursee.ch", "Sursee", self.ch) is True

    def test_rejects_feuerwehr_unhyphenated(self):
        assert _is_municipality_domain("feuerwehrbaden.ch", "Baden", self.ch) is False

    def test_joined_multiword_name(self):
        assert _is_municipality_domain("uetikonamsee.ch", "Uetikon am See", self.ch) is True

    def test_empty_name(self):
        assert _is_municipality_domain("test.ch", "", self.ch) is False


class TestScoreDomainRelevance:
    def setup_method(self):
        self.ch_config = SwitzerlandConfig()
        self.at_config = AustriaConfig()

    def test_exact_name_match(self):
        score = score_domain_relevance("aarberg.ch", "Aarberg", self.ch_config, set())
        assert score == 1.0

    def test_cantonal_domain_match(self):
        score = score_domain_relevance("herisau.ar.ch", "Herisau", self.ch_config, set())
        assert score == 1.0

    def test_prefixed_match(self):
        score = score_domain_relevance("gemeinde-aarberg.ch", "Aarberg", self.ch_config, set())
        assert score == 1.0

    def test_austrian_gv_at_match(self):
        score = score_domain_relevance("eisenstadt.gv.at", "Eisenstadt", self.at_config, set())
        assert score == 1.0

    def test_feuerwehr_rejected(self):
        score = score_domain_relevance("feuerwehr-baden.ch", "Baden", self.ch_config, set())
        assert score == 0.0

    def test_schule_rejected(self):
        score = score_domain_relevance("schulen-aarberg.ch", "Aarberg", self.ch_config, set())
        assert score == 0.0

    def test_static_candidate(self):
        score = score_domain_relevance("unrelated.ch", "Aarberg", self.ch_config, {"unrelated.ch"})
        assert score == 0.4

    def test_correct_tld_no_match(self):
        score = score_domain_relevance("garage-cuenot.ch", "Aarberg", self.ch_config, set())
        assert score == 0.0

    def test_no_affinity(self):
        score = score_domain_relevance("randomsite.com", "Aarberg", self.ch_config, set())
        assert score == 0.0

    def test_foreign_tld_still_scores_if_not_filtered(self):
        """Name match with foreign TLD scores 1.0 (filtering happens in filter_scraped_pool)."""
        score = score_domain_relevance("ipsach.be", "Ipsach", self.ch_config, set())
        assert score == 1.0

    def test_joined_multiword_name(self):
        score = score_domain_relevance("uetikonamsee.ch", "Uetikon am See", self.ch_config, set())
        assert score == 1.0

    def test_cantonal_domain_correct_region(self):
        score = score_domain_relevance(
            "nw.ch", "Oberdorf", self.ch_config, set(), region="Kanton Nidwalden"
        )
        assert score == 0.5

    def test_cantonal_domain_wrong_region(self):
        score = score_domain_relevance(
            "nw.ch", "Oberdorf", self.ch_config, set(), region="Kanton Zürich"
        )
        assert score == 0.0

    def test_cantonal_domain_no_region(self):
        score = score_domain_relevance("nw.ch", "Oberdorf", self.ch_config, set())
        assert score == 0.0

    def test_ne_ch_for_neuchatel(self):
        score = score_domain_relevance(
            "ne.ch", "Boudry", self.ch_config, set(), region="Kanton Neuenburg"
        )
        assert score == 0.5


# ── Orchestrator: filter_scraped_pool ──────────────────────────────


class TestFilterScrapedPool:
    def setup_method(self):
        self.config = SwitzerlandConfig()

    def test_frequency_blocked_removed(self):
        pool = {"aarberg.ch", "common-service.ch"}
        result = filter_scraped_pool(
            pool,
            municipality_name="Aarberg",
            config=self.config,
            frequency_blocklist={"common-service.ch"},
            candidate_domains=set(),
        )
        assert "aarberg.ch" in result
        assert "common-service.ch" not in result

    def test_candidate_exempt_from_frequency(self):
        pool = {"bern.ch", "common.ch"}
        result = filter_scraped_pool(
            pool,
            municipality_name="Bern",
            config=self.config,
            frequency_blocklist={"bern.ch", "common.ch"},
            candidate_domains={"bern.ch"},
        )
        assert "bern.ch" in result
        assert "common.ch" not in result

    def test_name_match_exempt_from_frequency(self):
        pool = {"aarberg.ch"}
        result = filter_scraped_pool(
            pool,
            municipality_name="Aarberg",
            config=self.config,
            frequency_blocklist={"aarberg.ch"},
            candidate_domains=set(),
        )
        assert "aarberg.ch" in result

    def test_cantonal_domain_kept(self):
        pool = {"herisau.ch", "herisau.ar.ch", "noise1.ch", "noise2.ch", "noise3.ch"}
        result = filter_scraped_pool(
            pool,
            municipality_name="Herisau",
            config=self.config,
            frequency_blocklist=set(),
            candidate_domains=set(),
        )
        assert "herisau.ch" in result
        assert "herisau.ar.ch" in result
        assert "noise1.ch" not in result

    def test_irrelevant_domains_removed(self):
        pool = {"baden.ch", "feuerwehr-baden.ch", "schule-baden.ch", "apothekedrkunz.ch"}
        result = filter_scraped_pool(
            pool,
            municipality_name="Baden",
            config=self.config,
            frequency_blocklist=set(),
            candidate_domains=set(),
        )
        assert result == {"baden.ch"}

    def test_small_pool_still_pruned(self):
        pool = {"sisikon.ch", "reichlinzuegeln.ch"}
        result = filter_scraped_pool(
            pool,
            municipality_name="Sisikon",
            config=self.config,
            frequency_blocklist=set(),
            candidate_domains=set(),
        )
        assert "sisikon.ch" in result
        assert "reichlinzuegeln.ch" not in result

    def test_no_match_returns_empty(self):
        # No name match -> empty pool -> decide phase falls to static/guess
        pool = {"apothekedrkunz.ch", "randomshop.ch"}
        result = filter_scraped_pool(
            pool,
            municipality_name="Baden",
            config=self.config,
            frequency_blocklist=set(),
            candidate_domains=set(),
        )
        assert result == set()

    def test_empty_pool(self):
        result = filter_scraped_pool(
            set(),
            municipality_name="Aarberg",
            config=self.config,
            frequency_blocklist=set(),
            candidate_domains=set(),
        )
        assert result == set()

    def test_cantonal_domain_kept_with_region(self):
        pool = {"nw.ch", "schule-oberdorf.ch"}
        result = filter_scraped_pool(
            pool,
            municipality_name="Oberdorf",
            config=self.config,
            frequency_blocklist=set(),
            candidate_domains=set(),
            region="Kanton Nidwalden",
        )
        assert "nw.ch" in result
        assert "schule-oberdorf.ch" not in result

    def test_cantonal_domain_exempt_from_frequency(self):
        pool = {"nw.ch", "oberdorf-nw.ch"}
        result = filter_scraped_pool(
            pool,
            municipality_name="Oberdorf",
            config=self.config,
            frequency_blocklist={"nw.ch"},
            candidate_domains=set(),
            region="Kanton Nidwalden",
        )
        assert "nw.ch" in result

    def test_foreign_tld_dropped(self):
        """Foreign ccTLD like .be is dropped for Swiss municipalities."""
        pool = {"ipsach.ch", "ipsach.be"}
        result = filter_scraped_pool(
            pool,
            municipality_name="Ipsach",
            config=self.config,
            frequency_blocklist=set(),
            candidate_domains=set(),
        )
        assert "ipsach.ch" in result
        assert "ipsach.be" not in result

    def test_generic_tld_kept(self):
        """Generic TLDs like .org are kept."""
        pool = {"uitikon.org", "uitikon.ch"}
        result = filter_scraped_pool(
            pool,
            municipality_name="Uitikon",
            config=self.config,
            frequency_blocklist=set(),
            candidate_domains=set(),
        )
        assert "uitikon.org" in result
        assert "uitikon.ch" in result

    def test_cantonal_domain_rejected_wrong_region(self):
        pool = {"nw.ch"}
        result = filter_scraped_pool(
            pool,
            municipality_name="Zürich",
            config=self.config,
            frequency_blocklist=set(),
            candidate_domains=set(),
            region="Kanton Zürich",
        )
        assert "nw.ch" not in result


# ── Swiss regional_suffixes ────────────────────────────────────────


class TestSwissRegionalSuffixes:
    def test_known_canton(self):
        config = SwitzerlandConfig()
        assert config.regional_suffixes("Kanton Appenzell Ausserrhoden") == ["ar.ch"]

    def test_bern(self):
        config = SwitzerlandConfig()
        assert config.regional_suffixes("Kanton Bern") == ["be.ch"]

    def test_unknown_region(self):
        config = SwitzerlandConfig()
        assert config.regional_suffixes("Unknown") == []
