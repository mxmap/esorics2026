"""Tests for email domain filtering layers."""

from municipality_email.countries.austria import AustriaConfig
from municipality_email.countries.switzerland import SwitzerlandConfig
from municipality_email.filtering import (
    build_frequency_blocklist,
    filter_scraped_pool,
    is_valid_tld,
    score_domain_relevance,
)
from municipality_email.schemas import Country, MunicipalityRecord


def _make_record(**kwargs) -> MunicipalityRecord:
    defaults = dict(code="001", name="Test", region="", country=Country.CH)
    defaults.update(kwargs)
    return MunicipalityRecord(**defaults)


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
        # 0.5% of 1000 = 5, shared.ch appears in all 1000
        blocklist = build_frequency_blocklist(records, threshold_pct=0.005, threshold_floor=5)
        assert "shared.ch" in blocklist


# ── Layer 3: Relevance Scoring ─────────────────────────────────────


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
        score = score_domain_relevance(
            "eisenstadt.gv.at", "Eisenstadt", self.at_config, set()
        )
        assert score == 1.0

    def test_partial_substring_match(self):
        score = score_domain_relevance(
            "schulen-aarberg.ch", "Aarberg", self.ch_config, set()
        )
        assert score >= 0.8

    def test_static_candidate(self):
        score = score_domain_relevance(
            "unrelated.ch", "Aarberg", self.ch_config, {"unrelated.ch"}
        )
        assert score == 0.4

    def test_country_tld_only(self):
        score = score_domain_relevance("garage-cuenot.ch", "Aarberg", self.ch_config, set())
        assert score == 0.2

    def test_foreign_tld(self):
        score = score_domain_relevance("garage-cuenot.de", "Aarberg", self.ch_config, set())
        assert score == 0.0

    def test_no_affinity(self):
        score = score_domain_relevance("randomsite.com", "Aarberg", self.ch_config, set())
        assert score == 0.0


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
        assert "bern.ch" in result  # exempt: in candidates
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
        assert "aarberg.ch" in result  # exempt: name match

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

    def test_relevance_prune_when_many(self):
        # >3 domains triggers relevance scoring; requires score >= 0.4 (name match or candidate)
        pool = {"aarberg.ch", "noise.ch", "other.ch", "junk.ch"}
        result = filter_scraped_pool(
            pool,
            municipality_name="Aarberg",
            config=self.config,
            frequency_blocklist=set(),
            candidate_domains=set(),
        )
        assert "aarberg.ch" in result  # score 1.0: name match
        assert "noise.ch" not in result  # score 0.2: only correct TLD
        assert "other.ch" not in result
        assert "junk.ch" not in result

    def test_small_pool_still_pruned(self):
        # Even with 2 domains, irrelevant ones are removed
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

    def test_fallback_when_all_score_low(self):
        # >3 domains all scoring below 0.4 -> fallback keeps them all
        pool = {"a.ch", "b.ch", "c.ch", "d.ch"}
        result = filter_scraped_pool(
            pool,
            municipality_name="Aarberg",
            config=self.config,
            frequency_blocklist=set(),
            candidate_domains=set(),
        )
        assert result == pool

    def test_empty_pool(self):
        result = filter_scraped_pool(
            set(),
            municipality_name="Aarberg",
            config=self.config,
            frequency_blocklist=set(),
            candidate_domains=set(),
        )
        assert result == set()


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
