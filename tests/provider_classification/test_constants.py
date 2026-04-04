from mail_municipalities.provider_classification.constants import (
    AT_STATE_ABBREVIATIONS,
    CANTON_ABBREVIATIONS,
    CANTON_SHORT_TO_FULL,
    DE_STATE_ABBREVIATIONS,
    REGION_ABBREVIATIONS,
)


def test_canton_abbreviations_count():
    assert len(CANTON_ABBREVIATIONS) == 26


def test_canton_abbreviations_keys():
    assert "Kanton Zürich" in CANTON_ABBREVIATIONS
    assert CANTON_ABBREVIATIONS["Kanton Zürich"] == "zh"


def test_canton_short_to_full_inverse():
    for full, short in CANTON_ABBREVIATIONS.items():
        assert CANTON_SHORT_TO_FULL[short] == full


def test_de_state_abbreviations_count():
    assert len(DE_STATE_ABBREVIATIONS) == 16


def test_de_state_abbreviations_keys():
    assert "Bayern" in DE_STATE_ABBREVIATIONS
    assert DE_STATE_ABBREVIATIONS["Bayern"] == "by"


def test_at_state_abbreviations_count():
    assert len(AT_STATE_ABBREVIATIONS) == 9


def test_at_state_abbreviations_keys():
    assert "Wien" in AT_STATE_ABBREVIATIONS
    assert AT_STATE_ABBREVIATIONS["Wien"] == "w"


def test_region_abbreviations_lookup():
    assert "ch" in REGION_ABBREVIATIONS
    assert "de" in REGION_ABBREVIATIONS
    assert "at" in REGION_ABBREVIATIONS
    assert REGION_ABBREVIATIONS["ch"] is CANTON_ABBREVIATIONS
    assert REGION_ABBREVIATIONS["de"] is DE_STATE_ABBREVIATIONS
    assert REGION_ABBREVIATIONS["at"] is AT_STATE_ABBREVIATIONS
