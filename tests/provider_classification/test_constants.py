from mail_municipalities.provider_classification.constants import (
    CANTON_ABBREVIATIONS,
    CANTON_SHORT_TO_FULL,
)


def test_canton_abbreviations_count():
    assert len(CANTON_ABBREVIATIONS) == 26


def test_canton_abbreviations_keys():
    assert "Kanton Zürich" in CANTON_ABBREVIATIONS
    assert CANTON_ABBREVIATIONS["Kanton Zürich"] == "zh"


def test_canton_short_to_full_inverse():
    for full, short in CANTON_ABBREVIATIONS.items():
        assert CANTON_SHORT_TO_FULL[short] == full
