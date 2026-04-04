"""Validate real provider classification output files.

Reuses the structural validation from validate.py — no duplicate checks.
"""

import json
from pathlib import Path

import pytest

from mail_municipalities.provider_classification.validate import validate_structure

PROVIDERS_DIR = Path(__file__).resolve().parent.parent.parent / "output" / "providers"


def _load(cc: str, suffix: str = "") -> dict | None:
    path = PROVIDERS_DIR / f"providers_{cc}{suffix}.json"
    if not path.exists():
        return None
    return json.loads(path.read_text())


class TestRealProviderOutputFiles:
    """Validate actual classification output files in output/providers/.

    Skipped when the corresponding file does not exist.
    """

    @pytest.mark.parametrize("cc", ["ch", "de", "at"])
    def test_structural_validation(self, cc):
        data = _load(cc)
        if data is None:
            pytest.skip(f"providers_{cc}.json not found")

        result = validate_structure(data)
        assert result.success, (
            f"[{cc}] structural validation failed:\n"
            + "\n".join(f"  - {e}" for e in result.errors)
        )

    @pytest.mark.parametrize("cc", ["ch", "de", "at"])
    def test_minified_consistent_with_full(self, cc):
        full = _load(cc)
        mini = _load(cc, ".min")
        if full is None or mini is None:
            pytest.skip(f"providers_{cc}.json or .min.json not found")

        assert mini["generated"] == full["generated"]

        full_codes = {m["code"] for m in full["municipalities"]}
        mini_codes = {m["code"] for m in mini["municipalities"]}
        assert full_codes == mini_codes, (
            f"[{cc}] code sets differ between full and minified"
        )

        full_by_code = {m["code"]: m for m in full["municipalities"]}
        mini_by_code = {m["code"]: m for m in mini["municipalities"]}
        for code in full_codes:
            f_entry = full_by_code[code]
            m_entry = mini_by_code[code]

            for field in ("name", "domain", "region", "provider", "category",
                          "classification_confidence", "mx", "spf"):
                assert f_entry[field] == m_entry[field], (
                    f"[{cc}] {code}: {field} mismatch"
                )

            assert len(f_entry["classification_signals"]) == len(
                m_entry["classification_signals"]
            ), f"[{cc}] {code}: signal count mismatch"

            for sig in m_entry["classification_signals"]:
                assert set(sig.keys()) == {"kind", "detail"}, (
                    f"[{cc}] {code}: minified signal has extra keys"
                )
