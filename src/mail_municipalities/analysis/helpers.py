"""Shared LaTeX formatting helpers for analysis modules."""

from __future__ import annotations

from collections.abc import Sequence

from mail_municipalities.provider_classification.constants import REGION_ABBREVIATIONS

COUNTRY_NAMES: dict[str, str] = {"ch": "Switzerland", "de": "Germany", "at": "Austria"}
COUNTRIES: Sequence[str] = ("de", "at", "ch")

MAX_REGION_LEN = 18


def num(n: int) -> str:
    """Format integer with LaTeX thousands separator."""
    if n < 1_000:
        return str(n)
    s = f"{n:,}"
    return s.replace(",", "{,}")


def pct(n: int, total: int) -> str:
    if total == 0:
        return "0.0"
    return f"{n / total * 100:.1f}"


def esc(text: str) -> str:
    """Escape special LaTeX characters in text."""
    for ch, repl in [("&", "\\&"), ("%", "\\%"), ("_", "\\_"), ("#", "\\#")]:
        text = text.replace(ch, repl)
    return text


def divider(title: str) -> str:
    line = "%" + " " + "-" * 70
    return f"\n{line}\n% {title}\n{line}\n"


def region_name(region: str) -> str:
    """Return a human-readable region name, truncated if needed."""
    name = region.removeprefix("Kanton ")
    if len(name) > MAX_REGION_LEN:
        return name[: MAX_REGION_LEN - 1] + "."
    return name


def make_region_lookup(cc: str) -> dict[str, str]:
    """Return a mapping from full region name to uppercase abbreviation."""
    abbrevs = REGION_ABBREVIATIONS.get(cc, {})
    return {k: v.upper() for k, v in abbrevs.items()}
