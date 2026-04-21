"""Content-based heuristics for classifying municipality homepages."""

from __future__ import annotations

MUNICIPALITY_KEYWORDS: dict[str, list[str]] = {
    "de": [
        "gemeinde",
        "stadtverwaltung",
        "rathaus",
        "bürgerservice",
        "bürgermeister",
        "verwaltung",
        "amtlich",
        "gemeindeamt",
        "einwohner",
        "ortsvorsteher",
        "ortsgemeinde",
        "amtsgericht",
    ],
    "fr": [
        "commune",
        "mairie",
        "administration communale",
        "syndic",
        "municipalité",
        "conseil communal",
        "secrétariat communal",
    ],
    "it": [
        "comune",
        "municipio",
        "amministrazione comunale",
        "sindaco",
        "segreteria comunale",
        "cancelleria comunale",
    ],
}

# Patterns that indicate a parked/placeholder/for-sale domain
PARKED_INDICATORS: list[str] = [
    "domain is for sale",
    "this domain is parked",
    "buy this domain",
    "domain parking",
    "unter konstruktion",
    "under construction",
    "coming soon",
    "website nicht verfügbar",
    "diese domain steht zum verkauf",
    "godaddy",
    "sedo.com",
    "aftermarket",
    "hugedomains",
    "dan.com",
]


def classify_homepage(html: str) -> list[str]:
    """Classify homepage HTML using keyword heuristics.

    Returns a list of flags:
    - ``["parked"]`` when parked indicators are found *and* no municipality
      keywords are present (avoids false positives on legit sites).
    - ``["has_municipality_keywords"]`` when municipality keywords are found.
    - ``["no_municipality_keywords"]`` when neither condition is met.
    """
    text = html.lower()
    has_parked = any(indicator in text for indicator in PARKED_INDICATORS)
    has_municipal = any(kw in text for keywords in MUNICIPALITY_KEYWORDS.values() for kw in keywords)
    if has_parked and not has_municipal:
        return ["parked"]
    if has_municipal:
        return ["has_municipality_keywords"]
    return ["no_municipality_keywords"]
