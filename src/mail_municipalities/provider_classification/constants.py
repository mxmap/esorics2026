"""Constants for provider classification analysis."""

CANTON_ABBREVIATIONS: dict[str, str] = {
    "Kanton Zürich": "zh",
    "Kanton Bern": "be",
    "Kanton Luzern": "lu",
    "Kanton Uri": "ur",
    "Kanton Schwyz": "sz",
    "Kanton Obwalden": "ow",
    "Kanton Nidwalden": "nw",
    "Kanton Glarus": "gl",
    "Kanton Zug": "zg",
    "Kanton Freiburg": "fr",
    "Kanton Solothurn": "so",
    "Kanton Basel-Stadt": "bs",
    "Kanton Basel-Landschaft": "bl",
    "Kanton Schaffhausen": "sh",
    "Kanton Appenzell Ausserrhoden": "ar",
    "Kanton Appenzell Innerrhoden": "ai",
    "Kanton St. Gallen": "sg",
    "Kanton Graubünden": "gr",
    "Kanton Aargau": "ag",
    "Kanton Thurgau": "tg",
    "Kanton Tessin": "ti",
    "Kanton Waadt": "vd",
    "Kanton Wallis": "vs",
    "Kanton Neuenburg": "ne",
    "Kanton Genf": "ge",
    "Kanton Jura": "ju",
}

CANTON_SHORT_TO_FULL: dict[str, str] = {v: k for k, v in CANTON_ABBREVIATIONS.items()}

DE_STATE_ABBREVIATIONS: dict[str, str] = {
    "Schleswig-Holstein": "sh",
    "Hamburg": "hh",
    "Niedersachsen": "ni",
    "Bremen": "hb",
    "Nordrhein-Westfalen": "nw",
    "Hessen": "he",
    "Rheinland-Pfalz": "rp",
    "Baden-Württemberg": "bw",
    "Bayern": "by",
    "Saarland": "sl",
    "Berlin": "be",
    "Brandenburg": "bb",
    "Mecklenburg-Vorpommern": "mv",
    "Sachsen": "sn",
    "Sachsen-Anhalt": "st",
    "Thüringen": "th",
}

AT_STATE_ABBREVIATIONS: dict[str, str] = {
    "Burgenland": "b",
    "Kärnten": "k",
    "Niederösterreich": "nö",
    "Oberösterreich": "oö",
    "Salzburg": "s",
    "Steiermark": "st",
    "Tirol": "t",
    "Vorarlberg": "v",
    "Wien": "w",
}

REGION_ABBREVIATIONS: dict[str, dict[str, str]] = {
    "ch": CANTON_ABBREVIATIONS,
    "de": DE_STATE_ABBREVIATIONS,
    "at": AT_STATE_ABBREVIATIONS,
}
