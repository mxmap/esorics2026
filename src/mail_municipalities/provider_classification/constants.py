"""Constants for provider classification analysis."""

CANTON_ABBREVIATIONS = {
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

CANTON_SHORT_TO_FULL = {v: k for k, v in CANTON_ABBREVIATIONS.items()}
