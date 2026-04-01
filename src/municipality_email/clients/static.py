"""File loaders for static municipality data sources."""

from __future__ import annotations

import csv
import json
import re
import unicodedata
from pathlib import Path

from municipality_email.scraping import url_to_domain


def normalize_csv_name(name: str) -> str:
    """Normalize a name for fuzzy CSV matching.

    Expands umlauts, strips accents, lowercases, and collapses whitespace.
    """
    name = name.strip().lower()
    for old, new in [("ä", "ae"), ("ö", "oe"), ("ü", "ue"), ("ß", "ss")]:
        name = name.replace(old, new)
    name = unicodedata.normalize("NFKD", name)
    name = "".join(c for c in name if not unicodedata.combining(c))
    name = re.sub(r"[^a-z0-9 ]", " ", name)
    return re.sub(r"\s+", " ", name).strip()


def load_livenson(path: Path) -> dict[str, dict]:
    """Load livenson dataset, keyed by 8-digit AGS.

    Returns dict mapping AGS -> {name, region, domain, osm_relation_id}.
    """
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    result: dict[str, dict] = {}
    for e in data:
        ags = e["id"].replace("DE-", "")
        result[ags] = {
            "name": e["name"],
            "region": e["region"],
            "domain": e.get("domain", ""),
            "osm_relation_id": e.get("osm_relation_id"),
        }
    return result


def load_b42labs(path: Path) -> dict[str, dict]:
    """Load b42labs dataset, keyed by 8-digit AGS."""
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    return data["municipalities"]


def load_csv_alex(path: Path, skip_domains: set[str]) -> dict[str, list[tuple[str, str]]]:
    """Load Alex's CSV, returning {normalized_name: [(email_domain, bundesland)]}.

    Filters out personal/generic email providers via skip_domains.
    """
    result: dict[str, list[tuple[str, str]]] = {}
    with open(path, encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        next(reader)  # skip header
        for r in reader:
            name = r[0].strip() if r else ""
            if not name:
                continue
            email = r[1].strip() if len(r) > 1 else ""
            if "@" not in email:
                continue
            domain = re.sub(r"[^\x21-\x7e]", "", email.split("@")[1]).lower()
            if domain in skip_domains:
                continue
            bundesland = r[2].strip() if len(r) > 2 else ""
            nn = normalize_csv_name(name)
            result.setdefault(nn, []).append((domain, bundesland))
    return result


def load_destatis(path: Path) -> dict[str, str]:
    """Load destatis reference dataset.

    Returns dict mapping AGS -> official name.
    """
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    return {ags: entry["name"] for ags, entry in data.items()}


def load_bresu(path: Path) -> dict[str, dict]:
    """Load BRESU Austrian municipality data, keyed by 5-digit GKZ.

    Returns dict mapping GKZ -> {name, plz, website, mail, website_domain, email_domain}.
    """
    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    result: dict[str, dict] = {}
    for entry in data:
        gkz = str(entry["Gemeindekennziffer"]).zfill(5)
        website = entry.get("Website", "")
        mail = entry.get("Mail", "")

        website_domain = url_to_domain(website) if website else None
        email_domain = mail.split("@")[1].lower() if "@" in mail else None

        result[gkz] = {
            "name": entry["Gemeindename"],
            "plz": entry.get("PLZ"),
            "website": website,
            "mail": mail,
            "website_domain": website_domain,
            "email_domain": email_domain,
        }
    return result
