"""Classification pipeline: orchestrate classify_many() and write output JSON."""

from __future__ import annotations

import json
import subprocess
import time
from pathlib import Path
from typing import Any

from loguru import logger

from .classifier import classify_many
from .models import ClassificationResult, Provider

# Map internal Provider enum values to output names
PROVIDER_OUTPUT_NAMES: dict[str, str] = {
    "ms365": "microsoft",
}


def _build_category_map(country_code: str) -> dict[str, str]:
    """Build provider → category mapping for a given country."""
    domestic = f"{country_code}-based"
    return {
        "microsoft": "us-cloud",
        "google": "us-cloud",
        "aws": "us-cloud",
        "domestic": domestic,
        "foreign": "foreign",
        "unknown": "unknown",
    }


_FRONTEND_FIELDS = {
    "code",
    "name",
    "domain",
    "region",
    "mx",
    "spf",
    "provider",
    "category",
    "classification_confidence",
    "classification_signals",
    "gateway",
}


def _minify_for_frontend(full_output: dict[str, Any]) -> dict[str, Any]:
    """Strip fields the frontend doesn't use, producing a compact payload."""
    municipalities = []
    for entry in full_output["municipalities"]:
        mini = {k: v for k, v in entry.items() if k in _FRONTEND_FIELDS}
        mini["classification_signals"] = [
            {"kind": s["kind"], "detail": s["detail"]} for s in entry.get("classification_signals", [])
        ]
        municipalities.append(mini)
    return {
        "generated": full_output["generated"],
        "commit": full_output.get("commit"),
        "municipalities": municipalities,
    }


def _output_provider(provider: Provider) -> str:
    """Map Provider enum to output name."""
    return PROVIDER_OUTPUT_NAMES.get(provider.value, provider.value)


def _serialize_result(
    entry: dict[str, Any], result: ClassificationResult, category_map: dict[str, str]
) -> dict[str, Any]:
    """Serialize a ClassificationResult into a municipality output entry."""
    provider = _output_provider(result.provider)
    category = category_map.get(provider, "unknown")
    out: dict[str, Any] = {
        "code": entry["code"],
        "name": entry["name"],
        "region": entry.get("region", ""),
        "domain": entry.get("_domain", ""),
        "mx": result.mx_hosts,
        "spf": result.spf_raw,
        "provider": provider,
        "category": category,
        "classification_confidence": round(result.confidence * 100, 1),
        "classification_signals": [
            {
                "kind": e.kind.value,
                "provider": PROVIDER_OUTPUT_NAMES.get(e.provider.value, e.provider.value),
                "weight": e.weight,
                "detail": e.detail,
            }
            for e in result.evidence
        ],
    }

    if result.gateway:
        out["gateway"] = result.gateway

    # Pass through resolve-level fields
    if "sources_detail" in entry:
        out["sources_detail"] = entry["sources_detail"]
    if "flags" in entry:
        out["resolve_flags"] = entry["flags"]

    return out


def _load_resolver_output(domains_path: Path) -> dict[str, dict[str, Any]]:
    """Load domain resolver output (array format) and return dict keyed by code.

    The resolver outputs municipalities as an array with fields:
    code, name, region, website, emails, source, confidence, sources_detail, flags.

    We map emails[0] -> _domain for classification input.
    """
    with open(domains_path, encoding="utf-8") as f:
        data = json.load(f)

    entries: dict[str, dict[str, Any]] = {}
    for muni in data["municipalities"]:
        entry = dict(muni)
        emails = entry.get("emails", [])
        entry["_domain"] = emails[0] if emails else ""
        entries[entry["code"]] = entry

    return entries


async def run(domains_path: Path, output_path: Path, *, country_code: str = "ch") -> None:
    category_map = _build_category_map(country_code)
    entries = _load_resolver_output(domains_path)
    total = len(entries)

    logger.info("Classifying {} municipalities", total)
    t0 = time.monotonic()

    # Build domain -> entry mapping
    domain_to_entries: dict[str, list[dict[str, Any]]] = {}
    no_domain_entries: list[dict[str, Any]] = []
    for entry in entries.values():
        domain = entry.get("_domain", "")
        if domain:
            domain_to_entries.setdefault(domain, []).append(entry)
        else:
            no_domain_entries.append(entry)

    unique_domains = list(domain_to_entries.keys())

    results: dict[str, dict[str, Any]] = {}
    done = 0

    # Handle entries without domains
    for entry in no_domain_entries:
        results[entry["code"]] = {
            "code": entry["code"],
            "name": entry["name"],
            "region": entry.get("region", ""),
            "domain": "",
            "mx": [],
            "spf": "",
            "provider": "unknown",
            "category": "unknown",
            "classification_confidence": 0.0,
            "classification_signals": [],
        }
        if "sources_detail" in entry:
            results[entry["code"]]["sources_detail"] = entry["sources_detail"]
        if "flags" in entry:
            results[entry["code"]]["resolve_flags"] = entry["flags"]

    # Classify domains
    async for domain, classification in classify_many(unique_domains, country_code=country_code):
        for entry in domain_to_entries[domain]:
            serialized = _serialize_result(entry, classification, category_map)
            results[entry["code"]] = serialized

        done += len(domain_to_entries[domain])
        logger.info(
            "[{:>4}/{}] {}: provider={} confidence={:.2f} signals={}",
            done,
            total,
            domain,
            classification.provider.value,
            classification.confidence,
            len(classification.evidence),
        )

    # Final counts
    domestic_label = f"{country_code}-based"
    counts: dict[str, int] = {}
    cat_counts: dict[str, int] = {}
    for r in results.values():
        counts[r["provider"]] = counts.get(r["provider"], 0) + 1
        cat = category_map.get(r["provider"], "unknown")
        cat_counts[cat] = cat_counts.get(cat, 0) + 1

    elapsed = time.monotonic() - t0
    logger.info("--- Classification: {} municipalities in {:.1f}s ---", len(results), elapsed)
    logger.info(
        "  US Cloud         {:>5}  (MS={} Google={} AWS={})",
        cat_counts.get("us-cloud", 0),
        counts.get("microsoft", 0),
        counts.get("google", 0),
        counts.get("aws", 0),
    )
    logger.info(
        "  Domestic         {:>5}",
        cat_counts.get(domestic_label, 0),
    )
    logger.info(
        "  Foreign          {:>5}",
        cat_counts.get("foreign", 0),
    )
    logger.info("  Unknown/No MX    {:>5}", cat_counts.get("unknown", 0))

    sorted_counts = dict(sorted(counts.items()))
    sorted_munis = sorted(results.values(), key=lambda m: int(m["code"]))

    commit = (
        subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
        ).stdout.strip()
        or None
    )

    output = {
        "generated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "commit": commit,
        "total": len(results),
        "counts": sorted_counts,
        "municipalities": sorted_munis,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2, separators=(",", ":"))

    size_kb = len(json.dumps(output)) / 1024

    mini_output = _minify_for_frontend(output)
    mini_path = output_path.with_suffix(".min.json")
    with open(mini_path, "w", encoding="utf-8") as f:
        json.dump(mini_output, f, ensure_ascii=False, separators=(",", ":"))

    mini_size_kb = mini_path.stat().st_size / 1024
    logger.info("Wrote {} ({} KB)", output_path, size_kb)
    logger.info("Wrote {} ({:.0f} KB)", mini_path, mini_size_kb)
