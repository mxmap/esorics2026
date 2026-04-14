"""Stratified sampling from provider classification output."""

from __future__ import annotations

import json
import random
import uuid
from collections import defaultdict
from pathlib import Path

from loguru import logger
from rich.console import Console
from rich.table import Table

from mail_municipalities.accuracy.models import Probe, ProbeStatus
from mail_municipalities.accuracy.state import StateDB

console = Console()

# Providers we can validate via NDR (exclude "unknown" — nothing to compare against).
_VALIDATABLE_PROVIDERS = {"microsoft", "google", "aws", "domestic", "foreign"}


def _load_municipalities(providers_path: Path) -> list[dict]:
    """Load municipality entries from a providers JSON file."""
    data = json.loads(providers_path.read_text())
    return data["municipalities"]


def _stratified_sample(
    entries: list[dict],
    total_size: int,
    min_per_class: int,
) -> list[dict]:
    """Return a stratified random sample, grouped by provider."""
    by_provider: dict[str, list[dict]] = defaultdict(list)
    for e in entries:
        provider = e.get("provider", "unknown")
        if provider in _VALIDATABLE_PROVIDERS and e.get("domain"):
            by_provider[provider].append(e)

    if not by_provider:
        return []

    # Guarantee minimum per class, then distribute remainder proportionally.
    sampled: list[dict] = []
    remaining_budget = total_size

    for provider, pool in by_provider.items():
        n = min(min_per_class, len(pool))
        picked = random.sample(pool, n)
        sampled.extend(picked)
        remaining_budget -= n
        # Remove picked from pool for proportional phase.
        picked_set = {id(e) for e in picked}
        by_provider[provider] = [e for e in pool if id(e) not in picked_set]

    if remaining_budget > 0:
        # Proportional allocation of remaining budget.
        total_remaining = sum(len(pool) for pool in by_provider.values())
        if total_remaining > 0:
            for provider, pool in by_provider.items():
                share = int(remaining_budget * len(pool) / total_remaining)
                n = min(share, len(pool))
                if n > 0:
                    sampled.extend(random.sample(pool, n))

    return sampled


def _make_probe(entry: dict, country: str) -> Probe:
    """Create a Probe from a municipality entry."""
    short_uuid = uuid.uuid4().hex[:12]
    domain = entry["domain"]
    return Probe(
        probe_id=uuid.uuid4().hex,
        domain=domain,
        municipality_code=str(entry["code"]),
        municipality_name=entry["name"],
        country=country,
        recipient=f"validation-probe-{short_uuid}@{domain}",
        predicted_provider=entry["provider"],
        predicted_confidence=entry.get("classification_confidence", 0.0),
        gateway=entry.get("gateway"),
        status=ProbeStatus.PENDING,
    )


async def create_sample(
    countries: list[str],
    total_size: int,
    min_per_class: int,
    providers_dir: Path,
    state: StateDB,
) -> list[Probe]:
    """Sample municipalities and insert probes into the state DB.

    Skips domains already present in the DB (for resumability).
    Returns the list of newly created probes.
    """
    existing_domains = await state.get_existing_domains()

    all_entries: list[tuple[dict, str]] = []
    for cc in countries:
        path = providers_dir / f"providers_{cc}.json"
        if not path.exists():
            logger.warning("Providers file not found: {}", path)
            continue
        for entry in _load_municipalities(path):
            if entry.get("domain") and entry["domain"] not in existing_domains:
                all_entries.append((entry, cc))

    # Flatten for sampling, then restore country.
    flat = [e for e, _ in all_entries]
    cc_map = {id(e): cc for e, cc in all_entries}

    sampled = _stratified_sample(flat, total_size, min_per_class)
    probes = [_make_probe(e, cc_map[id(e)]) for e in sampled]

    inserted = await state.insert_probes(probes)
    logger.info("Created {} probes ({} skipped as duplicates)", inserted, len(probes) - inserted)

    _print_summary(probes)
    return probes


def _print_summary(probes: list[Probe]) -> None:
    """Print a Rich summary of the sample."""
    by_provider: dict[str, int] = defaultdict(int)
    by_country: dict[str, int] = defaultdict(int)
    for p in probes:
        by_provider[p.predicted_provider] += 1
        by_country[p.country] += 1

    table = Table(title="Sample Summary", show_lines=True)
    table.add_column("Provider", style="bold")
    table.add_column("Count", justify="right")
    for provider in sorted(by_provider):
        table.add_row(provider, str(by_provider[provider]))
    table.add_row("Total", str(len(probes)), style="bold")
    console.print(table)

    country_table = Table(title="By Country", show_lines=True)
    country_table.add_column("Country", style="bold")
    country_table.add_column("Count", justify="right")
    for cc in sorted(by_country):
        country_table.add_row(cc.upper(), str(by_country[cc]))
    console.print(country_table)
