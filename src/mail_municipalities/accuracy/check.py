"""Spot-check provider classification for specific domains.

Creates probes in state.db so that ``accuracy send`` / ``accuracy collect``
can verify the classification via NDR.  Running ``accuracy check`` again
after collection shows the verified result.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from pathlib import Path

from loguru import logger
from rich.console import Console
from rich.table import Table

from mail_municipalities.accuracy.models import (
    CLASSIFIER_TO_EVAL,
    NDR_TO_CLASSIFIER,
    Probe,
    ProbeStatus,
)
from mail_municipalities.accuracy.state import StateDB

console = Console()

_COUNTRIES = ("de", "at", "ch")


@dataclass(frozen=True)
class CheckResult:
    domain: str
    name: str | None
    country: str | None
    provider: str | None
    confidence: float | None
    status: str  # "not_found" | probe status value | "new"
    actual: str | None
    match: bool | None


async def check_domains(
    domains: list[str],
    providers_dir: Path,
    state: StateDB,
) -> list[CheckResult]:
    """Look up domains, create probes for new ones, show status for existing."""
    # Build domain → (entry, cc) from all provider files.
    catalog: dict[str, tuple[dict, str]] = {}
    for cc in _COUNTRIES:
        path = providers_dir / f"providers_{cc}.json"
        if not path.exists():
            continue
        data = json.loads(path.read_text())
        for entry in data["municipalities"]:
            d = entry.get("domain")
            if d:
                catalog[d] = (entry, cc)

    # Load existing probes and NDRs.
    all_probes = await state.get_all_probes()
    all_ndrs = await state.get_all_ndrs()
    probes_by_domain: dict[str, Probe] = {}
    for p in all_probes:
        probes_by_domain[p.domain] = p
    ndr_by_probe = {ndr.probe_id: ndr for ndr in all_ndrs}

    results: list[CheckResult] = []
    new_probes: list[Probe] = []

    for domain in domains:
        if domain not in catalog:
            results.append(
                CheckResult(
                    domain=domain,
                    name=None,
                    country=None,
                    provider=None,
                    confidence=None,
                    status="not_found",
                    actual=None,
                    match=None,
                )
            )
            continue

        entry, cc = catalog[domain]
        provider = entry.get("provider")
        confidence = entry.get("classification_confidence")

        existing = probes_by_domain.get(domain)
        if existing is not None:
            # Already probed — show current status and NDR result if available.
            actual = None
            match = None
            ndr = ndr_by_probe.get(existing.probe_id)
            if ndr is not None:
                actual = NDR_TO_CLASSIFIER.get(ndr.ndr_provider.value, "unknown")
                pred_eval = CLASSIFIER_TO_EVAL.get(provider or "", "unknown")
                match = pred_eval == actual
            results.append(
                CheckResult(
                    domain=domain,
                    name=entry.get("name"),
                    country=cc.upper(),
                    provider=provider,
                    confidence=confidence,
                    status=existing.status.value,
                    actual=actual,
                    match=match,
                )
            )
        else:
            # New — create a probe.
            probe = _make_probe(entry, cc)
            new_probes.append(probe)
            results.append(
                CheckResult(
                    domain=domain,
                    name=entry.get("name"),
                    country=cc.upper(),
                    provider=provider,
                    confidence=confidence,
                    status="new",
                    actual=None,
                    match=None,
                )
            )

    if new_probes:
        inserted = await state.insert_probes(new_probes)
        logger.info("Created {} probe(s) for check domains", inserted)

    return results


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


def print_check_table(results: list[CheckResult]) -> None:
    """Print check results as a Rich table."""
    table = Table(title="Domain Classification Check", show_lines=True)
    table.add_column("Domain", style="bold")
    table.add_column("City")
    table.add_column("Country")
    table.add_column("Provider")
    table.add_column("Confidence", justify="right")
    table.add_column("Status")
    table.add_column("Actual")
    table.add_column("Match")

    for r in results:
        if r.status == "not_found":
            table.add_row(r.domain, "[dim]not found[/dim]", "", "", "", "", "", "")
            continue

        match_str = ""
        if r.match is True:
            match_str = "[green]yes[/green]"
        elif r.match is False:
            match_str = "[red]no[/red]"

        status_str = r.status
        if r.status == "new":
            status_str = "[cyan]pending[/cyan]"
        elif r.status == "ndr_received":
            status_str = "[green]ndr_received[/green]"

        table.add_row(
            r.domain,
            r.name or "",
            r.country or "",
            r.provider or "",
            f"{r.confidence:.0f}" if r.confidence is not None else "",
            status_str,
            r.actual or "",
            match_str,
        )

    console.print(table)

    new_count = sum(1 for r in results if r.status == "new")
    if new_count:
        console.print(
            f"\n[cyan]{new_count} new probe(s) created.[/cyan] Run [bold]accuracy send --no-dry-run[/bold] then [bold]accuracy collect[/bold] to verify."
        )
