"""Validate provider classification output: structural checks + baseline regression."""

from __future__ import annotations

import json
import re
import statistics
from collections import Counter
from pathlib import Path
from typing import Any

from rich.console import Console
from rich.table import Table

from .models import Provider, SignalKind
from .probes import WEIGHTS
from .runner import PROVIDER_OUTPUT_NAMES, _build_category_map

# ── Constants ─────────────────────────────────────────────────────────

#: Maximum percentage of municipalities that can change provider before
#: the regression check fails.  Exceeding this likely indicates a logic
#: bug rather than natural DNS drift.
MAX_PROVIDER_CHANGES_PCT: float = 5.0

#: A confidence drop (in percentage-points) larger than this for a
#: single municipality triggers a warning in the regression report.
CONFIDENCE_DROP_THRESHOLD: float = 20.0

#: Minimum share of municipalities that should have confidence >= 50 %.
#: Falling below this signals a systemic scoring problem.
MIN_HIGH_CONFIDENCE_SHARE: float = 0.90

#: Maximum share of municipalities that may have confidence == 0.
#: Exceeding this signals wholesale probe or data failures.
MAX_ZERO_CONFIDENCE_SHARE: float = 0.05

# ── Derived valid-value sets (from enums + runner maps) ───────────────

VALID_PROVIDERS = {PROVIDER_OUTPUT_NAMES.get(p.value, p.value) for p in Provider} | {"unknown"}


def _valid_categories(country_code: str = "ch") -> set[str]:
    return set(_build_category_map(country_code).values()) | {"unknown"}


VALID_SIGNAL_KINDS = {k.value for k in SignalKind}

VALID_SIGNAL_PROVIDERS = {PROVIDER_OUTPUT_NAMES.get(p.value, p.value) for p in Provider}

# ── Rich console ──────────────────────────────────────────────────────

console = Console()

# ── Result collector ──────────────────────────────────────────────────


class ValidationResult:
    """Collects pass/fail/warn results from checks."""

    def __init__(self) -> None:
        self.errors: list[str] = []
        self.warnings: list[str] = []
        self.passed: int = 0

    def ok(self, _msg: str = "") -> None:
        self.passed += 1

    def error(self, msg: str) -> None:
        self.errors.append(msg)

    def warn(self, msg: str) -> None:
        self.warnings.append(msg)

    @property
    def success(self) -> bool:
        return len(self.errors) == 0

    def summary_markup(self) -> str:
        parts = [f"[green]{self.passed} passed[/green]"]
        if self.warnings:
            parts.append(f"[yellow]{len(self.warnings)} warnings[/yellow]")
        if self.errors:
            parts.append(f"[red]{len(self.errors)} errors[/red]")
        return ", ".join(parts)


# ── Structural validation ─────────────────────────────────────────────


def _check_metadata(data: dict, r: ValidationResult) -> None:
    for field in ("generated", "total", "counts", "municipalities"):
        if field not in data:
            r.error(f"missing top-level field '{field}'")
            return

    if not isinstance(data["total"], int) or data["total"] <= 0:
        r.error(f"'total' must be positive int, got {data['total']!r}")
        return

    munis = data["municipalities"]
    if not isinstance(munis, list):
        r.error("'municipalities' must be a list")
        return

    if len(munis) != data["total"]:
        r.error(f"total={data['total']} but municipalities has {len(munis)} entries")
    else:
        r.ok("total matches municipality count")

    counts = data["counts"]
    if sum(counts.values()) != data["total"]:
        r.error(f"counts sum {sum(counts.values())} != total {data['total']}")
    else:
        r.ok("counts sum matches total")

    actual: dict[str, int] = Counter()  # type: ignore[assignment]
    for entry in munis:
        actual[entry.get("provider", "?")] += 1
    if dict(sorted(actual.items())) != dict(sorted(counts.items())):
        r.error("counts header doesn't match actual distribution")
    else:
        r.ok("counts match actual provider distribution")

    if list(counts.keys()) != sorted(counts.keys()):
        r.warn("counts keys not sorted alphabetically")
    else:
        r.ok("counts keys sorted")


def _check_entry(entry: dict, r: ValidationResult, category_map: dict[str, str], valid_cats: set[str]) -> None:
    code = entry.get("code", "?")
    required = (
        "code",
        "name",
        "region",
        "domain",
        "mx",
        "spf",
        "provider",
        "category",
        "classification_confidence",
        "classification_signals",
    )
    for field in required:
        if field not in entry:
            r.error(f"{code}: missing field '{field}'")
            return

    provider = entry["provider"]
    if provider not in VALID_PROVIDERS:
        r.error(f"{code}: unknown provider '{provider}'")

    category = entry["category"]
    if category not in valid_cats:
        r.error(f"{code}: unknown category '{category}'")
    expected_cat = category_map.get(provider, "unknown")
    if category != expected_cat:
        r.error(f"{code}: provider '{provider}' should map to '{expected_cat}', got '{category}'")

    conf = entry["classification_confidence"]
    if not isinstance(conf, (int, float)) or conf < 0.0 or conf > 100.0:
        r.error(f"{code}: confidence {conf!r} out of [0, 100] range")

    if provider == "unknown":
        if conf != 0.0:
            r.error(f"{code}: unknown provider with confidence {conf} (expected 0)")
        if entry["classification_signals"]:
            r.error(f"{code}: unknown provider with signals")

    domain = entry["domain"]
    if domain:
        if "@" in domain:
            r.error(f"{code}: domain looks like email: {domain}")
    else:
        if provider not in ("unknown", "independent"):
            r.warn(f"{code}: empty domain for provider '{provider}'")

    for sig in entry["classification_signals"]:
        _check_signal(code, sig, r)

    if "gateway" in entry and not entry["gateway"]:
        r.warn(f"{code}: gateway field present but empty")


def _check_signal(code: str, sig: dict, r: ValidationResult) -> None:
    for field in ("kind", "provider", "weight", "detail"):
        if field not in sig:
            r.error(f"{code}: signal missing '{field}'")
            return

    if sig["kind"] not in VALID_SIGNAL_KINDS:
        r.error(f"{code}: unknown signal kind '{sig['kind']}'")

    if sig["provider"] not in VALID_SIGNAL_PROVIDERS:
        r.error(f"{code}: unknown signal provider '{sig['provider']}'")

    weight = sig["weight"]
    if not isinstance(weight, (int, float)) or weight < 0.0 or weight > 1.0:
        r.error(f"{code}: signal weight {weight!r} out of [0, 1] range")
    else:
        expected = WEIGHTS.get(SignalKind(sig["kind"]))
        if expected is not None and abs(weight - expected) > 0.001:
            r.error(f"{code}: signal '{sig['kind']}' weight {weight} != expected {expected}")

    if not sig.get("detail"):
        r.warn(f"{code}: signal detail empty")


def validate_structure(data: dict, *, country_code: str = "ch") -> ValidationResult:
    """Run all structural validation checks on a provider output file."""
    r = ValidationResult()
    category_map = _build_category_map(country_code)
    valid_cats = _valid_categories(country_code)

    _check_metadata(data, r)
    if not r.success:
        return r

    munis = data["municipalities"]

    codes = [m["code"] for m in munis]
    if codes != sorted(codes, key=int):
        r.warn("municipality codes not sorted numerically")
    else:
        r.ok("codes sorted numerically")

    for entry in munis:
        _check_entry(entry, r, category_map, valid_cats)

    # Confidence distribution sanity
    confs = [e["classification_confidence"] for e in munis]
    total = len(confs)
    high = sum(1 for c in confs if c >= 50.0)
    zero = sum(1 for c in confs if c == 0.0)
    if high / total < MIN_HIGH_CONFIDENCE_SHARE:
        r.warn(
            f"only {high}/{total} ({high / total:.0%}) entries have confidence >= 50 "
            f"(threshold: {MIN_HIGH_CONFIDENCE_SHARE:.0%})"
        )
    else:
        r.ok(f"{high / total:.0%} entries have confidence >= 50")
    if zero / total > MAX_ZERO_CONFIDENCE_SHARE:
        r.warn(
            f"{zero}/{total} ({zero / total:.1%}) entries have 0 confidence "
            f"(threshold: {MAX_ZERO_CONFIDENCE_SHARE:.0%})"
        )
    else:
        r.ok("zero-confidence entries within threshold")

    r.ok(f"validated {len(munis)} entries")
    return r


# ── Baseline regression ───────────────────────────────────────────────


def _normalize_baseline(data: dict) -> dict[str, dict]:
    """Normalize baseline municipalities to {code: entry} regardless of old/new format."""
    munis = data.get("municipalities", {})
    if isinstance(munis, list):
        return {m.get("code", m.get("bfs", "?")): m for m in munis}
    result = {}
    for key, entry in munis.items():
        code = entry.get("code", entry.get("bfs", key))
        result[code] = entry
    return result


def validate_regression(
    current: dict,
    baseline: dict,
) -> ValidationResult:
    """Compare current output against a baseline for regressions."""
    r = ValidationResult()

    cur_munis = _normalize_baseline(current)
    base_munis = _normalize_baseline(baseline)

    cur_keys = set(cur_munis.keys())
    base_keys = set(base_munis.keys())
    common = cur_keys & base_keys

    only_base = base_keys - cur_keys
    only_cur = cur_keys - base_keys
    if only_base:
        r.warn(f"{len(only_base)} municipalities in baseline but not in current")
    if only_cur:
        r.warn(f"{len(only_cur)} municipalities in current but not in baseline")
    r.ok(f"{len(common)} common municipalities matched")

    if not common:
        r.error("no common municipalities to compare")
        return r

    # Provider agreement
    provider_changes: list[dict[str, Any]] = []
    for key in sorted(common, key=int):
        bp = base_munis[key].get("provider", "?")
        cp = cur_munis[key].get("provider", "?")
        if bp != cp:
            provider_changes.append(
                {
                    "code": key,
                    "name": cur_munis[key].get("name", "?"),
                    "domain": cur_munis[key].get("domain", "?"),
                    "old": bp,
                    "new": cp,
                    "old_conf": base_munis[key].get("classification_confidence", 0.0),
                    "new_conf": cur_munis[key].get("classification_confidence", 0.0),
                }
            )

    change_pct = len(provider_changes) / len(common) * 100
    if change_pct > MAX_PROVIDER_CHANGES_PCT:
        r.error(
            f"{len(provider_changes)} provider changes ({change_pct:.1f}%) "
            f"exceeds {MAX_PROVIDER_CHANGES_PCT}% threshold"
        )
    elif provider_changes:
        r.warn(f"{len(provider_changes)} provider changes ({change_pct:.1f}%)")
    else:
        r.ok("no provider changes")

    # Provider distribution
    base_counts: Counter[str] = Counter()
    cur_counts: Counter[str] = Counter()
    for key in common:
        base_counts[base_munis[key].get("provider", "?")] += 1
        cur_counts[cur_munis[key].get("provider", "?")] += 1

    # Confidence deltas (where provider agrees)
    deltas: list[float] = []
    big_drops: list[dict[str, Any]] = []
    for key in sorted(common, key=int):
        bp = base_munis[key].get("provider", "?")
        cp = cur_munis[key].get("provider", "?")
        if bp == cp:
            bc = base_munis[key].get("classification_confidence", 0.0)
            cc = cur_munis[key].get("classification_confidence", 0.0)
            delta = cc - bc
            deltas.append(delta)
            if delta < -CONFIDENCE_DROP_THRESHOLD:
                big_drops.append(
                    {
                        "code": key,
                        "name": cur_munis[key].get("name", "?"),
                        "provider": cp,
                        "old_conf": bc,
                        "new_conf": cc,
                        "delta": delta,
                    }
                )

    if big_drops:
        r.warn(f"{len(big_drops)} entries dropped confidence by >{CONFIDENCE_DROP_THRESHOLD:.0f} points")
    elif deltas:
        r.ok(f"no confidence drops >{CONFIDENCE_DROP_THRESHOLD:.0f} points")

    new_unknowns = [c for c in provider_changes if c["new"] == "unknown"]
    if new_unknowns:
        r.warn(f"{len(new_unknowns)} municipalities became 'unknown'")

    # Stash details for the reporter
    r._regression = {  # type: ignore[attr-defined]
        "common": len(common),
        "provider_changes": provider_changes,
        "base_counts": dict(base_counts),
        "cur_counts": dict(cur_counts),
        "deltas": deltas,
        "big_drops": big_drops,
        "new_unknowns": new_unknowns,
    }
    return r


# ── Reporting ─────────────────────────────────────────────────────────


def _print_result_list(items: list[str], style: str, limit: int = 30) -> None:
    symbol = "[red]x[/red]" if style == "error" else "[yellow]![/yellow]"
    for item in items[:limit]:
        console.print(f"    {symbol} {item}")
    if len(items) > limit:
        console.print(f"    [dim]... and {len(items) - limit} more[/dim]")


def print_structural_report(r: ValidationResult) -> None:
    console.rule("[bold]Structural Validation[/bold]")
    console.print(f"  Result: {r.summary_markup()}")

    if r.errors:
        console.print("\n  [red]Errors:[/red]")
        _print_result_list(r.errors, "error")

    if r.warnings:
        console.print("\n  [yellow]Warnings:[/yellow]")
        _print_result_list(r.warnings, "warn", limit=15)


def print_regression_report(r: ValidationResult) -> None:
    reg: dict[str, Any] | None = getattr(r, "_regression", None)
    if reg is None:
        return

    console.rule("[bold]Baseline Regression Check[/bold]")
    console.print(f"  Result: {r.summary_markup()}")
    console.print(f"  Compared {reg['common']} common municipalities\n")

    # Provider distribution table
    all_provs = sorted(set(list(reg["base_counts"].keys()) + list(reg["cur_counts"].keys())))
    table = Table(title="Provider Distribution", show_edge=False, pad_edge=False)
    table.add_column("Provider", style="bold")
    table.add_column("Baseline", justify="right")
    table.add_column("Current", justify="right")
    table.add_column("Delta", justify="right")
    for p in all_provs:
        b = reg["base_counts"].get(p, 0)
        c = reg["cur_counts"].get(p, 0)
        d = c - b
        d_style = "red" if d < 0 else ("green" if d > 0 else "dim")
        table.add_row(p, str(b), str(c), f"[{d_style}]{d:+d}[/{d_style}]")
    console.print(table)

    # Provider changes
    changes = reg["provider_changes"]
    if changes:
        console.print(f"\n  [yellow]Provider changes: {len(changes)}[/yellow]")
        ct = Table(show_edge=False, pad_edge=False)
        ct.add_column("Code")
        ct.add_column("Name")
        ct.add_column("Domain")
        ct.add_column("Old -> New")
        ct.add_column("Old Conf", justify="right")
        ct.add_column("New Conf", justify="right")
        for c in changes[:30]:
            ct.add_row(
                c["code"],
                c["name"],
                c["domain"],
                f"{c['old']} -> {c['new']}",
                f"{c['old_conf']:.0f}%",
                f"{c['new_conf']:.0f}%",
            )
        console.print(ct)
        if len(changes) > 30:
            console.print(f"    [dim]... and {len(changes) - 30} more[/dim]")

    # Confidence deltas
    deltas = reg["deltas"]
    if deltas:
        mean = statistics.mean(deltas)
        median = statistics.median(deltas)
        console.print(f"\n  Confidence delta (same-provider entries, N={len(deltas)}):")
        console.print(f"    Mean:   {mean:>+.2f}  Median: {median:>+.2f}", end="")
        if len(deltas) > 1:
            console.print(f"  Stdev: {statistics.stdev(deltas):.2f}")
        else:
            console.print()

    # Big drops
    drops = reg["big_drops"]
    if drops:
        console.print(f"\n  [yellow]Large confidence drops (>{CONFIDENCE_DROP_THRESHOLD:.0f} pts):[/yellow]")
        for d in drops[:15]:
            console.print(
                f"    {d['code']} {d['name']:<28} {d['provider']:<14} "
                f"{d['old_conf']:.0f}% -> {d['new_conf']:.0f}% ({d['delta']:+.0f})"
            )

    # New unknowns
    unknowns = reg["new_unknowns"]
    if unknowns:
        console.print("\n  [yellow]Became unknown:[/yellow]")
        for u in unknowns:
            console.print(f"    {u['code']} {u['name']:<28} was: {u['old']}")


# ── Main entry point ──────────────────────────────────────────────────


def _infer_country(path: Path) -> str:
    """Infer country code from filename like ``providers_ch.json``."""
    match = re.search(r"providers_(\w{2})", path.stem)
    return match.group(1) if match else "ch"


def run_validation(
    output_path: Path,
    baseline_path: Path | None = None,
) -> bool:
    """Run validation and print report. Returns True if all checks pass."""
    console.rule("[bold]Provider Output Validation[/bold]")
    console.print(f"  File: {output_path}")
    if baseline_path:
        console.print(f"  Baseline: {baseline_path}")

    if not output_path.exists():
        console.print(f"\n  [red]Error[/red]: output file not found: {output_path}")
        return False

    country_code = _infer_country(output_path)

    with open(output_path, encoding="utf-8") as f:
        data = json.load(f)

    struct_result = validate_structure(data, country_code=country_code)
    print_structural_report(struct_result)

    reg_result = None
    if baseline_path:
        if not baseline_path.exists():
            console.print(f"\n  [red]Error[/red]: baseline file not found: {baseline_path}")
            return False

        with open(baseline_path, encoding="utf-8") as f:
            baseline = json.load(f)

        reg_result = validate_regression(data, baseline)
        print_regression_report(reg_result)

    all_ok = struct_result.success and (reg_result is None or reg_result.success)
    console.print()
    if all_ok:
        console.rule("[bold green]PASSED[/bold green]")
    else:
        console.rule("[bold red]FAILED[/bold red]")
    return all_ok
