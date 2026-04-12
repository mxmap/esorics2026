"""Security scan pipeline: orchestrate Docker scanner/evaluator and write output JSON."""

from __future__ import annotations

import json
import shutil
import subprocess
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

from loguru import logger

from .defaults import DEFAULTS
from .models import DaneSummary, DssSummary, MunicipalitySecurity, SecurityOutput

# Path to the security_test directory (sibling of mail_municipalities under src/)
_SECURITY_TEST_DIR = Path(__file__).resolve().parents[2] / "security_test"


# ── Docker helpers ─────────────────────────────────────────────────────


def find_docker_compose() -> list[str]:
    """Return the docker compose command as a list of args."""
    if shutil.which("docker"):
        result = subprocess.run(
            ["docker", "compose", "version"],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            return ["docker", "compose"]
    if shutil.which("docker-compose"):
        return ["docker-compose"]
    msg = "Neither 'docker compose' (v2) nor 'docker-compose' (v1) found. Install Docker first."
    raise RuntimeError(msg)


def ensure_env(security_test_dir: Path) -> None:
    """Create a .env file with defaults if one doesn't exist."""
    env_path = security_test_dir / ".env"
    if env_path.exists():
        logger.debug("Using existing .env at {}", env_path)
        return
    lines = [f"{k}={v}" for k, v in DEFAULTS.items()]
    env_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    logger.info("Generated default .env at {}", env_path)


def _clear_dir(directory: Path) -> None:
    """Remove all files in a directory (keep the directory and .gitkeep)."""
    if not directory.exists():
        return
    for f in directory.iterdir():
        if f.is_file() and f.name != ".gitkeep":
            f.unlink()


# ── Input preparation ──────────────────────────────────────────────────


def prepare_scanner_input(domains_path: Path, scanner_input_dir: Path, cc: str) -> Path:
    """Read domains JSON and write scanner input in checked_emails format.

    Returns the path to the written scanner input file.
    """
    with open(domains_path, encoding="utf-8") as f:
        data = json.load(f)

    domains: set[str] = set()
    for muni in data["municipalities"]:
        for email_domain in muni.get("emails", []):
            if email_domain:
                domains.add(email_domain)

    checked_emails = sorted(f"info@{d}" for d in domains)
    logger.info("Prepared {} unique domains for scanning from {}", len(checked_emails), domains_path.name)

    _clear_dir(scanner_input_dir)
    scanner_input_dir.mkdir(parents=True, exist_ok=True)
    out_path = scanner_input_dir / f"{cc}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump({"checked_emails": checked_emails}, f, ensure_ascii=False, indent=2)

    return out_path


# ── Docker execution ───────────────────────────────────────────────────


def _run_container(compose_cmd: list[str], service: str, cwd: Path, verbose: bool) -> None:
    """Run a docker compose service, streaming output to the logger."""
    cmd = [*compose_cmd, "up", "--build", service]
    logger.info("Running: {} (cwd={})", " ".join(cmd), cwd)

    with subprocess.Popen(
        cmd,
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    ) as proc:
        assert proc.stdout is not None
        for line in proc.stdout:
            line = line.rstrip()
            if not line:
                continue
            if verbose:
                logger.info("[{}] {}", service, line)
            else:
                # Always surface key progress lines from the scanner
                lower = line.lower()
                if any(
                    kw in lower
                    for kw in ("starting", "finished", "loaded", "using", "resolving", "error", "warn", "failed")
                ):
                    logger.info("[{}] {}", service, line)

    if proc.returncode != 0:
        msg = f"Docker service '{service}' exited with code {proc.returncode}"
        raise RuntimeError(msg)


def run_docker_scanner(security_test_dir: Path, compose_cmd: list[str], verbose: bool) -> None:
    """Run the scanner Docker container."""
    _run_container(compose_cmd, "scanner", security_test_dir, verbose)


def run_docker_evaluator(security_test_dir: Path, compose_cmd: list[str], cc: str, verbose: bool) -> Path:
    """Move scanner results to evaluator input, run evaluator, return result path."""
    scanner_result_dir = security_test_dir / "scanner" / "scanner-result"
    evaluator_input_dir = security_test_dir / "evaluator" / "evaluator-input"
    evaluator_result_dir = security_test_dir / "evaluator" / "evaluator-result"

    _clear_dir(evaluator_input_dir)
    _clear_dir(evaluator_result_dir)

    # Move scanner results to evaluator input
    for f in scanner_result_dir.iterdir():
        if f.is_file() and f.suffix == ".json":
            shutil.move(str(f), evaluator_input_dir / f.name)

    _run_container(compose_cmd, "evaluator", security_test_dir, verbose)

    result_path = evaluator_result_dir / f"{cc}_database.json"
    if not result_path.exists():
        msg = f"Expected evaluator output not found: {result_path}"
        raise FileNotFoundError(msg)
    return result_path


# ── Output transformation ─────────────────────────────────────────────


def build_domain_security(rows: list[dict]) -> dict[str, dict]:
    """Group evaluator DatabaseRow dicts by domain, aggregate DANE + DSS.

    TLS fields are present in the rows but intentionally ignored.
    """
    by_domain: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        by_domain[row["domainName"]].append(row)

    result: dict[str, dict] = {}
    for domain, domain_rows in by_domain.items():
        mx_records = sorted({r["mxrecordName"] for r in domain_rows if r.get("mxrecordName")})

        # DANE: check across all MX/IP combos
        dane_values = [r.get("hasDane") for r in domain_rows if r.get("hasDane") is not None]
        dane_all = all(dane_values) if dane_values else False
        dane_any = any(dane_values) if dane_values else False

        # DSS: domain-level (same for all rows of a domain, take first non-null)
        dss_row = next((r for r in domain_rows if r.get("hasDmarc") is not None or r.get("hasSpf") is not None), {})

        result[domain] = {
            "mx_records": mx_records,
            "dane": DaneSummary(supported=dane_all, partial=dane_any),
            "dss": DssSummary(
                has_spf=bool(dss_row.get("hasSpf")),
                has_good_spf=bool(dss_row.get("hasGoodSpf")),
                has_dmarc=bool(dss_row.get("hasDmarc")),
                has_good_dmarc=bool(dss_row.get("hasGoodDmarc")),
                has_dkim=bool(dss_row.get("hasDkim")),
            ),
            "scan_valid": True,
        }

    return result


def _load_security_overrides(cc: str) -> dict[str, dict[str, Any]]:
    """Load security scan overrides from ``data/{cc}/security_overrides.json``.

    Returns dict keyed by municipality code.  Returns empty dict if the file does not exist.
    """
    path = Path("data") / cc / "security_overrides.json"
    if not path.exists():
        return {}

    with open(path, encoding="utf-8") as f:
        raw: dict[str, Any] = json.load(f)

    overrides: dict[str, dict[str, Any]] = {}
    for code, entry in raw.items():
        if "source" not in entry:
            logger.warning("security override {}: missing 'source' field — skipped", code)
            continue
        overrides[code] = entry

    logger.info("Loaded {} security overrides from {}", len(overrides), path)
    return overrides


def _apply_security_overrides(
    municipalities: list[MunicipalitySecurity],
    overrides: dict[str, dict[str, Any]],
) -> int:
    """Apply security overrides to municipality results.  Returns count applied."""
    by_code = {m.code: m for m in municipalities}
    applied = 0
    for code, override in overrides.items():
        if code not in by_code:
            logger.warning("security override for {} but municipality not in results", code)
            continue

        muni = by_code[code]

        # Merge DSS fields
        if "dss" in override and muni.dss is not None:
            for field, value in override["dss"].items():
                setattr(muni.dss, field, value)

        # Merge DANE fields
        if "dane" in override and muni.dane is not None:
            for field, value in override["dane"].items():
                setattr(muni.dane, field, value)

        muni.scan_valid = True
        muni.override = {"source": override["source"]}
        applied += 1

    return applied


def build_output(domains_path: Path, domain_security: dict[str, dict], cc: str) -> SecurityOutput:
    """Map security data back to municipalities and build the output envelope."""
    with open(domains_path, encoding="utf-8") as f:
        data = json.load(f)

    commit = (
        subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
        ).stdout.strip()
        or None
    )

    municipalities: list[MunicipalitySecurity] = []
    for muni in data["municipalities"]:
        emails = muni.get("emails", [])
        domain = emails[0] if emails else ""
        sec = domain_security.get(domain, {})

        municipalities.append(
            MunicipalitySecurity(
                code=muni["code"],
                name=muni["name"],
                region=muni.get("region", ""),
                domain=domain,
                mx_records=sec.get("mx_records", []),
                dane=sec.get("dane"),
                dss=sec.get("dss"),
                scan_valid=sec.get("scan_valid", False),
            )
        )

    municipalities.sort(key=lambda m: m.code)

    # Apply security overrides
    security_overrides = _load_security_overrides(cc)
    if security_overrides:
        overrides_applied = _apply_security_overrides(municipalities, security_overrides)
        logger.info("Security overrides: {} available, {} applied", len(security_overrides), overrides_applied)

    # Aggregate counts
    scanned = sum(1 for m in municipalities if m.scan_valid)
    counts = {
        "scanned": scanned,
        "dane_supported": sum(1 for m in municipalities if m.dane and m.dane.supported),
        "spf": sum(1 for m in municipalities if m.dss and m.dss.has_spf),
        "good_spf": sum(1 for m in municipalities if m.dss and m.dss.has_good_spf),
        "dmarc": sum(1 for m in municipalities if m.dss and m.dss.has_dmarc),
        "good_dmarc": sum(1 for m in municipalities if m.dss and m.dss.has_good_dmarc),
        "dkim": sum(1 for m in municipalities if m.dss and m.dss.has_dkim),
    }

    return SecurityOutput(
        generated=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        commit=commit,
        total=len(municipalities),
        counts=counts,
        municipalities=municipalities,
    )


# ── Top-level orchestrator ─────────────────────────────────────────────


def run(domains_path: Path, output_path: Path, *, cc: str, verbose: bool = False) -> None:
    """Run the full security scan pipeline for a country."""
    security_test_dir = _SECURITY_TEST_DIR
    if not security_test_dir.exists():
        msg = f"Security test directory not found: {security_test_dir}"
        raise FileNotFoundError(msg)

    compose_cmd = find_docker_compose()
    ensure_env(security_test_dir)

    scanner_input_dir = security_test_dir / "scanner" / "scanner-input"
    scanner_result_dir = security_test_dir / "scanner" / "scanner-result"

    # Clean up from previous runs
    _clear_dir(scanner_result_dir)

    # Phase 1: Prepare input
    logger.info("Phase 1: Preparing scanner input for {}", cc)
    prepare_scanner_input(domains_path, scanner_input_dir, cc)

    # Phase 2: Run scanner
    logger.info("Phase 2: Running security scanner (Docker)")
    t0 = time.monotonic()
    run_docker_scanner(security_test_dir, compose_cmd, verbose)
    logger.info("Scanner completed in {:.1f}s", time.monotonic() - t0)

    # Phase 3: Run evaluator
    logger.info("Phase 3: Running evaluator (Docker)")
    t1 = time.monotonic()
    evaluator_result = run_docker_evaluator(security_test_dir, compose_cmd, cc, verbose)
    logger.info("Evaluator completed in {:.1f}s", time.monotonic() - t1)

    # Phase 4: Transform output
    logger.info("Phase 4: Building output")
    with open(evaluator_result, encoding="utf-8") as f:
        rows = json.load(f)

    domain_security = build_domain_security(rows)
    output = build_output(domains_path, domain_security, cc)

    # Write output
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output.model_dump(), f, ensure_ascii=False, indent=2, separators=(",", ":"))

    size_kb = output_path.stat().st_size / 1024
    logger.info("Wrote {} ({:.0f} KB)", output_path, size_kb)
    logger.info(
        "--- Security scan: {} municipalities, {} scanned, {} with SPF, {} with DMARC, {} with DKIM ---",
        output.total,
        output.counts["scanned"],
        output.counts["spf"],
        output.counts["dmarc"],
        output.counts["dkim"],
    )
