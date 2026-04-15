"""Unified CLI for mail-municipalities."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Annotated, Optional

import typer

from mail_municipalities.core.log import setup as setup_logging

app = typer.Typer(add_completion=False)


def _get_config(cc: str):
    """Lazily import and instantiate a country config."""
    if cc == "ch":
        from mail_municipalities.domain_resolver.countries.switzerland import SwitzerlandConfig

        return SwitzerlandConfig()
    elif cc == "de":
        from mail_municipalities.domain_resolver.countries.germany import GermanyConfig

        return GermanyConfig()
    elif cc == "at":
        from mail_municipalities.domain_resolver.countries.austria import AustriaConfig

        return AustriaConfig()
    else:
        typer.echo(f"Unknown country: {cc}. Must be one of: ch, de, at", err=True)
        raise typer.Exit(code=1)


def _resolve_impl(
    country: str | None = None,
    all_countries: bool = False,
    dry_run: bool = False,
    verbose: bool = False,
    output: Path | None = None,
    no_cache: bool = False,
) -> None:
    """Shared resolve implementation."""
    if not country and not all_countries:
        typer.echo("Provide a country code (ch, de, at) or use --all", err=True)
        raise typer.Exit(code=1)

    from mail_municipalities.domain_resolver.pipeline import run_pipeline

    if all_countries:
        countries = ["ch", "de", "at"]
    else:
        assert country is not None
        countries = [country]
    output_dir = output or Path("output/domains")
    data_base = Path("data")

    for cc in countries:
        setup_logging(verbose, log_path=output_dir / f"resolve_{cc}.log")
        config = _get_config(cc)
        data_dir = data_base / cc
        asyncio.run(
            run_pipeline(
                config,
                data_dir=data_dir,
                output_dir=output_dir,
                dry_run=dry_run,
                no_cache=no_cache,
            )
        )


@app.command("resolve")
def resolve_cmd(
    country: Annotated[
        Optional[str],
        typer.Argument(help="Country code: ch, de, at"),
    ] = None,
    all_countries: Annotated[
        bool,
        typer.Option("--all", help="Resolve all countries"),
    ] = False,
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run", help="Statistics only, no scraping/output"),
    ] = False,
    verbose: Annotated[
        bool,
        typer.Option("-v", "--verbose", help="Enable debug logging"),
    ] = False,
    output: Annotated[
        Optional[Path],
        typer.Option("-o", "--output", help="Custom output directory"),
    ] = None,
    no_cache: Annotated[
        bool,
        typer.Option("--no-cache", help="Disable all caching"),
    ] = False,
) -> None:
    """Resolve email domains for municipalities."""
    _resolve_impl(country, all_countries, dry_run, verbose, output, no_cache)


@app.command("classify")
def classify_cmd(
    country: Annotated[
        str,
        typer.Argument(help="Country code: ch, de, at"),
    ],
    verbose: Annotated[
        bool,
        typer.Option("-v", "--verbose", help="Enable debug logging"),
    ] = False,
    domains_dir: Annotated[
        Path,
        typer.Option("--domains-dir", help="Directory with domain resolver output"),
    ] = Path("output/domains"),
    output: Annotated[
        Optional[Path],
        typer.Option("-o", "--output", help="Custom output directory"),
    ] = None,
    validate: Annotated[
        bool,
        typer.Option("--validate", help="Validate existing output instead of classifying"),
    ] = False,
    baseline: Annotated[
        Optional[Path],
        typer.Option("--baseline", help="Baseline file for regression comparison"),
    ] = None,
) -> None:
    """Classify email providers for municipalities."""
    output_dir = output or Path("output/providers")
    setup_logging(verbose, log_path=output_dir / f"classify_{country}.log")

    output_path = output_dir / f"providers_{country}.json"

    if validate:
        from mail_municipalities.provider_classification.validate import run_validation

        ok = run_validation(output_path, baseline_path=baseline)
        raise typer.Exit(code=0 if ok else 1)

    from mail_municipalities.provider_classification.runner import run

    domains_path = domains_dir / f"domains_{country}_detailed.json"
    asyncio.run(run(domains_path, output_path, country_code=country))


@app.command("scan")
def scan_cmd(
    country: Annotated[
        str,
        typer.Argument(help="Country code: ch, de, at"),
    ],
    verbose: Annotated[
        bool,
        typer.Option("-v", "--verbose", help="Enable debug logging"),
    ] = False,
    domains_dir: Annotated[
        Path,
        typer.Option("--domains-dir", help="Directory with domain resolver output"),
    ] = Path("output/domains"),
    output: Annotated[
        Optional[Path],
        typer.Option("-o", "--output", help="Custom output directory"),
    ] = None,
) -> None:
    """Run security scan (DANE, SPF, DKIM, DMARC) for municipalities."""
    output_dir = output or Path("output/security")
    setup_logging(verbose, log_path=output_dir / f"scan_{country}.log")

    domains_path = domains_dir / f"domains_{country}.json"
    output_path = output_dir / f"security_{country}.json"

    from mail_municipalities.security_analysis.runner import run

    run(domains_path, output_path, cc=country, verbose=verbose)


# ── Analyze subcommands ──────────────────────────────────────────────

_analyze_app = typer.Typer(add_completion=False, help="Analyze classification and security results.")
app.add_typer(_analyze_app, name="analyze")


def _analyze_providers_impl(
    data_path: Path | None = None,
    all_countries: bool = False,
    latex: bool = False,
) -> None:
    if all_countries:
        if latex:
            from mail_municipalities.analysis.provider_combined import export_combined_latex

            export_combined_latex()
            return
        from mail_municipalities.analysis.provider_combined import print_combined_summary

        print_combined_summary()
        return

    from mail_municipalities.provider_classification.analyze import main

    main(data_path, latex=latex)


def _analyze_security_impl(
    data_path: Path | None = None,
    all_countries: bool = False,
    latex: bool = False,
) -> None:
    if all_countries:
        if latex:
            from mail_municipalities.analysis.security_combined import export_combined_security_latex

            export_combined_security_latex()
            return
        from mail_municipalities.analysis.security_combined import print_combined_security_summary

        print_combined_security_summary()
        return

    from mail_municipalities.analysis.security_latex import main as security_main

    security_main(data_path, latex=latex)


@_analyze_app.command("providers")
def analyze_providers_cmd(
    data_path: Annotated[
        Optional[Path],
        typer.Argument(help="Path to providers JSON file"),
    ] = None,
    all_countries: Annotated[
        bool,
        typer.Option("--all", help="Analyze all countries and produce combined table"),
    ] = False,
    latex: Annotated[
        bool,
        typer.Option("--latex", help="Export tables as LNCS-formatted LaTeX file"),
    ] = False,
) -> None:
    """Analyze provider classification results."""
    _analyze_providers_impl(data_path, all_countries, latex)


@_analyze_app.command("security")
def analyze_security_cmd(
    data_path: Annotated[
        Optional[Path],
        typer.Argument(help="Path to security JSON file"),
    ] = None,
    all_countries: Annotated[
        bool,
        typer.Option("--all", help="Analyze all countries and produce combined table"),
    ] = False,
    latex: Annotated[
        bool,
        typer.Option("--latex", help="Export tables as LNCS-formatted LaTeX file"),
    ] = False,
) -> None:
    """Analyze security scan results."""
    _analyze_security_impl(data_path, all_countries, latex)


@_analyze_app.command("adhoc")
def analyze_adhoc_cmd() -> None:
    """Ad-hoc analysis: provider–security correlations."""
    from mail_municipalities.analysis.adhoc import main as adhoc_main

    adhoc_main()


@_analyze_app.command("charts")
def analyze_charts_cmd() -> None:
    """Generate security & provider charts for the paper."""
    from mail_municipalities.analysis.charts import main as charts_main

    charts_main()


@_analyze_app.command("timestamps")
def analyze_timestamps_cmd(
    latex: Annotated[
        bool,
        typer.Option("--latex", help="Export table as LaTeX file"),
    ] = False,
) -> None:
    """Pipeline execution timestamps for the appendix."""
    from mail_municipalities.analysis.timestamps import export_latex, print_summary

    if latex:
        export_latex()
    else:
        print_summary()


@_analyze_app.command("merged")
def analyze_merged_cmd(
    latex: Annotated[
        bool,
        typer.Option("--latex", help="Export merged table as LNCS-formatted LaTeX file"),
    ] = False,
) -> None:
    """Merged provider + security regional table (all countries)."""
    from mail_municipalities.analysis.merged_combined import (
        export_merged_latex,
        print_merged_summary,
    )

    if latex:
        export_merged_latex()
    else:
        print_merged_summary()


@_analyze_app.command("outliers")
def analyze_outliers_cmd(
    country: Annotated[
        Optional[str],
        typer.Option("--country", help="Limit to one country (ch, de, at)"),
    ] = None,
    verify: Annotated[
        bool,
        typer.Option("--verify", help="Run DNS verification on sample findings"),
    ] = False,
) -> None:
    """Investigate outliers and potential errors in classification and security data."""
    from mail_municipalities.analysis.outliers import main as outliers_main

    outliers_main(country=country, verify=verify)


# ── Script entry points (called by [project.scripts]) ──────────────


_resolve_app = typer.Typer(add_completion=False)


@_resolve_app.command()
def _resolve_main(
    country: Annotated[
        Optional[str],
        typer.Argument(help="Country code: ch, de, at"),
    ] = None,
    all_countries: Annotated[
        bool,
        typer.Option("--all", help="Resolve all countries"),
    ] = False,
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run", help="Statistics only, no scraping/output"),
    ] = False,
    verbose: Annotated[
        bool,
        typer.Option("-v", "--verbose", help="Enable debug logging"),
    ] = False,
    output: Annotated[
        Optional[Path],
        typer.Option("-o", "--output", help="Custom output directory"),
    ] = None,
    no_cache: Annotated[
        bool,
        typer.Option("--no-cache", help="Disable all caching"),
    ] = False,
) -> None:
    """Resolve email domains for municipalities."""
    _resolve_impl(country, all_countries, dry_run, verbose, output, no_cache)


_classify_app = typer.Typer(add_completion=False)


@_classify_app.command()
def _classify_main(
    country: Annotated[
        str,
        typer.Argument(help="Country code: ch, de, at"),
    ],
    verbose: Annotated[
        bool,
        typer.Option("-v", "--verbose", help="Enable debug logging"),
    ] = False,
    domains_dir: Annotated[
        Path,
        typer.Option("--domains-dir", help="Directory with domain resolver output"),
    ] = Path("output/domains"),
    output: Annotated[
        Optional[Path],
        typer.Option("-o", "--output", help="Custom output directory"),
    ] = None,
    validate: Annotated[
        bool,
        typer.Option("--validate", help="Validate existing output instead of classifying"),
    ] = False,
    baseline: Annotated[
        Optional[Path],
        typer.Option("--baseline", help="Baseline file for regression comparison"),
    ] = None,
) -> None:
    """Classify email providers for municipalities."""
    output_dir = output or Path("output/providers")
    setup_logging(verbose, log_path=output_dir / f"classify_{country}.log")

    output_path = output_dir / f"providers_{country}.json"

    if validate:
        from mail_municipalities.provider_classification.validate import run_validation

        ok = run_validation(output_path, baseline_path=baseline)
        raise typer.Exit(code=0 if ok else 1)

    from mail_municipalities.provider_classification.runner import run

    domains_path = domains_dir / f"domains_{country}_detailed.json"
    asyncio.run(run(domains_path, output_path, country_code=country))


_scan_app = typer.Typer(add_completion=False)


@_scan_app.command()
def _scan_main(
    country: Annotated[
        str,
        typer.Argument(help="Country code: ch, de, at"),
    ],
    verbose: Annotated[
        bool,
        typer.Option("-v", "--verbose", help="Enable debug logging"),
    ] = False,
    domains_dir: Annotated[
        Path,
        typer.Option("--domains-dir", help="Directory with domain resolver output"),
    ] = Path("output/domains"),
    output: Annotated[
        Optional[Path],
        typer.Option("-o", "--output", help="Custom output directory"),
    ] = None,
) -> None:
    """Run security scan (DANE, SPF, DKIM, DMARC) for municipalities."""
    output_dir = output or Path("output/security")
    setup_logging(verbose, log_path=output_dir / f"scan_{country}.log")

    domains_path = domains_dir / f"domains_{country}.json"
    output_path = output_dir / f"security_{country}.json"

    from mail_municipalities.security_analysis.runner import run

    run(domains_path, output_path, cc=country, verbose=verbose)


# ── Accuracy subcommands ────────────────────────────────────────────

_accuracy_app = typer.Typer(add_completion=False, help="Bounce-probe accuracy validation for provider classification.")
app.add_typer(_accuracy_app, name="accuracy")


def _accuracy_config(output: Path | None = None):
    """Lazily load accuracy config with optional output dir override."""
    from mail_municipalities.accuracy.config import AccuracyConfig

    cfg = AccuracyConfig()
    if output is not None:
        cfg = cfg.model_copy(update={"output_dir": output})
    return cfg


@_accuracy_app.command("sample")
def accuracy_sample_cmd(
    country: Annotated[
        Optional[str],
        typer.Argument(help="Country code: ch, de, at"),
    ] = None,
    all_countries: Annotated[
        bool,
        typer.Option("--all", help="Sample from all countries"),
    ] = False,
    size: Annotated[
        int,
        typer.Option("--size", help="Total sample size"),
    ] = 50,
    min_per_class: Annotated[
        int,
        typer.Option("--min-per-class", help="Minimum samples per provider class"),
    ] = 5,
    providers_dir: Annotated[
        Path,
        typer.Option("--providers-dir", help="Directory with provider classification output"),
    ] = Path("output/providers"),
    verbose: Annotated[
        bool,
        typer.Option("-v", "--verbose", help="Enable debug logging"),
    ] = False,
    output: Annotated[
        Optional[Path],
        typer.Option("-o", "--output", help="Custom output directory"),
    ] = None,
) -> None:
    """Create a stratified sample and prepare probes (no sending)."""
    if not country and not all_countries:
        typer.echo("Provide a country code (ch, de, at) or use --all", err=True)
        raise typer.Exit(code=1)

    cfg = _accuracy_config(output)
    setup_logging(verbose, log_path=cfg.output_dir / "accuracy.log")
    assert all_countries or country is not None
    countries: list[str] = ["de", "at", "ch"] if all_countries else [country]  # type: ignore[list-item]

    from mail_municipalities.accuracy.sampler import create_sample
    from mail_municipalities.accuracy.state import StateDB

    async def _run() -> None:
        async with StateDB(cfg.state_db_path) as state:
            await create_sample(countries, size, min_per_class, providers_dir, state)

    asyncio.run(_run())


@_accuracy_app.command("send")
def accuracy_send_cmd(
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run/--no-dry-run", help="Dry run (default: enabled)"),
    ] = True,
    confirm: Annotated[
        bool,
        typer.Option("--confirm", help="Skip interactive confirmation prompt"),
    ] = False,
    batch_size: Annotated[
        Optional[int],
        typer.Option("--batch-size", help="Emails per batch before pause"),
    ] = None,
    max_probes: Annotated[
        Optional[int],
        typer.Option("--max-probes", help="Maximum probes to send this run"),
    ] = None,
    rate: Annotated[
        Optional[float],
        typer.Option("--rate", help="Emails per second"),
    ] = None,
    verbose: Annotated[
        bool,
        typer.Option("-v", "--verbose", help="Enable debug logging"),
    ] = False,
    output: Annotated[
        Optional[Path],
        typer.Option("-o", "--output", help="Custom output directory"),
    ] = None,
) -> None:
    """Send probe emails to sampled municipality domains."""
    cfg = _accuracy_config(output)
    setup_logging(verbose, log_path=cfg.output_dir / "accuracy.log")

    from mail_municipalities.accuracy.sender import send_probes
    from mail_municipalities.accuracy.state import StateDB

    async def _run() -> None:
        async with StateDB(cfg.state_db_path) as state:
            await send_probes(
                state,
                cfg,
                max_probes=max_probes,
                dry_run=dry_run,
                confirm=confirm,
                batch_size=batch_size,
                rate=rate,
            )

    asyncio.run(_run())


@_accuracy_app.command("collect")
def accuracy_collect_cmd(
    poll_once: Annotated[
        bool,
        typer.Option("--poll-once", help="Single IMAP check then exit"),
    ] = False,
    wait_hours: Annotated[
        Optional[float],
        typer.Option("--wait-hours", help="Max hours to keep polling"),
    ] = None,
    poll_interval: Annotated[
        Optional[int],
        typer.Option("--poll-interval", help="Seconds between IMAP polls"),
    ] = None,
    verbose: Annotated[
        bool,
        typer.Option("-v", "--verbose", help="Enable debug logging"),
    ] = False,
    output: Annotated[
        Optional[Path],
        typer.Option("-o", "--output", help="Custom output directory"),
    ] = None,
) -> None:
    """Poll IMAP for NDRs and match to sent probes."""
    cfg = _accuracy_config(output)
    setup_logging(verbose, log_path=cfg.output_dir / "accuracy.log")

    from mail_municipalities.accuracy.collector import collect_ndrs
    from mail_municipalities.accuracy.state import StateDB

    async def _run() -> None:
        async with StateDB(cfg.state_db_path) as state:
            await collect_ndrs(
                state,
                cfg,
                poll_once=poll_once,
                wait_hours=wait_hours,
                poll_interval=poll_interval,
            )

    asyncio.run(_run())


@_accuracy_app.command("report")
def accuracy_report_cmd(
    latex: Annotated[
        bool,
        typer.Option("--latex", help="Export LaTeX tables for the paper"),
    ] = False,
    verbose: Annotated[
        bool,
        typer.Option("-v", "--verbose", help="Enable debug logging"),
    ] = False,
    output: Annotated[
        Optional[Path],
        typer.Option("-o", "--output", help="Custom output directory"),
    ] = None,
) -> None:
    """Compute and display accuracy metrics from collected NDRs."""
    cfg = _accuracy_config(output)
    setup_logging(verbose, log_path=cfg.output_dir / "accuracy.log")

    from mail_municipalities.accuracy.metrics import compute_accuracy
    from mail_municipalities.accuracy.report import export_report_json, export_report_latex, print_report
    from mail_municipalities.accuracy.state import StateDB

    async def _run() -> None:
        async with StateDB(cfg.state_db_path) as state:
            report = await compute_accuracy(state)
        print_report(report)
        export_report_json(report, cfg.output_dir)
        if latex:
            export_report_latex(report, cfg.output_dir)

    asyncio.run(_run())


@_accuracy_app.command("status")
def accuracy_status_cmd(
    verbose: Annotated[
        bool,
        typer.Option("-v", "--verbose", help="Enable debug logging"),
    ] = False,
    output: Annotated[
        Optional[Path],
        typer.Option("-o", "--output", help="Custom output directory"),
    ] = None,
) -> None:
    """Show current probe lifecycle state."""
    cfg = _accuracy_config(output)
    setup_logging(verbose, log_path=cfg.output_dir / "accuracy.log")

    from mail_municipalities.accuracy.report import print_status
    from mail_municipalities.accuracy.state import StateDB

    async def _run() -> None:
        async with StateDB(cfg.state_db_path) as state:
            await print_status(state)

    asyncio.run(_run())


@_accuracy_app.command("check")
def accuracy_check_cmd(
    domains: Annotated[
        list[str],
        typer.Argument(help="One or more email domains to look up"),
    ],
    providers_dir: Annotated[
        Path,
        typer.Option("--providers-dir", help="Directory with provider classification output"),
    ] = Path("output/providers"),
    verbose: Annotated[
        bool,
        typer.Option("-v", "--verbose", help="Enable debug logging"),
    ] = False,
    output: Annotated[
        Optional[Path],
        typer.Option("-o", "--output", help="Custom output directory"),
    ] = None,
) -> None:
    """Spot-check provider classification for specific domains."""
    cfg = _accuracy_config(output)
    setup_logging(verbose, log_path=cfg.output_dir / "accuracy.log")

    from mail_municipalities.accuracy.check import check_domains, print_check_table
    from mail_municipalities.accuracy.state import StateDB

    async def _run() -> None:
        async with StateDB(cfg.state_db_path) as state:
            results = await check_domains(domains, providers_dir, state)
        print_check_table(results)

    asyncio.run(_run())


_accuracy_standalone_app = typer.Typer(add_completion=False)


@_accuracy_standalone_app.command("sample")
def _accuracy_sample_main(
    country: Annotated[
        Optional[str],
        typer.Argument(help="Country code: ch, de, at"),
    ] = None,
    all_countries: Annotated[
        bool,
        typer.Option("--all", help="Sample from all countries"),
    ] = False,
    size: Annotated[
        int,
        typer.Option("--size", help="Total sample size"),
    ] = 50,
    min_per_class: Annotated[
        int,
        typer.Option("--min-per-class", help="Minimum samples per provider class"),
    ] = 5,
    providers_dir: Annotated[
        Path,
        typer.Option("--providers-dir", help="Directory with provider classification output"),
    ] = Path("output/providers"),
    verbose: Annotated[
        bool,
        typer.Option("-v", "--verbose", help="Enable debug logging"),
    ] = False,
    output: Annotated[
        Optional[Path],
        typer.Option("-o", "--output", help="Custom output directory"),
    ] = None,
) -> None:
    """Create a stratified sample and prepare probes (no sending)."""
    accuracy_sample_cmd(
        country=country,
        all_countries=all_countries,
        size=size,
        min_per_class=min_per_class,
        providers_dir=providers_dir,
        verbose=verbose,
        output=output,
    )


@_accuracy_standalone_app.command("send")
def _accuracy_send_main(
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run/--no-dry-run", help="Dry run (default: enabled)"),
    ] = True,
    confirm: Annotated[
        bool,
        typer.Option("--confirm", help="Skip interactive confirmation prompt"),
    ] = False,
    batch_size: Annotated[
        Optional[int],
        typer.Option("--batch-size", help="Emails per batch before pause"),
    ] = None,
    max_probes: Annotated[
        Optional[int],
        typer.Option("--max-probes", help="Maximum probes to send this run"),
    ] = None,
    rate: Annotated[
        Optional[float],
        typer.Option("--rate", help="Emails per second"),
    ] = None,
    verbose: Annotated[
        bool,
        typer.Option("-v", "--verbose", help="Enable debug logging"),
    ] = False,
    output: Annotated[
        Optional[Path],
        typer.Option("-o", "--output", help="Custom output directory"),
    ] = None,
) -> None:
    """Send probe emails to sampled municipality domains."""
    accuracy_send_cmd(
        dry_run=dry_run,
        confirm=confirm,
        batch_size=batch_size,
        max_probes=max_probes,
        rate=rate,
        verbose=verbose,
        output=output,
    )


@_accuracy_standalone_app.command("collect")
def _accuracy_collect_main(
    poll_once: Annotated[
        bool,
        typer.Option("--poll-once", help="Single IMAP check then exit"),
    ] = False,
    wait_hours: Annotated[
        Optional[float],
        typer.Option("--wait-hours", help="Max hours to keep polling"),
    ] = None,
    poll_interval: Annotated[
        Optional[int],
        typer.Option("--poll-interval", help="Seconds between IMAP polls"),
    ] = None,
    verbose: Annotated[
        bool,
        typer.Option("-v", "--verbose", help="Enable debug logging"),
    ] = False,
    output: Annotated[
        Optional[Path],
        typer.Option("-o", "--output", help="Custom output directory"),
    ] = None,
) -> None:
    """Poll IMAP for NDRs and match to sent probes."""
    accuracy_collect_cmd(
        poll_once=poll_once,
        wait_hours=wait_hours,
        poll_interval=poll_interval,
        verbose=verbose,
        output=output,
    )


@_accuracy_standalone_app.command("report")
def _accuracy_report_main(
    latex: Annotated[
        bool,
        typer.Option("--latex", help="Export LaTeX tables for the paper"),
    ] = False,
    verbose: Annotated[
        bool,
        typer.Option("-v", "--verbose", help="Enable debug logging"),
    ] = False,
    output: Annotated[
        Optional[Path],
        typer.Option("-o", "--output", help="Custom output directory"),
    ] = None,
) -> None:
    """Compute and display accuracy metrics from collected NDRs."""
    accuracy_report_cmd(latex=latex, verbose=verbose, output=output)


@_accuracy_standalone_app.command("status")
def _accuracy_status_main(
    verbose: Annotated[
        bool,
        typer.Option("-v", "--verbose", help="Enable debug logging"),
    ] = False,
    output: Annotated[
        Optional[Path],
        typer.Option("-o", "--output", help="Custom output directory"),
    ] = None,
) -> None:
    """Show current probe lifecycle state."""
    accuracy_status_cmd(verbose=verbose, output=output)


@_accuracy_standalone_app.command("check")
def _accuracy_check_main(
    domains: Annotated[
        list[str],
        typer.Argument(help="One or more email domains to look up"),
    ],
    providers_dir: Annotated[
        Path,
        typer.Option("--providers-dir", help="Directory with provider classification output"),
    ] = Path("output/providers"),
    verbose: Annotated[
        bool,
        typer.Option("-v", "--verbose", help="Enable debug logging"),
    ] = False,
    output: Annotated[
        Optional[Path],
        typer.Option("-o", "--output", help="Custom output directory"),
    ] = None,
) -> None:
    """Spot-check provider classification for specific domains."""
    accuracy_check_cmd(domains=domains, providers_dir=providers_dir, verbose=verbose, output=output)


def resolve() -> None:
    """Entry point for 'resolve' script."""
    _resolve_app()


def classify() -> None:
    """Entry point for 'classify' script."""
    _classify_app()


def analyze() -> None:
    """Entry point for 'analyze' script."""
    _analyze_app()


def scan() -> None:
    """Entry point for 'scan' script."""
    _scan_app()


def accuracy() -> None:
    """Entry point for 'accuracy' script."""
    _accuracy_standalone_app()
