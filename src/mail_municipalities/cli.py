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


@app.command("analyze")
def analyze_cmd(
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
    if all_countries:
        from mail_municipalities.provider_classification.combined_analysis import (
            export_combined_latex,
            print_combined_summary,
        )

        if latex:
            export_combined_latex()
        else:
            print_combined_summary()
        return

    from mail_municipalities.provider_classification.analyze import main

    main(data_path, latex=latex)


def _sec_analyze_impl(
    data_path: Path | None = None,
    all_countries: bool = False,
    latex: bool = False,
) -> None:
    """Shared sec-analyze implementation."""
    if all_countries:
        from mail_municipalities.analysis.security_combined import (
            export_combined_security_latex,
            print_combined_security_summary,
        )

        if latex:
            export_combined_security_latex()
        else:
            print_combined_security_summary()
        return

    from mail_municipalities.analysis.security_latex import main as security_main

    security_main(data_path, latex=latex)


@app.command("sec-analyze")
def sec_analyze_cmd(
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
    _sec_analyze_impl(data_path, all_countries, latex)


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


_analyze_app = typer.Typer(add_completion=False)


@_analyze_app.command()
def _analyze_main(
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
    if all_countries:
        from mail_municipalities.provider_classification.combined_analysis import (
            export_combined_latex,
            print_combined_summary,
        )

        if latex:
            export_combined_latex()
        else:
            print_combined_summary()
        return

    from mail_municipalities.provider_classification.analyze import main

    main(data_path, latex=latex)


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


_sec_analyze_app = typer.Typer(add_completion=False)


@_sec_analyze_app.command()
def _sec_analyze_main(
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
    _sec_analyze_impl(data_path, all_countries, latex)


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


def sec_analyze() -> None:
    """Entry point for 'sec-analyze' script."""
    _sec_analyze_app()
