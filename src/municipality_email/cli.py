"""Typer CLI for municipality email domain collection."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Annotated, Optional

import typer

from municipality_email.log import setup as setup_logging

app = typer.Typer(add_completion=False)


def _get_config(cc: str):
    """Lazily import and instantiate a country config."""
    if cc == "ch":
        from municipality_email.countries.switzerland import SwitzerlandConfig

        return SwitzerlandConfig()
    elif cc == "de":
        from municipality_email.countries.germany import GermanyConfig

        return GermanyConfig()
    elif cc == "at":
        from municipality_email.countries.austria import AustriaConfig

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
    setup_logging(verbose)

    if not country and not all_countries:
        typer.echo("Provide a country code (ch, de, at) or use --all", err=True)
        raise typer.Exit(code=1)

    from municipality_email.pipeline import run_pipeline

    countries = ["ch", "de", "at"] if all_countries else [country]
    output_dir = output or Path("domains")
    data_base = Path("data")

    for cc in countries:
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
        typer.Option("--no-cache", help="Ignore scrape cache"),
    ] = False,
) -> None:
    """Resolve email domains for municipalities."""
    _resolve_impl(country, all_countries, dry_run, verbose, output, no_cache)


@app.command("classify")
def classify_cmd(
    country: Annotated[str, typer.Argument(help="Country code: ch, de, at")],
) -> None:
    """Provider classification (not yet implemented)."""
    typer.echo("Provider classification is not yet implemented.", err=True)
    raise typer.Exit(code=1)


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
        typer.Option("--no-cache", help="Ignore scrape cache"),
    ] = False,
) -> None:
    """Resolve email domains for municipalities."""
    _resolve_impl(country, all_countries, dry_run, verbose, output, no_cache)


_classify_app = typer.Typer(add_completion=False)


@_classify_app.command()
def _classify_main(
    country: Annotated[str, typer.Argument(help="Country code: ch, de, at")],
) -> None:
    """Provider classification (not yet implemented)."""
    typer.echo("Provider classification is not yet implemented.", err=True)
    raise typer.Exit(code=1)


def resolve() -> None:
    """Entry point for 'resolve' script."""
    _resolve_app()


def classify() -> None:
    """Entry point for 'classify' script."""
    _classify_app()
