"""Pipeline execution timestamps table for the paper appendix."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from rich.console import Console
from rich.table import Table

from .helpers import COUNTRIES, COUNTRY_NAMES

_STAGES: list[tuple[str, str, str]] = [
    ("Domain Resolution", "output/domains", "domains_{cc}.json"),
    ("Provider Classification", "output/providers", "providers_{cc}.json"),
    ("Security Scan", "output/security", "security_{cc}.json"),
]


def _read_mtime(path: Path) -> str:
    """Return file modification date as YYYY-MM-DD, or '—' if missing."""
    if not path.exists():
        return "—"
    ts = path.stat().st_mtime
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")


def _collect() -> list[tuple[str, dict[str, str]]]:
    """Collect timestamps for all stages and countries."""
    rows: list[tuple[str, dict[str, str]]] = []
    for label, directory, pattern in _STAGES:
        dates: dict[str, str] = {}
        for cc in COUNTRIES:
            path = Path(directory) / pattern.format(cc=cc)
            dates[cc] = _read_mtime(path)
        rows.append((label, dates))
    return rows


def print_summary() -> None:
    """Print pipeline timestamps to the console."""
    rows = _collect()
    table = Table(title="Pipeline Execution Dates")
    table.add_column("Stage")
    for cc in COUNTRIES:
        table.add_column(COUNTRY_NAMES[cc])
    for label, dates in rows:
        table.add_row(label, *(dates[cc] for cc in COUNTRIES))
    Console().print(table)


def export_latex() -> Path:
    """Write a LaTeX table of pipeline timestamps to output/analysis/."""
    rows = _collect()
    out_dir = Path("output/analysis")
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_path = out_dir / f"table_timestamps_{ts}.tex"

    header = " & ".join(COUNTRY_NAMES[cc] for cc in COUNTRIES)
    body_lines: list[str] = []
    for label, dates in rows:
        vals = " & ".join(dates[cc] for cc in COUNTRIES)
        body_lines.append(f"  {label:<24s} & {vals} \\\\")

    latex = (
        "\\begin{table}[h]\n"
        "\\centering\n"
        "\\caption{Pipeline execution dates per stage and country.}\n"
        "\\label{tab:pipeline-timestamps}\n"
        "\\begin{tabular}{lccc}\n"
        "\\toprule\n"
        f"  Stage & {header} \\\\\n"
        "\\midrule\n" + "\n".join(body_lines) + "\n"
        "\\bottomrule\n"
        "\\end{tabular}\n"
        "\\end{table}\n"
    )

    out_path.write_text(latex, encoding="utf-8")
    Console().print(f"[green]Wrote {out_path}[/green]")
    return out_path
