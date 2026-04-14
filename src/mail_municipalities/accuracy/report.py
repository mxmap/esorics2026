"""Console and LaTeX reporting for accuracy metrics."""

from __future__ import annotations

import json
from pathlib import Path

from rich.console import Console
from rich.table import Table

from mail_municipalities.accuracy.metrics import EVAL_LABELS
from mail_municipalities.accuracy.models import AccuracyReport, ProbeStatus
from mail_municipalities.accuracy.state import StateDB

console = Console()


async def print_status(state: StateDB) -> None:
    """Print current probe lifecycle status."""
    counts = await state.status_counts()
    country_counts = await state.country_counts()

    table = Table(title="Probe Status", show_lines=True)
    table.add_column("Status", style="bold")
    table.add_column("Count", justify="right")
    total = 0
    for status in ProbeStatus:
        n = counts.get(status.value, 0)
        total += n
        table.add_row(status.value, str(n))
    table.add_row("Total", str(total), style="bold")
    console.print(table)

    if country_counts:
        ct = Table(title="By Country", show_lines=True)
        ct.add_column("Country", style="bold")
        ct.add_column("Count", justify="right")
        for cc in sorted(country_counts):
            ct.add_row(cc.upper(), str(country_counts[cc]))
        console.print(ct)


def print_report(report: AccuracyReport) -> None:
    """Print accuracy metrics to the console using Rich tables."""
    console.print()
    console.print("[bold]Accuracy Report[/bold]")
    console.print(f"  Generated:       {report.generated}")
    console.print(f"  Total probes:    {report.total_probes}")
    console.print(f"  Total sent:      {report.total_sent}")
    console.print(f"  NDRs received:   {report.total_ndrs}")
    console.print(f"  Response rate:   {report.response_rate:.1%}")
    console.print(f"  Overall accuracy:{report.overall_accuracy:.1%}")
    labels_str = ", ".join(report.weighted_f1_labels)
    console.print(f"  Weighted F1:     {report.weighted_f1:.3f}  ({labels_str})")
    console.print()

    # ── Per-class metrics ─────────────────────────────────────────
    mt = Table(title="Per-Class Metrics", show_lines=True)
    mt.add_column("Provider", style="bold")
    mt.add_column("Precision", justify="right")
    mt.add_column("Recall", justify="right")
    mt.add_column("F1", justify="right")
    mt.add_column("Support", justify="right")

    for label in EVAL_LABELS:
        if label not in report.per_class:
            continue
        m = report.per_class[label]
        mt.add_row(
            label,
            f"{m.precision:.3f}",
            f"{m.recall:.3f}",
            f"{m.f1:.3f}",
            str(m.support),
        )
    console.print(mt)

    # ── Confusion matrix ──────────────────────────────────────────
    labels = [
        label
        for label in EVAL_LABELS
        if label in report.confusion_matrix or any(label in row for row in report.confusion_matrix.values())
    ]
    if labels:
        cm = Table(title="Confusion Matrix (rows=predicted, cols=actual)", show_lines=True)
        cm.add_column("Predicted \\ Actual", style="bold")
        for label in labels:
            cm.add_column(label, justify="right")

        for pred in labels:
            row_data = report.confusion_matrix.get(pred, {})
            cells = [str(row_data.get(actual, 0)) for actual in labels]
            cm.add_row(pred, *cells)
        console.print(cm)


def export_report_json(report: AccuracyReport, output_dir: Path) -> Path:
    """Write the accuracy report as JSON."""
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "accuracy_report.json"
    path.write_text(json.dumps(report.model_dump(), indent=2, default=str))
    console.print(f"[green]Report written to {path}[/green]")
    return path


def export_report_latex(report: AccuracyReport, output_dir: Path) -> Path:
    """Export accuracy metrics as a LaTeX table."""
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "accuracy_report.tex"

    lines: list[str] = []
    lines.append(r"\begin{table}[t]")
    lines.append(r"\centering")
    lines.append(r"\caption{Provider classification accuracy (bounce-probe validation)}")
    lines.append(r"\label{tab:accuracy}")
    lines.append(r"\begin{tabular}{lrrrr}")
    lines.append(r"\toprule")
    lines.append(r"Provider & Precision & Recall & F1 & Support \\")
    lines.append(r"\midrule")

    for label in EVAL_LABELS:
        if label not in report.per_class:
            continue
        m = report.per_class[label]
        name = label.replace("-", " ").title()
        lines.append(f"{name} & {m.precision:.3f} & {m.recall:.3f} & {m.f1:.3f} & {m.support} \\\\")

    lines.append(r"\midrule")
    lines.append(f"Weighted F1 & \\multicolumn{{4}}{{r}}{{{report.weighted_f1:.3f}}} \\\\")
    lines.append(f"Overall accuracy & \\multicolumn{{4}}{{r}}{{{report.overall_accuracy:.1%}}} \\\\")
    lines.append(f"Response rate & \\multicolumn{{4}}{{r}}{{{report.response_rate:.1%}}} \\\\")
    lines.append(r"\bottomrule")
    lines.append(r"\end{tabular}")
    lines.append(r"\end{table}")

    path.write_text("\n".join(lines) + "\n")
    console.print(f"[green]LaTeX table written to {path}[/green]")
    return path
