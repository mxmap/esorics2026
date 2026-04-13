"""Generate publication-quality charts for the paper."""

from __future__ import annotations

from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from mail_municipalities.analysis.helpers import COUNTRIES, COUNTRY_NAMES

matplotlib.use("pdf")


EXPORT_PATH = Path("output/export.xlsx")
OUTPUT_DIR = Path("output/analysis")

COUNTRY_LABELS = {cc: COUNTRY_NAMES[cc] for cc in COUNTRIES}

_CAT_MAP = {
    "de-based": "Domestic",
    "at-based": "Domestic",
    "ch-based": "Domestic",
    "us-cloud": "US Cloud",
    "foreign": "Other",
    "unknown": "Other",
}

_METRIC_COLS = [
    ("has_spf", "SPF\n(any)"),
    ("has_good_spf", "SPF\n(strict)"),
    ("has_dmarc", "DMARC\n(any)"),
    ("has_good_dmarc", "DMARC\n(enforce)"),
    ("dane_supported", "DANE"),
]
_METRIC_ORDER = [label for _, label in _METRIC_COLS]

_COUNTRY_PALETTE = {
    COUNTRY_LABELS["de"]: "#d3c4f7",  # violet
    COUNTRY_LABELS["at"]: "#98daa7",  # green
    COUNTRY_LABELS["ch"]: "#ffb974",  # orange
}


def _load_data() -> pd.DataFrame:
    return pd.read_excel(EXPORT_PATH, sheet_name="Municipalities")


def _regional_security_by_provider(df: pd.DataFrame) -> pd.DataFrame:
    """Per-region security rates, split by provider category."""
    valid = df[df["scan_valid"] == True].copy()  # noqa: E712
    valid["cat"] = valid["category"].map(_CAT_MAP)

    rows: list[dict] = []
    for cc in COUNTRIES:
        for cat in ["Domestic", "US Cloud"]:
            sub = valid[(valid["country"] == cc.upper()) & (valid["cat"] == cat)]
            for region, grp in sub.groupby("region"):
                n = len(grp)
                if n < 5:  # drop city-states/tiny cantons (<28 of 15k, 0.2%)
                    continue
                for col, label in _METRIC_COLS:
                    rows.append(
                        {
                            "country": COUNTRY_LABELS[cc],
                            "provider": cat,
                            "region": region,
                            "metric": label,
                            "value": grp[col].sum() / n * 100,
                        }
                    )
    return pd.DataFrame(rows)


def _plot_panel(
    ax: matplotlib.axes.Axes,
    data: pd.DataFrame,
    title: str,
) -> None:
    sns.boxplot(
        data=data,
        x="metric",
        y="value",
        hue="country",
        ax=ax,
        palette=_COUNTRY_PALETTE,
        order=_METRIC_ORDER,
        width=0.6,
        linewidth=0.5,
        fliersize=2,
        flierprops={"alpha": 0.6, "markeredgewidth": 0.3},
        boxprops={"alpha": 1},
        whiskerprops={"linewidth": 0.5},
        capprops={"linewidth": 0.5},
        medianprops={"linewidth": 0.8},
    )
    # Subtle separators between metric groups
    for x_sep in [1.5, 3.5]:
        ax.axvline(x_sep, color="#8f8f8f", linewidth=0.4, linestyle="--")

    ax.set_ylim(-3, 105)
    ax.set_ylabel("Share of municipalities (%)", fontsize=7.5)
    ax.set_xlabel("")
    ax.set_title(title, fontsize=8, fontweight="bold")
    ax.tick_params(axis="both", labelsize=7)
    sns.despine(ax=ax, offset=10, trim=True)

    ax.get_legend().remove()


def generate_figure() -> Path:
    """Generate horizontal box plot chart with provider split."""
    df = _load_data()
    rdf = _regional_security_by_provider(df)

    _apply_style()

    fig, (ax_dom, ax_us) = plt.subplots(
        1,
        2,
        sharey=True,
        figsize=(7.0, 3.2),
        gridspec_kw={"wspace": 0.15},
    )
    fig.subplots_adjust(bottom=0.22)

    dom = rdf[rdf["provider"] == "Domestic"]
    us = rdf[rdf["provider"] == "US Cloud"]

    _plot_panel(ax_dom, dom, "(a) Domestic providers")
    _plot_panel(ax_us, us, "(b) US Cloud providers")
    ax_us.set_ylabel("")

    # Shared legend below both panels
    handles, labels = ax_dom.get_legend_handles_labels()
    fig.legend(
        handles[:3],
        labels[:3],
        loc="lower center",
        ncol=3,
        fontsize=7,
        frameon=False,
        bbox_to_anchor=(0.5, -0.02),
    )

    out_pdf = OUTPUT_DIR / "figure_security_provider.pdf"
    out_png = OUTPUT_DIR / "figure_security_provider.png"
    fig.savefig(out_pdf, format="pdf")
    fig.savefig(out_png, format="png")
    plt.close(fig)

    print(f"Saved: {out_pdf}")
    print(f"Saved: {out_png}")
    return out_pdf


def _apply_style() -> None:
    sns.set_theme(style="ticks", font_scale=0.9)
    plt.rcParams.update(
        {
            "font.family": "serif",
            "font.serif": ["Charter", "Palatino", "DejaVu Serif"],
            "figure.dpi": 300,
            "savefig.dpi": 300,
            "savefig.bbox": "tight",
            "savefig.pad_inches": 0.03,
            "axes.linewidth": 0.5,
            "grid.linewidth": 0.3,
        }
    )


def main() -> None:
    generate_figure()
