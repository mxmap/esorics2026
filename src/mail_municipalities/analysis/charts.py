"""Generate publication-quality charts for the paper."""

from __future__ import annotations

from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
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


_PROVIDER_ORDER = ["Microsoft", "Google", "AWS", "Domestic", "Foreign", "Unknown"]
_PROVIDER_PALETTE = {
    "Microsoft": "#4e79a7",
    "Google": "#f28e2b",
    "AWS": "#e15759",
    "Domestic": "#76b7b2",
    "Foreign": "#edc948",
    "Unknown": "#bab0ac",
}


def _provider_data(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build stacked-bar shares and confidence/gateway metrics per country."""
    prov_map = {
        "microsoft": "Microsoft",
        "google": "Google",
        "aws": "AWS",
        "domestic": "Domestic",
        "foreign": "Foreign",
        "unknown": "Unknown",
    }
    share_rows: list[dict] = []
    meta_rows: list[dict] = []

    for cc in COUNTRIES:
        sub = df[df["country"] == cc.upper()]
        n = len(sub)
        label = COUNTRY_LABELS[cc]

        for raw, nice in prov_map.items():
            cnt = len(sub[sub["provider"] == raw])
            share_rows.append(
                {"country": label, "provider": nice, "share": cnt / n * 100}
            )

        gw = sub["gateway"].notna().sum()
        meta_rows.append(
            {
                "country": label,
                "Avg. confidence": sub["confidence"].mean(),
                "Gateway share": gw / n * 100,
            }
        )

    shares = pd.DataFrame(share_rows)
    meta = pd.DataFrame(meta_rows)
    return shares, meta


def generate_provider_figure() -> Path:
    """Generate provider classification chart."""
    df = _load_data()
    shares, meta = _provider_data(df)
    _apply_style()

    fig, (ax_prov, ax_meta) = plt.subplots(
        1,
        2,
        figsize=(7.0, 2.6),
        gridspec_kw={"width_ratios": [2, 1], "wspace": 0.3},
    )
    fig.subplots_adjust(bottom=0.3)

    # --- (a) 100% stacked horizontal bar ---
    country_order = [COUNTRY_LABELS[cc] for cc in COUNTRIES]
    y_pos = np.arange(len(country_order))
    bar_h = 0.5
    left = np.zeros(len(country_order))

    for prov in _PROVIDER_ORDER:
        widths = []
        for country in country_order:
            val = shares[(shares["country"] == country) & (shares["provider"] == prov)][
                "share"
            ].values
            widths.append(val[0] if len(val) > 0 else 0.0)
        widths_arr = np.array(widths)
        ax_prov.barh(
            y_pos,
            widths_arr,
            height=bar_h,
            left=left,
            color=_PROVIDER_PALETTE[prov],
            label=prov,
            edgecolor="white",
            linewidth=0.4,
        )
        # Labels inside segments
        for j, (offset, w) in enumerate(zip(left, widths_arr)):
            if w >= 6:
                ax_prov.text(
                    offset + w / 2,
                    y_pos[j],
                    f"{w:.0f}%",
                    ha="center",
                    va="center",
                    fontsize=6,
                    color="#333333",
                )
        left += widths_arr

    ax_prov.set_yticks(y_pos)
    ax_prov.set_yticklabels(country_order)
    ax_prov.set_xlim(0, 100)
    ax_prov.set_xlabel("Share of municipalities (%)", fontsize=7.5)
    ax_prov.set_title("(a) Provider distribution", fontsize=8, fontweight="bold")
    ax_prov.tick_params(axis="both", labelsize=7)
    ax_prov.invert_yaxis()
    sns.despine(ax=ax_prov, offset=10, trim=True)

    ax_prov.legend(
        fontsize=6,
        loc="lower center",
        bbox_to_anchor=(0.5, -0.55),
        ncol=6,
        frameon=False,
        columnspacing=1.0,
    )

    # --- (b) Confidence + Gateway grouped bar ---
    metrics = ["Avg. confidence", "Gateway share"]
    metric_colors = ["#4e79a7", "#e15759"]
    x_pos = np.arange(len(country_order))
    bar_w = 0.3

    for i, (metric, color) in enumerate(zip(metrics, metric_colors)):
        vals = [
            meta[meta["country"] == c][metric].values[0] for c in country_order
        ]
        bars = ax_meta.bar(
            x_pos + i * bar_w - bar_w / 2,
            vals,
            width=bar_w,
            color=color,
            label=metric,
            edgecolor="white",
            linewidth=0.4,
        )
        for bar, v in zip(bars, vals):
            ax_meta.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height() + 1.5,
                f"{v:.0f}%",
                ha="center",
                va="bottom",
                fontsize=6,
            )

    ax_meta.set_xticks(x_pos)
    ax_meta.set_xticklabels(country_order, fontsize=7)
    ax_meta.set_ylim(0, 105)
    ax_meta.set_ylabel("(%)", fontsize=7.5)
    ax_meta.set_title("(b) Confidence & gateways", fontsize=8, fontweight="bold")
    ax_meta.tick_params(axis="both", labelsize=7)
    sns.despine(ax=ax_meta, offset=10, trim=True)

    ax_meta.legend(
        fontsize=6,
        loc="lower center",
        bbox_to_anchor=(0.5, -0.55),
        ncol=2,
        frameon=False,
    )

    out_pdf = OUTPUT_DIR / "figure_provider.pdf"
    out_png = OUTPUT_DIR / "figure_provider.png"
    fig.savefig(out_pdf, format="pdf")
    fig.savefig(out_png, format="png")
    plt.close(fig)

    print(f"Saved: {out_pdf}")
    print(f"Saved: {out_png}")
    return out_pdf


def main() -> None:
    generate_figure()
    generate_provider_figure()
