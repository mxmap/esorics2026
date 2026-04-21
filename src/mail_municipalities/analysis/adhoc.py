"""Ad-hoc analysis: backing data for Discussion §cross-cutting observations.

Each function corresponds to a specific claim in the Discussion text.
Run via CLI:  uv run analyze adhoc
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from mail_municipalities.export import OUTPUT_DIR, flatten_provider, flatten_security, load_json

# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

_COUNTRIES = ("ch", "de", "at")

# Security metrics (DKIM excluded per paper scope)
_SECURITY_COLS = ["has_spf", "has_good_spf", "has_dmarc", "has_good_dmarc", "dane_supported"]


def load_merged_dataframe(output_dir: Path = OUTPUT_DIR) -> pd.DataFrame:  # pragma: no cover
    """Load provider + security data for all countries into a single DataFrame.

    Reuses the same flatten/merge logic as ``export_xlsx`` so the numbers
    are guaranteed to match the export.  Only rows with a resolved domain
    and a valid security scan are kept.
    """
    frames = []
    for cc in _COUNTRIES:
        provider_data = load_json(output_dir / "providers" / f"providers_{cc}.json")
        security_data = load_json(output_dir / "security" / f"security_{cc}.json")

        cc_upper = cc.upper()
        prov_df = pd.DataFrame([flatten_provider(m, cc_upper) for m in provider_data["municipalities"]])
        sec_df = pd.DataFrame([flatten_security(m) for m in security_data["municipalities"]])

        merged = prov_df.merge(sec_df, on=["code", "domain"], how="left")
        frames.append(merged)

    df = pd.concat(frames, ignore_index=True)
    df.sort_values(["country", "code"], inplace=True, ignore_index=True)

    # Keep only rows with a domain and a valid security scan
    df = df[df["domain"].notna() & (df["scan_valid"] == True)].copy()  # noqa: E712
    return df  # pyright: ignore[reportReturnType]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _pct(series: object) -> float:
    """Compute percentage of True values, returning 0.0 for empty series."""
    s: pd.Series = series  # type: ignore[assignment]
    if len(s) == 0:
        return 0.0
    return round(float(s.mean()) * 100, 1)


def _security_row(label: str, subset: pd.DataFrame) -> dict:  # pyright: ignore[reportMissingTypeArgument]
    """Build one summary row: label, n, and % for each security metric."""
    return {
        "group": label,
        "n": len(subset),
        **{col: _pct(subset[col]) for col in _SECURITY_COLS},
    }


def _print_df(df: pd.DataFrame, float_fmt: str = "{:.1f}") -> None:  # pragma: no cover
    """Print DataFrame with consistent formatting."""
    pd.set_option("display.width", 200)
    pd.set_option("display.float_format", float_fmt.format)
    print(df.to_string(index=False))


# ---------------------------------------------------------------------------
# 1. "Microsoft 365 leads on well-configured SPF everywhere"
#    "DMARC remains below 18% well-configured across all provider categories"
# ---------------------------------------------------------------------------


def provider_country_security(df: pd.DataFrame) -> None:  # pragma: no cover
    """Provider × country security matrix.

    Backs the claim that MS365 leads on good SPF in every country,
    and that good DMARC stays below 18% for all provider–country pairs.
    """
    print("=" * 80)
    print("1. PROVIDER × COUNTRY — SECURITY METRICS")
    print("   Claim: MS365 leads on good SPF everywhere;")
    print("   good DMARC < 18% for all provider categories and countries")
    print("=" * 80)
    print()

    rows = []
    for provider in ("microsoft", "domestic"):
        for country in ("DE", "AT", "CH"):
            subset = df[(df["provider"] == provider) & (df["country"] == country)]
            if len(subset) < 10:
                continue
            rows.append({"provider": provider, "country": country, **_security_row("", pd.DataFrame(subset))})

    result = pd.DataFrame(rows)[["provider", "country", "n", *_SECURITY_COLS]]
    _print_df(pd.DataFrame(result))

    # Verify claims
    print()
    for country in ("DE", "AT", "CH"):
        ms = df[(df["provider"] == "microsoft") & (df["country"] == country)]
        dom = df[(df["provider"] == "domestic") & (df["country"] == country)]
        delta = _pct(ms["has_good_spf"]) - _pct(dom["has_good_spf"])
        print(f"  {country}: MS365 good SPF advantage over domestic: {delta:+.1f} pp")

    print()
    print("  Max good DMARC across all provider×country pairs:")
    for provider in ("microsoft", "domestic", "foreign", "unknown"):
        for country in ("DE", "AT", "CH"):
            subset = df[(df["provider"] == provider) & (df["country"] == country)]
            if len(subset) >= 10:
                val = _pct(subset["has_good_dmarc"])
                if val > 15:
                    print(f"    {provider}/{country}: {val}%")
    print()


# ---------------------------------------------------------------------------
# 2. "Gateways boost DMARC presence but reduce well-configured SPF"
#    "Strongest in Austria, where over a third of municipalities use a gateway"
# ---------------------------------------------------------------------------


def gateway_prevalence_and_effect(df: pd.DataFrame) -> None:  # pragma: no cover
    """Gateway prevalence by country and overall security deltas.

    Backs the claim that gateways trade SPF quality for DMARC presence,
    and that Austria has the highest gateway adoption.
    """
    print("=" * 80)
    print("2. GATEWAY PREVALENCE & EFFECT BY COUNTRY")
    print("   Claim: gateways boost DMARC but reduce good SPF;")
    print("   Austria > 1/3 gateway adoption")
    print("=" * 80)
    print()

    df = df.copy()
    df["has_gateway"] = df["gateway"].notna() & (df["gateway"] != "")

    # Prevalence by country
    print("  Gateway prevalence:")
    for country in ("DE", "AT", "CH"):
        csub = df[df["country"] == country]
        gw_n = csub["has_gateway"].sum()
        print(f"    {country}: {gw_n}/{len(csub)} = {gw_n / len(csub) * 100:.1f}%")
    print()

    # Overall effect
    gw = df[df["has_gateway"]]
    no_gw = df[~df["has_gateway"]]
    print("  Overall delta (gateway minus no-gateway):")
    for col, label in [
        ("has_spf", "SPF"),
        ("has_good_spf", "Good SPF"),
        ("has_dmarc", "DMARC"),
        ("has_good_dmarc", "Good DMARC"),
    ]:
        delta = _pct(gw[col]) - _pct(no_gw[col])
        print(f"    {label:>10}: {delta:+.1f} pp")
    print()

    # Per-country breakdown
    rows = []
    for country in ("DE", "AT", "CH"):
        csub = df[df["country"] == country]
        for has_gw, label in [(True, "gateway"), (False, "no gateway")]:
            subset = csub[csub["has_gateway"] == has_gw]  # pyright: ignore[reportArgumentType]
            rows.append(_security_row(f"{country} {label}", subset))  # pyright: ignore[reportArgumentType]

    _print_df(pd.DataFrame(rows))
    print()


# ---------------------------------------------------------------------------
# 3. "SEPPmail provisions DMARC but not SPF; Sophos does the reverse"
# ---------------------------------------------------------------------------


def gateway_product_profiles(df: pd.DataFrame) -> None:  # pragma: no cover
    """Per-gateway-product security breakdown.

    Backs the claim that SEPPmail and Sophos auto-configure
    different mechanisms.
    """
    print("=" * 80)
    print("3. GATEWAY PRODUCT SECURITY PROFILES")
    print("   Claim: SEPPmail → high DMARC / low good SPF;")
    print("   Sophos → high good SPF / low DMARC")
    print("=" * 80)
    print()

    rows = []
    gw_names = df["gateway"].dropna().unique()
    for gw_name in gw_names:
        if not gw_name or not str(gw_name).strip():
            continue
        subset = df[df["gateway"] == gw_name]  # pyright: ignore[reportArgumentType]
        if len(subset) >= 20:
            rows.append(_security_row(str(gw_name), subset))  # pyright: ignore[reportArgumentType]

    result = pd.DataFrame(rows).sort_values("n", ascending=False)
    _print_df(result)
    print()


# ---------------------------------------------------------------------------
# 4. "Strato auto-sets DMARC; Infomaniak stands out with DANE"
# ---------------------------------------------------------------------------

# Mapping from MX hostname substring to infrastructure label (first match wins)
_MX_INFRA_RULES: list[tuple[str, str]] = [
    ("rzone.de", "Strato"),
    ("infomaniak", "Infomaniak"),
    ("ionos.de", "IONOS"),
    ("ionos.com", "IONOS"),
    ("kasserver.com", "All-Inkl"),
    ("format-ag.ch", "format-ag"),
    ("hostpoint.ch", "Hostpoint"),
    ("ekom21", "ekom21"),
    ("kgrz", "ekom21"),
    ("kvnbw.de", "KVNBW"),
    ("seppmail", "SEPPmail"),
    ("sophos", "Sophos"),
]


def _classify_mx_infra(mx: str | float) -> str:
    """Map a primary MX hostname to an infrastructure label."""
    if not isinstance(mx, str) or not mx.strip():
        return "other"
    mx_lower = mx.lower().rstrip(".")
    for substring, label in _MX_INFRA_RULES:
        if substring in mx_lower:
            return label
    return "other"


def domestic_operator_profiles(df: pd.DataFrame) -> None:  # pragma: no cover
    """Security profiles for key domestic operators.

    Backs the claim that 'domestic' conceals wide heterogeneity,
    with Strato inflating DMARC and Infomaniak leading on DANE.
    """
    print("=" * 80)
    print("4. DOMESTIC OPERATOR HETEROGENEITY")
    print("   Claim: Strato auto-sets DMARC; Infomaniak has substantial DANE")
    print("=" * 80)
    print()

    domestic = df[df["provider"] == "domestic"].copy()
    domestic["primary_mx"] = domestic["mx"].str.split(";").str[0].str.strip()  # pyright: ignore[reportAttributeAccessIssue]
    domestic["infra"] = domestic["primary_mx"].apply(_classify_mx_infra)  # pyright: ignore[reportAttributeAccessIssue]

    rows = []
    for infra_label in domestic["infra"].unique():  # pyright: ignore[reportAttributeAccessIssue]
        subset = domestic[domestic["infra"] == infra_label]  # pyright: ignore[reportArgumentType]
        if len(subset) >= 20 and infra_label != "other":
            rows.append(_security_row(infra_label, subset))  # pyright: ignore[reportArgumentType]

    result = pd.DataFrame(rows).sort_values("n", ascending=False)
    _print_df(result)

    # Highlight the specific claims
    strato = domestic[domestic["infra"] == "Strato"]
    infomaniak = domestic[domestic["infra"] == "Infomaniak"]
    print()
    if len(strato) > 0:
        print(f"  Strato (n={len(strato)}):")
        print(f"    DMARC={_pct(strato['has_dmarc'])}%, good DMARC={_pct(strato['has_good_dmarc'])}%")
        print(f"    SPF={_pct(strato['has_spf'])}%, good SPF={_pct(strato['has_good_spf'])}%")
    if len(infomaniak) > 0:
        print(f"  Infomaniak (n={len(infomaniak)}):")
        print(f"    DANE={_pct(infomaniak['dane_supported'])}%")
        print(f"    DMARC={_pct(infomaniak['has_dmarc'])}%, good SPF={_pct(infomaniak['has_good_spf'])}%")
    print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:  # pragma: no cover
    """Run all analyses that back the Discussion claims."""
    df = load_merged_dataframe()
    print(f"\nLoaded {len(df)} municipalities with domain + valid scan\n")

    provider_country_security(df)
    gateway_prevalence_and_effect(df)
    gateway_product_profiles(df)
    domestic_operator_profiles(df)


if __name__ == "__main__":  # pragma: no cover
    main()
