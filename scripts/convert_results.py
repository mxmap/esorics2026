"""Convert collaborator *_Result.json files into the repo's security_{cc}.json format.

Usage:
    uv run python scripts/convert_results.py
"""

from __future__ import annotations

import json
import subprocess
import sys
import time
from pathlib import Path

SECURITY_DIR = Path("output/security")
DOMAINS_DIR = Path("output/domains")

COUNTRIES = {
    "ch": {"result": "CH_Result.json", "code_width": None},
    "de": {"result": "DE_Result.json", "code_width": 8},
    "at": {"result": "AT_Result.json", "code_width": None},
}


def to_bool(v: object) -> bool:
    """Convert WAHR/FALSCH/0/None to bool."""
    if v == "WAHR":
        return True
    if v in ("FALSCH", 0, "0", None, "None"):
        return False
    return bool(v)


def normalize_code(code: int, width: int | None) -> str:
    """Convert int code to the string format used in domains JSON."""
    s = str(code)
    if width and len(s) < width:
        s = s.zfill(width)
    return s


def load_result(path: Path) -> list[dict]:
    with open(path, encoding="utf-8-sig") as f:
        raw = json.load(f)
    # Strip BOM from keys
    cleaned = []
    for entry in raw:
        cleaned.append({k.lstrip("\ufeff"): v for k, v in entry.items()})
    return cleaned


def load_domains(cc: str) -> dict:
    path = DOMAINS_DIR / f"domains_{cc}.json"
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def get_commit() -> str | None:
    result = subprocess.run(
        ["git", "rev-parse", "--short", "HEAD"],
        capture_output=True,
        text=True,
    )
    return result.stdout.strip() or None


def convert(cc: str) -> None:
    cfg = COUNTRIES[cc]
    result_path = SECURITY_DIR / cfg["result"]
    if not result_path.exists():
        print(f"  SKIP {cc}: {result_path} not found")
        return

    result_data = load_result(result_path)
    domains_data = load_domains(cc)

    # Build lookup: code_str -> domain municipality info
    domain_by_code: dict[str, dict] = {}
    for muni in domains_data["municipalities"]:
        domain_by_code[muni["code"]] = muni

    # Build lookup: code_str -> result entry
    result_by_code: dict[str, dict] = {}
    for entry in result_data:
        code_str = normalize_code(entry["code"], cfg["code_width"])
        result_by_code[code_str] = entry

    # Convert
    municipalities = []
    for muni in domains_data["municipalities"]:
        code = muni["code"]
        entry = result_by_code.get(code)

        if entry is None:
            # Municipality in domains but not in Result — mark as not scanned
            municipalities.append({
                "code": code,
                "name": muni["name"],
                "region": muni.get("region", ""),
                "domain": muni.get("emails", [""])[0] if muni.get("emails") else "",
                "mx_records": [],
                "dane": None,
                "dss": None,
                "scan_valid": False,
            })
            continue

        mx_raw = entry.get("mxrecordName")
        mx_records = [mx_raw] if mx_raw and mx_raw != 0 else []
        has_scan = bool(mx_records) or any(
            to_bool(entry.get(f)) for f in ("hasSpf", "hasDmarc", "hasDane")
        )

        emails = muni.get("emails", [])
        domain = emails[0] if emails else entry.get("emails", "")

        dane_supported = to_bool(entry.get("hasDane"))
        municipalities.append({
            "code": code,
            "name": muni["name"],
            "region": muni.get("region", ""),
            "domain": domain,
            "mx_records": mx_records,
            "dane": {"supported": dane_supported, "partial": dane_supported},
            "dss": {
                "has_spf": to_bool(entry.get("hasSpf")),
                "has_good_spf": to_bool(entry.get("hasGoodSpf")),
                "has_dmarc": to_bool(entry.get("hasDmarc")),
                "has_good_dmarc": to_bool(entry.get("hasGoodDmarc")),
                "has_dkim": False,  # not in Result.json
            },
            "scan_valid": has_scan,
        })

    municipalities.sort(key=lambda m: m["code"])

    scanned = sum(1 for m in municipalities if m["scan_valid"])
    counts = {
        "scanned": scanned,
        "dane_supported": sum(1 for m in municipalities if m["dane"] and m["dane"]["supported"]),
        "spf": sum(1 for m in municipalities if m["dss"] and m["dss"]["has_spf"]),
        "good_spf": sum(1 for m in municipalities if m["dss"] and m["dss"]["has_good_spf"]),
        "dmarc": sum(1 for m in municipalities if m["dss"] and m["dss"]["has_dmarc"]),
        "good_dmarc": sum(1 for m in municipalities if m["dss"] and m["dss"]["has_good_dmarc"]),
        "dkim": sum(1 for m in municipalities if m["dss"] and m["dss"]["has_dkim"]),
    }

    output = {
        "generated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "commit": get_commit(),
        "total": len(municipalities),
        "counts": counts,
        "municipalities": municipalities,
    }

    out_path = SECURITY_DIR / f"security_{cc}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2, separators=(",", ":"))

    print(f"  Wrote {out_path} ({out_path.stat().st_size / 1024:.0f} KB)")
    print(f"    total={output['total']}, scanned={counts['scanned']}, "
          f"dane={counts['dane_supported']}, spf={counts['spf']}, dmarc={counts['dmarc']}")

    # ── Consistency check ─────────────────────────────────────────────
    verify(cc, result_data, output, cfg["code_width"])


def verify(cc: str, result_data: list[dict], output: dict, code_width: int | None) -> None:
    """Compare converted output against source Result.json for consistency."""
    errors = []

    # Build output lookup
    out_by_code = {m["code"]: m for m in output["municipalities"]}

    # Check every Result entry is represented
    for entry in result_data:
        code = normalize_code(entry["code"], code_width)
        out = out_by_code.get(code)
        if out is None:
            errors.append(f"code {code} ({entry['name']}): missing from output")
            continue

        # Compare fields
        for src_field, out_path in [
            ("hasDane", ("dane", "supported")),
            ("hasSpf", ("dss", "has_spf")),
            ("hasGoodSpf", ("dss", "has_good_spf")),
            ("hasDmarc", ("dss", "has_dmarc")),
            ("hasGoodDmarc", ("dss", "has_good_dmarc")),
        ]:
            src_val = to_bool(entry.get(src_field))
            section = out.get(out_path[0])
            out_val = section[out_path[1]] if section else False
            if src_val != out_val:
                errors.append(
                    f"code {code} ({entry['name']}): {src_field} "
                    f"src={src_val} != out={out_val}"
                )

    # Check counts match manual recount
    recount_spf = sum(1 for m in output["municipalities"] if m["dss"] and m["dss"]["has_spf"])
    if recount_spf != output["counts"]["spf"]:
        errors.append(f"SPF count mismatch: header={output['counts']['spf']}, recount={recount_spf}")

    if errors:
        print(f"  ERRORS ({len(errors)}):")
        for e in errors[:10]:
            print(f"    {e}")
        if len(errors) > 10:
            print(f"    ... and {len(errors) - 10} more")
        sys.exit(1)
    else:
        print(f"  Consistency check passed for {cc.upper()}")


def merge_dane(cc: str) -> None:
    """Merge DANE values from *_Result.json into an existing scanner-produced security_{cc}.json.

    Preserves all scanner data (MX records, DKIM, SPF/DMARC) and only updates DANE fields.
    """
    cfg = COUNTRIES[cc]
    result_path = SECURITY_DIR / cfg["result"]
    security_path = SECURITY_DIR / f"security_{cc}.json"

    if not result_path.exists():
        print(f"  SKIP {cc}: {result_path} not found")
        return
    if not security_path.exists():
        print(f"  SKIP {cc}: {security_path} not found (use convert instead)")
        return

    result_data = load_result(result_path)
    with open(security_path, encoding="utf-8") as f:
        output = json.load(f)

    # Build code -> hasDane lookup from Result.json
    dane_by_code: dict[str, bool] = {}
    for entry in result_data:
        code_str = normalize_code(entry["code"], cfg["code_width"])
        dane_by_code[code_str] = to_bool(entry.get("hasDane"))

    # Update DANE fields in-place
    updated = 0
    for muni in output["municipalities"]:
        dane_val = dane_by_code.get(muni["code"])
        if dane_val is None:
            continue
        if muni.get("dane") is None:
            muni["dane"] = {"supported": False, "partial": False}
        old_val = muni["dane"]["supported"]
        muni["dane"]["supported"] = dane_val
        muni["dane"]["partial"] = dane_val
        if old_val != dane_val:
            updated += 1

    # Recompute dane_supported count
    output["counts"]["dane_supported"] = sum(
        1 for m in output["municipalities"] if m.get("dane") and m["dane"]["supported"]
    )

    with open(security_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2, separators=(",", ":"))

    print(f"  Merged DANE into {security_path} ({updated} municipalities updated)")
    print(f"    dane_supported={output['counts']['dane_supported']}, "
          f"spf={output['counts']['spf']}, dmarc={output['counts']['dmarc']}, "
          f"dkim={output['counts']['dkim']}")

    # Verify DANE values match
    verify_dane(cc, result_data, output, cfg["code_width"])


def verify_dane(cc: str, result_data: list[dict], output: dict, code_width: int | None) -> None:
    """Verify DANE fields match between Result.json and merged output."""
    errors = []
    out_by_code = {m["code"]: m for m in output["municipalities"]}
    for entry in result_data:
        code = normalize_code(entry["code"], code_width)
        out = out_by_code.get(code)
        if out is None:
            continue
        src_val = to_bool(entry.get("hasDane"))
        out_val = out.get("dane", {}).get("supported", False) if out.get("dane") else False
        if src_val != out_val:
            errors.append(f"code {code} ({entry['name']}): hasDane src={src_val} != out={out_val}")
    if errors:
        print(f"  ERRORS ({len(errors)}):")
        for e in errors:
            print(f"    {e}")
        sys.exit(1)
    else:
        print(f"  DANE consistency check passed for {cc.upper()}")


def main() -> None:
    print("Converting/merging Result.json files into security_{cc}.json format\n")
    for cc in COUNTRIES:
        print(f"[{cc.upper()}]")
        security_path = SECURITY_DIR / f"security_{cc}.json"
        if security_path.exists():
            # Scanner-produced file exists — merge DANE only
            merge_dane(cc)
        else:
            # No scanner file — full conversion
            convert(cc)
        print()


if __name__ == "__main__":
    main()