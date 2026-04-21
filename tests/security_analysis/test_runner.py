"""Tests for security_analysis.runner transformation functions."""

from __future__ import annotations

import json
from pathlib import Path

from mail_municipalities.security_analysis.runner import (
    build_domain_security,
    build_output,
    prepare_scanner_input,
)


def _make_domains_json(tmp_path: Path, municipalities: list[dict]) -> Path:
    """Write a minimal domains JSON file and return its path."""
    path = tmp_path / "domains_ch.json"
    path.write_text(
        json.dumps(
            {"generated": "2026-01-01T00:00:00Z", "total": len(municipalities), "municipalities": municipalities}
        ),
        encoding="utf-8",
    )
    return path


class TestPrepareInput:
    def test_basic(self, tmp_path: Path) -> None:
        domains_path = _make_domains_json(
            tmp_path,
            [
                {"code": "1", "name": "A", "region": "R", "emails": ["example.ch"]},
                {"code": "2", "name": "B", "region": "R", "emails": ["example2.ch"]},
            ],
        )
        scanner_input_dir = tmp_path / "scanner-input"

        result = prepare_scanner_input(domains_path, scanner_input_dir, "ch")

        with open(result, encoding="utf-8") as f:
            data = json.load(f)

        assert set(data["checked_emails"]) == {"info@example.ch", "info@example2.ch"}
        assert result.name == "ch.json"

    def test_deduplication(self, tmp_path: Path) -> None:
        """Multiple municipalities sharing a domain should produce one entry."""
        domains_path = _make_domains_json(
            tmp_path,
            [
                {"code": "1", "name": "A", "region": "R", "emails": ["shared.ch"]},
                {"code": "2", "name": "B", "region": "R", "emails": ["shared.ch"]},
                {"code": "3", "name": "C", "region": "R", "emails": ["other.ch"]},
            ],
        )
        scanner_input_dir = tmp_path / "scanner-input"
        result = prepare_scanner_input(domains_path, scanner_input_dir, "ch")

        with open(result, encoding="utf-8") as f:
            data = json.load(f)

        assert len(data["checked_emails"]) == 2

    def test_empty_emails(self, tmp_path: Path) -> None:
        """Municipalities with no emails should be skipped."""
        domains_path = _make_domains_json(
            tmp_path,
            [
                {"code": "1", "name": "A", "region": "R", "emails": []},
                {"code": "2", "name": "B", "region": "R", "emails": ["ok.ch"]},
            ],
        )
        scanner_input_dir = tmp_path / "scanner-input"
        result = prepare_scanner_input(domains_path, scanner_input_dir, "ch")

        with open(result, encoding="utf-8") as f:
            data = json.load(f)

        assert data["checked_emails"] == ["info@ok.ch"]

    def test_clears_stale_files(self, tmp_path: Path) -> None:
        """Pre-existing files in scanner-input should be removed."""
        scanner_input_dir = tmp_path / "scanner-input"
        scanner_input_dir.mkdir()
        stale = scanner_input_dir / "old.json"
        stale.write_text("{}")

        domains_path = _make_domains_json(tmp_path, [{"code": "1", "name": "A", "region": "R", "emails": ["x.ch"]}])
        prepare_scanner_input(domains_path, scanner_input_dir, "ch")

        assert not stale.exists()


class TestBuildDomainSecurity:
    def test_basic_aggregation(self) -> None:
        rows = [
            {
                "domainName": "example.ch",
                "mxrecordName": "mx1.example.ch",
                "ipAddress": "1.2.3.4",
                "hasDane": True,
                "hasSslv2": False,
                "hasSslv3": False,
                "hasTls1": False,
                "hasTls1_1": False,
                "hasTls1_2": True,
                "hasTls1_3": True,
                "hasDmarc": True,
                "hasGoodDmarc": False,
                "hasSpf": True,
                "hasGoodSpf": True,
                "hasDkim": True,
            },
            {
                "domainName": "example.ch",
                "mxrecordName": "mx2.example.ch",
                "ipAddress": "5.6.7.8",
                "hasDane": False,
                "hasSslv2": False,
                "hasSslv3": False,
                "hasTls1": False,
                "hasTls1_1": False,
                "hasTls1_2": True,
                "hasTls1_3": True,
                "hasDmarc": True,
                "hasGoodDmarc": False,
                "hasSpf": True,
                "hasGoodSpf": True,
                "hasDkim": True,
            },
        ]

        result = build_domain_security(rows)

        assert "example.ch" in result
        sec = result["example.ch"]
        assert sec["mx_records"] == ["mx1.example.ch", "mx2.example.ch"]
        # DANE: not all MTA have it, but at least one does
        assert sec["dane"].supported is False
        assert sec["dane"].partial is True
        # DSS
        assert sec["dss"].has_spf is True
        assert sec["dss"].has_good_spf is True
        assert sec["dss"].has_dmarc is True
        assert sec["dss"].has_dkim is True

    def test_null_fields(self) -> None:
        """Rows with null DANE/DSS values should not crash."""
        rows = [
            {
                "domainName": "null.ch",
                "mxrecordName": None,
                "ipAddress": None,
                "hasDane": None,
                "hasSslv2": None,
                "hasSslv3": None,
                "hasTls1": None,
                "hasTls1_1": None,
                "hasTls1_2": None,
                "hasTls1_3": None,
                "hasDmarc": None,
                "hasGoodDmarc": None,
                "hasSpf": None,
                "hasGoodSpf": None,
                "hasDkim": None,
            }
        ]

        result = build_domain_security(rows)
        sec = result["null.ch"]
        assert sec["dane"].supported is False
        assert sec["dane"].partial is False
        assert sec["dss"].has_spf is False

    def test_tls_fields_ignored(self) -> None:
        """TLS fields should be present in input but not in output."""
        rows = [
            {
                "domainName": "tls.ch",
                "mxrecordName": "mx.tls.ch",
                "ipAddress": "1.1.1.1",
                "hasDane": False,
                "hasSslv2": True,
                "hasSslv3": True,
                "hasTls1": True,
                "hasTls1_1": True,
                "hasTls1_2": True,
                "hasTls1_3": False,
                "hasDmarc": True,
                "hasGoodDmarc": True,
                "hasSpf": True,
                "hasGoodSpf": True,
                "hasDkim": False,
            }
        ]

        result = build_domain_security(rows)
        sec = result["tls.ch"]
        # Output should have dane, dss, mx_records, scan_valid — no TLS
        assert "tls" not in sec
        assert "dane" in sec
        assert "dss" in sec


class TestBuildOutput:
    def test_municipality_mapping(self, tmp_path: Path) -> None:
        domains_path = _make_domains_json(
            tmp_path,
            [
                {"code": "1", "name": "Alpha", "region": "ZH", "emails": ["alpha.ch"]},
                {"code": "2", "name": "Beta", "region": "BE", "emails": ["beta.ch"]},
                {"code": "3", "name": "Gamma", "region": "AG", "emails": []},
            ],
        )

        from mail_municipalities.security_analysis.models import DaneSummary, DssSummary

        domain_security = {
            "alpha.ch": {
                "mx_records": ["mx.alpha.ch"],
                "dane": DaneSummary(supported=True, partial=True),
                "dss": DssSummary(has_spf=True, has_good_spf=True, has_dmarc=True, has_good_dmarc=True, has_dkim=True),
                "scan_valid": True,
            },
            "beta.ch": {
                "mx_records": ["mx.beta.ch"],
                "dane": DaneSummary(supported=False, partial=False),
                "dss": DssSummary(
                    has_spf=True, has_good_spf=False, has_dmarc=False, has_good_dmarc=False, has_dkim=False
                ),
                "scan_valid": True,
            },
        }

        output = build_output(domains_path, domain_security, "ch")

        assert output.total == 3
        assert output.counts["scanned"] == 2
        assert output.counts["dane_supported"] == 1
        assert output.counts["spf"] == 2
        assert output.counts["dkim"] == 1

        # Gamma has no domain, so no security data
        gamma = next(m for m in output.municipalities if m.code == "3")
        assert gamma.scan_valid is False
        assert gamma.dane is None

    def test_shared_domain(self, tmp_path: Path) -> None:
        """Two municipalities sharing a domain get the same security data."""
        domains_path = _make_domains_json(
            tmp_path,
            [
                {"code": "1", "name": "A", "region": "R", "emails": ["shared.ch"]},
                {"code": "2", "name": "B", "region": "R", "emails": ["shared.ch"]},
            ],
        )

        from mail_municipalities.security_analysis.models import DaneSummary, DssSummary

        domain_security = {
            "shared.ch": {
                "mx_records": ["mx.shared.ch"],
                "dane": DaneSummary(supported=False, partial=False),
                "dss": DssSummary(has_spf=True, has_good_spf=True, has_dmarc=True, has_good_dmarc=True, has_dkim=True),
                "scan_valid": True,
            },
        }

        output = build_output(domains_path, domain_security, "ch")
        assert output.counts["scanned"] == 2
        assert all(m.dss is not None and m.dss.has_spf for m in output.municipalities)
