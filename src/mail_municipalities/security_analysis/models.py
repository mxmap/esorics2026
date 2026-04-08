"""Pydantic models for security analysis output."""

from __future__ import annotations

from pydantic import BaseModel


class DaneSummary(BaseModel):
    """DANE/DNSSEC support summary across all MTA for a domain."""

    supported: bool = False
    """True if all MTA have valid DANE/TLSA records."""
    partial: bool = False
    """True if at least one MTA has valid DANE/TLSA records."""


class DssSummary(BaseModel):
    """Email authentication (SPF/DKIM/DMARC) summary for a domain."""

    has_spf: bool = False
    has_good_spf: bool = False
    has_dmarc: bool = False
    has_good_dmarc: bool = False
    has_dkim: bool = False


class MunicipalitySecurity(BaseModel):
    """Security assessment for a single municipality's email domain."""

    code: str
    name: str
    region: str
    domain: str
    mx_records: list[str] = []
    dane: DaneSummary | None = None
    dss: DssSummary | None = None
    scan_valid: bool = False


class SecurityOutput(BaseModel):
    """Top-level output envelope for security scan results."""

    generated: str
    commit: str | None = None
    total: int
    counts: dict[str, int]
    municipalities: list[MunicipalitySecurity]
