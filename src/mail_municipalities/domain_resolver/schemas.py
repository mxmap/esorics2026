"""Pydantic models for the municipality email pipeline."""

from __future__ import annotations

from datetime import datetime
from enum import Enum

from pydantic import BaseModel


class Country(str, Enum):
    CH = "ch"
    DE = "de"
    AT = "at"


class Confidence(str, Enum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    NONE = "none"


class Source(str, Enum):
    OVERRIDE = "override"
    SCRAPE = "scrape"
    REDIRECT = "redirect"
    WIKIDATA = "wikidata"
    STATIC = "static"
    GUESS = "guess"
    NONE = "none"


class DomainCandidate(BaseModel):
    """A candidate domain from a specific source."""

    domain: str
    source: str  # e.g. "livenson", "wikidata", "bresu_email", "guess"
    is_email_domain: bool = False  # True if source provides email (not just website)


class MunicipalityRecord(BaseModel):
    """Internal working record for a municipality through the pipeline."""

    code: str
    name: str
    region: str
    country: Country
    candidates: list[DomainCandidate] = []
    override_domain: str | None = None
    override_name: str | None = None
    website_domain: str | None = None
    # Populated during scraping phase
    scraped_emails: dict[str, list[str]] = {}  # domain -> list of email domains found
    redirects: dict[str, str] = {}  # domain -> redirect target
    accessible: dict[str, bool] = {}  # domain -> is accessible
    # Populated during content validation phase
    content_flags: dict[str, list[str]] = {}  # domain -> content validation flags
    # Populated during MX phase
    mx_valid: dict[str, bool] = {}  # domain -> has MX records
    # Populated during decision phase
    emails: list[str] = []
    source: Source = Source.NONE
    confidence: Confidence = Confidence.NONE
    sources_detail: dict[str, list[str]] = {}
    flags: list[str] = []


class MunicipalityOutput(BaseModel):
    """Minimal publication output."""

    code: str
    name: str
    region: str
    website: str = ""
    emails: list[str] = []


class MunicipalityDetailedOutput(MunicipalityOutput):
    """Enriched internal output."""

    source: str = "none"
    confidence: str = "none"
    sources_detail: dict[str, list[str]] = {}
    flags: list[str] = []


class PipelineOutput(BaseModel):
    """Top-level output wrapper."""

    generated: datetime
    total: int
    municipalities: list[MunicipalityOutput | MunicipalityDetailedOutput]
