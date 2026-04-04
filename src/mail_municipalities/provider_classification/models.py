"""Pydantic models for the mail sovereignty classifier."""

from __future__ import annotations

import enum

from pydantic import BaseModel, ConfigDict, Field, ValidationError


class Provider(str, enum.Enum):
    MS365 = "ms365"
    GOOGLE = "google"
    AWS = "aws"
    DOMESTIC = "domestic"
    FOREIGN = "foreign"
    UNKNOWN = "unknown"


class SignalKind(str, enum.Enum):
    MX = "mx"
    SPF = "spf"
    DKIM = "dkim"
    DMARC = "dmarc"
    AUTODISCOVER = "autodiscover"
    CNAME_CHAIN = "cname_chain"
    SMTP = "smtp"
    TENANT = "tenant"
    ASN = "asn"
    TXT_VERIFICATION = "txt_verification"
    SPF_IP = "spf_ip"


class Evidence(BaseModel):
    model_config = ConfigDict(frozen=True)

    kind: SignalKind
    provider: Provider
    weight: float = Field(ge=0.0, le=1.0)
    detail: str
    raw: str = ""


class ClassificationResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    provider: Provider
    confidence: float = Field(ge=0.0, le=1.0)
    evidence: list[Evidence] = []
    gateway: str | None = None
    mx_hosts: list[str] = []
    spf_raw: str = ""


class CymruResult(BaseModel):
    """Parsed Team Cymru IP-to-ASN DNS response."""

    model_config = ConfigDict(frozen=True)

    asn: int
    country_code: str  # 2-letter ISO (lowercased)

    @classmethod
    def from_txt(cls, txt: str) -> CymruResult | None:
        """Parse ``'ASN | IP/Prefix | CC | Registry | Allocated'`` response."""
        parts = txt.split("|")
        if len(parts) < 3:
            return None
        try:
            return cls(asn=int(parts[0].strip()), country_code=parts[2].strip().lower())
        except (ValueError, ValidationError):
            return None
