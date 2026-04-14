"""Pydantic models for bounce-probe accuracy validation."""

from __future__ import annotations

import enum
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class ProbeStatus(str, enum.Enum):
    PENDING = "pending"
    SENT = "sent"
    SEND_FAILED = "failed"
    NDR_RECEIVED = "ndr_received"
    NO_NDR = "no_ndr"


class NdrProvider(str, enum.Enum):
    """Provider identified from NDR headers/body."""

    MICROSOFT = "microsoft"
    GOOGLE = "google"
    AWS = "aws"
    POSTFIX = "postfix"
    EXIM = "exim"
    EXCHANGE_ONPREM = "exchange_onprem"
    OTHER = "other"
    UNKNOWN = "unknown"


# Map NDR-detected provider to classifier output labels.
# NDR cannot distinguish domestic vs foreign — both use Postfix/Exim.
# For evaluation we merge domestic+foreign into "self-hosted".
NDR_TO_CLASSIFIER: dict[str, str] = {
    NdrProvider.MICROSOFT: "microsoft",
    NdrProvider.GOOGLE: "google",
    NdrProvider.AWS: "aws",
    NdrProvider.POSTFIX: "self-hosted",
    NdrProvider.EXIM: "self-hosted",
    NdrProvider.EXCHANGE_ONPREM: "self-hosted",
    NdrProvider.OTHER: "unknown",
    NdrProvider.UNKNOWN: "unknown",
}

# Map classifier output labels to the common evaluation label set.
CLASSIFIER_TO_EVAL: dict[str, str] = {
    "microsoft": "microsoft",
    "google": "google",
    "aws": "aws",
    "domestic": "self-hosted",
    "foreign": "self-hosted",
    "unknown": "unknown",
}


class Probe(BaseModel):
    """A single probe email targeting a municipality domain."""

    model_config = ConfigDict(frozen=True)

    probe_id: str
    domain: str
    municipality_code: str
    municipality_name: str
    country: str
    recipient: str
    predicted_provider: str
    predicted_confidence: float
    gateway: str | None = None
    status: ProbeStatus = ProbeStatus.PENDING
    sent_at: datetime | None = None
    message_id: str | None = None
    smtp_response: str | None = None


class NdrEvidence(BaseModel):
    """A single piece of evidence extracted from an NDR."""

    model_config = ConfigDict(frozen=True)

    pattern: str
    matched_value: str


class NdrResult(BaseModel):
    """Parsed NDR matched to a probe."""

    model_config = ConfigDict(frozen=True)

    probe_id: str
    received_at: datetime
    ndr_from: str
    ndr_provider: NdrProvider
    generating_mta: str
    confidence: float = Field(ge=0.0, le=1.0)
    evidence: list[NdrEvidence] = []
    raw_headers: str = ""


class ClassMetrics(BaseModel):
    """Per-class precision / recall / F1."""

    model_config = ConfigDict(frozen=True)

    precision: float
    recall: float
    f1: float
    support: int


class AccuracyReport(BaseModel):
    """Aggregate accuracy metrics."""

    model_config = ConfigDict(frozen=True)

    generated: str
    total_probes: int
    total_sent: int
    total_ndrs: int
    response_rate: float
    overall_accuracy: float
    per_class: dict[str, ClassMetrics]
    confusion_matrix: dict[str, dict[str, int]]
