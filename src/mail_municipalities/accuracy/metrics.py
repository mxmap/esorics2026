"""Accuracy metrics: confusion matrix, precision, recall, F1."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone

from mail_municipalities.accuracy.models import (
    CLASSIFIER_TO_EVAL,
    NDR_TO_CLASSIFIER,
    AccuracyReport,
    ClassMetrics,
    NdrResult,
    Probe,
    ProbeStatus,
)
from mail_municipalities.accuracy.state import StateDB

# Label set used for evaluation (after mapping both sides).
EVAL_LABELS = ("microsoft", "google", "aws", "self-hosted", "unknown")


async def compute_accuracy(state: StateDB) -> AccuracyReport:
    """Compute accuracy metrics from matched probes and NDRs."""
    all_probes = await state.get_all_probes()
    all_ndrs = await state.get_all_ndrs()

    # Index NDRs by probe_id.
    ndr_by_probe: dict[str, NdrResult] = {}
    for ndr in all_ndrs:
        ndr_by_probe[ndr.probe_id] = ndr

    total_probes = len(all_probes)
    total_sent = sum(
        1 for p in all_probes if p.status in (ProbeStatus.SENT, ProbeStatus.NDR_RECEIVED, ProbeStatus.NO_NDR)
    )
    total_ndrs = len(all_ndrs)

    # Build confusion matrix from matched pairs.
    confusion: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    matched_probes: list[tuple[Probe, NdrResult]] = []

    for probe in all_probes:
        if probe.probe_id not in ndr_by_probe:
            continue
        ndr = ndr_by_probe[probe.probe_id]
        predicted = CLASSIFIER_TO_EVAL.get(probe.predicted_provider, "unknown")
        actual = NDR_TO_CLASSIFIER.get(ndr.ndr_provider.value, "unknown")
        confusion[predicted][actual] += 1
        matched_probes.append((probe, ndr))

    # Per-class metrics.
    per_class: dict[str, ClassMetrics] = {}
    for label in EVAL_LABELS:
        tp = confusion[label].get(label, 0)
        fp = sum(confusion[other].get(label, 0) for other in EVAL_LABELS if other != label)
        fn = sum(confusion[label].get(other, 0) for other in EVAL_LABELS if other != label)
        support = tp + fn  # actual positives

        precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
        f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0

        per_class[label] = ClassMetrics(precision=precision, recall=recall, f1=f1, support=support)

    total_correct = sum(confusion[label].get(label, 0) for label in EVAL_LABELS)
    total_evaluated = sum(sum(row.values()) for row in confusion.values())
    overall_accuracy = total_correct / total_evaluated if total_evaluated > 0 else 0.0
    response_rate = total_ndrs / total_sent if total_sent > 0 else 0.0

    # Serialize confusion matrix to plain dict.
    cm: dict[str, dict[str, int]] = {}
    for pred in EVAL_LABELS:
        cm[pred] = {actual: confusion[pred].get(actual, 0) for actual in EVAL_LABELS}

    return AccuracyReport(
        generated=datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        total_probes=total_probes,
        total_sent=total_sent,
        total_ndrs=total_ndrs,
        response_rate=round(response_rate, 4),
        overall_accuracy=round(overall_accuracy, 4),
        per_class=per_class,
        confusion_matrix=cm,
    )
