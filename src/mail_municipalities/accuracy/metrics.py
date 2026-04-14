"""Accuracy metrics via scikit-learn."""

from __future__ import annotations

from datetime import datetime, timezone

from sklearn.metrics import accuracy_score, confusion_matrix, f1_score, precision_recall_fscore_support

from mail_municipalities.accuracy.models import (
    CLASSIFIER_TO_EVAL,
    NDR_TO_CLASSIFIER,
    AccuracyReport,
    ClassMetrics,
    NdrResult,
    ProbeStatus,
)
from mail_municipalities.accuracy.state import StateDB

# Label set used for evaluation (after mapping both sides).
EVAL_LABELS = ("microsoft", "google", "aws", "self-hosted", "unknown")

# Labels included in the weighted F1 headline metric.
# AWS is excluded: DNS probes see the inbound relay (AWS SES) while the
# bounce reveals the on-prem backend — a methodological boundary, not a
# classifier error.  "unknown" is excluded because it has 0 support.
WEIGHTED_F1_LABELS = ("microsoft", "google", "self-hosted")


async def compute_accuracy(state: StateDB) -> AccuracyReport:
    """Compute accuracy metrics from matched probes and NDRs."""
    all_probes = await state.get_all_probes()
    all_ndrs = await state.get_all_ndrs()

    ndr_by_probe: dict[str, NdrResult] = {ndr.probe_id: ndr for ndr in all_ndrs}

    total_probes = len(all_probes)
    total_sent = sum(
        1 for p in all_probes if p.status in (ProbeStatus.SENT, ProbeStatus.NDR_RECEIVED, ProbeStatus.NO_NDR)
    )
    total_ndrs = len(all_ndrs)

    # Build parallel label lists from matched pairs.
    y_true: list[str] = []
    y_pred: list[str] = []
    for probe in all_probes:
        if probe.probe_id not in ndr_by_probe:
            continue
        ndr = ndr_by_probe[probe.probe_id]
        y_pred.append(CLASSIFIER_TO_EVAL.get(probe.predicted_provider, "unknown"))
        y_true.append(NDR_TO_CLASSIFIER.get(ndr.ndr_provider.value, "unknown"))

    response_rate = total_ndrs / total_sent if total_sent > 0 else 0.0

    if not y_true:
        return _empty_report(total_probes, total_sent, total_ndrs, response_rate)

    # Overall accuracy across all labels.
    overall_acc = float(accuracy_score(y_true, y_pred))

    # Per-class metrics across all EVAL_LABELS.
    present_labels = [label for label in EVAL_LABELS if label in set(y_true) | set(y_pred)]
    precision, recall, f1, support = precision_recall_fscore_support(
        y_true, y_pred, labels=present_labels, zero_division=0.0,
    )
    per_class: dict[str, ClassMetrics] = {}
    for i, label in enumerate(present_labels):
        per_class[label] = ClassMetrics(
            precision=float(precision[i]),
            recall=float(recall[i]),
            f1=float(f1[i]),
            support=int(support[i]),
        )
    # Fill missing labels with zeros.
    for label in EVAL_LABELS:
        if label not in per_class:
            per_class[label] = ClassMetrics(precision=0.0, recall=0.0, f1=0.0, support=0)

    # Weighted F1 over the dominant classes only.
    # Filter to pairs where BOTH predicted and actual are in WEIGHTED_F1_LABELS,
    # so excluded classes (AWS) don't affect precision/recall of included ones.
    wf1_true = [t for t, p in zip(y_true, y_pred) if t in WEIGHTED_F1_LABELS and p in WEIGHTED_F1_LABELS]
    wf1_pred = [p for t, p in zip(y_true, y_pred) if t in WEIGHTED_F1_LABELS and p in WEIGHTED_F1_LABELS]
    if wf1_true:
        weighted_f1 = float(f1_score(wf1_true, wf1_pred, labels=list(WEIGHTED_F1_LABELS), average="weighted", zero_division=0.0))
    else:
        weighted_f1 = 0.0

    # Confusion matrix as nested dict (rows=predicted, cols=actual).
    # sklearn returns rows=true, cols=predicted — transpose via indexing.
    cm_labels = [label for label in EVAL_LABELS if label in set(y_true) | set(y_pred)]
    cm_array = confusion_matrix(y_true, y_pred, labels=cm_labels)
    cm: dict[str, dict[str, int]] = {}
    for i, pred in enumerate(cm_labels):
        cm[pred] = {actual: int(cm_array[j][i]) for j, actual in enumerate(cm_labels)}
    # Fill missing labels.
    for label in EVAL_LABELS:
        if label not in cm:
            cm[label] = {a: 0 for a in EVAL_LABELS}
        for a in EVAL_LABELS:
            cm[label].setdefault(a, 0)

    return AccuracyReport(
        generated=datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        total_probes=total_probes,
        total_sent=total_sent,
        total_ndrs=total_ndrs,
        response_rate=round(response_rate, 4),
        overall_accuracy=round(overall_acc, 4),
        weighted_f1=round(weighted_f1, 4),
        weighted_f1_labels=list(WEIGHTED_F1_LABELS),
        per_class=per_class,
        confusion_matrix=cm,
    )


def _empty_report(total_probes: int, total_sent: int, total_ndrs: int, response_rate: float) -> AccuracyReport:
    per_class = {label: ClassMetrics(precision=0.0, recall=0.0, f1=0.0, support=0) for label in EVAL_LABELS}
    cm = {p: {a: 0 for a in EVAL_LABELS} for p in EVAL_LABELS}
    return AccuracyReport(
        generated=datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        total_probes=total_probes,
        total_sent=total_sent,
        total_ndrs=total_ndrs,
        response_rate=round(response_rate, 4),
        overall_accuracy=0.0,
        weighted_f1=0.0,
        weighted_f1_labels=list(WEIGHTED_F1_LABELS),
        per_class=per_class,
        confusion_matrix=cm,
    )
