"""Parse NDR (bounce) emails to identify the generating MTA's provider."""

from __future__ import annotations

import re
from email.message import EmailMessage

from mail_municipalities.accuracy.models import NdrEvidence, NdrProvider


def parse_ndr(msg: EmailMessage) -> tuple[NdrProvider, float, str, list[NdrEvidence]]:
    """Analyse an NDR email and identify the backend provider.

    Returns ``(provider, confidence, generating_mta, evidence)``.
    """
    evidence: list[NdrEvidence] = []
    scores: dict[NdrProvider, float] = {}

    # Collect raw material for matching.
    headers_str = _headers_text(msg)
    body = _body_text(msg)
    dsn_fields = _extract_dsn_fields(msg)
    from_addr = (msg.get("From") or "").lower()
    subject = (msg.get("Subject") or "").lower()
    received_chain = [v for v in msg.get_all("Received", []) if isinstance(v, str)]

    # ── Microsoft 365 / Exchange Online ───────────────────────────
    ms_patterns: list[tuple[str, str]] = []

    # X-MS-Exchange-* headers
    for hdr in msg.keys():
        if hdr.lower().startswith("x-ms-exchange"):
            ms_patterns.append(("x-ms-exchange header", hdr))
            break

    if re.search(r"postmaster@.*outlook\.com", from_addr):
        ms_patterns.append(("from postmaster@outlook", from_addr))
    if re.search(r"\.onmicrosoft\.com", from_addr):
        ms_patterns.append(("from *.onmicrosoft.com", from_addr))

    reporting_mta = dsn_fields.get("reporting-mta", "")
    if re.search(r"protection\.outlook\.com|\.outlook\.com", reporting_mta):
        ms_patterns.append(("reporting-mta outlook", reporting_mta))

    if any(re.search(r"\.outlook\.com|protection\.outlook\.com", r, re.I) for r in received_chain):
        ms_patterns.append(("received via outlook", "received chain"))

    if "delivery has failed" in body.lower():
        ms_patterns.append(("exchange dsn body", "delivery has failed"))

    if ms_patterns:
        # Distinguish Exchange Online from on-premises.
        is_online = any(
            "outlook.com" in p[1].lower() or "protection.outlook" in p[1].lower()
            for p in ms_patterns
            if p[0] in ("reporting-mta outlook", "received via outlook", "from postmaster@outlook")
        )
        provider = NdrProvider.MICROSOFT if is_online else NdrProvider.EXCHANGE_ONPREM
        conf = min(0.3 + 0.15 * len(ms_patterns), 1.0)
        for pat, val in ms_patterns:
            evidence.append(NdrEvidence(pattern=pat, matched_value=val))
        scores[provider] = conf

    # ── Google Workspace ──────────────────────────────────────────
    goog_patterns: list[tuple[str, str]] = []

    if re.search(r"mailer-daemon@google(mail)?\.com", from_addr):
        goog_patterns.append(("from mailer-daemon@google", from_addr))

    if msg.get("X-Gm-Message-State"):
        goog_patterns.append(("x-gm-message-state header", "present"))

    if any(re.search(r"\.google\.com|\.googlemail\.com", r, re.I) for r in received_chain):
        goog_patterns.append(("received via google", "received chain"))

    if "delivery to the following recipient failed" in body.lower():
        goog_patterns.append(("google dsn body", "delivery failed permanently"))

    if re.search(r"google\.com", reporting_mta):
        goog_patterns.append(("reporting-mta google", reporting_mta))

    if goog_patterns:
        conf = min(0.3 + 0.2 * len(goog_patterns), 1.0)
        for pat, val in goog_patterns:
            evidence.append(NdrEvidence(pattern=pat, matched_value=val))
        scores[NdrProvider.GOOGLE] = conf

    # ── AWS SES ───────────────────────────────────────────────────
    aws_patterns: list[tuple[str, str]] = []

    if "amazonses.com" in from_addr:
        aws_patterns.append(("from amazonses", from_addr))

    if msg.get("X-SES-Outgoing"):
        aws_patterns.append(("x-ses-outgoing header", "present"))

    if re.search(r"amazonses\.com|amazonaws\.com", reporting_mta):
        aws_patterns.append(("reporting-mta aws", reporting_mta))

    if any(re.search(r"amazonses\.com|amazonaws\.com", r, re.I) for r in received_chain):
        aws_patterns.append(("received via aws", "received chain"))

    if aws_patterns:
        conf = min(0.3 + 0.2 * len(aws_patterns), 1.0)
        for pat, val in aws_patterns:
            evidence.append(NdrEvidence(pattern=pat, matched_value=val))
        scores[NdrProvider.AWS] = conf

    # ── Postfix ───────────────────────────────────────────────────
    postfix_patterns: list[tuple[str, str]] = []

    if "this is the mail system at host" in body.lower():
        postfix_patterns.append(("postfix dsn body", "this is the mail system at host"))

    for hdr in msg.keys():
        if hdr.lower().startswith("x-postfix"):
            postfix_patterns.append(("x-postfix header", hdr))
            break

    if any("postfix" in r.lower() for r in received_chain):
        postfix_patterns.append(("received via postfix", "received chain"))

    if "postfix" in headers_str.lower():
        postfix_patterns.append(("postfix in headers", "header text"))

    if postfix_patterns:
        conf = min(0.3 + 0.2 * len(postfix_patterns), 1.0)
        for pat, val in postfix_patterns:
            evidence.append(NdrEvidence(pattern=pat, matched_value=val))
        scores[NdrProvider.POSTFIX] = conf

    # ── Exim ──────────────────────────────────────────────────────
    exim_patterns: list[tuple[str, str]] = []

    if "mail delivery failed" in subject:
        exim_patterns.append(("exim subject", subject))

    if "a message that you sent could not be delivered" in body.lower():
        exim_patterns.append(("exim dsn body", "could not be delivered"))

    if any(re.search(r"exim\s+\d+", r, re.I) for r in received_chain):
        exim_patterns.append(("received via exim", "received chain"))

    if exim_patterns:
        conf = min(0.3 + 0.2 * len(exim_patterns), 1.0)
        for pat, val in exim_patterns:
            evidence.append(NdrEvidence(pattern=pat, matched_value=val))
        scores[NdrProvider.EXIM] = conf

    # ── Winner selection ──────────────────────────────────────────
    if not scores:
        generating_mta = reporting_mta or _guess_mta_from_received(received_chain)
        return NdrProvider.UNKNOWN, 0.0, generating_mta, evidence

    winner = max(scores, key=lambda p: scores[p])
    generating_mta = reporting_mta or _guess_mta_from_received(received_chain)
    return winner, scores[winner], generating_mta, evidence


def _headers_text(msg: EmailMessage) -> str:
    """Concatenate all header key-value pairs into a single string."""
    parts: list[str] = []
    for k, v in msg.items():
        parts.append(f"{k}: {v}")
    return "\n".join(parts)


def _body_text(msg: EmailMessage) -> str:
    """Extract the plain-text body from the message (first text/plain part)."""
    if msg.is_multipart():
        for part in msg.walk():
            ct = part.get_content_type()
            if ct == "text/plain":
                payload = part.get_payload(decode=True)
                if isinstance(payload, bytes):
                    return payload.decode("utf-8", errors="replace")
        # Fall back to the first delivery-status part text.
        for part in msg.walk():
            ct = part.get_content_type()
            if ct == "message/delivery-status":
                payload = part.get_payload(decode=True)
                if isinstance(payload, bytes):
                    return payload.decode("utf-8", errors="replace")
                # delivery-status is often a sub-message
                if isinstance(payload, list):
                    return "\n".join(str(p) for p in payload)
                return str(payload) if payload else ""
        return ""
    payload = msg.get_payload(decode=True)
    return payload.decode("utf-8", errors="replace") if isinstance(payload, bytes) else ""


def _extract_dsn_fields(msg: EmailMessage) -> dict[str, str]:
    """Extract DSN fields (Reporting-MTA, Remote-MTA, Final-Recipient, etc.)."""
    fields: dict[str, str] = {}
    if not msg.is_multipart():
        return fields
    for part in msg.walk():
        ct = part.get_content_type()
        if ct == "message/delivery-status":
            payload = part.get_payload(decode=True)
            text = ""
            if isinstance(payload, bytes):
                text = payload.decode("utf-8", errors="replace")
            elif isinstance(payload, list):
                text = "\n".join(str(p) for p in payload)
            elif isinstance(payload, str):
                text = payload
            for line in text.splitlines():
                if ":" in line:
                    key, _, value = line.partition(":")
                    key = key.strip().lower()
                    value = value.strip()
                    # Strip DSN type prefix (e.g. "dns;" or "rfc822;").
                    if ";" in value:
                        value = value.split(";", 1)[1].strip()
                    fields[key] = value
    return fields


def _guess_mta_from_received(received_chain: list[str]) -> str:
    """Best-effort extraction of the innermost MTA hostname from Received headers."""
    if not received_chain:
        return ""
    # The last Received header was added by the first hop.
    # Look for "by <hostname>" pattern.
    last = received_chain[-1]
    m = re.search(r"\bby\s+([\w.\-]+)", last)
    return m.group(1) if m else last[:120]
