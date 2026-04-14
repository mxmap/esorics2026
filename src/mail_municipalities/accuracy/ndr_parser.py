"""Parse NDR (bounce) emails to identify the generating MTA's provider.

IMPORTANT: when NDRs are collected via Gmail IMAP, every message carries
Google relay headers (X-Gm-*, Received from *.google.com).  The parser must
ignore these and focus on the DSN content (Reporting-MTA, Remote-MTA,
Diagnostic-Code) and the bounce body text, which originate from the actual
target MTA.
"""

from __future__ import annotations

import re
from email.message import EmailMessage

from mail_municipalities.accuracy.models import NdrEvidence, NdrProvider

# Patterns that indicate a Received header belongs to our Gmail relay
# (not the target MTA).  These are filtered out before analysis.
_RELAY_RECEIVED_RE = re.compile(
    r"google\.com|googlemail\.com|gmail-smtp|smtp\.gmail\.com",
    re.I,
)


def parse_ndr(msg: EmailMessage) -> tuple[NdrProvider, float, str, list[NdrEvidence]]:
    """Analyse an NDR email and identify the backend provider.

    Returns ``(provider, confidence, generating_mta, evidence)``.
    """
    evidence: list[NdrEvidence] = []
    scores: dict[NdrProvider, float] = {}

    # ── Collect raw material ──────────────────────────────────────
    body = _body_text(msg)
    dsn_fields = _extract_dsn_fields(msg)
    from_addr = (msg.get("From") or "").lower()
    subject = (msg.get("Subject") or "").lower()

    # DSN fields — these are authoritative (from the actual bouncing MTA).
    reporting_mta = dsn_fields.get("reporting-mta", "")
    remote_mta = dsn_fields.get("remote-mta", "")
    diagnostic_code = dsn_fields.get("diagnostic-code", "")

    # Filter Received chain: remove hops from our Gmail relay.
    all_received = [v for v in msg.get_all("Received", []) if isinstance(v, str)]
    received_chain = [r for r in all_received if not _RELAY_RECEIVED_RE.search(r)]

    # Collect non-relay X-headers (skip X-Gm-* and X-Google-* from Gmail).
    target_headers: dict[str, str] = {}
    for key in msg.keys():
        kl = key.lower()
        if kl.startswith("x-gm-") or kl.startswith("x-google-"):
            continue  # Gmail relay artifact
        if kl.startswith("x-"):
            target_headers[kl] = str(msg[key])

    # Combined text for Diagnostic-Code + Remote-MTA scanning.
    dsn_text = f"{reporting_mta} {remote_mta} {diagnostic_code}".lower()

    # ── Microsoft 365 / Exchange Online ───────────────────────────
    ms_patterns: list[tuple[str, str]] = []

    # X-MS-Exchange-* headers (these survive Gmail relay).
    if any(k.startswith("x-ms-exchange") for k in target_headers):
        ms_patterns.append(("x-ms-exchange header", "present"))

    if re.search(r"postmaster@.*outlook\.com", from_addr):
        ms_patterns.append(("from postmaster@outlook", from_addr))
    if re.search(r"\.onmicrosoft\.com", from_addr):
        ms_patterns.append(("from *.onmicrosoft.com", from_addr))

    if re.search(r"protection\.outlook\.com|\.outlook\.com", reporting_mta):
        ms_patterns.append(("reporting-mta outlook", reporting_mta))

    if re.search(r"protection\.outlook\.com|\.outlook\.com", dsn_text):
        ms_patterns.append(("dsn mentions outlook", dsn_text[:120]))

    if any(re.search(r"\.outlook\.com|protection\.outlook\.com", r, re.I) for r in received_chain):
        ms_patterns.append(("received via outlook", "received chain"))

    if "delivery has failed" in body.lower() and "recipients or groups" in body.lower():
        ms_patterns.append(("exchange dsn body", "delivery has failed to recipients"))

    if ms_patterns:
        is_online = any(
            "outlook.com" in p[1].lower() or "protection.outlook" in p[1].lower()
            for p in ms_patterns
            if p[0]
            in ("reporting-mta outlook", "received via outlook", "from postmaster@outlook", "dsn mentions outlook")
        )
        provider = NdrProvider.MICROSOFT if is_online else NdrProvider.EXCHANGE_ONPREM
        conf = min(0.3 + 0.15 * len(ms_patterns), 1.0)
        for pat, val in ms_patterns:
            evidence.append(NdrEvidence(pattern=pat, matched_value=val))
        scores[provider] = conf

    # ── Google Workspace ──────────────────────────────────────────
    # Only count Google if DSN fields or From explicitly point to Google
    # as the *generating* MTA — NOT just because our relay is Gmail.
    goog_patterns: list[tuple[str, str]] = []

    if re.search(r"mailer-daemon@google(mail)?\.com", from_addr):
        # Gmail itself generated this bounce.  The actual target info is
        # in Remote-MTA / Diagnostic-Code.  If those point elsewhere,
        # this bounce is Gmail reporting a remote failure, not Google
        # being the destination.  We handle that below.
        goog_patterns.append(("from mailer-daemon@google", from_addr))

    if re.search(r"google\.com|googlemail\.com", reporting_mta):
        goog_patterns.append(("reporting-mta google", reporting_mta))

    if "delivery to the following recipient failed" in body.lower():
        goog_patterns.append(("google dsn body", "delivery failed permanently"))

    # If From is Gmail but Remote-MTA / Diagnostic-Code point to a
    # different provider, this is a *relay bounce*: Gmail forwarding a
    # rejection from the actual target.  Detect the real target instead.
    if goog_patterns and (remote_mta or diagnostic_code):
        relay_target = _detect_relay_target(remote_mta, diagnostic_code)
        if relay_target is not None and relay_target != NdrProvider.GOOGLE:
            # Override: the real target is not Google.
            goog_patterns.clear()
            scores.pop(NdrProvider.GOOGLE, None)
            evidence_relay = [e for e in evidence if not e.pattern.startswith("from mailer-daemon@google")]
            evidence[:] = evidence_relay
            relay_patterns = _relay_target_evidence(relay_target, remote_mta, diagnostic_code)
            for pat, val in relay_patterns:
                evidence.append(NdrEvidence(pattern=pat, matched_value=val))
            scores[relay_target] = min(0.3 + 0.15 * len(relay_patterns), 1.0)

    if goog_patterns:
        conf = min(0.3 + 0.2 * len(goog_patterns), 1.0)
        for pat, val in goog_patterns:
            evidence.append(NdrEvidence(pattern=pat, matched_value=val))
        scores[NdrProvider.GOOGLE] = conf

    # ── AWS SES ───────────────────────────────────────────────────
    aws_patterns: list[tuple[str, str]] = []

    if "amazonses.com" in from_addr:
        aws_patterns.append(("from amazonses", from_addr))

    if "x-ses-outgoing" in target_headers:
        aws_patterns.append(("x-ses-outgoing header", "present"))

    if re.search(r"amazonses\.com|amazonaws\.com", dsn_text):
        aws_patterns.append(("dsn mentions aws", dsn_text[:120]))

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

    if any(k.startswith("x-postfix") for k in target_headers):
        postfix_patterns.append(("x-postfix header", "present"))

    if any("postfix" in r.lower() for r in received_chain):
        postfix_patterns.append(("received via postfix", "received chain"))

    if "postfix" in reporting_mta.lower():
        postfix_patterns.append(("reporting-mta postfix", reporting_mta))

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

    # ── Fallback: check DSN fields for provider hints ─────────────
    # If no provider scored yet, try to identify from Reporting-MTA,
    # Remote-MTA, or Diagnostic-Code alone.
    if not scores:
        dsn_provider = _detect_from_dsn(reporting_mta, remote_mta, diagnostic_code, body)
        if dsn_provider is not None:
            patterns = _relay_target_evidence(dsn_provider, remote_mta, diagnostic_code)
            if reporting_mta:
                patterns.append(("reporting-mta", reporting_mta))
            for pat, val in patterns:
                evidence.append(NdrEvidence(pattern=pat, matched_value=val))
            scores[dsn_provider] = min(0.3 + 0.15 * len(patterns), 1.0)

    # ── Winner selection ──────────────────────────────────────────
    if not scores:
        generating_mta = reporting_mta or _guess_mta_from_received(all_received)
        return NdrProvider.UNKNOWN, 0.0, generating_mta, evidence

    winner = max(scores, key=lambda p: scores[p])
    generating_mta = reporting_mta or _guess_mta_from_received(all_received)
    return winner, scores[winner], generating_mta, evidence


# ── Relay bounce detection ────────────────────────────────────────


def _detect_relay_target(remote_mta: str, diagnostic_code: str) -> NdrProvider | None:
    """Detect the actual target provider from DSN Remote-MTA / Diagnostic-Code.

    When Gmail generates a bounce on behalf of a remote rejection, the real
    target's identity is encoded in these fields.
    """
    combined = f"{remote_mta} {diagnostic_code}".lower()
    return _match_provider_in_text(combined)


def _detect_from_dsn(reporting_mta: str, remote_mta: str, diagnostic_code: str, body: str) -> NdrProvider | None:
    """Try to identify provider purely from DSN fields and body text."""
    combined = f"{reporting_mta} {remote_mta} {diagnostic_code}".lower()
    result = _match_provider_in_text(combined)
    if result:
        return result
    # Try body text for MTA-specific templates.
    bl = body.lower()
    if "this is the mail system at host" in bl:
        return NdrProvider.POSTFIX
    if "a message that you sent could not be delivered" in bl:
        return NdrProvider.EXIM
    return None


def _match_provider_in_text(text: str) -> NdrProvider | None:
    """Match provider patterns in a combined DSN text string."""
    if re.search(r"protection\.outlook\.com|\.outlook\.com|\.onmicrosoft\.com|microsoft", text):
        return NdrProvider.MICROSOFT
    if re.search(r"amazonses\.com|amazonaws\.com|\.awsapps\.com", text):
        return NdrProvider.AWS
    if re.search(r"\.google\.com|googlemail\.com|aspmx\.l\.google\.com", text):
        return NdrProvider.GOOGLE
    return None


def _relay_target_evidence(provider: NdrProvider, remote_mta: str, diagnostic_code: str) -> list[tuple[str, str]]:
    """Build evidence entries for a relay-detected target."""
    patterns: list[tuple[str, str]] = []
    if remote_mta:
        patterns.append(("remote-mta (relay bounce)", remote_mta))
    if diagnostic_code:
        patterns.append(("diagnostic-code (relay bounce)", diagnostic_code[:200]))
    return patterns


# ── Text extraction helpers ───────────────────────────────────────


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
                if isinstance(payload, list):
                    return "\n".join(str(p) for p in payload)
                return str(payload) if payload else ""
        return ""
    payload = msg.get_payload(decode=True)
    return payload.decode("utf-8", errors="replace") if isinstance(payload, bytes) else ""


def _extract_dsn_fields(msg: EmailMessage) -> dict[str, str]:
    """Extract DSN fields (Reporting-MTA, Remote-MTA, Final-Recipient, etc.).

    Handles both ``message/delivery-status`` (RFC 3464) and the common
    ``text/delivery-status`` variant.  Also scans all text parts for
    DSN-like key-value lines as a fallback (some MTAs embed DSN fields
    in the plain-text body).
    """
    fields: dict[str, str] = {}
    if not msg.is_multipart():
        _parse_dsn_text(msg.get_payload(decode=True), fields)
        return fields

    dsn_types = ("message/delivery-status", "text/delivery-status")
    for part in msg.walk():
        ct = part.get_content_type()
        if ct in dsn_types:
            _parse_dsn_text(part.get_payload(decode=True), fields)
            # Also try string/list payloads (email lib quirks).
            if not fields:
                raw = part.get_payload()
                if isinstance(raw, list):
                    _parse_dsn_text("\n".join(str(p) for p in raw), fields)
                elif isinstance(raw, str):
                    _parse_dsn_text(raw, fields)

    # Fallback: scan all text/plain parts for DSN-like fields.
    if not fields:
        for part in msg.walk():
            if part.get_content_type() == "text/plain":
                _parse_dsn_text(part.get_payload(decode=True), fields)
                if fields:
                    break

    return fields


def _parse_dsn_text(payload: object, fields: dict[str, str]) -> None:
    """Parse DSN key-value lines from raw payload into *fields*."""
    text = ""
    if isinstance(payload, bytes):
        text = payload.decode("utf-8", errors="replace")
    elif isinstance(payload, str):
        text = payload
    else:
        return

    dsn_keys = {
        "reporting-mta",
        "remote-mta",
        "diagnostic-code",
        "final-recipient",
        "original-envelope-id",
        "action",
        "status",
        "last-attempt-date",
    }
    for line in text.splitlines():
        if ":" not in line:
            continue
        key, _, value = line.partition(":")
        key = key.strip().lower()
        if key not in dsn_keys:
            continue
        value = value.strip()
        # Strip DSN type prefix (e.g. "dns;" or "rfc822;").
        if ";" in value:
            value = value.split(";", 1)[1].strip()
        fields[key] = value


def _guess_mta_from_received(received_chain: list[str]) -> str:
    """Best-effort extraction of the innermost MTA hostname from Received headers."""
    if not received_chain:
        return ""
    last = received_chain[-1]
    m = re.search(r"\bby\s+([\w.\-]+)", last)
    return m.group(1) if m else last[:120]
