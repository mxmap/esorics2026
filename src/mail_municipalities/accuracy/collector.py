"""IMAP NDR collector — polls a mailbox for bounce messages and matches them to probes."""

from __future__ import annotations

import asyncio
import email
import imaplib
import re
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage

from loguru import logger
from rich.console import Console

from mail_municipalities.accuracy.config import AccuracyConfig
from mail_municipalities.accuracy.models import NdrResult, ProbeStatus
from mail_municipalities.accuracy.ndr_parser import parse_ndr
from mail_municipalities.accuracy.state import StateDB

console = Console()

# UUID pattern used in probe recipient addresses (12 hex chars).
_UUID_RE = re.compile(r"validation-probe-([0-9a-f]{12})")


async def collect_ndrs(
    state: StateDB,
    config: AccuracyConfig,
    *,
    poll_once: bool = False,
    wait_hours: float | None = None,
    poll_interval: int | None = None,
) -> None:
    """Poll IMAP for NDRs, match to probes, and update state."""
    max_wait = wait_hours or config.ndr_max_wait_hours
    interval = poll_interval or config.ndr_poll_interval_seconds
    deadline = datetime.now(tz=timezone.utc) + timedelta(hours=max_wait)

    if not config.imap_user or not config.imap_password.get_secret_value():
        console.print("[red]IMAP credentials not configured. Set ACCURACY_IMAP_USER and ACCURACY_IMAP_PASSWORD.[/red]")
        return

    round_num = 0
    while True:
        round_num += 1
        logger.info("Collection round {} ...", round_num)
        matched, skipped = await _collect_one_round(state, config)
        console.print(f"  Round {round_num}: {matched} matched, {skipped} skipped")

        # Mark timed-out probes.
        cutoff = datetime.now(tz=timezone.utc) - timedelta(hours=max_wait)
        timed_out = await state.mark_timed_out(cutoff)
        if timed_out:
            logger.info("Marked {} probes as timed out (no NDR)", timed_out)

        if poll_once:
            break
        if datetime.now(tz=timezone.utc) >= deadline:
            logger.info("Reached wait deadline ({:.1f}h). Stopping.", max_wait)
            break

        logger.info("Sleeping {}s until next poll...", interval)
        await asyncio.sleep(interval)


async def _collect_one_round(state: StateDB, config: AccuracyConfig) -> tuple[int, int]:
    """Fetch NDR messages via IMAP, parse, and match. Returns (matched, skipped)."""

    def _imap_fetch() -> list[EmailMessage]:
        with imaplib.IMAP4_SSL(config.imap_host, config.imap_port) as imap:
            imap.login(config.imap_user, config.imap_password.get_secret_value())
            imap.select(config.imap_folder, readonly=True)
            # Search for bounce-like messages.
            _status, data = imap.search(None, '(OR FROM "mailer-daemon" FROM "postmaster")')
            msg_nums = data[0].split() if data[0] else []
            messages: list[EmailMessage] = []
            for num in msg_nums:
                _status, msg_data = imap.fetch(num, "(RFC822)")
                if msg_data and msg_data[0] and isinstance(msg_data[0], tuple):
                    raw = msg_data[0][1]
                    if isinstance(raw, bytes):
                        parsed = email.message_from_bytes(raw, policy=email.policy.default)
                        if isinstance(parsed, EmailMessage):
                            messages.append(parsed)
            return messages

    messages = await asyncio.to_thread(_imap_fetch)
    logger.debug("Fetched {} candidate NDR messages", len(messages))

    matched = 0
    skipped = 0

    for msg in messages:
        probe = await _match_ndr_to_probe(msg, state)
        if probe is None:
            skipped += 1
            continue

        # Check if we already have an NDR for this probe.
        if await state.has_ndr_for_probe(probe.probe_id):
            skipped += 1
            continue

        provider, confidence, generating_mta, evidence = parse_ndr(msg)

        ndr = NdrResult(
            probe_id=probe.probe_id,
            received_at=datetime.now(tz=timezone.utc),
            ndr_from=(msg.get("From") or ""),
            ndr_provider=provider,
            generating_mta=generating_mta,
            confidence=confidence,
            evidence=evidence,
            raw_headers=_extract_headers(msg),
        )
        await state.insert_ndr(ndr)
        await state.update_probe_status(probe.probe_id, ProbeStatus.NDR_RECEIVED)
        matched += 1
        logger.debug("Matched NDR to probe {} ({}): {}", probe.probe_id[:8], probe.domain, provider.value)

    return matched, skipped


async def _match_ndr_to_probe(msg: EmailMessage, state: StateDB):
    """Try to match an NDR message to a probe via multiple strategies."""
    # Strategy 1: In-Reply-To / References contain the original Message-ID.
    for hdr in ("In-Reply-To", "References"):
        ref = msg.get(hdr)
        if ref:
            # May contain multiple message-ids; try each.
            for mid in re.findall(r"<[^>]+>", ref):
                probe = await state.find_probe_by_message_id(mid)
                if probe:
                    return probe

    # Strategy 2: Extract recipient UUID from DSN Final-Recipient or body.
    body_text = _get_text(msg)
    for m in _UUID_RE.finditer(body_text):
        uuid_part = m.group(1)
        probe = await state.find_probe_by_recipient_substring(uuid_part)
        if probe:
            return probe

    # Strategy 3: Look for probe recipient in any header or body text.
    headers_text = "\n".join(f"{k}: {v}" for k, v in msg.items())
    for m in _UUID_RE.finditer(headers_text):
        uuid_part = m.group(1)
        probe = await state.find_probe_by_recipient_substring(uuid_part)
        if probe:
            return probe

    return None


def _get_text(msg: EmailMessage) -> str:
    """Extract all text content from a message."""
    parts: list[str] = []
    if msg.is_multipart():
        for part in msg.walk():
            payload = part.get_payload(decode=True)
            if isinstance(payload, bytes):
                parts.append(payload.decode("utf-8", errors="replace"))
    else:
        payload = msg.get_payload(decode=True)
        if isinstance(payload, bytes):
            parts.append(payload.decode("utf-8", errors="replace"))
    return "\n".join(parts)


def _extract_headers(msg: EmailMessage) -> str:
    """Return the raw header block as a string."""
    return "\n".join(f"{k}: {v}" for k, v in msg.items())
