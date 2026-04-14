"""SMTP probe sender with rate limiting and dry-run support."""

from __future__ import annotations

import asyncio
import smtplib
from datetime import datetime, timezone
from email.message import EmailMessage
from email.utils import formatdate, make_msgid

from loguru import logger
from rich.console import Console
from rich.table import Table

from mail_municipalities.accuracy.config import AccuracyConfig
from mail_municipalities.accuracy.models import Probe, ProbeStatus
from mail_municipalities.accuracy.state import StateDB

console = Console()

_SUBJECT = "Automated delivery validation probe \u2014 please disregard"

_BODY_TEMPLATE = """\
This is an automated email delivery validation probe sent as part of
academic research on email infrastructure of public administrations.

This message was sent to a randomly generated, non-existent address
and requires no action on your part.  It will likely generate an
automatic bounce/NDR, which is the intended outcome.

If you have questions, please contact: {sender}

Probe-ID: {probe_id}
"""


async def send_probes(
    state: StateDB,
    config: AccuracyConfig,
    *,
    max_probes: int | None = None,
    dry_run: bool = True,
    confirm: bool = False,
    batch_size: int | None = None,
    rate: float | None = None,
) -> None:
    """Send pending probes via SMTP.

    In *dry_run* mode (default), only prints what would be sent.
    """
    cap = max_probes or config.max_probes_per_run
    probes = await state.get_probes_by_status(ProbeStatus.PENDING, limit=cap)

    if not probes:
        console.print("[yellow]No pending probes to send.[/yellow]")
        return

    _print_send_plan(probes)

    if dry_run:
        console.print(f"\n[bold cyan]DRY RUN[/bold cyan]: would send {len(probes)} probe(s). No emails sent.")
        return

    # Live mode — require confirmation.
    if not confirm:
        answer = input(f"\nAbout to send {len(probes)} real emails. Type YES to proceed: ")
        if answer.strip() != "YES":
            console.print("[red]Aborted.[/red]")
            return

    # Validate config.
    if not config.smtp_user or not config.smtp_password.get_secret_value():
        console.print("[red]SMTP credentials not configured. Set ACCURACY_SMTP_USER and ACCURACY_SMTP_PASSWORD.[/red]")
        return

    effective_rate = rate or config.send_rate_per_second
    effective_batch = batch_size or config.send_batch_size
    delay = 1.0 / effective_rate if effective_rate > 0 else 1.0

    sent = 0
    failed = 0
    start = datetime.now(tz=timezone.utc)

    for i, probe in enumerate(probes):
        try:
            message_id = await _send_one(probe, config)
            await state.update_probe_status(
                probe.probe_id,
                ProbeStatus.SENT,
                sent_at=datetime.now(tz=timezone.utc),
                message_id=message_id,
            )
            sent += 1
            logger.debug("Sent probe {} to {}", probe.probe_id[:8], probe.recipient)
        except Exception as exc:
            await state.update_probe_status(
                probe.probe_id,
                ProbeStatus.SEND_FAILED,
                smtp_response=str(exc)[:500],
            )
            failed += 1
            logger.warning("Failed to send probe {} to {}: {}", probe.probe_id[:8], probe.recipient, exc)

        # Rate limiting.
        if i + 1 < len(probes):
            await asyncio.sleep(delay)
            # Batch pause.
            if (i + 1) % effective_batch == 0:
                logger.info("Batch pause ({} sent so far)...", sent)
                await asyncio.sleep(config.send_batch_pause_seconds)

    elapsed = (datetime.now(tz=timezone.utc) - start).total_seconds()
    console.print(f"\n[bold green]Done.[/bold green] Sent: {sent}, Failed: {failed}, Elapsed: {elapsed:.1f}s")


async def _send_one(probe: Probe, config: AccuracyConfig) -> str:
    """Send a single probe email. Returns the Message-ID."""
    msg = EmailMessage()
    msg["From"] = config.sender_address
    msg["To"] = probe.recipient
    msg["Subject"] = _SUBJECT
    msg["Date"] = formatdate(localtime=True)
    msg_id = make_msgid(domain=config.sender_address.split("@")[-1] if "@" in config.sender_address else "probe.local")
    msg["Message-ID"] = msg_id
    msg["Auto-Submitted"] = "auto-generated"  # RFC 3834
    msg["Precedence"] = "junk"
    msg["X-Probe-ID"] = probe.probe_id

    body = _BODY_TEMPLATE.format(sender=config.sender_address, probe_id=probe.probe_id)
    msg.set_content(body)

    # Send via smtplib in a thread to keep async compat.
    def _smtp_send() -> None:
        with smtplib.SMTP(config.smtp_host, config.smtp_port, timeout=30) as smtp:
            smtp.starttls()
            smtp.login(config.smtp_user, config.smtp_password.get_secret_value())
            smtp.send_message(msg)

    await asyncio.to_thread(_smtp_send)
    return msg_id


def _print_send_plan(probes: list[Probe]) -> None:
    """Print a summary of probes about to be sent."""
    table = Table(title=f"Probes to Send ({len(probes)})", show_lines=False)
    table.add_column("#", justify="right", width=4)
    table.add_column("Domain", min_width=25)
    table.add_column("Recipient", min_width=35)
    table.add_column("Predicted", min_width=12)
    table.add_column("Country", width=4)

    for i, p in enumerate(probes[:20], 1):
        table.add_row(str(i), p.domain, p.recipient, p.predicted_provider, p.country.upper())
    if len(probes) > 20:
        table.add_row("...", f"({len(probes) - 20} more)", "", "", "")
    console.print(table)
