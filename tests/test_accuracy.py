"""Tests for the accuracy validation package."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from email.message import EmailMessage

import pytest

from mail_municipalities.accuracy.metrics import compute_accuracy
from mail_municipalities.accuracy.models import (
    CLASSIFIER_TO_EVAL,
    NDR_TO_CLASSIFIER,
    NdrEvidence,
    NdrProvider,
    NdrResult,
    Probe,
    ProbeStatus,
)
from mail_municipalities.accuracy.ndr_parser import parse_ndr
from mail_municipalities.accuracy.sampler import _make_probe, _stratified_sample
from mail_municipalities.accuracy.state import StateDB


# ── Model tests ───────────────────────────────────────────────────


class TestModels:
    def test_probe_status_values(self):
        assert ProbeStatus.PENDING.value == "pending"
        assert ProbeStatus.SENT.value == "sent"
        assert ProbeStatus.NDR_RECEIVED.value == "ndr_received"

    def test_ndr_provider_values(self):
        assert NdrProvider.MICROSOFT.value == "microsoft"
        assert NdrProvider.POSTFIX.value == "postfix"

    def test_ndr_to_classifier_mapping(self):
        assert NDR_TO_CLASSIFIER[NdrProvider.MICROSOFT] == "microsoft"
        assert NDR_TO_CLASSIFIER[NdrProvider.POSTFIX] == "self-hosted"
        assert NDR_TO_CLASSIFIER[NdrProvider.EXIM] == "self-hosted"
        assert NDR_TO_CLASSIFIER[NdrProvider.EXCHANGE_ONPREM] == "self-hosted"
        assert NDR_TO_CLASSIFIER[NdrProvider.UNKNOWN] == "unknown"

    def test_classifier_to_eval_mapping(self):
        assert CLASSIFIER_TO_EVAL["domestic"] == "self-hosted"
        assert CLASSIFIER_TO_EVAL["foreign"] == "self-hosted"
        assert CLASSIFIER_TO_EVAL["microsoft"] == "microsoft"

    def test_probe_creation(self):
        p = Probe(
            probe_id="abc123",
            domain="example.ch",
            municipality_code="42",
            municipality_name="Testingen",
            country="ch",
            recipient="validation-probe-abc@example.ch",
            predicted_provider="microsoft",
            predicted_confidence=90.0,
        )
        assert p.status == ProbeStatus.PENDING
        assert p.sent_at is None
        assert p.gateway is None


# ── Sampler tests ─────────────────────────────────────────────────


class TestSampler:
    @pytest.fixture
    def entries(self):
        """Fake municipality entries for sampling."""
        result = []
        providers = ["microsoft"] * 50 + ["google"] * 5 + ["aws"] * 10 + ["domestic"] * 30 + ["foreign"] * 5
        for i, prov in enumerate(providers):
            result.append(
                {
                    "code": str(i),
                    "name": f"Municipality {i}",
                    "domain": f"m{i}.example.ch",
                    "provider": prov,
                    "classification_confidence": 80.0,
                }
            )
        return result

    def test_stratified_sample_respects_size(self, entries):
        sample = _stratified_sample(entries, total_size=20, min_per_class=2)
        assert len(sample) <= 20

    def test_stratified_sample_minimum_per_class(self, entries):
        sample = _stratified_sample(entries, total_size=20, min_per_class=3)
        by_provider = {}
        for e in sample:
            p = e["provider"]
            by_provider[p] = by_provider.get(p, 0) + 1
        # Each validatable class should have at least min_per_class (or all if fewer exist).
        for provider in ("microsoft", "google", "aws", "domestic", "foreign"):
            assert by_provider.get(provider, 0) >= min(3, sum(1 for e in entries if e["provider"] == provider))

    def test_excludes_unknown_provider(self, entries):
        entries.append({"code": "999", "name": "Unknown", "domain": "unk.ch", "provider": "unknown"})
        sample = _stratified_sample(entries, total_size=200, min_per_class=1)
        providers = {e["provider"] for e in sample}
        assert "unknown" not in providers

    def test_excludes_empty_domain(self, entries):
        entries.append({"code": "998", "name": "NoDomain", "domain": "", "provider": "microsoft"})
        sample = _stratified_sample(entries, total_size=200, min_per_class=1)
        assert not any(e["domain"] == "" for e in sample)

    def test_make_probe(self):
        entry = {
            "code": "42",
            "name": "Test",
            "domain": "test.ch",
            "provider": "microsoft",
            "classification_confidence": 90.0,
            "gateway": "seppmail",
        }
        probe = _make_probe(entry, "ch")
        assert probe.domain == "test.ch"
        assert probe.country == "ch"
        assert probe.predicted_provider == "microsoft"
        assert probe.gateway == "seppmail"
        assert "validation-probe-" in probe.recipient
        assert probe.recipient.endswith("@test.ch")
        assert probe.status == ProbeStatus.PENDING


# ── NDR parser tests ──────────────────────────────────────────────


def _make_ndr_email(**kwargs) -> EmailMessage:
    """Build a minimal NDR EmailMessage for testing."""
    msg = EmailMessage()
    msg["From"] = kwargs.get("from_addr", "mailer-daemon@example.com")
    msg["Subject"] = kwargs.get("subject", "Delivery Status Notification")
    for hdr, val in kwargs.get("extra_headers", {}).items():
        msg[hdr] = val
    if "received" in kwargs:
        for r in kwargs["received"]:
            msg["Received"] = r
    msg.set_content(kwargs.get("body", ""))
    return msg


class TestNdrParser:
    def test_microsoft_exchange_online(self):
        msg = _make_ndr_email(
            from_addr="postmaster@outlook.com",
            extra_headers={
                "X-MS-Exchange-Message-Sent-Representing-Type": "1",
            },
            received=["from mail-eopbgr70045.outbound.protection.outlook.com by mx.example.com"],
            body="Delivery has failed to these recipients or groups.",
        )
        provider, confidence, mta, evidence = parse_ndr(msg)
        assert provider == NdrProvider.MICROSOFT
        assert confidence > 0.3
        assert len(evidence) >= 2

    def test_google_workspace_relay_bounce(self):
        """Gmail relay bounce where the target IS Google Workspace.

        The Remote-MTA in the DSN points to Google, confirming the
        target is actually Google (not just our relay).
        """
        from email.mime.multipart import MIMEMultipart
        from email.mime.text import MIMEText

        outer = MIMEMultipart("report", report_type="delivery-status")
        outer["From"] = "mailer-daemon@googlemail.com"
        outer["Received"] = "from mail-wr1-f54.google.com by mx.google.com"

        text_part = MIMEText("Delivery to the following recipient failed permanently.")
        outer.attach(text_part)

        dsn_part = MIMEText(
            "Reporting-MTA: dns; googlemail.com\n"
            "Remote-MTA: dns; aspmx.l.google.com\n"
            "Diagnostic-Code: smtp; 550 5.1.1 not exist\n",
            "delivery-status",
        )
        outer.attach(dsn_part)

        import email
        import email.policy

        parsed = email.message_from_bytes(outer.as_bytes(), policy=email.policy.default)
        assert isinstance(parsed, EmailMessage)
        provider, confidence, mta, evidence = parse_ndr(parsed)
        assert provider == NdrProvider.GOOGLE

    def test_google_workspace_direct_ndr(self):
        """NDR sent directly by Google (not via our Gmail relay)."""
        msg = _make_ndr_email(
            from_addr="postmaster@google.com",
            received=["from mail-wr1-f54.google.com by mx.example.com"],
            body="Delivery to the following recipient failed permanently.",
        )
        provider, confidence, mta, evidence = parse_ndr(msg)
        assert provider == NdrProvider.GOOGLE
        assert confidence > 0.3

    def test_aws_ses(self):
        msg = _make_ndr_email(
            from_addr="MAILER-DAEMON@amazonses.com",
            extra_headers={"X-SES-Outgoing": "2024.01.01-54.240.0.1"},
            received=["from a48-93.smtp-out.amazonses.com by mx.example.com"],
        )
        provider, confidence, mta, evidence = parse_ndr(msg)
        assert provider == NdrProvider.AWS
        assert confidence > 0.3

    def test_postfix(self):
        msg = _make_ndr_email(
            from_addr="MAILER-DAEMON@mail.example.ch",
            received=["from mail.example.ch (Postfix) by mail.example.ch"],
            body="This is the mail system at host mail.example.ch.\n\nI'm sorry to have to inform you...",
        )
        provider, confidence, mta, evidence = parse_ndr(msg)
        assert provider == NdrProvider.POSTFIX
        assert confidence > 0.3

    def test_exim(self):
        msg = _make_ndr_email(
            from_addr="MAILER-DAEMON@mail.example.de",
            subject="Mail delivery failed: returning message to sender",
            received=["from mail.example.de (Exim 4.96) by mail.example.de"],
            body="A message that you sent could not be delivered to one or more of its recipients.",
        )
        provider, confidence, mta, evidence = parse_ndr(msg)
        assert provider == NdrProvider.EXIM
        assert confidence > 0.3

    def test_exchange_onprem(self):
        msg = _make_ndr_email(
            from_addr="postmaster@internal.example.ch",
            extra_headers={
                "X-MS-Exchange-Organization-SCL": "-1",
            },
            received=["from mail.internal.example.ch by mail.internal.example.ch"],
            body="Delivery has failed to these recipients.",
        )
        provider, confidence, mta, evidence = parse_ndr(msg)
        # Has Exchange headers but no outlook.com in the chain -> on-premises.
        assert provider == NdrProvider.EXCHANGE_ONPREM

    def test_gmail_relay_bounce_to_microsoft(self):
        """Gmail generates the NDR but the actual target is MS365.

        This is the key scenario: our Gmail relay reports a rejection from
        a Microsoft MTA.  The parser must see through Gmail's headers.
        """
        from email.mime.multipart import MIMEMultipart
        from email.mime.text import MIMEText

        outer = MIMEMultipart("report", report_type="delivery-status")
        outer["From"] = "mailer-daemon@googlemail.com"
        outer["X-Gm-Message-State"] = "some-state"
        outer["Received"] = "from mail-wr1-f54.google.com by mx.google.com"

        text_part = MIMEText("Delivery to the following recipient failed permanently.")
        outer.attach(text_part)

        dsn_part = MIMEText(
            "Reporting-MTA: dns; googlemail.com\n"
            "Remote-MTA: dns; municipality.mail.protection.outlook.com\n"
            "Diagnostic-Code: smtp; 550 5.1.1 The email account does not exist\n",
            "delivery-status",
        )
        outer.attach(dsn_part)

        # Parse via email.message.EmailMessage
        import email
        import email.policy

        parsed = email.message_from_bytes(outer.as_bytes(), policy=email.policy.default)
        assert isinstance(parsed, EmailMessage)
        provider, confidence, mta, evidence = parse_ndr(parsed)
        assert provider == NdrProvider.MICROSOFT, f"Expected MICROSOFT, got {provider}"

    def test_gmail_relay_bounce_to_aws(self):
        """Gmail NDR where the actual target is AWS SES."""
        from email.mime.multipart import MIMEMultipart
        from email.mime.text import MIMEText

        outer = MIMEMultipart("report", report_type="delivery-status")
        outer["From"] = "mailer-daemon@googlemail.com"
        outer["X-Gm-Message-State"] = "some-state"
        outer["Received"] = "from mail-wr1-f54.google.com by mx.google.com"

        text_part = MIMEText("Delivery to the following recipient failed permanently.")
        outer.attach(text_part)

        dsn_part = MIMEText(
            "Reporting-MTA: dns; googlemail.com\n"
            "Remote-MTA: dns; inbound-smtp.eu-west-1.amazonaws.com\n"
            "Diagnostic-Code: smtp; 550 5.1.1 unknown user\n",
            "delivery-status",
        )
        outer.attach(dsn_part)

        import email
        import email.policy

        parsed = email.message_from_bytes(outer.as_bytes(), policy=email.policy.default)
        assert isinstance(parsed, EmailMessage)
        provider, confidence, mta, evidence = parse_ndr(parsed)
        assert provider == NdrProvider.AWS, f"Expected AWS, got {provider}"

    def test_gmail_relay_bounce_to_selfhosted(self):
        """Gmail relay bounce to a generic self-hosted MTA (not a cloud provider)."""
        from email.mime.multipart import MIMEMultipart
        from email.mime.text import MIMEText

        outer = MIMEMultipart("report", report_type="delivery-status")
        outer["From"] = "mailer-daemon@googlemail.com"
        outer["X-Gm-Message-State"] = "some-state"
        outer["Received"] = "from mail-wr1-f54.google.com by mx.google.com"

        text_part = MIMEText("Delivery to the following recipient failed permanently.")
        outer.attach(text_part)

        dsn_part = MIMEText(
            "Reporting-MTA: dns; googlemail.com\n"
            "Remote-MTA: dns; mail.gemeinde-insul.de\n"
            "Diagnostic-Code: smtp; 550 5.1.1 user unknown\n",
            "delivery-status",
        )
        outer.attach(dsn_part)

        import email
        import email.policy

        parsed = email.message_from_bytes(outer.as_bytes(), policy=email.policy.default)
        assert isinstance(parsed, EmailMessage)
        provider, confidence, mta, evidence = parse_ndr(parsed)
        # Remote-MTA is a generic hostname — should be self-hosted, NOT Google.
        assert provider == NdrProvider.POSTFIX, f"Expected POSTFIX (self-hosted), got {provider}"

    def test_gmail_relay_headers_ignored(self):
        """X-Gm-* and Received from google.com should not trigger Google detection."""
        msg = _make_ndr_email(
            from_addr="postmaster@mx.example.ch",
            extra_headers={
                "X-Gm-Message-State": "some-state",
                "X-MS-Exchange-Organization-SCL": "-1",
            },
            received=[
                "from mail-wr1-f54.google.com by mx.google.com",
                "from mx.example.ch by mx.example.ch",
            ],
            body="Delivery has failed to these recipients or groups.",
        )
        provider, confidence, mta, evidence = parse_ndr(msg)
        # Should detect Exchange, not Google.
        assert provider in (NdrProvider.MICROSOFT, NdrProvider.EXCHANGE_ONPREM)

    def test_unknown_ndr(self):
        msg = _make_ndr_email(
            from_addr="postmaster@somegateway.net",
            body="Your message could not be delivered.",
        )
        provider, confidence, mta, evidence = parse_ndr(msg)
        assert provider == NdrProvider.UNKNOWN
        assert confidence == 0.0


# ── State DB tests ────────────────────────────────────────────────


class TestStateDB:
    @pytest.fixture
    async def state(self, tmp_path):
        db_path = tmp_path / "test_state.db"
        async with StateDB(db_path) as s:
            yield s

    async def test_insert_and_retrieve_probes(self, state: StateDB):
        probe = Probe(
            probe_id=uuid.uuid4().hex,
            domain="test.ch",
            municipality_code="1",
            municipality_name="Test",
            country="ch",
            recipient="validation-probe-abc@test.ch",
            predicted_provider="microsoft",
            predicted_confidence=90.0,
        )
        inserted = await state.insert_probes([probe])
        assert inserted == 1

        probes = await state.get_probes_by_status(ProbeStatus.PENDING)
        assert len(probes) == 1
        assert probes[0].domain == "test.ch"

    async def test_duplicate_probe_skipped(self, state: StateDB):
        pid = uuid.uuid4().hex
        probe = Probe(
            probe_id=pid,
            domain="test.ch",
            municipality_code="1",
            municipality_name="Test",
            country="ch",
            recipient="validation-probe-abc@test.ch",
            predicted_provider="microsoft",
            predicted_confidence=90.0,
        )
        await state.insert_probes([probe])
        inserted = await state.insert_probes([probe])
        assert inserted == 0

    async def test_update_probe_status(self, state: StateDB):
        pid = uuid.uuid4().hex
        probe = Probe(
            probe_id=pid,
            domain="test.ch",
            municipality_code="1",
            municipality_name="Test",
            country="ch",
            recipient="validation-probe-abc@test.ch",
            predicted_provider="microsoft",
            predicted_confidence=90.0,
        )
        await state.insert_probes([probe])
        now = datetime.now(tz=timezone.utc)
        await state.update_probe_status(pid, ProbeStatus.SENT, sent_at=now, message_id="<test@example.com>")

        probes = await state.get_probes_by_status(ProbeStatus.SENT)
        assert len(probes) == 1
        assert probes[0].message_id == "<test@example.com>"

    async def test_find_by_message_id(self, state: StateDB):
        pid = uuid.uuid4().hex
        probe = Probe(
            probe_id=pid,
            domain="test.ch",
            municipality_code="1",
            municipality_name="Test",
            country="ch",
            recipient="validation-probe-abc@test.ch",
            predicted_provider="microsoft",
            predicted_confidence=90.0,
        )
        await state.insert_probes([probe])
        await state.update_probe_status(pid, ProbeStatus.SENT, message_id="<unique@example.com>")

        found = await state.find_probe_by_message_id("<unique@example.com>")
        assert found is not None
        assert found.probe_id == pid

    async def test_find_by_recipient_substring(self, state: StateDB):
        pid = uuid.uuid4().hex
        probe = Probe(
            probe_id=pid,
            domain="test.ch",
            municipality_code="1",
            municipality_name="Test",
            country="ch",
            recipient="validation-probe-abc123def456@test.ch",
            predicted_provider="microsoft",
            predicted_confidence=90.0,
        )
        await state.insert_probes([probe])

        found = await state.find_probe_by_recipient_substring("abc123def456")
        assert found is not None
        assert found.probe_id == pid

    async def test_insert_and_retrieve_ndr(self, state: StateDB):
        pid = uuid.uuid4().hex
        probe = Probe(
            probe_id=pid,
            domain="test.ch",
            municipality_code="1",
            municipality_name="Test",
            country="ch",
            recipient="validation-probe-abc@test.ch",
            predicted_provider="microsoft",
            predicted_confidence=90.0,
        )
        await state.insert_probes([probe])

        ndr = NdrResult(
            probe_id=pid,
            received_at=datetime.now(tz=timezone.utc),
            ndr_from="mailer-daemon@googlemail.com",
            ndr_provider=NdrProvider.GOOGLE,
            generating_mta="mail-wr1-f54.google.com",
            confidence=0.9,
            evidence=[NdrEvidence(pattern="from mailer-daemon@google", matched_value="mailer-daemon@googlemail.com")],
        )
        await state.insert_ndr(ndr)

        ndrs = await state.get_all_ndrs()
        assert len(ndrs) == 1
        assert ndrs[0].ndr_provider == NdrProvider.GOOGLE
        assert len(ndrs[0].evidence) == 1

    async def test_status_counts(self, state: StateDB):
        for i in range(3):
            probe = Probe(
                probe_id=uuid.uuid4().hex,
                domain=f"test{i}.ch",
                municipality_code=str(i),
                municipality_name=f"Test {i}",
                country="ch",
                recipient=f"validation-probe-{i}@test{i}.ch",
                predicted_provider="microsoft",
                predicted_confidence=90.0,
            )
            await state.insert_probes([probe])

        counts = await state.status_counts()
        assert counts["pending"] == 3

    async def test_existing_domains(self, state: StateDB):
        for domain in ("a.ch", "b.ch", "a.ch"):
            probe = Probe(
                probe_id=uuid.uuid4().hex,
                domain=domain,
                municipality_code="1",
                municipality_name="Test",
                country="ch",
                recipient=f"validation-probe-{uuid.uuid4().hex[:6]}@{domain}",
                predicted_provider="microsoft",
                predicted_confidence=90.0,
            )
            await state.insert_probes([probe])

        domains = await state.get_existing_domains()
        assert domains == {"a.ch", "b.ch"}


# ── Metrics tests ─────────────────────────────────────────────────


class TestMetrics:
    @pytest.fixture
    async def state_with_data(self, tmp_path):
        """State DB with matched probes and NDRs for metrics testing."""
        db_path = tmp_path / "metrics_test.db"
        async with StateDB(db_path) as state:
            # Create probes: 3 microsoft, 2 domestic, 1 google.
            test_data = [
                ("microsoft", NdrProvider.MICROSOFT),  # correct
                ("microsoft", NdrProvider.MICROSOFT),  # correct
                ("microsoft", NdrProvider.GOOGLE),  # wrong
                ("domestic", NdrProvider.POSTFIX),  # correct (domestic -> self-hosted, postfix -> self-hosted)
                ("domestic", NdrProvider.MICROSOFT),  # wrong
                ("google", NdrProvider.GOOGLE),  # correct
            ]

            for i, (predicted, actual_ndr) in enumerate(test_data):
                pid = uuid.uuid4().hex
                probe = Probe(
                    probe_id=pid,
                    domain=f"test{i}.ch",
                    municipality_code=str(i),
                    municipality_name=f"Test {i}",
                    country="ch",
                    recipient=f"probe-{i}@test{i}.ch",
                    predicted_provider=predicted,
                    predicted_confidence=80.0,
                    status=ProbeStatus.NDR_RECEIVED,
                )
                await state.insert_probes([probe])
                await state.update_probe_status(pid, ProbeStatus.NDR_RECEIVED, sent_at=datetime.now(tz=timezone.utc))

                ndr = NdrResult(
                    probe_id=pid,
                    received_at=datetime.now(tz=timezone.utc),
                    ndr_from="mailer-daemon@example.com",
                    ndr_provider=actual_ndr,
                    generating_mta="mta.example.com",
                    confidence=0.8,
                )
                await state.insert_ndr(ndr)

            yield state

    async def test_overall_accuracy(self, state_with_data: StateDB):
        report = await compute_accuracy(state_with_data)
        # 4 correct out of 6 total.
        assert abs(report.overall_accuracy - 4 / 6) < 0.01

    async def test_per_class_metrics(self, state_with_data: StateDB):
        report = await compute_accuracy(state_with_data)
        # Microsoft: TP=2, FP=1 (domestic predicted microsoft actual), FN=1 (microsoft predicted google actual).
        ms = report.per_class["microsoft"]
        assert ms.support == 3  # 3 actual microsoft
        assert ms.precision == pytest.approx(2 / 3, abs=0.01)
        assert ms.recall == pytest.approx(2 / 3, abs=0.01)

    async def test_confusion_matrix(self, state_with_data: StateDB):
        report = await compute_accuracy(state_with_data)
        cm = report.confusion_matrix
        assert cm["microsoft"]["microsoft"] == 2
        assert cm["microsoft"]["google"] == 1
        assert cm["self-hosted"]["microsoft"] == 1
        assert cm["self-hosted"]["self-hosted"] == 1

    async def test_response_rate(self, state_with_data: StateDB):
        report = await compute_accuracy(state_with_data)
        assert report.total_ndrs == 6
        assert report.total_probes == 6

    async def test_weighted_f1_no_aws(self, state_with_data: StateDB):
        """Weighted F1 over microsoft, google, self-hosted (no AWS in fixture)."""
        report = await compute_accuracy(state_with_data)
        assert report.weighted_f1_labels == ["microsoft", "google", "self-hosted"]
        # Manually compute: weight each class F1 by its support.
        ms = report.per_class["microsoft"]
        go = report.per_class["google"]
        sh = report.per_class["self-hosted"]
        expected = (ms.f1 * ms.support + go.f1 * go.support + sh.f1 * sh.support) / (
            ms.support + go.support + sh.support
        )
        assert report.weighted_f1 == pytest.approx(expected, abs=0.001)

    async def test_weighted_f1_excludes_aws(self, tmp_path):
        """AWS probes must not affect the weighted F1 score."""
        db_path = tmp_path / "wf1_test.db"
        async with StateDB(db_path) as state:
            # 2 correct microsoft, 2 correct domestic, 1 aws->self-hosted (wrong but excluded)
            test_data = [
                ("microsoft", NdrProvider.MICROSOFT),
                ("microsoft", NdrProvider.MICROSOFT),
                ("domestic", NdrProvider.POSTFIX),
                ("domestic", NdrProvider.POSTFIX),
                ("aws", NdrProvider.POSTFIX),  # AWS misclassified — should NOT affect weighted F1
            ]
            for i, (predicted, actual_ndr) in enumerate(test_data):
                pid = uuid.uuid4().hex
                probe = Probe(
                    probe_id=pid,
                    domain=f"test{i}.ch",
                    municipality_code=str(i),
                    municipality_name=f"Test {i}",
                    country="ch",
                    recipient=f"probe-{i}@test{i}.ch",
                    predicted_provider=predicted,
                    predicted_confidence=80.0,
                    status=ProbeStatus.NDR_RECEIVED,
                )
                await state.insert_probes([probe])
                await state.update_probe_status(pid, ProbeStatus.NDR_RECEIVED, sent_at=datetime.now(tz=timezone.utc))
                ndr = NdrResult(
                    probe_id=pid,
                    received_at=datetime.now(tz=timezone.utc),
                    ndr_from="mailer-daemon@example.com",
                    ndr_provider=actual_ndr,
                    generating_mta="mta.example.com",
                    confidence=0.8,
                )
                await state.insert_ndr(ndr)

            report = await compute_accuracy(state)

        # Overall accuracy includes AWS miss: 4/5 = 0.80
        assert report.overall_accuracy == pytest.approx(0.8, abs=0.01)
        # Weighted F1 excludes AWS: microsoft and self-hosted both perfect -> 1.0
        assert report.weighted_f1 == pytest.approx(1.0, abs=0.001)
        assert "aws" not in report.weighted_f1_labels


# ── Config tests ──────────────────────────────────────────────────


class TestConfig:
    def test_defaults(self):
        from mail_municipalities.accuracy.config import AccuracyConfig

        cfg = AccuracyConfig(
            _env_file=None,  # type: ignore[call-arg]
        )
        assert cfg.smtp_host == "smtp.gmail.com"
        assert cfg.dry_run is True
        assert cfg.max_probes_per_run == 100
        assert cfg.send_rate_per_second == 1.0

    def test_sender_address_fallback(self):
        from mail_municipalities.accuracy.config import AccuracyConfig

        cfg = AccuracyConfig(
            smtp_user="user@gmail.com",
            _env_file=None,  # type: ignore[call-arg]
        )
        assert cfg.sender_address == "user@gmail.com"

    def test_sender_address_explicit(self):
        from mail_municipalities.accuracy.config import AccuracyConfig

        cfg = AccuracyConfig(
            smtp_user="user@gmail.com",
            smtp_from="sender@custom.com",
            _env_file=None,  # type: ignore[call-arg]
        )
        assert cfg.sender_address == "sender@custom.com"
