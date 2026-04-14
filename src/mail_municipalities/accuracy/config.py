"""Configuration for bounce-probe accuracy validation."""

from __future__ import annotations

from pathlib import Path

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class AccuracyConfig(BaseSettings):
    """Settings loaded from environment variables with ``ACCURACY_`` prefix.

    Credentials can be set via a ``.env`` file or exported in the shell.
    """

    model_config = SettingsConfigDict(env_prefix="ACCURACY_", env_file=".env", env_file_encoding="utf-8")

    # ── SMTP (Gmail relay) ─────────────────────────────────────────
    smtp_host: str = "smtp.gmail.com"
    smtp_port: int = 587
    smtp_user: str = ""
    smtp_password: SecretStr = SecretStr("")
    smtp_from: str = ""  # envelope sender; defaults to smtp_user if empty

    # ── IMAP (Gmail) ──────────────────────────────────────────────
    imap_host: str = "imap.gmail.com"
    imap_port: int = 993
    imap_user: str = ""
    imap_password: SecretStr = SecretStr("")
    imap_folder: str = "INBOX"

    # ── Rate limiting ─────────────────────────────────────────────
    send_rate_per_second: float = 1.0
    send_batch_size: int = 25
    send_batch_pause_seconds: float = 30.0

    # ── Safety ────────────────────────────────────────────────────
    max_probes_per_run: int = 100
    dry_run: bool = True  # must explicitly disable

    # ── NDR collection ────────────────────────────────────────────
    ndr_poll_interval_seconds: int = 300
    ndr_max_wait_hours: float = 24.0

    # ── Paths ─────────────────────────────────────────────────────
    output_dir: Path = Path("output/accuracy")
    providers_dir: Path = Path("output/providers")

    @property
    def sender_address(self) -> str:
        return self.smtp_from or self.smtp_user

    @property
    def state_db_path(self) -> Path:
        return self.output_dir / "state.db"
