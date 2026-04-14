"""SQLite state management for probe lifecycle tracking."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import aiosqlite

from mail_municipalities.accuracy.models import NdrEvidence, NdrProvider, NdrResult, Probe, ProbeStatus

_SCHEMA = """
CREATE TABLE IF NOT EXISTS probes (
    probe_id TEXT PRIMARY KEY,
    domain TEXT NOT NULL,
    municipality_code TEXT NOT NULL,
    municipality_name TEXT NOT NULL,
    country TEXT NOT NULL,
    recipient TEXT NOT NULL,
    predicted_provider TEXT NOT NULL,
    predicted_confidence REAL NOT NULL,
    gateway TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    sent_at TEXT,
    message_id TEXT,
    smtp_response TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ndrs (
    ndr_id INTEGER PRIMARY KEY AUTOINCREMENT,
    probe_id TEXT NOT NULL REFERENCES probes(probe_id),
    received_at TEXT NOT NULL,
    ndr_from TEXT NOT NULL,
    ndr_provider TEXT NOT NULL,
    generating_mta TEXT NOT NULL,
    confidence REAL NOT NULL,
    evidence_json TEXT NOT NULL DEFAULT '[]',
    raw_headers TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS runs (
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at TEXT NOT NULL,
    command TEXT NOT NULL,
    config_json TEXT NOT NULL DEFAULT '{}',
    completed_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_probes_status ON probes(status);
CREATE INDEX IF NOT EXISTS idx_probes_domain ON probes(domain);
CREATE INDEX IF NOT EXISTS idx_probes_message_id ON probes(message_id);
CREATE INDEX IF NOT EXISTS idx_ndrs_probe_id ON ndrs(probe_id);
"""


def _now_utc() -> str:
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class StateDB:
    """Async SQLite state store for probe lifecycle."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._db: aiosqlite.Connection | None = None

    async def __aenter__(self) -> StateDB:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._db = await aiosqlite.connect(self._path)
        self._db.row_factory = aiosqlite.Row
        await self._db.execute("PRAGMA journal_mode=WAL")
        await self._db.execute("PRAGMA synchronous=NORMAL")
        await self._db.executescript(_SCHEMA)
        await self._db.commit()
        return self

    async def __aexit__(self, *exc: object) -> None:
        if self._db is not None:
            await self._db.close()
            self._db = None

    # ── Runs ──────────────────────────────────────────────────────

    async def start_run(self, command: str, config_json: str = "{}") -> int:
        assert self._db is not None
        now = _now_utc()
        cur = await self._db.execute(
            "INSERT INTO runs (started_at, command, config_json) VALUES (?, ?, ?)",
            (now, command, config_json),
        )
        await self._db.commit()
        assert cur.lastrowid is not None
        return cur.lastrowid

    async def finish_run(self, run_id: int) -> None:
        assert self._db is not None
        await self._db.execute("UPDATE runs SET completed_at = ? WHERE run_id = ?", (_now_utc(), run_id))
        await self._db.commit()

    # ── Probes ────────────────────────────────────────────────────

    async def insert_probes(self, probes: list[Probe]) -> int:
        """Insert probes, skipping domains that already exist. Returns count inserted."""
        assert self._db is not None
        now = _now_utc()
        inserted = 0
        for p in probes:
            try:
                await self._db.execute(
                    "INSERT INTO probes "
                    "(probe_id, domain, municipality_code, municipality_name, country, "
                    "recipient, predicted_provider, predicted_confidence, gateway, "
                    "status, sent_at, message_id, smtp_response, created_at, updated_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        p.probe_id,
                        p.domain,
                        p.municipality_code,
                        p.municipality_name,
                        p.country,
                        p.recipient,
                        p.predicted_provider,
                        p.predicted_confidence,
                        p.gateway,
                        p.status.value,
                        p.sent_at.isoformat() if p.sent_at else None,
                        p.message_id,
                        p.smtp_response,
                        now,
                        now,
                    ),
                )
                inserted += 1
            except aiosqlite.IntegrityError:
                pass  # duplicate probe_id — skip
        await self._db.commit()
        return inserted

    async def get_probes_by_status(self, status: ProbeStatus, limit: int | None = None) -> list[Probe]:
        assert self._db is not None
        sql = "SELECT * FROM probes WHERE status = ? ORDER BY created_at"
        params: list[object] = [status.value]
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)
        rows = await self._db.execute_fetchall(sql, params)
        return [_row_to_probe(r) for r in rows]

    async def get_all_probes(self) -> list[Probe]:
        assert self._db is not None
        rows = await self._db.execute_fetchall("SELECT * FROM probes ORDER BY created_at")
        return [_row_to_probe(r) for r in rows]

    async def get_existing_domains(self) -> set[str]:
        """Return domains that already have probes."""
        assert self._db is not None
        rows = await self._db.execute_fetchall("SELECT DISTINCT domain FROM probes")
        return {r[0] for r in rows}

    async def update_probe_status(
        self,
        probe_id: str,
        status: ProbeStatus,
        *,
        sent_at: datetime | None = None,
        message_id: str | None = None,
        smtp_response: str | None = None,
    ) -> None:
        assert self._db is not None
        now = _now_utc()
        await self._db.execute(
            "UPDATE probes SET status = ?, sent_at = COALESCE(?, sent_at), "
            "message_id = COALESCE(?, message_id), smtp_response = COALESCE(?, smtp_response), "
            "updated_at = ? WHERE probe_id = ?",
            (
                status.value,
                sent_at.isoformat() if sent_at else None,
                message_id,
                smtp_response,
                now,
                probe_id,
            ),
        )
        await self._db.commit()

    async def mark_timed_out(self, cutoff: datetime) -> int:
        """Mark SENT probes older than *cutoff* as NO_NDR. Returns count updated."""
        assert self._db is not None
        now = _now_utc()
        cur = await self._db.execute(
            "UPDATE probes SET status = ?, updated_at = ? WHERE status = ? AND sent_at < ?",
            (ProbeStatus.NO_NDR.value, now, ProbeStatus.SENT.value, cutoff.isoformat()),
        )
        await self._db.commit()
        return cur.rowcount

    async def find_probe_by_message_id(self, message_id: str) -> Probe | None:
        assert self._db is not None
        rows = await self._db.execute_fetchall("SELECT * FROM probes WHERE message_id = ?", (message_id,))
        return _row_to_probe(rows[0]) if rows else None

    async def find_probe_by_recipient(self, recipient: str) -> Probe | None:
        assert self._db is not None
        rows = await self._db.execute_fetchall("SELECT * FROM probes WHERE recipient = ?", (recipient,))
        return _row_to_probe(rows[0]) if rows else None

    async def find_probe_by_recipient_substring(self, substring: str) -> Probe | None:
        """Find a probe whose recipient contains *substring* (e.g. the UUID portion)."""
        assert self._db is not None
        rows = await self._db.execute_fetchall(
            "SELECT * FROM probes WHERE recipient LIKE ? LIMIT 1", (f"%{substring}%",)
        )
        return _row_to_probe(rows[0]) if rows else None

    # ── NDRs ──────────────────────────────────────────────────────

    async def insert_ndr(self, ndr: NdrResult) -> None:
        assert self._db is not None
        now = _now_utc()
        evidence_json = json.dumps([{"pattern": e.pattern, "matched_value": e.matched_value} for e in ndr.evidence])
        await self._db.execute(
            "INSERT INTO ndrs "
            "(probe_id, received_at, ndr_from, ndr_provider, generating_mta, "
            "confidence, evidence_json, raw_headers, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                ndr.probe_id,
                ndr.received_at.isoformat(),
                ndr.ndr_from,
                ndr.ndr_provider.value,
                ndr.generating_mta,
                ndr.confidence,
                evidence_json,
                ndr.raw_headers,
                now,
            ),
        )
        await self._db.commit()

    async def get_all_ndrs(self) -> list[NdrResult]:
        assert self._db is not None
        rows = await self._db.execute_fetchall("SELECT * FROM ndrs ORDER BY created_at")
        return [_row_to_ndr(r) for r in rows]

    async def has_ndr_for_probe(self, probe_id: str) -> bool:
        assert self._db is not None
        rows = await self._db.execute_fetchall("SELECT 1 FROM ndrs WHERE probe_id = ? LIMIT 1", (probe_id,))
        return len(rows) > 0

    # ── Status summary ────────────────────────────────────────────

    async def status_counts(self) -> dict[str, int]:
        assert self._db is not None
        rows = await self._db.execute_fetchall("SELECT status, COUNT(*) FROM probes GROUP BY status")
        return {r[0]: r[1] for r in rows}

    async def country_counts(self) -> dict[str, int]:
        assert self._db is not None
        rows = await self._db.execute_fetchall("SELECT country, COUNT(*) FROM probes GROUP BY country")
        return {r[0]: r[1] for r in rows}


def _row_to_probe(row: aiosqlite.Row) -> Probe:
    return Probe(
        probe_id=row["probe_id"],
        domain=row["domain"],
        municipality_code=row["municipality_code"],
        municipality_name=row["municipality_name"],
        country=row["country"],
        recipient=row["recipient"],
        predicted_provider=row["predicted_provider"],
        predicted_confidence=row["predicted_confidence"],
        gateway=row["gateway"],
        status=ProbeStatus(row["status"]),
        sent_at=datetime.fromisoformat(row["sent_at"]) if row["sent_at"] else None,
        message_id=row["message_id"],
        smtp_response=row["smtp_response"],
    )


def _row_to_ndr(row: aiosqlite.Row) -> NdrResult:
    evidence_raw = json.loads(row["evidence_json"]) if row["evidence_json"] else []
    return NdrResult(
        probe_id=row["probe_id"],
        received_at=datetime.fromisoformat(row["received_at"]),
        ndr_from=row["ndr_from"],
        ndr_provider=NdrProvider(row["ndr_provider"]),
        generating_mta=row["generating_mta"],
        confidence=row["confidence"],
        evidence=[NdrEvidence(pattern=e["pattern"], matched_value=e["matched_value"]) for e in evidence_raw],
        raw_headers=row["raw_headers"],
    )
