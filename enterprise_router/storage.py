from __future__ import annotations

import sqlite3
from contextlib import closing
from pathlib import Path


class Database:
    def __init__(self, path: str) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def _initialize(self) -> None:
        with closing(self.connect()) as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS agents (
                    agent_name TEXT PRIMARY KEY,
                    role TEXT NOT NULL,
                    hierarchy_level INTEGER NOT NULL,
                    trust_level INTEGER NOT NULL,
                    file_path TEXT,
                    endpoint TEXT,
                    active INTEGER NOT NULL,
                    registration_status TEXT NOT NULL,
                    allowed_senders TEXT NOT NULL DEFAULT '[]',
                    allowed_task_types TEXT NOT NULL DEFAULT '[]',
                    created_at TEXT NOT NULL,
                    approved_at TEXT
                );

                CREATE TABLE IF NOT EXISTS registration_requests (
                    agent_name TEXT PRIMARY KEY,
                    role TEXT NOT NULL,
                    token_hash TEXT NOT NULL,
                    file_path TEXT,
                    endpoint TEXT,
                    metadata TEXT NOT NULL DEFAULT '{}',
                    status TEXT NOT NULL,
                    rejection_reason TEXT NOT NULL DEFAULT '',
                    requested_at TEXT NOT NULL,
                    reviewed_at TEXT,
                    reviewed_by TEXT
                );

                CREATE TABLE IF NOT EXISTS messages (
                    id TEXT PRIMARY KEY,
                    timestamp TEXT NOT NULL,
                    sender TEXT NOT NULL,
                    recipient TEXT NOT NULL,
                    task_type TEXT NOT NULL,
                    context TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    status TEXT NOT NULL,
                    error TEXT NOT NULL DEFAULT ''
                );

                CREATE TABLE IF NOT EXISTS routing_metadata (
                    message_id TEXT PRIMARY KEY,
                    provenance_source TEXT,
                    provenance_agent TEXT,
                    provenance_trust_level INTEGER NOT NULL,
                    urgency TEXT NOT NULL,
                    ttl_seconds INTEGER,
                    dedupe_key TEXT,
                    recipient_level INTEGER NOT NULL,
                    sender_level INTEGER NOT NULL,
                    recipient_weight INTEGER NOT NULL,
                    hierarchy_penalty INTEGER NOT NULL,
                    computed_priority INTEGER NOT NULL,
                    attempt_count INTEGER NOT NULL DEFAULT 0,
                    lease_until TEXT,
                    blocked_reason TEXT NOT NULL DEFAULT '',
                    delivery_state TEXT NOT NULL DEFAULT 'pending',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY(message_id) REFERENCES messages(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS audit_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_type TEXT NOT NULL,
                    subject_id TEXT NOT NULL,
                    actor TEXT NOT NULL,
                    details TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_messages_recipient
                    ON messages(recipient, status);

                CREATE INDEX IF NOT EXISTS idx_routing_delivery
                    ON routing_metadata(delivery_state, computed_priority);

                CREATE INDEX IF NOT EXISTS idx_audit_subject
                    ON audit_log(subject_id, created_at);
                """
            )
