from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from datetime import timedelta
from pathlib import Path
from typing import Any

from .models import AgentApiKeyRecord, AgentRecord, MessageEnvelope, RegistrationRequest, iso_now, parse_timestamp
from .storage import RouterStorage


class SQLiteStorage(RouterStorage):
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

                CREATE TABLE IF NOT EXISTS agent_api_keys (
                    agent_name TEXT PRIMARY KEY,
                    key_hash TEXT NOT NULL,
                    label TEXT NOT NULL DEFAULT 'default',
                    created_at TEXT NOT NULL,
                    last_used_at TEXT,
                    active INTEGER NOT NULL DEFAULT 1,
                    FOREIGN KEY(agent_name) REFERENCES agents(agent_name) ON DELETE CASCADE
                );

                CREATE INDEX IF NOT EXISTS idx_messages_recipient
                    ON messages(recipient, status);

                CREATE INDEX IF NOT EXISTS idx_routing_delivery
                    ON routing_metadata(delivery_state, computed_priority);

                CREATE INDEX IF NOT EXISTS idx_audit_subject
                    ON audit_log(subject_id, created_at);
                """
            )

    def register_agent(self, agent: AgentRecord) -> None:
        with closing(self.connect()) as conn:
            conn.execute(
                """
                INSERT INTO agents (
                    agent_name, role, hierarchy_level, trust_level, file_path, endpoint,
                    active, registration_status, allowed_senders, allowed_task_types,
                    created_at, approved_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(agent_name) DO UPDATE SET
                    role=excluded.role,
                    hierarchy_level=excluded.hierarchy_level,
                    trust_level=excluded.trust_level,
                    file_path=excluded.file_path,
                    endpoint=excluded.endpoint,
                    active=excluded.active,
                    registration_status=excluded.registration_status,
                    allowed_senders=excluded.allowed_senders,
                    allowed_task_types=excluded.allowed_task_types,
                    approved_at=excluded.approved_at
                """,
                (
                    agent.agent_name,
                    agent.role,
                    agent.hierarchy_level,
                    agent.trust_level,
                    agent.file_path,
                    agent.endpoint,
                    int(agent.active),
                    agent.registration_status,
                    self._json(agent.allowed_senders),
                    self._json(agent.allowed_task_types),
                    agent.created_at,
                    agent.approved_at,
                ),
            )

    def get_agent(self, agent_name: str) -> AgentRecord | None:
        with closing(self.connect()) as conn:
            row = conn.execute(
                "SELECT * FROM agents WHERE agent_name = ?",
                (agent_name,),
            ).fetchone()
        return self._agent_from_row(row) if row else None

    def list_agents(self, status: str | None = None) -> list[AgentRecord]:
        with closing(self.connect()) as conn:
            if status:
                rows = conn.execute(
                    "SELECT * FROM agents WHERE registration_status = ? ORDER BY agent_name",
                    (status,),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM agents ORDER BY hierarchy_level, agent_name"
                ).fetchall()
        return [self._agent_from_row(row) for row in rows]

    def request_registration(self, req: RegistrationRequest, token_hash: str) -> str:
        with closing(self.connect()) as conn:
            conn.execute(
                """
                INSERT INTO registration_requests (
                    agent_name, role, token_hash, file_path, endpoint, metadata,
                    status, rejection_reason, requested_at, reviewed_at, reviewed_by
                ) VALUES (?, ?, ?, ?, ?, ?, 'pending', '', ?, NULL, NULL)
                """,
                (
                    req.agent_name,
                    req.role,
                    token_hash,
                    req.file_path,
                    req.endpoint,
                    self._json(req.metadata),
                    iso_now(),
                ),
            )
        return req.agent_name

    def get_registration_request(self, agent_name: str) -> dict[str, Any] | None:
        with closing(self.connect()) as conn:
            row = conn.execute(
                "SELECT * FROM registration_requests WHERE agent_name = ?",
                (agent_name,),
            ).fetchone()
        return self._registration_from_row(row) if row else None

    def list_registration_requests(self, status: str | None = None) -> list[dict[str, Any]]:
        with closing(self.connect()) as conn:
            if status:
                rows = conn.execute(
                    """
                    SELECT * FROM registration_requests
                    WHERE status = ?
                    ORDER BY requested_at
                    """,
                    (status,),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM registration_requests ORDER BY requested_at"
                ).fetchall()
        return [self._registration_from_row(row) for row in rows]

    def update_registration_status(
        self,
        agent_name: str,
        status: str,
        reviewed_at: str,
        reviewed_by: str,
        rejection_reason: str = "",
    ) -> bool:
        with closing(self.connect()) as conn:
            result = conn.execute(
                """
                UPDATE registration_requests
                SET status = ?, rejection_reason = ?, reviewed_at = ?, reviewed_by = ?
                WHERE agent_name = ?
                """,
                (status, rejection_reason, reviewed_at, reviewed_by, agent_name),
            )
            return result.rowcount > 0

    def find_message_by_dedupe(
        self, sender: str, recipient: str, task_type: str, dedupe_key: str
    ) -> str | None:
        with closing(self.connect()) as conn:
            row = conn.execute(
                """
                SELECT message_id FROM routing_metadata
                JOIN messages ON messages.id = routing_metadata.message_id
                WHERE dedupe_key = ? AND messages.sender = ? AND messages.recipient = ?
                AND messages.task_type = ?
                AND routing_metadata.delivery_state NOT IN ('dead_lettered', 'expired')
                LIMIT 1
                """,
                (dedupe_key, sender, recipient, task_type),
            ).fetchone()
        return row["message_id"] if row else None

    def insert_message(self, message: MessageEnvelope, routing: dict[str, Any]) -> None:
        with closing(self.connect()) as conn:
            conn.execute(
                """
                INSERT INTO messages (
                    id, timestamp, sender, recipient, task_type, context, payload, status, error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    message.id,
                    message.timestamp,
                    message.sender,
                    message.recipient,
                    message.task_type,
                    self._json(message.context),
                    self._json(message.payload),
                    message.status,
                    message.error,
                ),
            )
            conn.execute(
                """
                INSERT INTO routing_metadata (
                    message_id, provenance_source, provenance_agent, provenance_trust_level,
                    urgency, ttl_seconds, dedupe_key, recipient_level, sender_level,
                    recipient_weight, hierarchy_penalty, computed_priority, attempt_count,
                    lease_until, blocked_reason, delivery_state, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    message.id,
                    routing["provenance_source"],
                    routing["provenance_agent"],
                    routing["provenance_trust_level"],
                    routing["urgency"],
                    routing["ttl_seconds"],
                    routing["dedupe_key"],
                    routing["recipient_level"],
                    routing["sender_level"],
                    routing["recipient_weight"],
                    routing["hierarchy_penalty"],
                    routing["computed_priority"],
                    routing["attempt_count"],
                    routing["lease_until"],
                    routing["blocked_reason"],
                    routing["delivery_state"],
                    routing["created_at"],
                    routing["updated_at"],
                ),
            )

    def get_queue_records(
        self,
        recipient: str,
        sender: str | None = None,
        task_type: str | None = None,
        min_priority: int | None = None,
        limit: int | None = None,
        pending_only: bool = False,
    ) -> list[dict[str, Any]]:
        query = self._queue_select() + " WHERE messages.recipient = ?"
        params: list[Any] = [recipient]
        if sender:
            query += " AND messages.sender = ?"
            params.append(sender)
        if task_type:
            query += " AND messages.task_type = ?"
            params.append(task_type)
        if pending_only:
            query += " AND messages.status = 'pending' AND routing_metadata.delivery_state = 'pending'"
        if min_priority is not None:
            query += f" AND ({self._priority_expression()}) >= ?"
            params.append(min_priority)
        query += " ORDER BY effective_priority DESC, messages.timestamp ASC"
        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)
        with closing(self.connect()) as conn:
            rows = conn.execute(query, params).fetchall()
        return [self._queue_from_row(row) for row in rows]

    def lease_next_message(self, recipient: str, lease_until: str) -> dict[str, Any] | None:
        with closing(self.connect()) as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                self._queue_select()
                + """
                WHERE messages.recipient = ?
                AND messages.status = 'pending'
                AND routing_metadata.delivery_state = 'pending'
                ORDER BY effective_priority DESC, messages.timestamp ASC
                LIMIT 1
                """,
                (recipient,),
            ).fetchone()
            if row is None:
                conn.commit()
                return None
            conn.execute(
                "UPDATE messages SET status = 'in_progress' WHERE id = ?",
                (row["id"],),
            )
            conn.execute(
                """
                UPDATE routing_metadata
                SET lease_until = ?, delivery_state = 'leased', updated_at = ?
                WHERE message_id = ?
                """,
                (lease_until, lease_until, row["id"]),
            )
            conn.commit()
        return self.get_message_state(row["id"], recipient)

    def get_message_state(self, message_id: str, recipient: str) -> dict[str, Any] | None:
        with closing(self.connect()) as conn:
            row = conn.execute(
                self._queue_select() + " WHERE messages.id = ? AND messages.recipient = ?",
                (message_id, recipient),
            ).fetchone()
        return self._queue_from_row(row) if row else None

    def mark_message_done(self, message_id: str, now: str) -> None:
        with closing(self.connect()) as conn:
            conn.execute(
                "UPDATE messages SET status = 'done', error = '' WHERE id = ?",
                (message_id,),
            )
            conn.execute(
                """
                UPDATE routing_metadata
                SET delivery_state = 'done', lease_until = NULL, blocked_reason = '', updated_at = ?
                WHERE message_id = ?
                """,
                (now, message_id),
            )

    def requeue_message(
        self, message_id: str, error: str, attempts: int, now: str
    ) -> None:
        with closing(self.connect()) as conn:
            conn.execute(
                "UPDATE messages SET status = 'pending', error = ? WHERE id = ?",
                (error, message_id),
            )
            conn.execute(
                """
                UPDATE routing_metadata
                SET attempt_count = ?, delivery_state = 'pending',
                    lease_until = NULL, blocked_reason = '', updated_at = ?
                WHERE message_id = ?
                """,
                (attempts, now, message_id),
            )

    def dead_letter_message(
        self, message_id: str, error: str, attempts: int, now: str
    ) -> None:
        with closing(self.connect()) as conn:
            conn.execute(
                "UPDATE messages SET status = 'error', error = ? WHERE id = ?",
                (error, message_id),
            )
            conn.execute(
                """
                UPDATE routing_metadata
                SET attempt_count = ?, delivery_state = 'dead_lettered',
                    lease_until = NULL, updated_at = ?, blocked_reason = ?
                WHERE message_id = ?
                """,
                (attempts, now, error, message_id),
            )

    def requeue_expired_leases(
        self, now: str, recipient: str | None = None
    ) -> list[dict[str, Any]]:
        rows = self.get_queue_records(recipient or "", pending_only=False) if recipient else self._all_queue_records()
        updated: list[dict[str, Any]] = []
        now_dt = parse_timestamp(now)
        for row in rows:
            lease_until = row.get("lease_until")
            if row["delivery_state"] == "leased" and lease_until and parse_timestamp(lease_until) <= now_dt:
                with closing(self.connect()) as conn:
                    conn.execute(
                        "UPDATE messages SET status = 'pending' WHERE id = ?",
                        (row["id"],),
                    )
                    conn.execute(
                        """
                        UPDATE routing_metadata
                        SET delivery_state = 'pending', lease_until = NULL, updated_at = ?
                        WHERE message_id = ?
                        """,
                        (now, row["id"]),
                    )
                row["status"] = "pending"
                row["delivery_state"] = "pending"
                row["lease_until"] = None
                updated.append(row)
        return updated

    def expire_ttl_messages(
        self, now: str, recipient: str | None = None
    ) -> list[dict[str, Any]]:
        rows = self.get_queue_records(recipient or "", pending_only=False) if recipient else self._all_queue_records()
        updated: list[dict[str, Any]] = []
        now_dt = parse_timestamp(now)
        for row in rows:
            ttl_seconds = row.get("ttl_seconds")
            if ttl_seconds is None:
                continue
            if row["delivery_state"] in {"expired", "dead_lettered", "done"}:
                continue
            expiry = parse_timestamp(row["timestamp"]) + timedelta(seconds=ttl_seconds)
            if expiry <= now_dt:
                with closing(self.connect()) as conn:
                    conn.execute(
                        "UPDATE messages SET status = 'error', error = 'Message TTL expired.' WHERE id = ?",
                        (row["id"],),
                    )
                    conn.execute(
                        """
                        UPDATE routing_metadata
                        SET delivery_state = 'expired', lease_until = NULL,
                            blocked_reason = 'Message TTL expired.', updated_at = ?
                        WHERE message_id = ?
                        """,
                        (now, row["id"]),
                    )
                row["status"] = "error"
                row["delivery_state"] = "expired"
                row["blocked_reason"] = "Message TTL expired."
                updated.append(row)
        return updated

    def log_audit(
        self, event_type: str, subject_id: str, actor: str, details: dict[str, Any], created_at: str
    ) -> None:
        with closing(self.connect()) as conn:
            conn.execute(
                """
                INSERT INTO audit_log (event_type, subject_id, actor, details, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (event_type, subject_id, actor, self._json(details), created_at),
            )

    def list_audit_log(
        self, limit: int = 50, subject_id: str | None = None
    ) -> list[dict[str, Any]]:
        with closing(self.connect()) as conn:
            if subject_id:
                rows = conn.execute(
                    """
                    SELECT event_type, subject_id, actor, details, created_at
                    FROM audit_log
                    WHERE subject_id = ?
                    ORDER BY id DESC
                    LIMIT ?
                    """,
                    (subject_id, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT event_type, subject_id, actor, details, created_at
                    FROM audit_log
                    ORDER BY id DESC
                    LIMIT ?
                    """,
                    (limit,),
                ).fetchall()
        return [
            {
                "event_type": row["event_type"],
                "subject_id": row["subject_id"],
                "actor": row["actor"],
                "details": self._loads(row["details"], default={}),
                "created_at": row["created_at"],
            }
            for row in rows
        ]

    def store_api_key(self, record: AgentApiKeyRecord) -> None:
        with closing(self.connect()) as conn:
            conn.execute(
                """
                INSERT INTO agent_api_keys (agent_name, key_hash, label, created_at, last_used_at, active)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(agent_name) DO UPDATE SET
                    key_hash=excluded.key_hash,
                    label=excluded.label,
                    created_at=excluded.created_at,
                    last_used_at=excluded.last_used_at,
                    active=excluded.active
                """,
                (
                    record.agent_name,
                    record.key_hash,
                    record.label,
                    record.created_at,
                    record.last_used_at,
                    int(record.active),
                ),
            )

    def get_api_key(self, agent_name: str) -> dict[str, Any] | None:
        with closing(self.connect()) as conn:
            row = conn.execute(
                "SELECT * FROM agent_api_keys WHERE agent_name = ?",
                (agent_name,),
            ).fetchone()
        if row is None:
            return None
        return {
            "agent_name": row["agent_name"],
            "key_hash": row["key_hash"],
            "label": row["label"],
            "created_at": row["created_at"],
            "last_used_at": row["last_used_at"],
            "active": bool(row["active"]),
        }

    def touch_api_key(self, agent_name: str, last_used_at: str) -> None:
        with closing(self.connect()) as conn:
            conn.execute(
                "UPDATE agent_api_keys SET last_used_at = ? WHERE agent_name = ?",
                (last_used_at, agent_name),
            )

    def _priority_expression(self) -> str:
        return (
            "routing_metadata.computed_priority + "
            "CAST(MAX((julianday('now') - julianday(messages.timestamp)) * 1440, 0) AS INTEGER)"
        )

    def _queue_select(self) -> str:
        return f"""
            SELECT
                messages.id,
                messages.timestamp,
                messages.sender,
                messages.recipient,
                messages.task_type,
                messages.context,
                messages.payload,
                messages.status,
                messages.error,
                routing_metadata.provenance_source,
                routing_metadata.provenance_agent,
                routing_metadata.provenance_trust_level,
                routing_metadata.ttl_seconds,
                routing_metadata.dedupe_key,
                routing_metadata.attempt_count,
                routing_metadata.lease_until,
                routing_metadata.delivery_state,
                routing_metadata.blocked_reason,
                routing_metadata.computed_priority,
                {self._priority_expression()} AS effective_priority
            FROM messages
            JOIN routing_metadata ON routing_metadata.message_id = messages.id
        """

    def _all_queue_records(self) -> list[dict[str, Any]]:
        with closing(self.connect()) as conn:
            rows = conn.execute(
                self._queue_select() + " ORDER BY effective_priority DESC, messages.timestamp ASC"
            ).fetchall()
        return [self._queue_from_row(row) for row in rows]

    def _agent_from_row(self, row: sqlite3.Row) -> AgentRecord:
        return AgentRecord(
            agent_name=row["agent_name"],
            role=row["role"],
            hierarchy_level=row["hierarchy_level"],
            trust_level=row["trust_level"],
            file_path=row["file_path"],
            endpoint=row["endpoint"],
            active=bool(row["active"]),
            registration_status=row["registration_status"],
            allowed_senders=self._loads(row["allowed_senders"], default=[]),
            allowed_task_types=self._loads(row["allowed_task_types"], default=[]),
            created_at=row["created_at"],
            approved_at=row["approved_at"],
        )

    def _registration_from_row(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "agent_name": row["agent_name"],
            "role": row["role"],
            "token_hash": row["token_hash"],
            "file_path": row["file_path"],
            "endpoint": row["endpoint"],
            "metadata": self._loads(row["metadata"], default={}),
            "status": row["status"],
            "rejection_reason": row["rejection_reason"],
            "requested_at": row["requested_at"],
            "reviewed_at": row["reviewed_at"],
            "reviewed_by": row["reviewed_by"],
        }

    def _queue_from_row(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "id": row["id"],
            "timestamp": row["timestamp"],
            "sender": row["sender"],
            "recipient": row["recipient"],
            "task_type": row["task_type"],
            "context": self._loads(row["context"], default={}),
            "payload": self._loads(row["payload"], default={}),
            "status": row["status"],
            "error": row["error"],
            "provenance_source": row["provenance_source"],
            "provenance_agent": row["provenance_agent"],
            "provenance_trust_level": row["provenance_trust_level"],
            "ttl_seconds": row["ttl_seconds"],
            "dedupe_key": row["dedupe_key"],
            "attempt_count": row["attempt_count"],
            "lease_until": row["lease_until"],
            "delivery_state": row["delivery_state"],
            "blocked_reason": row["blocked_reason"],
            "computed_priority": row["effective_priority"],
        }

    def _json(self, value: Any) -> str:
        return json.dumps(value, sort_keys=True)

    def _loads(self, value: str | None, default: Any) -> Any:
        if value is None:
            return default
        return json.loads(value)
