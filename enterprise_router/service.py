from __future__ import annotations

import hashlib
import json
import os
from contextlib import closing
from dataclasses import replace
from datetime import timedelta
from typing import Any

from .exceptions import AccessError, RegistrationError, ValidationError
from .models import (
    MESSAGE_STATUSES,
    REGISTRATION_STATUSES,
    URGENCY_WEIGHTS,
    AgentRecord,
    MessageEnvelope,
    QueuedMessage,
    RegistrationRequest,
    RoutingHints,
    iso_now,
    make_message,
    normalize_role,
    parse_timestamp,
    role_defaults,
    utc_now,
)
from .storage import Database


class EnterpriseRouter:
    def __init__(
        self,
        db_path: str = "enterprise_router.db",
        shared_secret: str | None = None,
        default_lease_seconds: int = 60,
        max_attempts: int = 3,
    ) -> None:
        self.storage = Database(db_path)
        self.shared_secret = shared_secret or os.getenv(
            "ENTERPRISE_ROUTER_SHARED_SECRET", "dev-shared-secret"
        )
        self.default_lease_seconds = default_lease_seconds
        self.max_attempts = max_attempts

    def register_agent(self, agent: AgentRecord) -> None:
        self._validate_agent(agent)
        approved_at = agent.approved_at or iso_now()
        created_at = agent.created_at or iso_now()
        with closing(self.storage.connect()) as conn:
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
                    normalize_role(agent.role),
                    agent.hierarchy_level,
                    agent.trust_level,
                    agent.file_path,
                    agent.endpoint,
                    int(agent.active),
                    "approved",
                    self._json(agent.allowed_senders),
                    self._json(agent.allowed_task_types),
                    created_at,
                    approved_at,
                ),
            )
            self._log_audit(
                conn,
                "registered",
                agent.agent_name,
                agent.agent_name,
                {"role": normalize_role(agent.role), "active": agent.active},
            )

    def request_registration(self, req: RegistrationRequest) -> str:
        self._validate_registration_request(req)
        token_hash = self._hash(req.secret_token)
        with closing(self.storage.connect()) as conn:
            if self._agent_exists(conn, req.agent_name) or self._registration_exists(
                conn, req.agent_name
            ):
                raise RegistrationError(f"Agent '{req.agent_name}' is already known.")

            conn.execute(
                """
                INSERT INTO registration_requests (
                    agent_name, role, token_hash, file_path, endpoint, metadata,
                    status, rejection_reason, requested_at, reviewed_at, reviewed_by
                ) VALUES (?, ?, ?, ?, ?, ?, 'pending', '', ?, NULL, NULL)
                """,
                (
                    req.agent_name,
                    normalize_role(req.role),
                    token_hash,
                    req.file_path,
                    req.endpoint,
                    self._json(req.metadata),
                    iso_now(),
                ),
            )
            self._log_audit(
                conn,
                "registration_requested",
                req.agent_name,
                req.agent_name,
                {"role": normalize_role(req.role), "metadata": req.metadata},
            )
        return req.agent_name

    def approve_registration(self, agent_name: str, approver: str) -> None:
        with closing(self.storage.connect()) as conn:
            row = conn.execute(
                """
                SELECT * FROM registration_requests
                WHERE agent_name = ? AND status = 'pending'
                """,
                (agent_name,),
            ).fetchone()
            if row is None:
                raise RegistrationError(f"No pending registration for '{agent_name}'.")

            defaults = role_defaults(row["role"])
            now = iso_now()
            agent = AgentRecord(
                agent_name=row["agent_name"],
                role=row["role"],
                hierarchy_level=defaults["hierarchy_level"],
                trust_level=defaults["trust_level"],
                file_path=row["file_path"],
                endpoint=row["endpoint"],
                active=True,
                registration_status="approved",
                allowed_senders=[],
                allowed_task_types=[],
                created_at=row["requested_at"],
                approved_at=now,
            )
            self.register_agent(agent)
            conn.execute(
                """
                UPDATE registration_requests
                SET status = 'approved', reviewed_at = ?, reviewed_by = ?
                WHERE agent_name = ?
                """,
                (now, approver, agent_name),
            )
            self._log_audit(
                conn,
                "registration_approved",
                agent_name,
                approver,
                {"approved_at": now},
            )

    def reject_registration(self, agent_name: str, approver: str, reason: str) -> None:
        if not reason.strip():
            raise ValidationError("Rejection reason is required.")
        with closing(self.storage.connect()) as conn:
            updated = conn.execute(
                """
                UPDATE registration_requests
                SET status = 'rejected', rejection_reason = ?, reviewed_at = ?, reviewed_by = ?
                WHERE agent_name = ? AND status = 'pending'
                """,
                (reason.strip(), iso_now(), approver, agent_name),
            )
            if updated.rowcount == 0:
                raise RegistrationError(f"No pending registration for '{agent_name}'.")
            self._log_audit(
                conn,
                "registration_rejected",
                agent_name,
                approver,
                {"reason": reason.strip()},
            )

    def get_agent(self, agent_name: str) -> AgentRecord | None:
        with closing(self.storage.connect()) as conn:
            row = conn.execute(
                "SELECT * FROM agents WHERE agent_name = ?",
                (agent_name,),
            ).fetchone()
        return self._agent_from_row(row) if row else None

    def list_agents(self, status: str | None = None) -> list[AgentRecord]:
        if status and status not in REGISTRATION_STATUSES:
            raise ValidationError(f"Unknown registration status '{status}'.")
        with closing(self.storage.connect()) as conn:
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

    def list_registration_requests(self, status: str | None = None) -> list[dict[str, Any]]:
        if status and status not in REGISTRATION_STATUSES:
            raise ValidationError(f"Unknown registration status '{status}'.")
        with closing(self.storage.connect()) as conn:
            if status:
                rows = conn.execute(
                    """
                    SELECT agent_name, role, file_path, endpoint, metadata, status,
                           rejection_reason, requested_at, reviewed_at, reviewed_by
                    FROM registration_requests
                    WHERE status = ?
                    ORDER BY requested_at
                    """,
                    (status,),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT agent_name, role, file_path, endpoint, metadata, status,
                           rejection_reason, requested_at, reviewed_at, reviewed_by
                    FROM registration_requests
                    ORDER BY requested_at
                    """
                ).fetchall()
        return [
            {
                "agent_name": row["agent_name"],
                "role": row["role"],
                "file_path": row["file_path"],
                "endpoint": row["endpoint"],
                "metadata": self._loads(row["metadata"], default={}),
                "status": row["status"],
                "rejection_reason": row["rejection_reason"],
                "requested_at": row["requested_at"],
                "reviewed_at": row["reviewed_at"],
                "reviewed_by": row["reviewed_by"],
            }
            for row in rows
        ]

    def create_message(
        self,
        sender: str,
        recipient: str,
        task_type: str,
        context: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
    ) -> MessageEnvelope:
        return make_message(sender, recipient, task_type, context, payload)

    def submit_message(
        self, message: MessageEnvelope, hints: RoutingHints | None = None
    ) -> str:
        self._validate_envelope(message)
        hints = hints or RoutingHints()
        self._validate_hints(hints)
        if message.recipient.lower() == "broadcast":
            raise ValidationError("Broadcast recipients are not supported in the MVP.")

        with closing(self.storage.connect()) as conn:
            self._expire_ttl_messages(conn)
            sender = self._require_active_agent(conn, message.sender)
            recipient = self._require_active_agent(conn, message.recipient)

            if hints.dedupe_key:
                existing = conn.execute(
                    """
                    SELECT message_id FROM routing_metadata
                    JOIN messages ON messages.id = routing_metadata.message_id
                    WHERE dedupe_key = ? AND messages.sender = ? AND messages.recipient = ?
                    AND messages.task_type = ?
                    AND routing_metadata.delivery_state NOT IN ('dead_lettered', 'expired')
                    LIMIT 1
                    """,
                    (hints.dedupe_key, message.sender, message.recipient, message.task_type),
                ).fetchone()
                if existing:
                    return existing["message_id"]

            provenance_trust_level = (
                hints.provenance_trust_level
                if hints.provenance_trust_level is not None
                else sender.trust_level
            )
            recipient_weight = role_defaults(recipient.role)["recipient_weight"]
            hierarchy_penalty = self._hierarchy_penalty(
                sender.hierarchy_level, recipient.hierarchy_level
            )
            computed_priority = (
                recipient_weight
                + URGENCY_WEIGHTS[hints.urgency]
                + provenance_trust_level
                - hierarchy_penalty
            )
            blocked_reason = self._blocked_reason(recipient, sender, message.task_type)
            delivery_state = "blocked" if blocked_reason else "pending"
            status = "error" if blocked_reason else "pending"
            error = blocked_reason if blocked_reason else message.error

            stored_message = replace(message, status=status, error=error)
            now = iso_now()
            conn.execute(
                """
                INSERT INTO messages (
                    id, timestamp, sender, recipient, task_type, context, payload, status, error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    stored_message.id,
                    stored_message.timestamp,
                    stored_message.sender,
                    stored_message.recipient,
                    stored_message.task_type,
                    self._json(stored_message.context),
                    self._json(stored_message.payload),
                    stored_message.status,
                    stored_message.error,
                ),
            )
            conn.execute(
                """
                INSERT INTO routing_metadata (
                    message_id, provenance_source, provenance_agent, provenance_trust_level,
                    urgency, ttl_seconds, dedupe_key, recipient_level, sender_level,
                    recipient_weight, hierarchy_penalty, computed_priority, attempt_count,
                    lease_until, blocked_reason, delivery_state, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, NULL, ?, ?, ?, ?)
                """,
                (
                    stored_message.id,
                    hints.provenance_source,
                    hints.provenance_agent,
                    provenance_trust_level,
                    hints.urgency,
                    hints.ttl_seconds,
                    hints.dedupe_key,
                    recipient.hierarchy_level,
                    sender.hierarchy_level,
                    recipient_weight,
                    hierarchy_penalty,
                    computed_priority,
                    blocked_reason,
                    delivery_state,
                    now,
                    now,
                ),
            )
            self._log_audit(
                conn,
                "blocked" if blocked_reason else "submitted",
                stored_message.id,
                stored_message.sender,
                {
                    "recipient": stored_message.recipient,
                    "task_type": stored_message.task_type,
                    "priority": computed_priority,
                    "blocked_reason": blocked_reason,
                },
            )
        return stored_message.id

    def peek_messages(
        self,
        recipient: str,
        min_priority: int | None = None,
        sender: str | None = None,
        task_type: str | None = None,
        limit: int = 10,
    ) -> list[QueuedMessage]:
        if limit <= 0:
            raise ValidationError("limit must be positive.")
        with closing(self.storage.connect()) as conn:
            self._require_active_agent(conn, recipient)
            self._expire_ttl_messages(conn, recipient)
            self.requeue_expired_leases(recipient=recipient, _conn=conn)
            query = self._queue_select() + " WHERE messages.recipient = ?"
            params: list[Any] = [recipient]
            if sender:
                query += " AND messages.sender = ?"
                params.append(sender)
            if task_type:
                query += " AND messages.task_type = ?"
                params.append(task_type)
            if min_priority is not None:
                query += f" AND ({self._priority_expression()}) >= ?"
                params.append(min_priority)
            query += " ORDER BY effective_priority DESC, messages.timestamp ASC LIMIT ?"
            params.append(limit)
            rows = conn.execute(query, params).fetchall()
        return [self._queued_from_row(row) for row in rows]

    def fetch_next(self, recipient: str) -> QueuedMessage | None:
        with closing(self.storage.connect()) as conn:
            self._require_active_agent(conn, recipient)
            self._expire_ttl_messages(conn, recipient)
            self.requeue_expired_leases(recipient=recipient, _conn=conn)
            conn.execute("BEGIN IMMEDIATE")
            try:
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

                lease_until = (
                    utc_now() + timedelta(seconds=self.default_lease_seconds)
                ).isoformat().replace("+00:00", "Z")
                conn.execute(
                    """
                    UPDATE messages
                    SET status = 'in_progress'
                    WHERE id = ?
                    """,
                    (row["id"],),
                )
                conn.execute(
                    """
                    UPDATE routing_metadata
                    SET lease_until = ?, delivery_state = 'leased', updated_at = ?
                    WHERE message_id = ?
                    """,
                    (lease_until, iso_now(), row["id"]),
                )
                self._log_audit(
                    conn,
                    "fetched",
                    row["id"],
                    recipient,
                    {"lease_until": lease_until},
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise

            updated = conn.execute(
                self._queue_select() + " WHERE messages.id = ?",
                (row["id"],),
            ).fetchone()
        return self._queued_from_row(updated) if updated else None

    def ack_message(self, message_id: str, recipient: str) -> None:
        with closing(self.storage.connect()) as conn:
            row = self._require_message_for_recipient(conn, message_id, recipient)
            if row["status"] != "in_progress":
                raise ValidationError("Only in-progress messages can be acknowledged.")
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
                (iso_now(), message_id),
            )
            self._log_audit(conn, "acked", message_id, recipient, {})

    def nack_message(self, message_id: str, recipient: str, reason: str) -> None:
        if not reason.strip():
            raise ValidationError("A nack reason is required.")
        with closing(self.storage.connect()) as conn:
            row = self._require_message_for_recipient(conn, message_id, recipient)
            if row["status"] != "in_progress":
                raise ValidationError(
                    "Only in-progress messages can be negatively acknowledged."
                )
            attempts = row["attempt_count"] + 1
            now = iso_now()
            if attempts >= self.max_attempts:
                conn.execute(
                    "UPDATE messages SET status = 'error', error = ? WHERE id = ?",
                    (reason.strip(), message_id),
                )
                conn.execute(
                    """
                    UPDATE routing_metadata
                    SET attempt_count = ?, delivery_state = 'dead_lettered',
                        lease_until = NULL, updated_at = ?, blocked_reason = ?
                    WHERE message_id = ?
                    """,
                    (attempts, now, reason.strip(), message_id),
                )
                self._log_audit(
                    conn,
                    "dead_lettered",
                    message_id,
                    recipient,
                    {"reason": reason.strip(), "attempt_count": attempts},
                )
                return

            conn.execute(
                "UPDATE messages SET status = 'pending', error = ? WHERE id = ?",
                (reason.strip(), message_id),
            )
            conn.execute(
                """
                UPDATE routing_metadata
                SET attempt_count = ?, delivery_state = 'pending',
                    lease_until = NULL, updated_at = ?, blocked_reason = ''
                WHERE message_id = ?
                """,
                (attempts, now, message_id),
            )
            self._log_audit(
                conn,
                "nacked",
                message_id,
                recipient,
                {"reason": reason.strip(), "attempt_count": attempts},
            )

    def requeue_expired_leases(
        self, recipient: str | None = None, _conn: Any | None = None
    ) -> int:
        conn = _conn or self.storage.connect()
        should_close = _conn is None
        try:
            now = utc_now()
            query = self._queue_select() + """
                WHERE routing_metadata.delivery_state = 'leased'
                AND routing_metadata.lease_until IS NOT NULL
            """
            params: list[Any] = []
            if recipient:
                query += " AND messages.recipient = ?"
                params.append(recipient)
            rows = conn.execute(query, params).fetchall()
            count = 0
            for row in rows:
                if parse_timestamp(row["lease_until"]) <= now:
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
                        (iso_now(), row["id"]),
                    )
                    self._log_audit(
                        conn,
                        "expired",
                        row["id"],
                        row["recipient"],
                        {"reason": "Lease expired"},
                    )
                    count += 1
            return count
        finally:
            if should_close:
                conn.close()

    def list_queue(self, recipient: str) -> list[QueuedMessage]:
        with closing(self.storage.connect()) as conn:
            self._require_active_agent(conn, recipient)
            self._expire_ttl_messages(conn, recipient)
            self.requeue_expired_leases(recipient=recipient, _conn=conn)
            rows = conn.execute(
                self._queue_select()
                + """
                WHERE messages.recipient = ?
                ORDER BY
                    CASE routing_metadata.delivery_state
                        WHEN 'pending' THEN 0
                        WHEN 'leased' THEN 1
                        WHEN 'blocked' THEN 2
                        WHEN 'expired' THEN 3
                        WHEN 'dead_lettered' THEN 4
                        WHEN 'done' THEN 5
                        ELSE 6
                    END,
                    effective_priority DESC,
                    messages.timestamp ASC
                """,
                (recipient,),
            ).fetchall()
        return [self._queued_from_row(row) for row in rows]

    def list_audit_log(
        self, limit: int = 50, subject_id: str | None = None
    ) -> list[dict[str, Any]]:
        if limit <= 0:
            raise ValidationError("limit must be positive.")
        with closing(self.storage.connect()) as conn:
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

    def _validate_agent(self, agent: AgentRecord) -> None:
        if not agent.agent_name.strip():
            raise ValidationError("agent_name is required.")
        if not agent.role.strip():
            raise ValidationError("role is required.")
        if agent.registration_status not in REGISTRATION_STATUSES:
            raise ValidationError("registration_status is invalid.")
        if agent.hierarchy_level < 1:
            raise ValidationError("hierarchy_level must be at least 1.")
        if not 0 <= agent.trust_level <= 100:
            raise ValidationError("trust_level must be between 0 and 100.")

    def _validate_registration_request(self, req: RegistrationRequest) -> None:
        if not req.agent_name.strip():
            raise ValidationError("agent_name is required.")
        if not req.role.strip():
            raise ValidationError("role is required.")
        if not req.secret_token.strip():
            raise ValidationError("secret_token is required.")
        if len(req.secret_token.strip()) < 8:
            raise ValidationError("secret_token must be at least 8 characters.")
        if self._hash(req.secret_token) != self._hash(self.shared_secret):
            raise RegistrationError("Registration token is invalid.")

    def _validate_envelope(self, message: MessageEnvelope) -> None:
        if message.status not in MESSAGE_STATUSES:
            raise ValidationError(f"Unsupported message status '{message.status}'.")
        for field_name in ("id", "timestamp", "sender", "recipient", "task_type"):
            value = getattr(message, field_name)
            if not isinstance(value, str) or not value.strip():
                raise ValidationError(f"{field_name} must be a non-empty string.")
        parse_timestamp(message.timestamp)
        if not isinstance(message.context, dict):
            raise ValidationError("context must be a JSON object.")
        if not isinstance(message.payload, dict):
            raise ValidationError("payload must be a JSON object.")
        if not isinstance(message.error, str):
            raise ValidationError("error must be a string.")

    def _validate_hints(self, hints: RoutingHints) -> None:
        if hints.urgency not in URGENCY_WEIGHTS:
            raise ValidationError(f"Unsupported urgency '{hints.urgency}'.")
        if hints.ttl_seconds is not None and hints.ttl_seconds <= 0:
            raise ValidationError("ttl_seconds must be positive if provided.")
        if (
            hints.provenance_trust_level is not None
            and not 0 <= hints.provenance_trust_level <= 100
        ):
            raise ValidationError("provenance_trust_level must be between 0 and 100.")

    def _agent_exists(self, conn: Any, agent_name: str) -> bool:
        row = conn.execute(
            "SELECT 1 FROM agents WHERE agent_name = ? LIMIT 1", (agent_name,)
        ).fetchone()
        return row is not None

    def _registration_exists(self, conn: Any, agent_name: str) -> bool:
        row = conn.execute(
            "SELECT 1 FROM registration_requests WHERE agent_name = ? LIMIT 1",
            (agent_name,),
        ).fetchone()
        return row is not None

    def _agent_from_row(self, row: Any) -> AgentRecord:
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

    def _require_active_agent(self, conn: Any, agent_name: str) -> AgentRecord:
        row = conn.execute(
            "SELECT * FROM agents WHERE agent_name = ?",
            (agent_name,),
        ).fetchone()
        if row is None:
            raise AccessError(f"Unknown agent '{agent_name}'.")
        agent = self._agent_from_row(row)
        if not agent.active or agent.registration_status != "approved":
            raise AccessError(f"Agent '{agent_name}' is not active.")
        return agent

    def _blocked_reason(
        self, recipient: AgentRecord, sender: AgentRecord, task_type: str
    ) -> str:
        if recipient.allowed_senders:
            sender_allowed = (
                sender.agent_name in recipient.allowed_senders
                or sender.role in recipient.allowed_senders
            )
            if not sender_allowed:
                return (
                    f"Sender '{sender.agent_name}' ({sender.role}) is not allowed to contact "
                    f"'{recipient.agent_name}'."
                )
        if recipient.allowed_task_types and task_type not in recipient.allowed_task_types:
            return f"Task type '{task_type}' is not allowed for recipient '{recipient.agent_name}'."
        return ""

    def _hierarchy_penalty(self, sender_level: int, recipient_level: int) -> int:
        return max(0, sender_level - recipient_level) * 20

    def _require_message_for_recipient(
        self, conn: Any, message_id: str, recipient: str
    ) -> Any:
        row = conn.execute(
            """
            SELECT messages.id, messages.status, messages.recipient, routing_metadata.attempt_count
            FROM messages
            JOIN routing_metadata ON routing_metadata.message_id = messages.id
            WHERE messages.id = ? AND messages.recipient = ?
            """,
            (message_id, recipient),
        ).fetchone()
        if row is None:
            raise ValidationError(f"Message '{message_id}' is not queued for '{recipient}'.")
        return row

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

    def _priority_expression(self) -> str:
        return (
            "routing_metadata.computed_priority + "
            "CAST(MAX((julianday('now') - julianday(messages.timestamp)) * 1440, 0) AS INTEGER)"
        )

    def _queued_from_row(self, row: Any) -> QueuedMessage:
        envelope = MessageEnvelope(
            id=row["id"],
            timestamp=row["timestamp"],
            sender=row["sender"],
            recipient=row["recipient"],
            task_type=row["task_type"],
            context=self._loads(row["context"], default={}),
            payload=self._loads(row["payload"], default={}),
            status=row["status"],
            error=row["error"],
        )
        return QueuedMessage(
            envelope=envelope,
            computed_priority=row["effective_priority"],
            attempt_count=row["attempt_count"],
            lease_until=row["lease_until"],
            delivery_state=row["delivery_state"],
            blocked_reason=row["blocked_reason"],
            provenance_source=row["provenance_source"],
            provenance_agent=row["provenance_agent"],
            provenance_trust_level=row["provenance_trust_level"],
            ttl_seconds=row["ttl_seconds"],
            dedupe_key=row["dedupe_key"],
        )

    def _expire_ttl_messages(self, conn: Any, recipient: str | None = None) -> None:
        query = self._queue_select() + """
            WHERE routing_metadata.ttl_seconds IS NOT NULL
            AND routing_metadata.delivery_state NOT IN ('expired', 'dead_lettered', 'done')
        """
        params: list[Any] = []
        if recipient:
            query += " AND messages.recipient = ?"
            params.append(recipient)
        rows = conn.execute(query, params).fetchall()
        now = utc_now()
        for row in rows:
            expiry = parse_timestamp(row["timestamp"]) + timedelta(seconds=row["ttl_seconds"])
            if expiry <= now:
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
                    (iso_now(), row["id"]),
                )
                self._log_audit(
                    conn,
                    "expired",
                    row["id"],
                    row["recipient"],
                    {"reason": "Message TTL expired"},
                )

    def _log_audit(
        self, conn: Any, event_type: str, subject_id: str, actor: str, details: dict[str, Any]
    ) -> None:
        conn.execute(
            """
            INSERT INTO audit_log (event_type, subject_id, actor, details, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (event_type, subject_id, actor, self._json(details), iso_now()),
        )

    def _json(self, value: Any) -> str:
        return json.dumps(value, sort_keys=True)

    def _loads(self, value: str | None, default: Any) -> Any:
        if value is None:
            return default
        return json.loads(value)

    def _hash(self, value: str) -> str:
        return hashlib.sha256(value.encode("utf-8")).hexdigest()
