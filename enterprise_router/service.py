from __future__ import annotations

import hashlib
import hmac
import os
import secrets
from dataclasses import replace
from datetime import timedelta
from typing import Any

from .exceptions import AccessError, RegistrationError, ValidationError
from .models import (
    MESSAGE_STATUSES,
    REGISTRATION_STATUSES,
    URGENCY_WEIGHTS,
    AgentApiKeyRecord,
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
from .storage import RouterStorage, create_storage


class EnterpriseRouter:
    def __init__(
        self,
        db_path: str = "enterprise_router.db",
        shared_secret: str | None = None,
        default_lease_seconds: int = 60,
        max_attempts: int = 3,
        storage: RouterStorage | None = None,
        backend: str | None = None,
        mongo_uri: str | None = None,
        mongo_db_name: str | None = None,
    ) -> None:
        selected_backend = backend or os.getenv("ROUTER_BACKEND", "sqlite")
        self.storage = storage or create_storage(
            backend=selected_backend,
            db_path=db_path or os.getenv("SQLITE_DB_PATH", "enterprise_router.db"),
            mongo_uri=mongo_uri or os.getenv("MONGODB_URI"),
            mongo_db_name=mongo_db_name or os.getenv("MONGODB_DB_NAME"),
        )
        self.backend = selected_backend
        self.shared_secret = shared_secret or os.getenv(
            "ENTERPRISE_ROUTER_SHARED_SECRET", "dev-shared-secret"
        )
        self.default_lease_seconds = default_lease_seconds
        self.max_attempts = max_attempts

    def register_agent(self, agent: AgentRecord) -> None:
        self._validate_agent(agent)
        stored = replace(
            agent,
            role=normalize_role(agent.role),
            registration_status="approved",
            approved_at=agent.approved_at or iso_now(),
            created_at=agent.created_at or iso_now(),
        )
        self.storage.register_agent(stored)
        self._log_audit(
            "registered",
            stored.agent_name,
            stored.agent_name,
            {"role": stored.role, "active": stored.active},
        )

    def request_registration(self, req: RegistrationRequest) -> str:
        self._validate_registration_request(req)
        if self.storage.get_agent(req.agent_name) or self.storage.get_registration_request(
            req.agent_name
        ):
            raise RegistrationError(f"Agent '{req.agent_name}' is already known.")
        token_hash = self._hash(req.secret_token)
        result = self.storage.request_registration(
            replace(req, role=normalize_role(req.role)),
            token_hash,
        )
        self._log_audit(
            "registration_requested",
            req.agent_name,
            req.agent_name,
            {"role": normalize_role(req.role), "metadata": req.metadata},
        )
        return result

    def approve_registration(
        self,
        agent_name: str,
        approver: str,
        *,
        issue_api_key: bool = False,
        key_label: str = "default",
    ) -> str | None:
        row = self.storage.get_registration_request(agent_name)
        if row is None or row["status"] != "pending":
            raise RegistrationError(f"No pending registration for '{agent_name}'.")

        defaults = role_defaults(row["role"])
        now = iso_now()
        agent = AgentRecord(
            agent_name=row["agent_name"],
            role=row["role"],
            hierarchy_level=defaults["hierarchy_level"],
            trust_level=defaults["trust_level"],
            file_path=row.get("file_path"),
            endpoint=row.get("endpoint"),
            active=True,
            registration_status="approved",
            allowed_senders=[],
            allowed_task_types=[],
            created_at=row["requested_at"],
            approved_at=now,
        )
        self.register_agent(agent)
        self.storage.update_registration_status(
            agent_name, "approved", now, approver, rejection_reason=""
        )
        self._log_audit(
            "registration_approved",
            agent_name,
            approver,
            {"approved_at": now},
        )
        if issue_api_key:
            return self.issue_api_key(agent_name, label=key_label)
        return None

    def reject_registration(self, agent_name: str, approver: str, reason: str) -> None:
        if not reason.strip():
            raise ValidationError("Rejection reason is required.")
        row = self.storage.get_registration_request(agent_name)
        if row is None or row["status"] != "pending":
            raise RegistrationError(f"No pending registration for '{agent_name}'.")
        self.storage.update_registration_status(
            agent_name, "rejected", iso_now(), approver, rejection_reason=reason.strip()
        )
        self._log_audit(
            "registration_rejected",
            agent_name,
            approver,
            {"reason": reason.strip()},
        )

    def get_agent(self, agent_name: str) -> AgentRecord | None:
        return self.storage.get_agent(agent_name)

    def list_agents(self, status: str | None = None) -> list[AgentRecord]:
        if status and status not in REGISTRATION_STATUSES:
            raise ValidationError(f"Unknown registration status '{status}'.")
        return self.storage.list_agents(status)

    def list_registration_requests(self, status: str | None = None) -> list[dict[str, Any]]:
        if status and status not in REGISTRATION_STATUSES:
            raise ValidationError(f"Unknown registration status '{status}'.")
        return self.storage.list_registration_requests(status)

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

        self._refresh_queue()
        sender = self._require_active_agent(message.sender)
        recipient = self._require_active_agent(message.recipient)

        if hints.dedupe_key:
            existing = self.storage.find_message_by_dedupe(
                message.sender, message.recipient, message.task_type, hints.dedupe_key
            )
            if existing:
                return existing

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
        self.storage.insert_message(
            stored_message,
            {
                "provenance_source": hints.provenance_source,
                "provenance_agent": hints.provenance_agent,
                "provenance_trust_level": provenance_trust_level,
                "urgency": hints.urgency,
                "ttl_seconds": hints.ttl_seconds,
                "dedupe_key": hints.dedupe_key,
                "recipient_level": recipient.hierarchy_level,
                "sender_level": sender.hierarchy_level,
                "recipient_weight": recipient_weight,
                "hierarchy_penalty": hierarchy_penalty,
                "computed_priority": computed_priority,
                "attempt_count": 0,
                "lease_until": None,
                "blocked_reason": blocked_reason,
                "delivery_state": delivery_state,
                "created_at": now,
                "updated_at": now,
            },
        )
        self._log_audit(
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
        self._require_active_agent(recipient)
        self._refresh_queue(recipient=recipient)
        rows = self.storage.get_queue_records(
            recipient=recipient,
            sender=sender,
            task_type=task_type,
            min_priority=min_priority,
            limit=limit,
            pending_only=False,
        )
        return [self._queued_from_record(row) for row in rows]

    def fetch_next(self, recipient: str) -> QueuedMessage | None:
        self._require_active_agent(recipient)
        self._refresh_queue(recipient=recipient)
        lease_until = (
            utc_now() + timedelta(seconds=self.default_lease_seconds)
        ).isoformat().replace("+00:00", "Z")
        leased = self.storage.lease_next_message(recipient, lease_until)
        if leased is None:
            return None
        self._log_audit(
            "fetched",
            leased["id"],
            recipient,
            {"lease_until": lease_until},
        )
        return self._queued_from_record(leased)

    def ack_message(self, message_id: str, recipient: str) -> None:
        row = self.storage.get_message_state(message_id, recipient)
        if row is None:
            raise ValidationError(f"Message '{message_id}' is not queued for '{recipient}'.")
        if row["status"] != "in_progress":
            raise ValidationError("Only in-progress messages can be acknowledged.")
        self.storage.mark_message_done(message_id, iso_now())
        self._log_audit("acked", message_id, recipient, {})

    def nack_message(self, message_id: str, recipient: str, reason: str) -> None:
        if not reason.strip():
            raise ValidationError("A nack reason is required.")
        row = self.storage.get_message_state(message_id, recipient)
        if row is None:
            raise ValidationError(f"Message '{message_id}' is not queued for '{recipient}'.")
        if row["status"] != "in_progress":
            raise ValidationError("Only in-progress messages can be negatively acknowledged.")
        attempts = row["attempt_count"] + 1
        now = iso_now()
        if attempts >= self.max_attempts:
            self.storage.dead_letter_message(message_id, reason.strip(), attempts, now)
            self._log_audit(
                "dead_lettered",
                message_id,
                recipient,
                {"reason": reason.strip(), "attempt_count": attempts},
            )
            return
        self.storage.requeue_message(message_id, reason.strip(), attempts, now)
        self._log_audit(
            "nacked",
            message_id,
            recipient,
            {"reason": reason.strip(), "attempt_count": attempts},
        )

    def requeue_expired_leases(self, recipient: str | None = None) -> int:
        rows = self.storage.requeue_expired_leases(iso_now(), recipient)
        for row in rows:
            self._log_audit(
                "expired",
                row["id"],
                row["recipient"],
                {"reason": "Lease expired"},
            )
        return len(rows)

    def list_queue(self, recipient: str) -> list[QueuedMessage]:
        self._require_active_agent(recipient)
        self._refresh_queue(recipient=recipient)
        return [self._queued_from_record(row) for row in self.storage.get_queue_records(recipient)]

    def list_audit_log(
        self, limit: int = 50, subject_id: str | None = None
    ) -> list[dict[str, Any]]:
        if limit <= 0:
            raise ValidationError("limit must be positive.")
        return self.storage.list_audit_log(limit=limit, subject_id=subject_id)

    def issue_api_key(self, agent_name: str, label: str = "default") -> str:
        agent = self._require_active_agent(agent_name)
        raw_key = secrets.token_urlsafe(32)
        record = AgentApiKeyRecord(
            agent_name=agent.agent_name,
            key_hash=self._hash(raw_key),
            label=label,
            created_at=iso_now(),
            active=True,
        )
        self.storage.store_api_key(record)
        self._log_audit(
            "api_key_issued",
            agent_name,
            agent_name,
            {"label": label},
        )
        return raw_key

    def authenticate_agent(self, agent_name: str, api_key: str) -> AgentRecord:
        if not api_key.strip():
            raise AccessError("API key is required.")
        record = self.storage.get_api_key(agent_name)
        if record is None or not record.get("active", True):
            raise AccessError(f"No active API key found for '{agent_name}'.")
        expected = record["key_hash"]
        actual = self._hash(api_key)
        if not hmac.compare_digest(expected, actual):
            raise AccessError("Invalid API key.")
        self.storage.touch_api_key(agent_name, iso_now())
        return self._require_active_agent(agent_name)

    def _refresh_queue(self, recipient: str | None = None) -> None:
        now = iso_now()
        expired = self.storage.expire_ttl_messages(now, recipient)
        for row in expired:
            self._log_audit(
                "expired",
                row["id"],
                row["recipient"],
                {"reason": "Message TTL expired"},
            )
        self.requeue_expired_leases(recipient=recipient)

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

    def _require_active_agent(self, agent_name: str) -> AgentRecord:
        agent = self.storage.get_agent(agent_name)
        if agent is None:
            raise AccessError(f"Unknown agent '{agent_name}'.")
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

    def _queued_from_record(self, row: dict[str, Any]) -> QueuedMessage:
        envelope = MessageEnvelope(
            id=row["id"],
            timestamp=row["timestamp"],
            sender=row["sender"],
            recipient=row["recipient"],
            task_type=row["task_type"],
            context=row["context"],
            payload=row["payload"],
            status=row["status"],
            error=row.get("error", ""),
        )
        return QueuedMessage(
            envelope=envelope,
            computed_priority=row["computed_priority"],
            attempt_count=row["attempt_count"],
            lease_until=row.get("lease_until"),
            delivery_state=row["delivery_state"],
            blocked_reason=row["blocked_reason"],
            provenance_source=row.get("provenance_source"),
            provenance_agent=row.get("provenance_agent"),
            provenance_trust_level=row.get("provenance_trust_level"),
            ttl_seconds=row.get("ttl_seconds"),
            dedupe_key=row.get("dedupe_key"),
        )

    def _log_audit(
        self, event_type: str, subject_id: str, actor: str, details: dict[str, Any]
    ) -> None:
        self.storage.log_audit(event_type, subject_id, actor, details, iso_now())

    def _hash(self, value: str) -> str:
        return hashlib.sha256(value.encode("utf-8")).hexdigest()
