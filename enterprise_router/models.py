from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

MESSAGE_STATUSES = {"pending", "in_progress", "done", "error"}
REGISTRATION_STATUSES = {"pending", "approved", "rejected"}
URGENCY_WEIGHTS = {
    "low": 10,
    "normal": 25,
    "high": 50,
    "critical": 100,
}
ROLE_DEFAULTS = {
    "CEO": {"hierarchy_level": 1, "trust_level": 100, "recipient_weight": 120},
    "MANAGER": {"hierarchy_level": 2, "trust_level": 95, "recipient_weight": 95},
    "PM": {"hierarchy_level": 2, "trust_level": 75, "recipient_weight": 80},
    "PRODUCT": {"hierarchy_level": 2, "trust_level": 75, "recipient_weight": 80},
    "ENGINEERING": {"hierarchy_level": 2, "trust_level": 75, "recipient_weight": 80},
    "ENG": {"hierarchy_level": 2, "trust_level": 75, "recipient_weight": 80},
    "FINANCE": {"hierarchy_level": 2, "trust_level": 85, "recipient_weight": 90},
    "MARKETING": {"hierarchy_level": 2, "trust_level": 70, "recipient_weight": 75},
    "SALES": {"hierarchy_level": 2, "trust_level": 70, "recipient_weight": 75},
    "HR": {"hierarchy_level": 2, "trust_level": 70, "recipient_weight": 75},
    "WORKER": {"hierarchy_level": 3, "trust_level": 45, "recipient_weight": 45},
    "SUB_AGENT": {"hierarchy_level": 3, "trust_level": 45, "recipient_weight": 45},
}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_now() -> str:
    return utc_now().isoformat().replace("+00:00", "Z")


def parse_timestamp(value: str) -> datetime:
    normalized = value.replace("Z", "+00:00")
    return datetime.fromisoformat(normalized).astimezone(timezone.utc)


def normalize_role(role: str) -> str:
    return role.strip().upper()


def role_defaults(role: str) -> dict[str, int]:
    return ROLE_DEFAULTS.get(
        normalize_role(role),
        {"hierarchy_level": 4, "trust_level": 30, "recipient_weight": 35},
    )


def make_message(
    sender: str,
    recipient: str,
    task_type: str,
    context: dict[str, Any] | None = None,
    payload: dict[str, Any] | None = None,
) -> "MessageEnvelope":
    return MessageEnvelope(
        id=str(uuid4()),
        timestamp=iso_now(),
        sender=sender,
        recipient=recipient,
        task_type=task_type,
        context=context or {},
        payload=payload or {},
        status="pending",
        error="",
    )


@dataclass(slots=True)
class AgentRecord:
    agent_name: str
    role: str
    hierarchy_level: int
    trust_level: int
    file_path: str | None = None
    endpoint: str | None = None
    active: bool = True
    registration_status: str = "approved"
    allowed_senders: list[str] = field(default_factory=list)
    allowed_task_types: list[str] = field(default_factory=list)
    created_at: str = field(default_factory=iso_now)
    approved_at: str | None = field(default_factory=iso_now)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class RegistrationRequest:
    agent_name: str
    role: str
    secret_token: str
    file_path: str | None = None
    endpoint: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data.pop("secret_token", None)
        return data


@dataclass(slots=True)
class MessageEnvelope:
    id: str
    timestamp: str
    sender: str
    recipient: str
    task_type: str
    context: dict[str, Any]
    payload: dict[str, Any]
    status: str
    error: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class RoutingHints:
    provenance_source: str | None = None
    provenance_agent: str | None = None
    provenance_trust_level: int | None = None
    urgency: str = "normal"
    ttl_seconds: int | None = None
    dedupe_key: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class QueuedMessage:
    envelope: MessageEnvelope
    computed_priority: int
    attempt_count: int
    lease_until: str | None
    delivery_state: str
    blocked_reason: str
    provenance_source: str | None = None
    provenance_agent: str | None = None
    provenance_trust_level: int | None = None
    ttl_seconds: int | None = None
    dedupe_key: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "envelope": self.envelope.to_dict(),
            "computed_priority": self.computed_priority,
            "attempt_count": self.attempt_count,
            "lease_until": self.lease_until,
            "delivery_state": self.delivery_state,
            "blocked_reason": self.blocked_reason,
            "provenance_source": self.provenance_source,
            "provenance_agent": self.provenance_agent,
            "provenance_trust_level": self.provenance_trust_level,
            "ttl_seconds": self.ttl_seconds,
            "dedupe_key": self.dedupe_key,
        }


@dataclass(slots=True)
class AgentApiKeyRecord:
    agent_name: str
    key_hash: str
    label: str = "default"
    created_at: str = field(default_factory=iso_now)
    last_used_at: str | None = None
    active: bool = True

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
