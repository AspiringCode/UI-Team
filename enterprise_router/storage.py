from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from .models import AgentApiKeyRecord, AgentRecord, MessageEnvelope, RegistrationRequest


class RouterStorage(ABC):
    @abstractmethod
    def register_agent(self, agent: AgentRecord) -> None: ...

    @abstractmethod
    def get_agent(self, agent_name: str) -> AgentRecord | None: ...

    @abstractmethod
    def list_agents(self, status: str | None = None) -> list[AgentRecord]: ...

    @abstractmethod
    def request_registration(self, req: RegistrationRequest, token_hash: str) -> str: ...

    @abstractmethod
    def get_registration_request(self, agent_name: str) -> dict[str, Any] | None: ...

    @abstractmethod
    def list_registration_requests(self, status: str | None = None) -> list[dict[str, Any]]: ...

    @abstractmethod
    def update_registration_status(
        self,
        agent_name: str,
        status: str,
        reviewed_at: str,
        reviewed_by: str,
        rejection_reason: str = "",
    ) -> bool: ...

    @abstractmethod
    def find_message_by_dedupe(
        self, sender: str, recipient: str, task_type: str, dedupe_key: str
    ) -> str | None: ...

    @abstractmethod
    def insert_message(self, message: MessageEnvelope, routing: dict[str, Any]) -> None: ...

    @abstractmethod
    def get_queue_records(
        self,
        recipient: str,
        sender: str | None = None,
        task_type: str | None = None,
        min_priority: int | None = None,
        limit: int | None = None,
        pending_only: bool = False,
    ) -> list[dict[str, Any]]: ...

    @abstractmethod
    def lease_next_message(
        self, recipient: str, lease_until: str
    ) -> dict[str, Any] | None: ...

    @abstractmethod
    def get_message_state(
        self, message_id: str, recipient: str
    ) -> dict[str, Any] | None: ...

    @abstractmethod
    def mark_message_done(self, message_id: str, now: str) -> None: ...

    @abstractmethod
    def requeue_message(
        self, message_id: str, error: str, attempts: int, now: str
    ) -> None: ...

    @abstractmethod
    def dead_letter_message(
        self, message_id: str, error: str, attempts: int, now: str
    ) -> None: ...

    @abstractmethod
    def requeue_expired_leases(
        self, now: str, recipient: str | None = None
    ) -> list[dict[str, Any]]: ...

    @abstractmethod
    def expire_ttl_messages(
        self, now: str, recipient: str | None = None
    ) -> list[dict[str, Any]]: ...

    @abstractmethod
    def log_audit(
        self, event_type: str, subject_id: str, actor: str, details: dict[str, Any], created_at: str
    ) -> None: ...

    @abstractmethod
    def list_audit_log(
        self, limit: int = 50, subject_id: str | None = None
    ) -> list[dict[str, Any]]: ...

    @abstractmethod
    def store_api_key(self, record: AgentApiKeyRecord) -> None: ...

    @abstractmethod
    def get_api_key(self, agent_name: str) -> dict[str, Any] | None: ...

    @abstractmethod
    def touch_api_key(self, agent_name: str, last_used_at: str) -> None: ...


def create_storage(
    backend: str = "sqlite",
    db_path: str = "enterprise_router.db",
    mongo_uri: str | None = None,
    mongo_db_name: str | None = None,
) -> RouterStorage:
    normalized = backend.strip().lower()
    if normalized == "sqlite":
        from .sqlite_storage import SQLiteStorage

        return SQLiteStorage(db_path)
    if normalized == "mongo":
        from .mongo_storage import MongoStorage

        return MongoStorage(uri=mongo_uri, db_name=mongo_db_name)
    raise ValueError(f"Unsupported router backend '{backend}'.")
