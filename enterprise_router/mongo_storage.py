from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from .models import AgentApiKeyRecord, AgentRecord, MessageEnvelope, RegistrationRequest, parse_timestamp
from .storage import RouterStorage

try:
    from pymongo import ASCENDING, DESCENDING, MongoClient, ReturnDocument
except ImportError:  # pragma: no cover - optional dependency
    ASCENDING = 1
    DESCENDING = -1
    MongoClient = None
    ReturnDocument = None


class MongoStorage(RouterStorage):
    def __init__(self, uri: str | None, db_name: str | None) -> None:
        if MongoClient is None:  # pragma: no cover - dependency guard
            raise RuntimeError("pymongo is required for the Mongo router backend.")
        if not uri:
            raise RuntimeError("MONGODB_URI is required for the Mongo router backend.")
        if not db_name:
            raise RuntimeError("MONGODB_DB_NAME is required for the Mongo router backend.")
        self.client = MongoClient(uri)
        self.db = self.client[db_name]
        self.agents = self.db["agents"]
        self.registration_requests = self.db["registration_requests"]
        self.messages = self.db["messages"]
        self.routing_metadata = self.db["routing_metadata"]
        self.audit_log = self.db["audit_log"]
        self.agent_api_keys = self.db["agent_api_keys"]
        self._initialize()

    def _initialize(self) -> None:
        self.agents.create_index("agent_name", unique=True)
        self.registration_requests.create_index("agent_name", unique=True)
        self.messages.create_index([("recipient", ASCENDING), ("status", ASCENDING), ("timestamp", ASCENDING)])
        self.routing_metadata.create_index([("message_id", ASCENDING)], unique=True)
        self.routing_metadata.create_index(
            [("delivery_state", ASCENDING), ("computed_priority", DESCENDING), ("recipient_level", ASCENDING)]
        )
        self.audit_log.create_index([("subject_id", ASCENDING), ("created_at", DESCENDING)])
        self.agent_api_keys.create_index("agent_name", unique=True)

    def register_agent(self, agent: AgentRecord) -> None:
        self.agents.replace_one({"agent_name": agent.agent_name}, agent.to_dict(), upsert=True)

    def get_agent(self, agent_name: str) -> AgentRecord | None:
        doc = self.agents.find_one({"agent_name": agent_name}, {"_id": 0})
        return AgentRecord(**doc) if doc else None

    def list_agents(self, status: str | None = None) -> list[AgentRecord]:
        query = {"registration_status": status} if status else {}
        docs = self.agents.find(query, {"_id": 0}).sort(
            [("hierarchy_level", ASCENDING), ("agent_name", ASCENDING)]
        )
        return [AgentRecord(**doc) for doc in docs]

    def request_registration(self, req: RegistrationRequest, token_hash: str) -> str:
        doc = req.to_dict()
        doc.update(
            {
                "token_hash": token_hash,
                "status": "pending",
                "rejection_reason": "",
                "requested_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                "reviewed_at": None,
                "reviewed_by": None,
            }
        )
        self.registration_requests.insert_one(doc)
        return req.agent_name

    def get_registration_request(self, agent_name: str) -> dict[str, Any] | None:
        return self.registration_requests.find_one({"agent_name": agent_name}, {"_id": 0})

    def list_registration_requests(self, status: str | None = None) -> list[dict[str, Any]]:
        query = {"status": status} if status else {}
        return list(self.registration_requests.find(query, {"_id": 0}).sort("requested_at", ASCENDING))

    def update_registration_status(
        self,
        agent_name: str,
        status: str,
        reviewed_at: str,
        reviewed_by: str,
        rejection_reason: str = "",
    ) -> bool:
        result = self.registration_requests.update_one(
            {"agent_name": agent_name},
            {
                "$set": {
                    "status": status,
                    "reviewed_at": reviewed_at,
                    "reviewed_by": reviewed_by,
                    "rejection_reason": rejection_reason,
                }
            },
        )
        return result.modified_count > 0

    def find_message_by_dedupe(
        self, sender: str, recipient: str, task_type: str, dedupe_key: str
    ) -> str | None:
        routing = self.routing_metadata.find_one(
            {
                "dedupe_key": dedupe_key,
                "delivery_state": {"$nin": ["dead_lettered", "expired"]},
            },
            {"_id": 0, "message_id": 1},
        )
        if not routing:
            return None
        message = self.messages.find_one(
            {
                "id": routing["message_id"],
                "sender": sender,
                "recipient": recipient,
                "task_type": task_type,
            },
            {"_id": 0, "id": 1},
        )
        return message["id"] if message else None

    def insert_message(self, message: MessageEnvelope, routing: dict[str, Any]) -> None:
        self.messages.insert_one(message.to_dict())
        routing_doc = dict(routing)
        routing_doc["message_id"] = message.id
        self.routing_metadata.insert_one(routing_doc)

    def get_queue_records(
        self,
        recipient: str,
        sender: str | None = None,
        task_type: str | None = None,
        min_priority: int | None = None,
        limit: int | None = None,
        pending_only: bool = False,
    ) -> list[dict[str, Any]]:
        message_query: dict[str, Any] = {"recipient": recipient} if recipient else {}
        if sender:
            message_query["sender"] = sender
        if task_type:
            message_query["task_type"] = task_type
        if pending_only:
            message_query["status"] = "pending"
        messages = list(self.messages.find(message_query, {"_id": 0}))
        if not messages:
            return []
        routing_map = {
            doc["message_id"]: doc
            for doc in self.routing_metadata.find(
                {"message_id": {"$in": [msg["id"] for msg in messages]}}, {"_id": 0}
            )
        }
        rows: list[dict[str, Any]] = []
        now = datetime.now(timezone.utc)
        for msg in messages:
            routing = routing_map.get(msg["id"])
            if not routing:
                continue
            if pending_only and routing["delivery_state"] != "pending":
                continue
            age_minutes = max(
                0,
                int((now - parse_timestamp(msg["timestamp"])).total_seconds() // 60),
            )
            effective_priority = routing["computed_priority"] + age_minutes
            row = {
                **msg,
                "provenance_source": routing.get("provenance_source"),
                "provenance_agent": routing.get("provenance_agent"),
                "provenance_trust_level": routing["provenance_trust_level"],
                "ttl_seconds": routing.get("ttl_seconds"),
                "dedupe_key": routing.get("dedupe_key"),
                "attempt_count": routing["attempt_count"],
                "lease_until": routing.get("lease_until"),
                "delivery_state": routing["delivery_state"],
                "blocked_reason": routing["blocked_reason"],
                "computed_priority": effective_priority,
            }
            if min_priority is None or effective_priority >= min_priority:
                rows.append(row)
        rows.sort(key=lambda item: (-item["computed_priority"], item["timestamp"]))
        return rows[:limit] if limit is not None else rows

    def lease_next_message(self, recipient: str, lease_until: str) -> dict[str, Any] | None:
        candidates = self.get_queue_records(recipient, pending_only=True)
        for candidate in candidates:
            with self.client.start_session() as session:
                with session.start_transaction():
                    routing = self.routing_metadata.find_one_and_update(
                        {
                            "message_id": candidate["id"],
                            "delivery_state": "pending",
                        },
                        {
                            "$set": {
                                "delivery_state": "leased",
                                "lease_until": lease_until,
                                "updated_at": lease_until,
                            }
                        },
                        return_document=ReturnDocument.AFTER,
                        session=session,
                    )
                    if routing is None:
                        continue
                    message = self.messages.find_one_and_update(
                        {"id": candidate["id"], "status": "pending"},
                        {"$set": {"status": "in_progress"}},
                        return_document=ReturnDocument.AFTER,
                        session=session,
                    )
                    if message is None:
                        self.routing_metadata.update_one(
                            {"message_id": candidate["id"]},
                            {"$set": {"delivery_state": "pending", "lease_until": None}},
                            session=session,
                        )
                        continue
            return self.get_message_state(candidate["id"], recipient)
        return None

    def get_message_state(self, message_id: str, recipient: str) -> dict[str, Any] | None:
        rows = self.get_queue_records(recipient)
        for row in rows:
            if row["id"] == message_id:
                return row
        message = self.messages.find_one({"id": message_id, "recipient": recipient}, {"_id": 0})
        if not message:
            return None
        routing = self.routing_metadata.find_one({"message_id": message_id}, {"_id": 0})
        if not routing:
            return None
        return {
            **message,
            "provenance_source": routing.get("provenance_source"),
            "provenance_agent": routing.get("provenance_agent"),
            "provenance_trust_level": routing["provenance_trust_level"],
            "ttl_seconds": routing.get("ttl_seconds"),
            "dedupe_key": routing.get("dedupe_key"),
            "attempt_count": routing["attempt_count"],
            "lease_until": routing.get("lease_until"),
            "delivery_state": routing["delivery_state"],
            "blocked_reason": routing["blocked_reason"],
            "computed_priority": routing["computed_priority"],
        }

    def mark_message_done(self, message_id: str, now: str) -> None:
        self.messages.update_one({"id": message_id}, {"$set": {"status": "done", "error": ""}})
        self.routing_metadata.update_one(
            {"message_id": message_id},
            {"$set": {"delivery_state": "done", "lease_until": None, "blocked_reason": "", "updated_at": now}},
        )

    def requeue_message(self, message_id: str, error: str, attempts: int, now: str) -> None:
        self.messages.update_one({"id": message_id}, {"$set": {"status": "pending", "error": error}})
        self.routing_metadata.update_one(
            {"message_id": message_id},
            {
                "$set": {
                    "attempt_count": attempts,
                    "delivery_state": "pending",
                    "lease_until": None,
                    "blocked_reason": "",
                    "updated_at": now,
                }
            },
        )

    def dead_letter_message(self, message_id: str, error: str, attempts: int, now: str) -> None:
        self.messages.update_one({"id": message_id}, {"$set": {"status": "error", "error": error}})
        self.routing_metadata.update_one(
            {"message_id": message_id},
            {
                "$set": {
                    "attempt_count": attempts,
                    "delivery_state": "dead_lettered",
                    "lease_until": None,
                    "blocked_reason": error,
                    "updated_at": now,
                }
            },
        )

    def requeue_expired_leases(
        self, now: str, recipient: str | None = None
    ) -> list[dict[str, Any]]:
        rows = self.get_queue_records(recipient) if recipient else self._all_queue_records()
        now_dt = parse_timestamp(now)
        updated: list[dict[str, Any]] = []
        for row in rows:
            lease_until = row.get("lease_until")
            if row["delivery_state"] == "leased" and lease_until and parse_timestamp(lease_until) <= now_dt:
                self.messages.update_one({"id": row["id"]}, {"$set": {"status": "pending"}})
                self.routing_metadata.update_one(
                    {"message_id": row["id"]},
                    {"$set": {"delivery_state": "pending", "lease_until": None, "updated_at": now}},
                )
                row["status"] = "pending"
                row["delivery_state"] = "pending"
                row["lease_until"] = None
                updated.append(row)
        return updated

    def expire_ttl_messages(
        self, now: str, recipient: str | None = None
    ) -> list[dict[str, Any]]:
        rows = self.get_queue_records(recipient) if recipient else self._all_queue_records()
        now_dt = parse_timestamp(now)
        updated: list[dict[str, Any]] = []
        for row in rows:
            ttl_seconds = row.get("ttl_seconds")
            if ttl_seconds is None:
                continue
            if row["delivery_state"] in {"expired", "dead_lettered", "done"}:
                continue
            expiry = parse_timestamp(row["timestamp"]) + timedelta(seconds=ttl_seconds)
            if expiry <= now_dt:
                self.messages.update_one(
                    {"id": row["id"]},
                    {"$set": {"status": "error", "error": "Message TTL expired."}},
                )
                self.routing_metadata.update_one(
                    {"message_id": row["id"]},
                    {
                        "$set": {
                            "delivery_state": "expired",
                            "lease_until": None,
                            "blocked_reason": "Message TTL expired.",
                            "updated_at": now,
                        }
                    },
                )
                row["status"] = "error"
                row["delivery_state"] = "expired"
                row["blocked_reason"] = "Message TTL expired."
                updated.append(row)
        return updated

    def log_audit(
        self, event_type: str, subject_id: str, actor: str, details: dict[str, Any], created_at: str
    ) -> None:
        self.audit_log.insert_one(
            {
                "event_type": event_type,
                "subject_id": subject_id,
                "actor": actor,
                "details": details,
                "created_at": created_at,
            }
        )

    def list_audit_log(
        self, limit: int = 50, subject_id: str | None = None
    ) -> list[dict[str, Any]]:
        query = {"subject_id": subject_id} if subject_id else {}
        return list(
            self.audit_log.find(query, {"_id": 0}).sort("created_at", DESCENDING).limit(limit)
        )

    def store_api_key(self, record: AgentApiKeyRecord) -> None:
        self.agent_api_keys.replace_one(
            {"agent_name": record.agent_name},
            record.to_dict(),
            upsert=True,
        )

    def get_api_key(self, agent_name: str) -> dict[str, Any] | None:
        return self.agent_api_keys.find_one({"agent_name": agent_name}, {"_id": 0})

    def touch_api_key(self, agent_name: str, last_used_at: str) -> None:
        self.agent_api_keys.update_one(
            {"agent_name": agent_name},
            {"$set": {"last_used_at": last_used_at}},
        )

    def _all_queue_records(self) -> list[dict[str, Any]]:
        messages = list(self.messages.find({}, {"_id": 0}))
        if not messages:
            return []
        routing_map = {
            doc["message_id"]: doc
            for doc in self.routing_metadata.find(
                {"message_id": {"$in": [msg["id"] for msg in messages]}}, {"_id": 0}
            )
        }
        now = datetime.now(timezone.utc)
        rows: list[dict[str, Any]] = []
        for msg in messages:
            routing = routing_map.get(msg["id"])
            if not routing:
                continue
            age_minutes = max(
                0,
                int((now - parse_timestamp(msg["timestamp"])).total_seconds() // 60),
            )
            rows.append(
                {
                    **msg,
                    "provenance_source": routing.get("provenance_source"),
                    "provenance_agent": routing.get("provenance_agent"),
                    "provenance_trust_level": routing["provenance_trust_level"],
                    "ttl_seconds": routing.get("ttl_seconds"),
                    "dedupe_key": routing.get("dedupe_key"),
                    "attempt_count": routing["attempt_count"],
                    "lease_until": routing.get("lease_until"),
                    "delivery_state": routing["delivery_state"],
                    "blocked_reason": routing["blocked_reason"],
                    "computed_priority": routing["computed_priority"] + age_minutes,
                }
            )
        rows.sort(key=lambda item: (-item["computed_priority"], item["timestamp"]))
        return rows
