from __future__ import annotations

import argparse
import json
from typing import Any

from .exceptions import RouterError
from .models import AgentRecord, RegistrationRequest, RoutingHints, role_defaults
from .service import EnterpriseRouter


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Enterprise Router CLI")
    parser.add_argument("--db", default="enterprise_router.db", help="SQLite database path.")
    parser.add_argument(
        "--shared-secret",
        default=None,
        help="Shared registration secret. Defaults to ENTERPRISE_ROUTER_SHARED_SECRET.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    request_registration = subparsers.add_parser("request-registration")
    request_registration.add_argument("agent_name")
    request_registration.add_argument("role")
    request_registration.add_argument("--token", required=True)
    request_registration.add_argument("--file-path")
    request_registration.add_argument("--endpoint")
    request_registration.add_argument("--metadata", default="{}")

    approve_registration = subparsers.add_parser("approve-registration")
    approve_registration.add_argument("agent_name")
    approve_registration.add_argument("--approver", required=True)

    reject_registration = subparsers.add_parser("reject-registration")
    reject_registration.add_argument("agent_name")
    reject_registration.add_argument("--approver", required=True)
    reject_registration.add_argument("--reason", required=True)

    register_agent = subparsers.add_parser("register-agent")
    register_agent.add_argument("agent_name")
    register_agent.add_argument("role")
    register_agent.add_argument("--hierarchy-level", type=int)
    register_agent.add_argument("--trust-level", type=int)
    register_agent.add_argument("--file-path")
    register_agent.add_argument("--endpoint")
    register_agent.add_argument("--inactive", action="store_true")
    register_agent.add_argument("--allowed-senders", default="[]")
    register_agent.add_argument("--allowed-task-types", default="[]")

    list_agents = subparsers.add_parser("list-agents")
    list_agents.add_argument("--status", choices=["pending", "approved", "rejected"])

    list_registrations = subparsers.add_parser("list-registrations")
    list_registrations.add_argument("--status", choices=["pending", "approved", "rejected"])

    send = subparsers.add_parser("send")
    send.add_argument("sender")
    send.add_argument("recipient")
    send.add_argument("task_type")
    send.add_argument("--context", default="{}")
    send.add_argument("--payload", default="{}")
    send.add_argument("--urgency", default="normal", choices=["low", "normal", "high", "critical"])
    send.add_argument("--provenance-source")
    send.add_argument("--provenance-agent")
    send.add_argument("--provenance-trust-level", type=int)
    send.add_argument("--ttl-seconds", type=int)
    send.add_argument("--dedupe-key")

    peek = subparsers.add_parser("peek")
    peek.add_argument("recipient")
    peek.add_argument("--min-priority", type=int)
    peek.add_argument("--sender")
    peek.add_argument("--task-type")
    peek.add_argument("--limit", type=int, default=10)

    fetch = subparsers.add_parser("fetch")
    fetch.add_argument("recipient")

    ack = subparsers.add_parser("ack")
    ack.add_argument("recipient")
    ack.add_argument("message_id")

    nack = subparsers.add_parser("nack")
    nack.add_argument("recipient")
    nack.add_argument("message_id")
    nack.add_argument("--reason", required=True)

    show_queue = subparsers.add_parser("show-queue")
    show_queue.add_argument("recipient")

    show_audit = subparsers.add_parser("show-audit")
    show_audit.add_argument("--limit", type=int, default=20)
    show_audit.add_argument("--subject-id")

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    router = EnterpriseRouter(db_path=args.db, shared_secret=args.shared_secret)

    try:
        result = dispatch(router, args)
    except RouterError as exc:
        parser.exit(status=1, message=f"error: {exc}\n")
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


def dispatch(router: EnterpriseRouter, args: argparse.Namespace) -> Any:
    if args.command == "request-registration":
        req = RegistrationRequest(
            agent_name=args.agent_name,
            role=args.role,
            secret_token=args.token,
            file_path=args.file_path,
            endpoint=args.endpoint,
            metadata=_loads(args.metadata, default={}),
        )
        return {"agent_name": router.request_registration(req), "status": "pending"}

    if args.command == "approve-registration":
        router.approve_registration(args.agent_name, args.approver)
        return {"agent_name": args.agent_name, "status": "approved"}

    if args.command == "reject-registration":
        router.reject_registration(args.agent_name, args.approver, args.reason)
        return {"agent_name": args.agent_name, "status": "rejected"}

    if args.command == "register-agent":
        defaults = role_defaults(args.role)
        agent = AgentRecord(
            agent_name=args.agent_name,
            role=args.role,
            hierarchy_level=args.hierarchy_level or defaults["hierarchy_level"],
            trust_level=args.trust_level or defaults["trust_level"],
            file_path=args.file_path,
            endpoint=args.endpoint,
            active=not args.inactive,
            allowed_senders=_loads(args.allowed_senders, default=[]),
            allowed_task_types=_loads(args.allowed_task_types, default=[]),
        )
        router.register_agent(agent)
        return agent.to_dict()

    if args.command == "list-agents":
        return [agent.to_dict() for agent in router.list_agents(status=args.status)]

    if args.command == "list-registrations":
        return router.list_registration_requests(status=args.status)

    if args.command == "send":
        message = router.create_message(
            sender=args.sender,
            recipient=args.recipient,
            task_type=args.task_type,
            context=_loads(args.context, default={}),
            payload=_loads(args.payload, default={}),
        )
        hints = RoutingHints(
            provenance_source=args.provenance_source,
            provenance_agent=args.provenance_agent,
            provenance_trust_level=args.provenance_trust_level,
            urgency=args.urgency,
            ttl_seconds=args.ttl_seconds,
            dedupe_key=args.dedupe_key,
        )
        message_id = router.submit_message(message, hints)
        return {"message_id": message_id}

    if args.command == "peek":
        return [
            item.to_dict()
            for item in router.peek_messages(
                recipient=args.recipient,
                min_priority=args.min_priority,
                sender=args.sender,
                task_type=args.task_type,
                limit=args.limit,
            )
        ]

    if args.command == "fetch":
        result = router.fetch_next(args.recipient)
        return result.to_dict() if result else {}

    if args.command == "ack":
        router.ack_message(args.message_id, args.recipient)
        return {"message_id": args.message_id, "status": "done"}

    if args.command == "nack":
        router.nack_message(args.message_id, args.recipient, args.reason)
        return {"message_id": args.message_id, "status": "requeued_or_dead_lettered"}

    if args.command == "show-queue":
        return [item.to_dict() for item in router.list_queue(args.recipient)]

    if args.command == "show-audit":
        return router.list_audit_log(limit=args.limit, subject_id=args.subject_id)

    raise RuntimeError(f"Unhandled command {args.command}")


def _loads(raw: str, default: Any) -> Any:
    if not raw:
        return default
    return json.loads(raw)
