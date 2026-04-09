from __future__ import annotations

import argparse
import json
from typing import Any
from urllib import parse, request

from .exceptions import RouterError
from .models import AgentRecord, MessageEnvelope, RegistrationRequest, RoutingHints, make_message, role_defaults
from .service import EnterpriseRouter


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Enterprise Router CLI")
    parser.add_argument("--db", default="enterprise_router.db", help="SQLite database path.")
    parser.add_argument("--backend", default="sqlite", choices=["sqlite", "mongo"])
    parser.add_argument("--mongo-uri")
    parser.add_argument("--mongo-db-name")
    parser.add_argument("--api-base-url")
    parser.add_argument("--api-key")
    parser.add_argument("--agent-id")
    parser.add_argument("--admin-secret")
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
    approve_registration.add_argument("--issue-api-key", action="store_true")
    approve_registration.add_argument("--key-label", default="default")

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
    register_agent.add_argument("--issue-api-key", action="store_true")

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

    issue_api_key = subparsers.add_parser("issue-api-key")
    issue_api_key.add_argument("agent_name")
    issue_api_key.add_argument("--label", default="default")

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        result = dispatch(args)
    except RouterError as exc:
        parser.exit(status=1, message=f"error: {exc}\n")
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


def dispatch(args: argparse.Namespace) -> Any:
    if args.api_base_url:
        return dispatch_remote(args)
    router = EnterpriseRouter(
        db_path=args.db,
        shared_secret=args.shared_secret,
        backend=args.backend,
        mongo_uri=args.mongo_uri,
        mongo_db_name=args.mongo_db_name,
    )
    return dispatch_local(router, args)


def dispatch_local(router: EnterpriseRouter, args: argparse.Namespace) -> Any:
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
        api_key = router.approve_registration(
            args.agent_name,
            args.approver,
            issue_api_key=args.issue_api_key,
            key_label=args.key_label,
        )
        result = {"agent_name": args.agent_name, "status": "approved"}
        if api_key:
            result["api_key"] = api_key
        return result

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
        result = agent.to_dict()
        if args.issue_api_key:
            result["api_key"] = router.issue_api_key(args.agent_name)
        return result

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
        return {"message_id": router.submit_message(message, hints)}

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
        item = router.fetch_next(args.recipient)
        return item.to_dict() if item else {}

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

    if args.command == "issue-api-key":
        return {"agent_name": args.agent_name, "api_key": router.issue_api_key(args.agent_name, label=args.label)}

    raise RuntimeError(f"Unhandled command {args.command}")


def dispatch_remote(args: argparse.Namespace) -> Any:
    base = args.api_base_url.rstrip("/")
    headers = {}
    if args.api_key:
        headers["Authorization"] = f"Bearer {args.api_key}"
    if args.agent_id:
        headers["X-Agent-Id"] = args.agent_id
    if args.admin_secret:
        headers["X-Admin-Secret"] = args.admin_secret

    if args.command == "request-registration":
        return _http_json(
            "POST",
            f"{base}/registrations/request",
            headers,
            {
                "agent_name": args.agent_name,
                "role": args.role,
                "secret_token": args.token,
                "file_path": args.file_path,
                "endpoint": args.endpoint,
                "metadata": _loads(args.metadata, default={}),
            },
        )

    if args.command == "approve-registration":
        return _http_json(
            "POST",
            f"{base}/registrations/{parse.quote(args.agent_name)}/approve",
            headers,
            {
                "approver": args.approver,
                "issue_api_key": args.issue_api_key,
                "key_label": args.key_label,
            },
        )

    if args.command == "reject-registration":
        return _http_json(
            "POST",
            f"{base}/registrations/{parse.quote(args.agent_name)}/reject",
            headers,
            {"approver": args.approver, "reason": args.reason},
        )

    if args.command == "list-agents":
        query = f"?status={parse.quote(args.status)}" if args.status else ""
        return _http_json("GET", f"{base}/agents{query}", headers, None)

    if args.command == "list-registrations":
        query = f"?status={parse.quote(args.status)}" if args.status else ""
        return _http_json("GET", f"{base}/registrations{query}", headers, None)

    if args.command == "register-agent":
        defaults = role_defaults(args.role)
        return _http_json(
            "POST",
            f"{base}/agents",
            headers,
            {
                "agent_name": args.agent_name,
                "role": args.role,
                "hierarchy_level": args.hierarchy_level or defaults["hierarchy_level"],
                "trust_level": args.trust_level or defaults["trust_level"],
                "file_path": args.file_path,
                "endpoint": args.endpoint,
                "active": not args.inactive,
                "allowed_senders": _loads(args.allowed_senders, default=[]),
                "allowed_task_types": _loads(args.allowed_task_types, default=[]),
                "issue_api_key": args.issue_api_key,
            },
        )

    if args.command == "send":
        message = MessageEnvelope(
            **make_message(
                sender=args.sender,
                recipient=args.recipient,
                task_type=args.task_type,
                context=_loads(args.context, default={}),
                payload=_loads(args.payload, default={}),
            ).to_dict()
        )
        return _http_json(
            "POST",
            f"{base}/messages",
            headers,
            {
                "message": message.to_dict(),
                "routing_hints": RoutingHints(
                    provenance_source=args.provenance_source,
                    provenance_agent=args.provenance_agent,
                    provenance_trust_level=args.provenance_trust_level,
                    urgency=args.urgency,
                    ttl_seconds=args.ttl_seconds,
                    dedupe_key=args.dedupe_key,
                ).to_dict(),
            },
        )

    if args.command == "peek":
        params = {"recipient": args.recipient, "limit": args.limit}
        if args.min_priority is not None:
            params["min_priority"] = args.min_priority
        if args.sender:
            params["sender"] = args.sender
        if args.task_type:
            params["task_type"] = args.task_type
        return _http_json("GET", f"{base}/messages/peek?{parse.urlencode(params)}", headers, None)

    if args.command == "fetch":
        return _http_json(
            "POST",
            f"{base}/messages/fetch-next",
            headers,
            {"recipient": args.recipient},
        )

    if args.command == "ack":
        return _http_json(
            "POST",
            f"{base}/messages/{parse.quote(args.message_id)}/ack",
            headers,
            {"recipient": args.recipient},
        )

    if args.command == "nack":
        return _http_json(
            "POST",
            f"{base}/messages/{parse.quote(args.message_id)}/nack",
            headers,
            {"recipient": args.recipient, "reason": args.reason},
        )

    if args.command == "show-audit":
        params = {"limit": args.limit}
        if args.subject_id:
            params["subject_id"] = args.subject_id
        return _http_json("GET", f"{base}/audit?{parse.urlencode(params)}", headers, None)

    if args.command == "show-queue":
        return _http_json("GET", f"{base}/queue/{parse.quote(args.recipient)}", headers, None)

    if args.command == "issue-api-key":
        return _http_json(
            "POST",
            f"{base}/agents/{parse.quote(args.agent_name)}/issue-api-key",
            headers,
            {},
        )

    raise RouterError(f"Remote command '{args.command}' is not supported.")


def _http_json(method: str, url: str, headers: dict[str, str], body: Any) -> Any:
    data = None
    request_headers = dict(headers)
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        request_headers["Content-Type"] = "application/json"
    req = request.Request(url, data=data, headers=request_headers, method=method)
    with request.urlopen(req) as response:  # nosec - local/dev CLI helper
        raw = response.read().decode("utf-8")
        return json.loads(raw) if raw else {}


def _loads(raw: str, default: Any) -> Any:
    if not raw:
        return default
    return json.loads(raw)
