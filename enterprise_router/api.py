from __future__ import annotations

from typing import Any

from .config import RouterSettings
from .exceptions import AccessError, RegistrationError, ValidationError
from .models import AgentRecord, MessageEnvelope, RegistrationRequest, RoutingHints
from .service import EnterpriseRouter

try:  # pragma: no cover - optional dependency
    from fastapi import Depends, FastAPI, Header, HTTPException
    from pydantic import BaseModel, Field
except ImportError:  # pragma: no cover - optional dependency
    FastAPI = None
    BaseModel = object
    Depends = Header = Field = HTTPException = None


if FastAPI is not None:  # pragma: no cover - imported only when fastapi exists
    class RegistrationRequestBody(BaseModel):
        agent_name: str
        role: str
        secret_token: str
        file_path: str | None = None
        endpoint: str | None = None
        metadata: dict[str, Any] = Field(default_factory=dict)


    class ApprovalBody(BaseModel):
        approver: str
        issue_api_key: bool = False
        key_label: str = "default"


    class RejectionBody(BaseModel):
        approver: str
        reason: str


    class MessageBody(BaseModel):
        id: str
        timestamp: str
        sender: str
        recipient: str
        task_type: str
        context: dict[str, Any] = Field(default_factory=dict)
        payload: dict[str, Any] = Field(default_factory=dict)
        status: str
        error: str = ""


    class SubmitMessageBody(BaseModel):
        message: MessageBody
        routing_hints: dict[str, Any] = Field(default_factory=dict)


    class AgentBody(BaseModel):
        agent_name: str
        role: str
        hierarchy_level: int
        trust_level: int
        file_path: str | None = None
        endpoint: str | None = None
        active: bool = True
        allowed_senders: list[str] = Field(default_factory=list)
        allowed_task_types: list[str] = Field(default_factory=list)
        issue_api_key: bool = False


    class FetchBody(BaseModel):
        recipient: str


    class AckBody(BaseModel):
        recipient: str


    class NackBody(BaseModel):
        recipient: str
        reason: str


def create_app(settings: RouterSettings | None = None):
    if FastAPI is None:  # pragma: no cover - dependency guard
        raise RuntimeError("fastapi is required to run the router API.")

    settings = settings or RouterSettings.from_env()
    router = EnterpriseRouter(
        backend=settings.backend,
        db_path=settings.sqlite_db_path,
        mongo_uri=settings.mongo_uri,
        mongo_db_name=settings.mongo_db_name,
        shared_secret=settings.shared_secret,
    )
    app = FastAPI(title="Enterprise Router API", version="0.2.0")

    def handle_router_error(exc: Exception) -> None:
        if isinstance(exc, AccessError):
            raise HTTPException(status_code=403, detail=str(exc))
        if isinstance(exc, (ValidationError, RegistrationError)):
            raise HTTPException(status_code=400, detail=str(exc))
        raise exc

    def require_admin(x_admin_secret: str = Header(default="")) -> None:
        if x_admin_secret != settings.admin_secret:
            raise HTTPException(status_code=401, detail="Invalid admin secret.")

    def require_agent(
        authorization: str = Header(default=""),
        x_agent_id: str = Header(default=""),
    ) -> str:
        if not authorization.startswith("Bearer "):
            raise HTTPException(status_code=401, detail="Missing bearer token.")
        api_key = authorization.split(" ", 1)[1].strip()
        try:
            router.authenticate_agent(x_agent_id, api_key)
        except Exception as exc:
            handle_router_error(exc)
        return x_agent_id

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok", "backend": settings.backend}

    @app.post("/registrations/request")
    def request_registration(body: RegistrationRequestBody) -> dict[str, Any]:
        try:
            agent_name = router.request_registration(RegistrationRequest(**body.model_dump()))
        except Exception as exc:
            handle_router_error(exc)
        return {"agent_name": agent_name, "status": "pending"}

    @app.post("/registrations/{agent_name}/approve")
    def approve_registration(
        agent_name: str,
        body: ApprovalBody,
        _: None = Depends(require_admin),
    ) -> dict[str, Any]:
        try:
            api_key = router.approve_registration(
                agent_name,
                body.approver,
                issue_api_key=body.issue_api_key,
                key_label=body.key_label,
            )
        except Exception as exc:
            handle_router_error(exc)
        response = {"agent_name": agent_name, "status": "approved"}
        if api_key:
            response["api_key"] = api_key
        return response

    @app.get("/registrations")
    def list_registrations(
        status: str | None = None,
        _: None = Depends(require_admin),
    ) -> list[dict[str, Any]]:
        try:
            return router.list_registration_requests(status=status)
        except Exception as exc:
            handle_router_error(exc)

    @app.post("/agents")
    def register_agent(
        body: AgentBody,
        _: None = Depends(require_admin),
    ) -> dict[str, Any]:
        try:
            router.register_agent(
                AgentRecord(
                    agent_name=body.agent_name,
                    role=body.role,
                    hierarchy_level=body.hierarchy_level,
                    trust_level=body.trust_level,
                    file_path=body.file_path,
                    endpoint=body.endpoint,
                    active=body.active,
                    allowed_senders=body.allowed_senders,
                    allowed_task_types=body.allowed_task_types,
                )
            )
            response = {"agent_name": body.agent_name, "status": "approved"}
            if body.issue_api_key:
                response["api_key"] = router.issue_api_key(body.agent_name)
            return response
        except Exception as exc:
            handle_router_error(exc)

    @app.post("/registrations/{agent_name}/reject")
    def reject_registration(
        agent_name: str,
        body: RejectionBody,
        _: None = Depends(require_admin),
    ) -> dict[str, Any]:
        try:
            router.reject_registration(agent_name, body.approver, body.reason)
        except Exception as exc:
            handle_router_error(exc)
        return {"agent_name": agent_name, "status": "rejected"}

    @app.get("/agents")
    def list_agents(
        status: str | None = None,
        _: str = Depends(require_agent),
    ) -> list[dict[str, Any]]:
        try:
            return [agent.to_dict() for agent in router.list_agents(status=status)]
        except Exception as exc:
            handle_router_error(exc)

    @app.post("/messages")
    def submit_message(
        body: SubmitMessageBody,
        agent_name: str = Depends(require_agent),
    ) -> dict[str, Any]:
        if body.message.sender != agent_name:
            raise HTTPException(status_code=403, detail="Authenticated agent does not match sender.")
        try:
            message_id = router.submit_message(
                MessageEnvelope(**body.message.model_dump()),
                RoutingHints(**body.routing_hints),
            )
        except Exception as exc:
            handle_router_error(exc)
        return {"message_id": message_id}

    @app.get("/messages/peek")
    def peek_messages(
        recipient: str,
        min_priority: int | None = None,
        sender: str | None = None,
        task_type: str | None = None,
        limit: int = 10,
        agent_name: str = Depends(require_agent),
    ) -> list[dict[str, Any]]:
        if agent_name != recipient:
            raise HTTPException(status_code=403, detail="Agents may only peek their own queue.")
        try:
            return [
                item.to_dict()
                for item in router.peek_messages(recipient, min_priority, sender, task_type, limit)
            ]
        except Exception as exc:
            handle_router_error(exc)

    @app.post("/messages/fetch-next")
    def fetch_next(body: FetchBody, agent_name: str = Depends(require_agent)) -> dict[str, Any]:
        if agent_name != body.recipient:
            raise HTTPException(status_code=403, detail="Agents may only fetch their own queue.")
        try:
            item = router.fetch_next(body.recipient)
        except Exception as exc:
            handle_router_error(exc)
        return item.to_dict() if item else {}

    @app.get("/queue/{recipient}")
    def queue_view(recipient: str, agent_name: str = Depends(require_agent)) -> list[dict[str, Any]]:
        if agent_name != recipient:
            raise HTTPException(status_code=403, detail="Agents may only inspect their own queue.")
        try:
            return [item.to_dict() for item in router.list_queue(recipient)]
        except Exception as exc:
            handle_router_error(exc)

    @app.post("/messages/{message_id}/ack")
    def ack_message(
        message_id: str,
        body: AckBody,
        agent_name: str = Depends(require_agent),
    ) -> dict[str, Any]:
        if agent_name != body.recipient:
            raise HTTPException(status_code=403, detail="Agents may only ack their own queue.")
        try:
            router.ack_message(message_id, body.recipient)
        except Exception as exc:
            handle_router_error(exc)
        return {"message_id": message_id, "status": "done"}

    @app.post("/messages/{message_id}/nack")
    def nack_message(
        message_id: str,
        body: NackBody,
        agent_name: str = Depends(require_agent),
    ) -> dict[str, Any]:
        if agent_name != body.recipient:
            raise HTTPException(status_code=403, detail="Agents may only nack their own queue.")
        try:
            router.nack_message(message_id, body.recipient, body.reason)
        except Exception as exc:
            handle_router_error(exc)
        return {"message_id": message_id, "status": "requeued_or_dead_lettered"}

    @app.get("/audit")
    def audit(
        limit: int = 20,
        subject_id: str | None = None,
        _: None = Depends(require_admin),
    ) -> list[dict[str, Any]]:
        try:
            return router.list_audit_log(limit=limit, subject_id=subject_id)
        except Exception as exc:
            handle_router_error(exc)

    @app.post("/agents/{agent_name}/issue-api-key")
    def issue_api_key(agent_name: str, _: None = Depends(require_admin)) -> dict[str, Any]:
        try:
            api_key = router.issue_api_key(agent_name)
        except Exception as exc:
            handle_router_error(exc)
        return {"agent_name": agent_name, "api_key": api_key}

    return app


def main() -> int:
    if FastAPI is None:  # pragma: no cover - dependency guard
        raise RuntimeError("fastapi is required to run the router API.")
    try:  # pragma: no cover - optional dependency
        import uvicorn
    except ImportError as exc:  # pragma: no cover - dependency guard
        raise RuntimeError("uvicorn is required to run the router API.") from exc

    settings = RouterSettings.from_env()
    uvicorn.run(
        create_app(settings),
        host=settings.api_host,
        port=settings.api_port,
    )
    return 0


if __name__ == "__main__":  # pragma: no cover - module entrypoint
    raise SystemExit(main())
