## Enterprise Router

`enterprise_router` is a Python package and CLI for routing messages between enterprise agents.
It keeps a persistent agent registry, supports approval-gated self-registration, stores the required
message envelope unchanged, and manages hierarchy-aware queues with audit logging.

### Features

- SQLite-backed agent registry and message queue
- Approval-gated self-registration with hashed shared secrets
- Shared JSON envelope for all messages
- Recipient queue ordering based on urgency, hierarchy, trust, and age
- Recipient-side filtering and `fetch_next()` leasing
- Retry, dead-letter, TTL expiry, and audit history
- CLI demo for local development

### Quick Start

```powershell
python -m enterprise_router.cli request-registration worker-1 worker --token team-secret
python -m enterprise_router.cli register-agent CEO CEO --allowed-task-types "[\"ESCALATE\"]"
python -m enterprise_router.cli approve-registration worker-1 --approver CEO
python -m enterprise_router.cli send worker-1 CEO ESCALATE --payload "{\"summary\": \"Need approval\"}"
python -m enterprise_router.cli peek CEO
python -m enterprise_router.cli fetch CEO
```

By default the CLI uses:

- Database: `enterprise_router.db`
- Shared secret: `ENTERPRISE_ROUTER_SHARED_SECRET` or `dev-shared-secret`

You can override the database path with `--db`.

### Development

Run the test suite:

```powershell
python -m unittest discover -s tests -v
```

### Package Structure

- `enterprise_router/models.py`: public dataclasses and defaults
- `enterprise_router/service.py`: routing and queue orchestration
- `enterprise_router/storage.py`: SQLite schema bootstrap
- `enterprise_router/cli.py`: CLI demo
- `tests/test_router.py`: unit tests covering registration, routing, queueing, and retries
