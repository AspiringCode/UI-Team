## Enterprise Router

`enterprise_router` is a Python package and CLI for routing messages between enterprise agents.
It keeps a persistent agent registry, supports approval-gated self-registration, stores the required
message envelope unchanged, and manages hierarchy-aware queues with audit logging.

### Features

- SQLite-backed agent registry and message queue
- Optional MongoDB Atlas backend for online/shared routing
- Approval-gated self-registration with hashed shared secrets
- Per-agent API key issuance and authentication
- Shared JSON envelope for all messages
- Recipient queue ordering based on urgency, hierarchy, trust, and age
- Recipient-side filtering and `fetch_next()` leasing
- Retry, dead-letter, TTL expiry, and audit history
- CLI demo for local development
- Optional FastAPI service for remote agent communication

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
- `enterprise_router/storage.py`: storage interface and backend factory
- `enterprise_router/sqlite_storage.py`: SQLite backend
- `enterprise_router/mongo_storage.py`: MongoDB Atlas backend
- `enterprise_router/api.py`: FastAPI service for remote agents
- `enterprise_router/cli.py`: local or remote CLI
- `tests/test_router.py`: SQLite/local unit tests
- `tests/test_online_router.py`: API key coverage and online-path test scaffolding

### Online Mode

Remote agents should communicate with the router over JSON/HTTP, not by writing directly to MongoDB.

Recommended flow:

1. Agent sends JSON request to FastAPI router.
2. Router authenticates the agent with `Authorization: Bearer <api_key>` and `X-Agent-Id`.
3. Router applies validation, priority, filtering, and queue rules.
4. Router stores shared state in MongoDB Atlas.

Core env vars:

- `ROUTER_BACKEND=sqlite|mongo`
- `SQLITE_DB_PATH`
- `MONGODB_URI`
- `MONGODB_DB_NAME`
- `ENTERPRISE_ROUTER_SHARED_SECRET`
- `ROUTER_ADMIN_SECRET`
- `ROUTER_API_HOST`
- `ROUTER_API_PORT`

Install optional dependencies when you want the online stack:

```powershell
pip install .[api,mongo]
```

Run the API locally:

```powershell
$env:ROUTER_BACKEND="mongo"
$env:MONGODB_URI="<your atlas uri>"
$env:MONGODB_DB_NAME="enterprise_router"
$env:ROUTER_ADMIN_SECRET="admin-secret"
python -m enterprise_router.api
```

### Atlas Setup

1. Create a MongoDB Atlas cluster.
2. Create a database user with read/write access to the app database.
3. Add your current IP to the Atlas network access list.
4. Copy the `mongodb+srv://...` connection string.
5. Set `MONGODB_URI` and `MONGODB_DB_NAME` locally before starting the API.
