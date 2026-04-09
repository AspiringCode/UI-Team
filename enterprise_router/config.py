from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(slots=True)
class RouterSettings:
    backend: str = "sqlite"
    sqlite_db_path: str = "enterprise_router.db"
    mongo_uri: str | None = None
    mongo_db_name: str | None = None
    shared_secret: str = "dev-shared-secret"
    admin_secret: str = "dev-admin-secret"
    api_host: str = "127.0.0.1"
    api_port: int = 8000

    @classmethod
    def from_env(cls) -> "RouterSettings":
        return cls(
            backend=os.getenv("ROUTER_BACKEND", "sqlite"),
            sqlite_db_path=os.getenv("SQLITE_DB_PATH", "enterprise_router.db"),
            mongo_uri=os.getenv("MONGODB_URI"),
            mongo_db_name=os.getenv("MONGODB_DB_NAME"),
            shared_secret=os.getenv("ENTERPRISE_ROUTER_SHARED_SECRET", "dev-shared-secret"),
            admin_secret=os.getenv("ROUTER_ADMIN_SECRET", "dev-admin-secret"),
            api_host=os.getenv("ROUTER_API_HOST", "127.0.0.1"),
            api_port=int(os.getenv("ROUTER_API_PORT", "8000")),
        )
