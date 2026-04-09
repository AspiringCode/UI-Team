from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path

from enterprise_router import AccessError, AgentRecord, EnterpriseRouter


class EnterpriseRouterApiKeyTests(unittest.TestCase):
    def setUp(self) -> None:
        temp_root = Path(__file__).resolve().parent / ".tmp"
        temp_root.mkdir(exist_ok=True)
        self.db_path = temp_root / f"{self._testMethodName}.db"
        if self.db_path.exists():
            self.db_path.unlink()
        self.router = EnterpriseRouter(db_path=str(self.db_path), shared_secret="team-secret")
        self.router.register_agent(
            AgentRecord(
                agent_name="CEO",
                role="CEO",
                hierarchy_level=1,
                trust_level=100,
            )
        )

    def tearDown(self) -> None:
        if self.db_path.exists():
            self.db_path.unlink()

    def test_issue_and_authenticate_api_key(self) -> None:
        raw_key = self.router.issue_api_key("CEO", label="primary")
        authenticated = self.router.authenticate_agent("CEO", raw_key)
        self.assertEqual(authenticated.agent_name, "CEO")

    def test_authenticate_rejects_bad_key(self) -> None:
        self.router.issue_api_key("CEO", label="primary")
        with self.assertRaises(AccessError):
            self.router.authenticate_agent("CEO", "bad-key")


@unittest.skipUnless(
    importlib.util.find_spec("pymongo") is not None,
    "pymongo not installed in this environment",
)
class MongoParityTests(unittest.TestCase):
    def test_placeholder(self) -> None:
        self.skipTest("Mongo parity tests require a configured Atlas/local Mongo instance.")


@unittest.skipUnless(
    importlib.util.find_spec("fastapi") is not None
    and importlib.util.find_spec("httpx") is not None,
    "fastapi/httpx not installed in this environment",
)
class ApiIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        from fastapi.testclient import TestClient

        from enterprise_router.api import create_app
        from enterprise_router.config import RouterSettings

        temp_root = Path(__file__).resolve().parent / ".tmp"
        temp_root.mkdir(exist_ok=True)
        self.db_path = temp_root / f"{self._testMethodName}.db"
        if self.db_path.exists():
            self.db_path.unlink()

        self.settings = RouterSettings(
            backend="sqlite",
            sqlite_db_path=str(self.db_path),
            shared_secret="team-secret",
            admin_secret="admin-secret",
        )
        self.client = TestClient(create_app(self.settings))
        self.admin_headers = {"X-Admin-Secret": "admin-secret"}

        for agent_name, role, level, trust in (
            ("CEO", "CEO", 1, 100),
            ("MANAGER", "MANAGER", 2, 95),
        ):
            response = self.client.post(
                "/agents",
                headers=self.admin_headers,
                json={
                    "agent_name": agent_name,
                    "role": role,
                    "hierarchy_level": level,
                    "trust_level": trust,
                },
            )
            self.assertEqual(response.status_code, 200)

        self.manager_api_key = self.client.post(
            "/agents/MANAGER/issue-api-key",
            headers=self.admin_headers,
        ).json()["api_key"]
        self.ceo_api_key = self.client.post(
            "/agents/CEO/issue-api-key",
            headers=self.admin_headers,
        ).json()["api_key"]

    def tearDown(self) -> None:
        if self.db_path.exists():
            self.db_path.unlink()

    def test_manager_intervention_route_accepts_manager_auth(self) -> None:
        response = self.client.post(
            "/manager/interventions",
            headers={
                "Authorization": f"Bearer {self.manager_api_key}",
                "X-Agent-Id": "MANAGER",
            },
            json={
                "recipient": "CEO",
                "instruction": "Shift focus to enterprise deals.",
                "priority": "high",
                "payload": {"reason": "operator override"},
                "requires_response": True,
            },
        )
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["sender"], "MANAGER")
        self.assertEqual(body["recipient"], "CEO")
        self.assertEqual(body["status"], "queued")

        queue_response = self.client.get(
            "/queue/CEO",
            headers={
                "Authorization": f"Bearer {self.ceo_api_key}",
                "X-Agent-Id": "CEO",
            },
        )
        self.assertEqual(queue_response.status_code, 200)
        queued = queue_response.json()[0]
        self.assertEqual(queued["envelope"]["sender"], "MANAGER")
        self.assertEqual(queued["envelope"]["task_type"], "MANAGER_INTERVENTION")

    def test_manager_intervention_route_rejects_non_manager_auth(self) -> None:
        response = self.client.post(
            "/manager/interventions",
            headers={
                "Authorization": f"Bearer {self.ceo_api_key}",
                "X-Agent-Id": "CEO",
            },
            json={
                "recipient": "CEO",
                "instruction": "This should be rejected.",
            },
        )
        self.assertEqual(response.status_code, 403)
