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
    def test_placeholder(self) -> None:
        self.skipTest("API integration tests require FastAPI test dependencies.")
