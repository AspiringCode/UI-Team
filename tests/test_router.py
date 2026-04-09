from __future__ import annotations

import unittest
from pathlib import Path

from enterprise_router import (
    AccessError,
    AgentRecord,
    EnterpriseRouter,
    RegistrationError,
    RegistrationRequest,
    RoutingHints,
    ValidationError,
)
from enterprise_router.models import role_defaults


class EnterpriseRouterTests(unittest.TestCase):
    def setUp(self) -> None:
        temp_root = Path(__file__).resolve().parent / ".tmp"
        temp_root.mkdir(exist_ok=True)
        self.db_path = temp_root / f"{self._testMethodName}.db"
        if self.db_path.exists():
            self.db_path.unlink()
        self.router = EnterpriseRouter(
            db_path=str(self.db_path),
            shared_secret="team-secret",
            default_lease_seconds=1,
            max_attempts=3,
        )
        self.router.register_agent(
            AgentRecord(
                agent_name="CEO",
                role="CEO",
                hierarchy_level=1,
                trust_level=100,
                allowed_task_types=["ESCALATE", "BUDGET"],
            )
        )
        self.router.register_agent(
            AgentRecord(
                agent_name="PM1",
                role="PM",
                hierarchy_level=2,
                trust_level=80,
            )
        )

    def tearDown(self) -> None:
        if self.db_path.exists():
            self.db_path.unlink()

    def test_self_registration_requires_approval(self) -> None:
        req = RegistrationRequest(
            agent_name="worker-1",
            role="worker",
            secret_token="team-secret",
        )
        result = self.router.request_registration(req)
        self.assertEqual(result, "worker-1")
        with self.assertRaises(AccessError):
            message = self.router.create_message("worker-1", "CEO", "ESCALATE", {}, {})
            self.router.submit_message(message)

        self.router.approve_registration("worker-1", "CEO")
        worker = self.router.get_agent("worker-1")
        self.assertIsNotNone(worker)
        self.assertTrue(worker.active)
        self.assertEqual(worker.registration_status, "approved")

    def test_invalid_registration_token_is_rejected(self) -> None:
        with self.assertRaises(RegistrationError):
            self.router.request_registration(
                RegistrationRequest(
                    agent_name="worker-2",
                    role="worker",
                    secret_token="wrong-secret",
                )
            )

    def test_duplicate_agent_names_are_rejected(self) -> None:
        self.router.request_registration(
            RegistrationRequest(
                agent_name="worker-3",
                role="worker",
                secret_token="team-secret",
            )
        )
        with self.assertRaises(RegistrationError):
            self.router.request_registration(
                RegistrationRequest(
                    agent_name="worker-3",
                    role="worker",
                    secret_token="team-secret",
                )
            )

    def test_low_level_message_is_delayed_but_kept(self) -> None:
        self.router.approve_registration(
            self.router.request_registration(
                RegistrationRequest(
                    agent_name="worker-4",
                    role="worker",
                    secret_token="team-secret",
                )
            ),
            "CEO",
        )
        message_id = self.router.submit_message(
            self.router.create_message(
                "worker-4",
                "CEO",
                "ESCALATE",
                {"quarter": "Q2"},
                {"summary": "Need sign off"},
            ),
            RoutingHints(urgency="normal"),
        )
        queued = self.router.list_queue("CEO")
        self.assertEqual(queued[0].envelope.id, message_id)
        self.assertEqual(queued[0].delivery_state, "pending")

    def test_blocked_message_is_stored_with_reason(self) -> None:
        self.router.register_agent(
            AgentRecord(
                agent_name="Finance1",
                role="Finance",
                hierarchy_level=2,
                trust_level=85,
                allowed_senders=["CEO"],
            )
        )
        blocked_id = self.router.submit_message(
            self.router.create_message(
                "PM1",
                "Finance1",
                "BUDGET",
                {},
                {"amount": 1000},
            )
        )
        queue = self.router.list_queue("Finance1")
        blocked = next(item for item in queue if item.envelope.id == blocked_id)
        self.assertEqual(blocked.delivery_state, "blocked")
        self.assertIn("not allowed", blocked.blocked_reason)

    def test_fetch_ack_and_nack_cycle(self) -> None:
        self.router.submit_message(
            self.router.create_message(
                "PM1",
                "CEO",
                "ESCALATE",
                {},
                {"summary": "Ship feature"},
            ),
            RoutingHints(urgency="high"),
        )
        fetched = self.router.fetch_next("CEO")
        self.assertIsNotNone(fetched)
        self.assertEqual(fetched.envelope.status, "in_progress")
        self.router.nack_message(fetched.envelope.id, "CEO", "Need more detail")
        queue = self.router.list_queue("CEO")
        self.assertEqual(queue[0].attempt_count, 1)
        refetched = self.router.fetch_next("CEO")
        self.assertIsNotNone(refetched)
        self.router.ack_message(refetched.envelope.id, "CEO")
        final = self.router.list_queue("CEO")[0]
        self.assertEqual(final.delivery_state, "done")

    def test_dead_letter_after_max_attempts(self) -> None:
        self.router.submit_message(
            self.router.create_message(
                "PM1",
                "CEO",
                "ESCALATE",
                {},
                {"summary": "Needs retries"},
            )
        )
        for _ in range(3):
            fetched = self.router.fetch_next("CEO")
            self.assertIsNotNone(fetched)
            self.router.nack_message(fetched.envelope.id, "CEO", "Still broken")
        queue = self.router.list_queue("CEO")
        self.assertEqual(queue[0].delivery_state, "dead_lettered")
        self.assertEqual(queue[0].envelope.status, "error")

    def test_ttl_expiry_removes_message_from_active_delivery(self) -> None:
        message = self.router.create_message("PM1", "CEO", "ESCALATE", {}, {"summary": "Old"})
        message.timestamp = "2000-01-01T00:00:00Z"
        self.router.submit_message(message, RoutingHints(ttl_seconds=1))
        fetched = self.router.fetch_next("CEO")
        self.assertIsNone(fetched)
        queue = self.router.list_queue("CEO")
        self.assertEqual(queue[0].delivery_state, "expired")

    def test_dedupe_key_returns_existing_message(self) -> None:
        first = self.router.submit_message(
            self.router.create_message("PM1", "CEO", "ESCALATE", {}, {"summary": "One"}),
            RoutingHints(dedupe_key="dup-1"),
        )
        second = self.router.submit_message(
            self.router.create_message("PM1", "CEO", "ESCALATE", {}, {"summary": "Two"}),
            RoutingHints(dedupe_key="dup-1"),
        )
        self.assertEqual(first, second)

    def test_peek_does_not_mutate_message_status(self) -> None:
        message_id = self.router.submit_message(
            self.router.create_message("PM1", "CEO", "ESCALATE", {}, {"summary": "Observe"}),
        )
        peeked = self.router.peek_messages("CEO")
        self.assertEqual(peeked[0].envelope.id, message_id)
        queue = self.router.list_queue("CEO")
        self.assertEqual(queue[0].envelope.status, "pending")

    def test_unknown_sender_is_rejected(self) -> None:
        with self.assertRaises(AccessError):
            self.router.submit_message(
                self.router.create_message("Unknown", "CEO", "ESCALATE", {}, {})
            )

    def test_invalid_envelope_is_rejected(self) -> None:
        message = self.router.create_message("PM1", "CEO", "ESCALATE", {}, {})
        message.context = []  # type: ignore[assignment]
        with self.assertRaises(ValidationError):
            self.router.submit_message(message)

    def test_manager_role_defaults_are_available(self) -> None:
        defaults = role_defaults("manager")
        self.assertEqual(defaults["hierarchy_level"], 2)
        self.assertEqual(defaults["trust_level"], 95)
        self.assertEqual(defaults["recipient_weight"], 95)

    def test_manager_intervention_queues_a_standard_message(self) -> None:
        self.router.register_agent(
            AgentRecord(
                agent_name="MANAGER",
                role="MANAGER",
                hierarchy_level=2,
                trust_level=95,
            )
        )

        message_id = self.router.submit_manager_intervention(
            recipient="CEO",
            instruction="Prioritize enterprise accounts this week.",
            urgency="high",
            context={"channel": "dashboard"},
            payload={"reason": "manual override"},
            requires_response=True,
            ttl_seconds=600,
            dedupe_key="manager-priority-shift",
        )

        queue = self.router.list_queue("CEO")
        queued = next(item for item in queue if item.envelope.id == message_id)
        self.assertEqual(queued.envelope.sender, "MANAGER")
        self.assertEqual(queued.envelope.recipient, "CEO")
        self.assertEqual(queued.envelope.task_type, "MANAGER_INTERVENTION")
        self.assertEqual(queued.provenance_source, "manager_dashboard")
        self.assertEqual(queued.dedupe_key, "manager-priority-shift")
        self.assertEqual(
            queued.envelope.payload["instruction"],
            "Prioritize enterprise accounts this week.",
        )
        self.assertTrue(queued.envelope.payload["requires_response"])


if __name__ == "__main__":
    unittest.main()
