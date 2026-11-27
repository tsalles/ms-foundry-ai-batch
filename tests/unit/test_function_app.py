"""Smoke test of the Functions entry points (needs azure-functions; runs in CI)."""

import importlib.util
import sys
import unittest
from pathlib import Path

HAS_FUNCTIONS = (
    importlib.util.find_spec("azure") is not None
    and importlib.util.find_spec("azure.functions") is not None
)


@unittest.skipUnless(HAS_FUNCTIONS, "azure-functions not installed")
class FunctionAppTests(unittest.TestCase):  # pragma: no cover - exercised in CI
    def test_routes_and_triggers_are_registered(self) -> None:
        sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "functions"))
        import function_app

        names = {f.get_function_name() for f in function_app.app.get_functions()}
        self.assertEqual(
            names,
            {
                "webhook",
                "process_event",
                "relay_event",
                "reconcile",
                "outbox_and_queues",
                "demo_events",
                "health",
            },
        )
