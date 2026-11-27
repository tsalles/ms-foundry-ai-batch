"""Functions entry points against a stand-in ``azure.functions`` (no host needed).

Checks the bindings the platform relies on (routes, auth levels, queue and timer names), that
every ``%SETTING%`` a binding references is set by Terraform, and that each entry point
forwards to the right runtime component.
"""

import importlib.util
import json
import logging
import os
import re
import sys
import types
import unittest
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from pathlib import Path
from typing import Any
from unittest import mock

import foundry_batch.composition
import foundry_batch.metrics.api_pressure
import foundry_batch.observability
import foundry_batch.webhook.http  # noqa: F401
from foundry_batch.adapters.memory import MemorySecrets
from foundry_batch.composition import Runtime, build_runtime, memory_adapters
from foundry_batch.domain.models import JobKey, JobRecord
from tests.stubs import azure_functions as func
from tests.support import (
    RELAY_SECRET,
    SECRET_A,
    FakeEndpoints,
    FakeProvider,
    FakeProviderFactory,
    batch_event,
    clock,
    make_settings,
    signed_request,
)

ROOT = Path(__file__).resolve().parents[2]


@contextmanager
def stub_modules(extra: dict[str, types.ModuleType] | None = None) -> Iterator[None]:
    azure = sys.modules.get("azure") or types.ModuleType("azure")
    if not hasattr(azure, "__path__"):
        azure.__path__ = []  # type: ignore[attr-defined]
    wanted: dict[str, types.ModuleType] = {"azure": azure, "azure.functions": func}  # type: ignore[dict-item]
    wanted.update(extra or {})
    saved = {name: sys.modules.get(name) for name in wanted}
    sys.modules.update(wanted)
    with ExitStack() as patches:
        for name, module in wanted.items():
            parent_name, _, attribute = name.rpartition(".")
            parent = sys.modules.get(parent_name)
            if parent is not None:
                patches.enter_context(mock.patch.object(parent, attribute, module, create=True))
        try:
            yield
        finally:
            for name, module in saved.items():
                if module is None:
                    sys.modules.pop(name, None)
                else:
                    sys.modules[name] = module


def load_function_app() -> types.ModuleType:
    env = {k: v for k, v in os.environ.items() if k != "APPLICATIONINSIGHTS_CONNECTION_STRING"}
    with stub_modules(), mock.patch.dict(os.environ, env, clear=True):
        spec = importlib.util.spec_from_file_location(
            "function_app_under_test", ROOT / "functions" / "function_app.py"
        )
        assert spec and spec.loader
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    return module


class FunctionAppWiringTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.app_module = load_function_app()

    def setUp(self) -> None:
        self.clock = clock()
        self.provider = FakeProvider(self.clock)
        self.settings = make_settings(BATCH_DOWNSTREAM_MODE="demo")
        adapters = memory_adapters(
            FakeProviderFactory({"eus2": self.provider, "swc": FakeProvider(self.clock)}),
            lambda _: FakeEndpoints(),
        )
        adapters.secrets = MemorySecrets(
            {"whsec-test-eus2-a": SECRET_A, "relay-signing-test": RELAY_SECRET}
        )
        self.runtime: Runtime = build_runtime(self.settings, adapters, self.clock)
        patcher = mock.patch.object(self.app_module, "get_runtime", lambda: self.runtime)
        patcher.start()
        self.addCleanup(patcher.stop)

    def functions(self) -> dict[str, Any]:
        return {f.get_function_name(): f for f in self.app_module.app.get_functions()}

    def test_bindings(self) -> None:
        f = self.functions()
        self.assertEqual(
            set(f),
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
        self.assertEqual(
            f["webhook"].options,
            {
                "route": "webhooks/{resource_key}",
                "methods": ["POST"],
                "auth_level": func.AuthLevel.ANONYMOUS,
            },
        )
        self.assertEqual(f["health"].options["auth_level"], func.AuthLevel.FUNCTION)
        self.assertEqual(f["health"].options["methods"], ["GET"])
        self.assertEqual(f["process_event"].options["queue_name"], "%BATCH_INCOMING_QUEUE%")
        self.assertEqual(f["relay_event"].options["queue_name"], "%BATCH_RELAY_QUEUE%")
        for name in ("process_event", "relay_event"):
            self.assertEqual(f[name].options["connection"], "BATCH_STORAGE")
        self.assertEqual(f["reconcile"].options["schedule"], "%BATCH_SWEEP_SCHEDULE%")
        self.assertEqual(f["outbox_and_queues"].options["schedule"], "%BATCH_OUTBOX_SCHEDULE%")
        self.assertFalse(any(fn.options.get("run_on_startup") for fn in f.values()))

    def test_every_binding_setting_is_provided_by_terraform(self) -> None:
        locals_tf = (ROOT / "infra" / "envs" / "poc" / "locals.tf").read_text(encoding="utf-8")
        modules = "\n".join(p.read_text(encoding="utf-8") for p in (ROOT / "infra").rglob("*.tf"))
        for fn in self.functions().values():
            for value in fn.options.values():
                for setting in re.findall(r"%([A-Z0-9_]+)%", str(value)):
                    self.assertRegex(locals_tf, rf"\b{setting}\s*=", setting)
            connection = fn.options.get("connection")
            if connection:
                self.assertIn(f"{connection}__queueServiceUri", locals_tf)
                self.assertIn(f"{connection}__credential", modules)

    def test_webhook_route_maps_request_and_response(self) -> None:
        signed = signed_request(batch_event("batch_1"))
        req = func.HttpRequest(
            "POST",
            "https://x/api/webhooks/eus2",
            headers={k.title(): v for k, v in signed.headers.items()},
            body=signed.body,
            route_params={"resource_key": "eus2"},
        )
        resp = self.functions()["webhook"].handler(req)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(json.loads(resp.get_body()), {"status": "accepted"})
        self.assertEqual(len(self.runtime.adapters.incoming_queue), 1)  # type: ignore[arg-type]
        rejected = self.functions()["webhook"].handler(
            func.HttpRequest(
                "POST", "u", headers={}, body=b"{}", route_params={"resource_key": "eus2"}
            )
        )
        self.assertEqual(rejected.status_code, 400)

    def test_queue_and_timer_entry_points(self) -> None:
        batch_id = self.provider.add("in_progress")
        self.runtime.registry.create_job(
            JobRecord("eus2", batch_id, "test", raw_status="in_progress")
        )
        self.functions()["webhook"].handler(
            func.HttpRequest(
                "POST",
                "u",
                headers=signed_request(batch_event(batch_id)).headers,
                body=signed_request(batch_event(batch_id)).body,
                route_params={"resource_key": "eus2"},
            )
        )
        message = self.runtime.adapters.incoming_queue.sent[-1]  # type: ignore[attr-defined]
        self.functions()["process_event"].handler(func.QueueMessage(body=message.encode()))
        stored = self.runtime.registry.load(JobKey("eus2", batch_id))
        assert stored is not None
        self.assertEqual(stored.record.outcome, "completed")
        nudge = self.runtime.adapters.relay_queue.sent[-1]  # type: ignore[attr-defined]
        with mock.patch.object(self.runtime.dispatcher, "dispatch_nudge") as dispatch:
            self.functions()["relay_event"].handler(func.QueueMessage(body=nudge.encode()))
        dispatch.assert_called_once_with(nudge)
        with mock.patch.object(self.runtime.reconciler, "run_all") as run_all:
            self.functions()["reconcile"].handler(func.TimerRequest())
        run_all.assert_called_once_with()

    def test_outbox_timer_logs_depths_and_survives_a_failed_read(self) -> None:
        admin = self.runtime.adapters.queue_admin
        real = admin.approximate_depth

        def depth(queue: str) -> int:
            if queue.endswith("-poison") and queue.startswith("relay"):
                raise RuntimeError("throttled")
            return real(queue)

        with (
            mock.patch.object(admin, "approximate_depth", depth),
            mock.patch.object(self.runtime.dispatcher, "dispatch_due") as due,
            self.assertLogs("foundry_batch", logging.INFO) as logs,
        ):
            self.functions()["outbox_and_queues"].handler(func.TimerRequest())
        due.assert_called_once_with()
        text = "\n".join(logs.output)
        self.assertEqual(text.count("ops.queue_depth "), 3)
        self.assertIn("ops.queue_depth_failed", text)
        self.assertIn("queue=relay-dispatch-poison", text)

    def test_demo_route_only_in_demo_mode(self) -> None:
        demo = self.functions()["demo_events"].handler
        resp = demo(func.HttpRequest("POST", "u", headers={}, body=b"{}"))
        self.assertEqual(resp.status_code, 401)  # handled by the demo API: unsigned
        self.runtime.settings = make_settings(
            BATCH_DOWNSTREAM_MODE="http", BATCH_DOWNSTREAM_URL="https://batch.example/events"
        )
        self.assertEqual(demo(func.HttpRequest("POST", "u", body=b"{}")).status_code, 404)

    def test_health_reports_booleans_only(self) -> None:
        resp = self.functions()["health"].handler(func.HttpRequest("GET", "u"))
        body = json.loads(resp.get_body())
        self.assertEqual(resp.status_code, 503)  # swc has no signing secret
        self.assertEqual(
            body, {"signing_secret:eus2": True, "signing_secret:swc": False, "relay_secret": True}
        )
        self.runtime.adapters.secrets.fail_reads = True  # type: ignore[attr-defined]
        self.clock.advance(3600)
        resp = self.functions()["health"].handler(func.HttpRequest("GET", "u"))
        self.assertEqual(resp.status_code, 503)  # an outage reads as not ready, never a 500

    def test_health_needs_a_signing_secret_that_decodes(self) -> None:
        secrets = self.runtime.adapters.secrets
        secrets.set_secret("whsec-test-eus2-a", "whsec_not*base64", tags={})
        secrets.set_secret("whsec-test-swc-a", SECRET_A, tags={})
        self.clock.advance(3600)  # past the resolver cache
        resp = self.functions()["health"].handler(func.HttpRequest("GET", "u"))
        self.assertEqual(resp.status_code, 503)
        body = json.loads(resp.get_body())
        self.assertEqual(
            body, {"signing_secret:eus2": False, "signing_secret:swc": True, "relay_secret": True}
        )
        # The receiver answers 503 for the same resource: readiness tells the truth.
        now = int(self.clock.now().timestamp())
        signed = signed_request(batch_event("batch_1"), timestamp=now)
        route = {"resource_key": "eus2"}
        request = func.HttpRequest(
            "POST", "u", headers=signed.headers, body=signed.body, route_params=route
        )
        delivery = self.functions()["webhook"].handler(request)
        self.assertEqual(delivery.status_code, 503)

    def test_one_usable_slot_is_enough_and_the_relay_secret_must_decode(self) -> None:
        secrets = self.runtime.adapters.secrets
        secrets.set_secret("whsec-test-eus2-b", SECRET_A, tags={})
        secrets.set_secret("whsec-test-eus2-a", "whsec_not*base64", tags={})
        secrets.set_secret("whsec-test-swc-a", SECRET_A, tags={})
        secrets.set_secret("relay-signing-test", "whsec_***", tags={})
        self.clock.advance(3600)
        status = self.runtime.readiness()
        self.assertEqual(
            status, {"signing_secret:eus2": True, "signing_secret:swc": True, "relay_secret": False}
        )

    def test_telemetry_uses_managed_identity_when_configured(self) -> None:
        calls: dict[str, Any] = {}
        identity = types.ModuleType("azure.identity")
        monitor = types.ModuleType("azure.monitor")
        monitor.__path__ = []  # type: ignore[attr-defined]
        otel = types.ModuleType("azure.monitor.opentelemetry")

        class ManagedIdentityCredential:
            def __init__(self, client_id: str | None = None) -> None:
                calls["client_id"] = client_id

        def configure_azure_monitor(**kwargs: Any) -> None:
            calls.update(kwargs)

        identity.ManagedIdentityCredential = ManagedIdentityCredential  # type: ignore[attr-defined]
        otel.configure_azure_monitor = configure_azure_monitor  # type: ignore[attr-defined]
        env = {
            "APPLICATIONINSIGHTS_CONNECTION_STRING": "InstrumentationKey=x",
            "AZURE_CLIENT_ID": "uami-client",
        }
        with (
            stub_modules(
                {
                    "azure.identity": identity,
                    "azure.monitor": monitor,
                    "azure.monitor.opentelemetry": otel,
                }
            ),
            mock.patch.dict(os.environ, env),
        ):
            self.app_module._configure_telemetry()
        self.assertEqual(calls["client_id"], "uami-client")
        self.assertEqual(calls["logger_name"], "foundry_batch")
        self.assertIsInstance(calls["credential"], ManagedIdentityCredential)


if __name__ == "__main__":
    unittest.main()
