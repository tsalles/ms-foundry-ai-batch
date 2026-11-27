"""Whole event path on the in-memory stack: provider -> receiver -> queue -> processor ->
registry/outbox -> relay -> demo API, plus reconciliation when webhooks are lost."""

import json
import unittest

from foundry_batch.adapters.memory import MemoryQueue, MemoryTableStore
from foundry_batch.composition import build_runtime, memory_adapters
from foundry_batch.domain.models import JobKey
from foundry_batch.ports import DeliveryResult
from foundry_batch.registration.registrar import WebhookRegistrar
from foundry_batch.relay.http_downstream import HttpDownstream
from foundry_batch.webhook.http import HttpRequest
from foundry_batch.webhook.signature import sign_headers
from tests.support import (
    RELAY_SECRET,
    FakeEndpoints,
    FakeProvider,
    FakeProviderFactory,
    batch_event,
    clock,
    make_settings,
    signed_request,
)


class DemoApiDownstream:
    """Downstream that calls the in-process demo API exactly as HTTP would (signed)."""

    def __init__(self, runtime_getter, clock) -> None:  # type: ignore[no-untyped-def]
        self._get = runtime_getter
        self._clock = clock
        self.down = False
        self.attempts = 0

    def deliver(self, body: bytes, *, idempotency_key: str) -> DeliveryResult:
        self.attempts += 1
        if self.down:
            return DeliveryResult(None, error="timeout")
        headers = sign_headers(
            RELAY_SECRET, idempotency_key, int(self._clock.now().timestamp()), body
        )
        response = self._get().demo_api.handle(HttpRequest("POST", body, headers))
        return DeliveryResult(response.status)


class EndToEndTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = clock()
        self.settings = make_settings()
        self.provider = FakeProvider(self.clock)
        self.endpoints = FakeEndpoints()
        self.downstream = DemoApiDownstream(lambda: self.runtime, self.clock)
        adapters = memory_adapters(
            FakeProviderFactory({"eus2": self.provider, "swc": FakeProvider(self.clock, "swc")}),
            lambda _: self.endpoints,
            secrets={"relay-signing-test": RELAY_SECRET},
            downstream=self.downstream,
        )
        self.runtime = build_runtime(self.settings, adapters, self.clock)
        self.incoming: MemoryQueue = adapters.incoming_queue  # type: ignore[assignment]
        self.relay: MemoryQueue = adapters.relay_queue  # type: ignore[assignment]
        self.store: MemoryTableStore = adapters.store  # type: ignore[assignment]
        registrar = WebhookRegistrar(
            self.settings,
            adapters.endpoints_for,
            adapters.secrets,
            self.runtime.control,
            self.clock,
        )
        registrar.ensure("eus2", "https://func.example.net/api/webhooks/eus2")
        endpoint_id = next(iter(self.endpoints.endpoints))
        self.signing_secret = self.endpoints.created_secrets[endpoint_id]

    def submit(self) -> str:
        from foundry_batch.foundry.submission import SubmissionService
        from foundry_batch.metrics.queued_tokens import CharacterHeuristicEstimator

        line = json.dumps(
            {
                "custom_id": "1",
                "method": "POST",
                "url": "/v1/chat/completions",
                "body": {"model": "gpt-batch", "messages": [{"role": "user", "content": "hi"}]},
            }
        )
        service = SubmissionService(
            self.settings,
            self.store,
            self.runtime.registry,
            self.runtime.control,
            self.runtime.adapters.batch_apis,
            CharacterHeuristicEstimator(),
            self.clock,
        )
        return service.submit("eus2", "gpt-batch", line.encode()).batch_id

    def deliver_webhook(self, batch_id: str, event_id: str = "evt_1") -> int:
        event = batch_event(
            batch_id, "batch.completed", event_id, created_at=int(self.clock.now().timestamp())
        )
        request = signed_request(
            event,
            secret=self.signing_secret,
            webhook_id=f"m-{event_id}",
            timestamp=int(self.clock.now().timestamp()),
        )
        return self.runtime.receiver.handle(request).status

    def pump(self) -> None:
        for message in self.incoming.drain():
            self.runtime.processor.handle_queue_message(message)
        for message in self.relay.drain():
            self.runtime.dispatcher.dispatch_nudge(message)

    def demo_rows(self) -> list[dict]:
        return [dict(e.data) for e in self.store.rows("demoreceipts")]

    def test_m1_happy_path_one_business_effect(self) -> None:
        batch_id = self.submit()
        self.clock.advance(600)
        self.provider.set_status(batch_id, "completed")
        self.assertEqual(self.deliver_webhook(batch_id), 200)
        self.assertEqual(self.deliver_webhook(batch_id), 200)  # provider retry
        self.pump()
        [row] = self.demo_rows()
        self.assertEqual((row["outcome"], row["source"]), ("completed", "webhook"))
        job = self.runtime.registry.load(JobKey("eus2", batch_id))
        assert job is not None
        self.assertTrue(job.record.webhook_covered)
        self.assertEqual(job.record.terminal_source, "webhook")
        self.assertEqual(self.provider.calls["retrieve"], 1)

    def test_tc_d03_receiver_off_reconciler_delivers_final_state(self) -> None:
        batch_id = self.submit()
        self.clock.advance(600)
        self.provider.set_status(batch_id, "failed")
        stats = self.runtime.reconciler.run("eus2")
        self.assertEqual(stats.terminal_found, 1)
        self.pump()
        [row] = self.demo_rows()
        self.assertEqual(
            (row["outcome"], row["raw_status"], row["source"]), ("failed", "failed", "polling")
        )
        # The webhook arrives late: no second business effect.
        self.deliver_webhook(batch_id)
        self.pump()
        self.assertEqual(len(self.demo_rows()), 1)

    def test_tc_d05_downstream_outage_then_recovery_without_loss(self) -> None:
        batch_id = self.submit()
        self.clock.advance(600)
        self.provider.set_status(batch_id, "completed")
        self.downstream.down = True
        self.deliver_webhook(batch_id)
        self.pump()
        self.assertEqual(self.demo_rows(), [])
        self.downstream.down = False
        self.clock.advance(3600)
        results = self.runtime.dispatcher.dispatch_due()
        self.assertEqual([r.status for r in results], ["delivered"])
        self.assertEqual(len(self.demo_rows()), 1)

    def test_tc_d10_downstream_commit_with_lost_response_is_deduplicated(self) -> None:
        batch_id = self.submit()
        self.clock.advance(600)
        self.provider.set_status(batch_id, "completed")

        original = self.downstream.deliver

        def commit_then_lose(body: bytes, *, idempotency_key: str) -> DeliveryResult:
            original(body, idempotency_key=idempotency_key)
            return DeliveryResult(None, error="timeout")

        self.downstream.deliver = commit_then_lose  # type: ignore[method-assign]
        self.deliver_webhook(batch_id)
        self.pump()
        self.downstream.deliver = original  # type: ignore[method-assign]
        self.clock.advance(3600)
        self.runtime.dispatcher.dispatch_due()
        self.assertEqual(len(self.demo_rows()), 1)

    def test_readiness_reports_booleans_only(self) -> None:
        status = self.runtime.readiness()
        self.assertEqual(
            status, {"signing_secret:eus2": True, "signing_secret:swc": False, "relay_secret": True}
        )
        self.assertEqual(self.runtime.relay_secret(), RELAY_SECRET)

    def test_http_downstream_rejects_plain_http(self) -> None:
        with self.assertRaises(ValueError):
            HttpDownstream("http://example.com", lambda: RELAY_SECRET, self.clock)


if __name__ == "__main__":
    unittest.main()
