import json
import threading
import unittest
from datetime import timedelta

from foundry_batch.adapters.memory import MemoryQueue, MemoryTableStore
from foundry_batch.domain.models import JobKey, JobRecord
from foundry_batch.errors import (
    ConcurrencyConflictError,
    ProviderRetryableError,
    QueueUnavailableError,
    RetryLater,
)
from foundry_batch.ports import Query
from foundry_batch.processing.decision import Observation, Source
from foundry_batch.processing.processor import (
    MAX_DEFERRALS,
    EventProcessor,
    observation_from_snapshot,
    relay_nudge,
)
from foundry_batch.processing.registry import JobRegistry, Receipt
from foundry_batch.webhook.envelope import Envelope
from tests.support import (
    T0,
    FakeProvider,
    FakeProviderFactory,
    batch_event,
    clock,
    make_settings,
)


class FailingQueue:
    def send(self, message: str, *, visibility_timeout: int | None = None) -> None:
        raise QueueUnavailableError("down")


def envelope(
    batch_id: str,
    event_type: str = "batch.completed",
    event_id: str = "evt_1",
    webhook_id: str = "msg_1",
    resource_key: str = "eus2",
    environment: str = "test",
    attempt: int = 0,
    enrichment_failures: int = 0,
) -> Envelope:
    event = batch_event(batch_id, event_type, event_id)
    return Envelope(
        environment,
        resource_key,
        webhook_id,
        int(T0.timestamp()),
        T0,
        event_id,
        event_type,
        event["created_at"],
        batch_id,
        event,
        attempt=attempt,
        enrichment_failures=enrichment_failures,
    )


class ProcessorTests(unittest.TestCase):
    def setUp(self, **settings: str) -> None:
        self.settings = make_settings(**settings)
        self.clock = clock()
        self.store = MemoryTableStore()
        self.registry = JobRegistry(self.store, self.settings, self.clock)
        self.provider = FakeProvider(self.clock)
        self.relay = MemoryQueue("relay")
        self.incoming = MemoryQueue("incoming")
        self.processor = EventProcessor(
            self.settings,
            self.registry,
            self.store,
            FakeProviderFactory({"eus2": self.provider, "swc": FakeProvider(self.clock)}),
            self.relay,
            self.clock,
            incoming_queue=self.incoming,
        )

    def track(self, status: str = "in_progress", **job: object) -> str:
        batch_id = self.provider.add(status)
        record = JobRecord(
            "eus2",
            batch_id,
            "test",
            raw_status=status,
            deployment="gpt-batch",
            region="eastus2",
            webhook_covered=True,
            **job,
        )  # type: ignore[arg-type]
        self.registry.create_job(record)
        return batch_id

    def outbox(self) -> list[dict]:
        return [e.data for e in self.store.query("batchjobs", Query(row_key_prefix="outbox:"))]

    def test_tc_b01_terminal_event_commits_state_receipt_outbox_and_nudges(self) -> None:
        batch_id = self.track()
        self.clock.advance(120)
        self.provider.set_status(batch_id, "completed")
        with self.assertLogs("foundry_batch", "INFO") as logs:
            result = self.processor.process_envelope(envelope(batch_id))
        self.assertEqual(result.kind, "terminal")
        stored = self.registry.load(JobKey("eus2", batch_id))
        assert stored is not None
        self.assertEqual(
            (stored.record.raw_status, stored.record.outcome), ("completed", "completed")
        )
        self.assertTrue(stored.record.details_complete)
        self.assertEqual(stored.record.request_completed, 3)
        self.assertEqual(self.provider.calls["retrieve"], 1)
        [item] = self.outbox()
        event = json.loads(item["event_json"])
        self.assertEqual((event["source"], event["event_id"]), ("webhook", "evt_1"))
        nudge = json.loads(self.relay.sent[0])
        self.assertEqual(nudge["key"], item["idempotency_key"])
        self.assertTrue(any("job.lifecycle" in line for line in logs.output))

    def test_tc_d01_sequential_duplicates_have_one_effect(self) -> None:
        batch_id = self.track()
        self.provider.set_status(batch_id, "completed")
        self.clock.advance(61)
        self.assertEqual(self.processor.process_envelope(envelope(batch_id)).kind, "terminal")
        self.assertEqual(self.processor.process_envelope(envelope(batch_id)).kind, "duplicate")
        other_id = envelope(batch_id, event_id="evt_2", webhook_id="msg_2")
        self.assertEqual(self.processor.process_envelope(other_id).kind, "duplicate")
        self.assertEqual(len(self.outbox()), 1)
        self.assertEqual(self.provider.calls["retrieve"], 1)

    def test_tc_d01_concurrent_duplicates_have_one_effect(self) -> None:
        batch_id = self.track(details_complete=True)
        results: list[str] = []
        barrier = threading.Barrier(8)

        def worker(i: int) -> None:
            barrier.wait()
            results.append(
                self.processor.process_envelope(
                    envelope(batch_id, event_id=f"evt_{i % 2}", webhook_id=f"m{i}")
                ).kind
            )

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(results.count("terminal"), 1)
        self.assertEqual(len(self.outbox()), 1)

    def test_tc_c09_unknown_job_and_bad_messages_are_quarantined(self) -> None:
        self.clock.advance(self.settings.unknown_job_grace_seconds)  # unknown jobs: after grace
        cases = [
            envelope("batch_unknown"),
            envelope("b", resource_key="zzz"),
            envelope("b", environment="prod"),
            envelope("b", event_type="batch.paused"),
        ]
        for env in cases:
            with self.subTest(env.event_type + env.resource_key + env.environment):
                self.assertEqual(self.processor.process_envelope(env).kind, "quarantined")
        self.assertEqual(self.processor.process_message("{nope").kind, "quarantined")
        self.assertEqual(
            self.processor.handle_queue_message('{"schema": "v0"}').kind, "quarantined"
        )
        reasons = sorted(e.data["reason"] for e in self.store.rows("batchquarantine"))
        self.assertEqual(
            reasons,
            [
                "not_json",
                "unknown_job",
                "unknown_resource",
                "unknown_schema",
                "unsupported_event",
                "wrong_environment",
            ],
        )
        self.assertEqual(self.outbox(), [])
        self.assertEqual(self.registry.open_jobs(), [])

    def test_adopt_policy_registers_untrusted_job_in_owned_resource(self) -> None:
        self.setUp(BATCH_UNKNOWN_JOB_POLICY="adopt")
        batch_id = self.provider.add("completed")
        self.clock.advance(61)
        with self.assertLogs("foundry_batch", "WARNING"):
            result = self.processor.process_envelope(envelope(batch_id))
        self.assertEqual(result.kind, "terminal")
        stored = self.registry.load(JobKey("eus2", batch_id))
        assert stored is not None
        self.assertFalse(stored.record.trusted)
        self.assertEqual(
            self.processor.process_envelope(envelope("batch_missing")).kind, "quarantined"
        )

    def test_enrichment_respects_the_60s_floor_by_deferring(self) -> None:
        batch_id = self.track(last_status_call_at=T0)
        self.provider.set_status(batch_id, "completed")
        self.clock.advance(20)
        with self.assertRaises(RetryLater) as ctx:
            self.processor.process_envelope(envelope(batch_id))
        self.assertAlmostEqual(ctx.exception.delay_seconds, 40)
        result = self.processor.handle_queue_message(envelope(batch_id).to_json())
        self.assertEqual(result.kind, "deferred")
        self.assertEqual(Envelope.from_json(self.incoming.sent[0]).attempt, 1)
        self.assertEqual(self.provider.calls["retrieve"], 0)

    def test_enrichment_failures_are_bounded_and_state_still_committed(self) -> None:
        batch_id = self.track()
        self.provider.fail(
            "retrieve",
            ProviderRetryableError("x", status_code=503),
            ProviderRetryableError("x", status_code=429, retry_after_seconds=90),
        )
        with self.assertRaises(RetryLater) as ctx:
            self.processor.process_envelope(envelope(batch_id))
        self.assertEqual(ctx.exception.delay_seconds, 60)
        self.clock.advance(60)
        with self.assertRaises(RetryLater) as ctx:
            self.processor.process_envelope(envelope(batch_id, enrichment_failures=1))
        self.assertEqual(ctx.exception.delay_seconds, 90)
        self.clock.advance(90)
        self.provider.fail("retrieve", ProviderRetryableError("x", status_code=503))
        result = self.processor.process_envelope(envelope(batch_id, enrichment_failures=2))
        self.assertEqual(result.kind, "terminal")
        stored = self.registry.load(JobKey("eus2", batch_id))
        assert stored is not None
        self.assertFalse(stored.record.details_complete)  # the reconciler completes details

    def test_retrieve_not_found_still_records_signed_state(self) -> None:
        self.registry.create_job(JobRecord("eus2", "batch_gone", "test", raw_status="in_progress"))
        self.assertEqual(self.processor.process_envelope(envelope("batch_gone")).kind, "terminal")

    def test_deferral_limit_propagates_to_poison_handling(self) -> None:
        batch_id = self.track(last_status_call_at=T0)
        with self.assertRaises(RetryLater):
            self.processor.handle_queue_message(envelope(batch_id, attempt=MAX_DEFERRALS).to_json())

    def test_event_and_retrieve_disagree_retrieve_wins(self) -> None:
        batch_id = self.track()
        self.provider.set_status(batch_id, "cancelled")
        with self.assertLogs("foundry_batch", "WARNING") as logs:
            self.processor.process_envelope(envelope(batch_id, "batch.completed"))
        stored = self.registry.load(JobKey("eus2", batch_id))
        assert stored is not None
        self.assertEqual(stored.record.raw_status, "cancelled")
        self.assertTrue(any("event_status_mismatch" in line for line in logs.output))

    def test_tc_d07_conflict_resolved_by_authoritative_retrieve(self) -> None:
        batch_id = self.track(details_complete=True)
        self.processor.process_envelope(envelope(batch_id, "batch.completed"))
        self.provider.set_status(batch_id, "completed")
        # A conflicting event: retrieve says completed, so the stored state is kept.
        result = self.processor.process_envelope(envelope(batch_id, "batch.failed", "evt_9", "m9"))
        self.assertEqual(result.kind, "duplicate")
        stored = self.registry.load(JobKey("eus2", batch_id))
        assert stored is not None
        self.assertEqual(stored.record.raw_status, "completed")
        self.assertEqual(stored.record.conflict_count, 1)
        # Provider really changed: a correction with its own key is emitted.
        self.clock.advance(60)
        self.provider.set_status(batch_id, "failed")
        result = self.processor.process_envelope(
            envelope(batch_id, "batch.failed", "evt_10", "m10")
        )
        self.assertEqual(result.kind, "correction")
        corrections = [json.loads(o["event_json"]) for o in self.outbox() if o.get("correction_of")]
        self.assertEqual(len(corrections), 1)
        self.assertTrue(corrections[0]["is_correction"])

    def test_conflict_without_provider_access_is_deferred(self) -> None:
        batch_id = self.track(details_complete=True)
        self.processor.process_envelope(envelope(batch_id, "batch.completed"))
        self.provider.fail("retrieve", ProviderRetryableError("x", status_code=503))
        with self.assertRaises(RetryLater):
            self.processor.process_envelope(envelope(batch_id, "batch.failed", "evt_9", "m9"))

    def test_nudge_failure_leaves_outbox_for_the_timer(self) -> None:
        processor = EventProcessor(
            self.settings,
            self.registry,
            self.store,
            FakeProviderFactory({"eus2": self.provider}),
            FailingQueue(),
            self.clock,
        )
        batch_id = self.track(details_complete=True)
        with self.assertLogs("foundry_batch", "WARNING") as logs:
            processor.process_envelope(envelope(batch_id))
        self.assertTrue(any("nudge_failed" in line for line in logs.output))
        self.assertEqual(len(self.registry.due_outbox(self.clock.now())), 1)

    def test_polling_and_webhook_share_one_key(self) -> None:
        batch_id = self.track(details_complete=True)
        self.provider.set_status(batch_id, "completed")
        snap = self.provider.retrieve_batch(batch_id)
        obs = observation_from_snapshot(snap, self.clock.now(), status_call_made=True)
        key = JobKey("eus2", batch_id)
        self.assertEqual(
            self.processor.apply(key, obs, Receipt(obs.receipt_id or "", "polling", T0)).kind,
            "terminal",
        )
        self.assertEqual(self.processor.process_envelope(envelope(batch_id)).kind, "duplicate")
        self.assertEqual(len(self.outbox()), 1)
        stored = self.registry.load(key)
        assert stored is not None
        self.assertTrue(stored.record.late_event)

    def test_apply_unknown_job_and_unchanged(self) -> None:
        obs = Observation(Source.POLLING, "in_progress", T0)
        self.assertEqual(
            self.processor.apply(JobKey("eus2", "nope"), obs, None).kind, "unknown_job"
        )
        batch_id = self.track()
        self.assertEqual(
            self.processor.apply(JobKey("eus2", batch_id), obs, None).kind, "unchanged"
        )

    def test_concurrency_exhaustion_is_retryable(self) -> None:
        batch_id = self.track(details_complete=True)

        class AlwaysConflict(JobRegistry):
            def commit(self, *args: object, **kwargs: object) -> str:  # type: ignore[override]
                raise ConcurrencyConflictError("busy")

        registry = AlwaysConflict(self.store, self.settings, self.clock)
        processor = EventProcessor(
            self.settings,
            registry,
            self.store,
            FakeProviderFactory({"eus2": self.provider}),
            self.relay,
            self.clock,
        )
        with self.assertRaises(RetryLater):
            processor.process_envelope(envelope(batch_id))

    def test_relay_nudge_format(self) -> None:
        self.assertEqual(json.loads(relay_nudge("p", "k")), {"pk": "p", "key": "k"})

    def test_deferral_without_incoming_queue_raises(self) -> None:
        processor = EventProcessor(
            self.settings,
            self.registry,
            self.store,
            FakeProviderFactory({"eus2": self.provider}),
            self.relay,
            self.clock,
        )
        batch_id = self.track(last_status_call_at=self.clock.now())
        self.clock.advance(timedelta(seconds=1).total_seconds())
        with self.assertRaises(RetryLater):
            processor.handle_queue_message(envelope(batch_id).to_json())


if __name__ == "__main__":
    unittest.main()
