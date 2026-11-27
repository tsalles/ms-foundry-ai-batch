"""Processor and decision edge cases found in the code review (failure paths, ordering, keys)."""

import json
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

from foundry_batch.adapters.memory import MemoryQueue, MemoryTableStore
from foundry_batch.domain.models import BatchSnapshot, JobKey, JobRecord
from foundry_batch.errors import (
    ProviderNotFoundError,
    ProviderRequestError,
    ProviderRetryableError,
    RetryLater,
)
from foundry_batch.ports import Query
from foundry_batch.processing.decision import Observation, Source, decide
from foundry_batch.processing.processor import (
    MAX_DEFERRAL_SECONDS,
    MAX_QUARANTINE_TEXT,
    EventProcessor,
    observation_from_snapshot,
)
from foundry_batch.processing.registry import JobRegistry
from foundry_batch.webhook.envelope import Envelope
from tests.support import T0, FakeProvider, FakeProviderFactory, batch_event, clock, make_settings


def envelope(
    batch_id: str,
    event_type: str = "batch.completed",
    *,
    attempt: int = 0,
    enrichment_failures: int = 0,
    event_id: str = "evt_1",
) -> Envelope:
    event = batch_event(batch_id, event_type, event_id)
    return Envelope(
        "test",
        "eus2",
        f"msg_{event_id}",
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


class Harness(unittest.TestCase):
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
        self.registry.create_job(
            JobRecord(
                "eus2",
                batch_id,
                "test",
                raw_status=status,
                deployment="gpt-batch",
                webhook_covered=True,
                **job,
            )
        )  # type: ignore[arg-type]
        return batch_id

    def job(self, batch_id: str) -> JobRecord:
        stored = self.registry.load(JobKey("eus2", batch_id))
        assert stored is not None
        return stored.record

    def outbox(self) -> list[dict[str, object]]:
        return [e.data for e in self.store.query("batchjobs", Query(row_key_prefix="outbox:"))]

    def quarantine(self) -> list[dict[str, object]]:
        return [e.data for e in self.store.query("batchquarantine", Query())]

    def poll(self, batch_id: str) -> str:
        snapshot = self.provider.retrieve_batch(batch_id)
        obs = observation_from_snapshot(snapshot, self.clock.now(), status_call_made=True)
        return self.processor.apply(JobKey("eus2", batch_id), obs, None).kind


class EnrichmentTests(Harness):
    def test_lifecycle_details_refresh_without_another_business_event(self) -> None:
        batch_id = self.track()
        self.provider.batches[batch_id]["created_at"] = int(T0.timestamp()) - 600
        self.provider.set_status(batch_id, "completed")
        self.provider.fail("retrieve", ProviderRequestError("forbidden", status_code=403))
        with self.assertLogs("foundry_batch", "INFO") as logs:
            self.processor.process_envelope(envelope(batch_id))
            self.clock.advance(60)
            self.poll(batch_id)
        lifecycle = [record for record in logs.records if record.batch_event == "job.lifecycle"]
        self.assertEqual(len(lifecycle), 2)
        self.assertIsNone(lifecycle[0].turnaround_s)
        self.assertEqual(lifecycle[1].turnaround_s, 600)
        self.assertEqual(len(self.outbox()), 1)

    def test_inflight_enrichment_reserves_the_floor_before_provider_io(self) -> None:
        batch_id = self.track()
        self.provider.set_status(batch_id, "completed")
        entered, release = threading.Event(), threading.Event()
        original = self.provider.retrieve_batch

        def blocked_retrieve(batch_id: str) -> BatchSnapshot:
            entered.set()
            if not release.wait(5):
                raise TimeoutError("test retrieval was not released")
            return original(batch_id)

        self.provider.retrieve_batch = blocked_retrieve
        with ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(self.processor.process_envelope, envelope(batch_id))
            try:
                self.assertTrue(entered.wait(5))
                with self.assertRaises(RetryLater) as ctx:
                    self.processor.process_envelope(envelope(batch_id, event_id="evt_2"))
                self.assertEqual(ctx.exception.reason, "status_call_floor")
            finally:
                release.set()
            self.assertEqual(pending.result(timeout=5).kind, "terminal")
        self.assertEqual(self.provider.calls["retrieve"], 1)

    def test_failed_enrichment_also_reserves_the_floor(self) -> None:
        batch_id = self.track()
        self.provider.fail("retrieve", ProviderRetryableError("busy", status_code=503))
        with self.assertRaises(RetryLater):
            self.processor.process_envelope(envelope(batch_id))
        with self.assertRaises(RetryLater) as ctx:
            self.processor.process_envelope(envelope(batch_id, event_id="evt_2"))
        self.assertEqual(ctx.exception.reason, "status_call_floor")
        self.assertEqual(self.provider.calls["retrieve"], 1)

    def test_non_retryable_provider_error_still_commits_the_signed_state(self) -> None:
        batch_id = self.track()
        self.provider.fail("retrieve", ProviderRequestError("forbidden", status_code=403))
        with self.assertLogs("foundry_batch", "WARNING") as logs:
            result = self.processor.process_envelope(envelope(batch_id))
        self.assertEqual(result.kind, "terminal")
        self.assertFalse(self.job(batch_id).details_complete)
        self.assertTrue(any("reason=non_retryable" in line for line in logs.output))
        self.assertEqual(self.job(batch_id).last_status_call_at, T0)  # the call is counted

    def test_retrieve_is_skipped_when_details_are_complete(self) -> None:
        batch_id = self.track(details_complete=True)
        self.processor.process_envelope(envelope(batch_id))
        self.assertEqual(self.provider.calls["retrieve"], 0)
        self.assertIsNone(self.job(batch_id).last_status_call_at)

    def test_successful_enrichment_records_the_status_call(self) -> None:
        batch_id = self.track()
        self.provider.set_status(batch_id, "completed")
        self.processor.process_envelope(envelope(batch_id))
        job = self.job(batch_id)
        self.assertEqual(job.last_status_call_at, T0)
        self.assertTrue(job.details_complete)
        self.assertEqual(job.request_completed, 3)

    def test_floor_deferrals_do_not_use_up_enrichment_attempts(self) -> None:
        batch_id = self.track(last_status_call_at=T0)
        self.assertEqual(
            self.processor.handle_queue_message(envelope(batch_id).to_json()).kind, "deferred"
        )
        deferred = Envelope.from_json(self.incoming.sent[-1])
        self.assertEqual((deferred.attempt, deferred.enrichment_failures), (1, 0))
        self.clock.advance(60)
        self.provider.fail("retrieve", ProviderRetryableError("busy", status_code=503))
        self.assertEqual(self.processor.handle_queue_message(deferred.to_json()).kind, "deferred")
        again = Envelope.from_json(self.incoming.sent[-1])
        self.assertEqual((again.attempt, again.enrichment_failures), (2, 1))

    def test_deferral_delay_is_capped_below_the_message_ttl(self) -> None:
        batch_id = self.track()
        self.provider.fail(
            "retrieve", ProviderRetryableError("x", status_code=429, retry_after_seconds=10**8)
        )
        self.processor.handle_queue_message(envelope(batch_id).to_json())
        self.assertEqual(self.incoming._messages[-1].visible_after, MAX_DEFERRAL_SECONDS)

    def test_short_floor_remainder_still_waits_at_least_a_second(self) -> None:
        batch_id = self.track(last_status_call_at=T0 - timedelta(seconds=59.9))
        self.processor.handle_queue_message(envelope(batch_id).to_json())
        self.assertEqual(self.incoming._messages[-1].visible_after, 1)


class AdoptPolicyTests(Harness):
    def setUp(self, **settings: str) -> None:
        super().setUp(BATCH_UNKNOWN_JOB_POLICY="adopt")

    def test_transient_provider_error_defers_instead_of_quarantining(self) -> None:
        batch_id = self.provider.add("completed")
        self.provider.fail("retrieve", ProviderRetryableError("busy", status_code=503))
        with self.assertRaises(RetryLater) as ctx:
            self.processor.process_envelope(envelope(batch_id))
        self.assertEqual(ctx.exception.reason, "adopt_retry")
        self.assertEqual(self.quarantine(), [])

    def test_job_unknown_to_the_provider_is_quarantined(self) -> None:
        result = self.processor.process_envelope(envelope("batch_ghost"))
        self.assertEqual(result.kind, "quarantined")
        self.assertEqual([q["reason"] for q in self.quarantine()], ["adopt_failed"])


class ConflictTests(Harness):
    def test_conflicting_list_evidence_requires_a_confirmation_retrieve(self) -> None:
        batch_id = self.track(details_complete=True)
        self.provider.set_status(batch_id, "completed")
        self.processor.process_envelope(envelope(batch_id))
        stale = BatchSnapshot(batch_id, "expired", {})
        observation = observation_from_snapshot(stale, T0, status_call_made=False)
        result = self.processor.apply(JobKey("eus2", batch_id), observation, None)
        self.assertEqual(result.kind, "duplicate")
        self.assertEqual(self.job(batch_id).raw_status, "completed")
        self.assertEqual(self.provider.calls["retrieve"], 1)
        self.assertEqual(len(self.outbox()), 1)

    def test_terminal_retrieve_settles_a_conflict_with_one_call(self) -> None:
        batch_id = self.track()
        self.provider.set_status(batch_id, "completed")
        self.poll(batch_id)  # stored: completed
        self.provider.batches[batch_id]["status"] = "failed"  # provider changed its mind
        self.provider.batches[batch_id]["failed_at"] = int(T0.timestamp())
        calls = self.provider.calls["retrieve"]
        stored = self.job(batch_id)
        self.registry.commit(  # details incomplete so the event triggers one retrieve
            stored.__class__(**{**stored.__dict__, "details_complete": False}),
            etag=self.registry.load(JobKey("eus2", batch_id)).etag,
        )  # type: ignore[union-attr]
        self.clock.advance(60)
        result = self.processor.process_envelope(envelope(batch_id, "batch.failed"))
        self.assertEqual(result.kind, "correction")
        self.assertEqual(self.provider.calls["retrieve"] - calls, 1)

    def test_unresolvable_conflict_keeps_the_stored_state(self) -> None:
        batch_id = self.track(details_complete=True)
        self.provider.set_status(batch_id, "completed")
        self.processor.process_envelope(envelope(batch_id))
        self.provider.fail("retrieve", ProviderNotFoundError("gone", status_code=404))
        with self.assertLogs("foundry_batch", "WARNING") as logs:
            result = self.processor.process_envelope(
                envelope(batch_id, "batch.failed", event_id="evt_2")
            )
        self.assertEqual(result.kind, "duplicate")
        job = self.job(batch_id)
        self.assertEqual((job.raw_status, job.conflict_count), ("completed", 1))
        self.assertTrue(any("processor.conflict_unresolved" in line for line in logs.output))
        self.assertEqual(len(self.outbox()), 1)

    def test_transient_error_while_resolving_retries_later(self) -> None:
        batch_id = self.track(details_complete=True)
        self.processor.process_envelope(envelope(batch_id))
        self.provider.fail("retrieve", ProviderRetryableError("busy", status_code=503))
        with self.assertRaises(RetryLater) as ctx:
            self.processor.process_envelope(envelope(batch_id, "batch.failed", event_id="evt_2"))
        self.assertEqual(
            (ctx.exception.reason, ctx.exception.delay_seconds), ("conflict_resolution_pending", 60)
        )

    def test_chained_corrections_reference_the_event_actually_sent(self) -> None:
        batch_id = self.track()
        self.provider.set_status(batch_id, "completed")
        self.assertEqual(self.poll(batch_id), "terminal")
        first = self.job(batch_id).last_event_key
        for status, stamp in (("failed", "failed_at"), ("completed", "completed_at")):
            self.provider.batches[batch_id]["status"] = status
            self.provider.batches[batch_id][stamp] = int(self.clock.now().timestamp())
            self.clock.advance(60)
            self.assertEqual(self.poll(batch_id), "correction")
        events = sorted(
            (json.loads(str(o["event_json"])) for o in self.outbox()),
            key=lambda e: e["observed"]["emitted_at"],
        )
        keys = [e["idempotency_key"] for e in events]
        self.assertEqual(len(set(keys)), 3)
        self.assertEqual(keys[0], first)
        self.assertEqual([e["correction_of"] for e in events], [None, keys[0], keys[1]])
        self.assertEqual(self.job(batch_id).conflict_count, 2)

    def test_expired_maps_to_failed_and_keeps_the_raw_status(self) -> None:
        batch_id = self.track()
        self.provider.set_status(batch_id, "expired")
        self.poll(batch_id)
        job = self.job(batch_id)
        self.assertEqual((job.raw_status, job.outcome), ("expired", "failed"))
        event = json.loads(str(self.outbox()[0]["event_json"]))
        self.assertEqual((event["raw_status"], event["outcome"]), ("expired", "failed"))

    def test_failed_then_expired_is_a_correction_with_its_own_key(self) -> None:
        batch_id = self.track(details_complete=True)
        self.processor.process_envelope(envelope(batch_id, "batch.failed"))
        self.provider.set_status(batch_id, "expired")
        self.assertEqual(self.poll(batch_id), "correction")
        keys = {json.loads(str(o["event_json"]))["idempotency_key"] for o in self.outbox()}
        self.assertEqual(len(keys), 2)


class SnapshotOrderingTests(Harness):
    def test_older_snapshot_never_regresses_final_counts(self) -> None:
        batch_id = self.track()
        stale = self.provider.retrieve_batch(batch_id)  # read while in progress
        self.provider.set_status(batch_id, "completed")
        self.poll(batch_id)
        self.assertEqual(self.job(batch_id).request_completed, 3)
        obs = observation_from_snapshot(stale, self.clock.now(), status_call_made=False)
        self.processor.apply(JobKey("eus2", batch_id), obs, None)
        self.assertEqual(self.job(batch_id).request_completed, 3)

    def test_decide_ignores_a_lower_ranked_snapshot(self) -> None:
        job = JobRecord("eus2", "b", "test", raw_status="finalizing", request_completed=5)
        old = BatchSnapshot("b", "in_progress", {}, metadata={})
        decision = decide(
            job, Observation(Source.POLLING, "in_progress", T0, snapshot=old, authoritative=True)
        )
        self.assertEqual(decision.record.request_completed, 5)
        self.assertEqual(decision.record.raw_status, "finalizing")

    def test_polling_observation_contract(self) -> None:
        final = BatchSnapshot("b", "completed", {})
        running = BatchSnapshot("b", "in_progress", {})
        obs = observation_from_snapshot(final, T0, status_call_made=True)
        self.assertTrue(obs.authoritative)
        self.assertTrue(obs.status_call_made)
        self.assertEqual((obs.source, obs.receipt_id), (Source.POLLING, "poll:completed"))
        self.assertIsNone(observation_from_snapshot(running, T0, status_call_made=False).receipt_id)


class QuarantineTests(Harness):
    def test_unsafe_batch_ids_produce_valid_table_keys(self) -> None:
        self.clock.advance(self.settings.unknown_job_grace_seconds)  # quarantined after grace
        result = self.processor.process_envelope(envelope("batch/../#?x"))
        self.assertEqual(result.kind, "quarantined")
        row = self.quarantine()[0]
        self.assertEqual((row["reason"], row["batch_id"]), ("unknown_job", "batch/../#?x"))

    def test_oversized_raw_message_is_truncated_and_flagged(self) -> None:
        self.assertEqual(self.processor.process_message("x" * 40_000).kind, "quarantined")
        row = self.quarantine()[0]
        self.assertEqual(len(str(row["envelope"])), MAX_QUARANTINE_TEXT)
        self.assertTrue(row["envelope_truncated"])
        self.assertFalse(row["resolved"])


if __name__ == "__main__":
    unittest.main()
