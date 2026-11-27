"""Boundaries and operational counters (sweep summaries feed the workbook and alerts).

Written from the mutation-testing survivors: each test pins a threshold or a counter whose
corruption the earlier suite would not have noticed.
"""

import json
import logging
import random
import unittest
from datetime import timedelta

from foundry_batch.adapters.memory import (
    MemoryQueue,
    MemorySecrets,
    MemoryTableStore,
    RecordingDownstream,
)
from foundry_batch.control import ControlStore, RegistrationRecord
from foundry_batch.domain.contract import NormalizedEvent
from foundry_batch.domain.models import JobKey, JobRecord
from foundry_batch.errors import ProviderRetryableError
from foundry_batch.ports import DeliveryResult
from foundry_batch.processing.processor import EventProcessor
from foundry_batch.processing.registry import JobRegistry, OutboxItem
from foundry_batch.reconcile.sweeper import Reconciler
from foundry_batch.relay.dispatcher import OutboxDispatcher
from foundry_batch.webhook.envelope import Envelope
from foundry_batch.webhook.http import HttpRequest
from foundry_batch.webhook.ratelimit import TokenBucket
from foundry_batch.webhook.receiver import WebhookReceiver
from tests.support import (
    SECRET_A,
    T0,
    FakeProvider,
    FakeProviderFactory,
    batch_event,
    clock,
    make_settings,
    signed_request,
)


class ReceiverBoundaryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.settings = make_settings(BATCH_MAX_BODY_BYTES="2048")
        self.clock = clock()
        self.queue = MemoryQueue()
        self.secrets = MemorySecrets({"whsec-test-eus2-a": SECRET_A})
        self.receiver = WebhookReceiver(self.settings, self.queue, self.secrets, self.clock)

    def body_of(self, size: int) -> bytes:
        event = batch_event("batch_1") | {"pad": ""}
        base = len(json.dumps(event).encode())
        event["pad"] = "x" * (size - base)
        body = json.dumps(event).encode()
        assert len(body) == size
        return body

    def test_body_cap_is_inclusive(self) -> None:
        self.assertEqual(
            self.receiver.process(signed_request(self.body_of(2048))).response.status, 200
        )
        over = signed_request(self.body_of(2049))
        self.assertEqual(self.receiver.process(over).reason, "body_too_large")
        self.assertEqual(self.receiver.process(over).response.status, 413)
        declared = signed_request(self.body_of(200))
        for value, status in (("2048", 200), ("2049", 413)):
            headers = dict(declared.headers) | {"content-length": value}
            request = HttpRequest("POST", declared.body, headers, declared.route_params)
            self.assertEqual(self.receiver.process(request).response.status, status, value)

    def test_signed_but_unparseable_bodies_are_400(self) -> None:
        for body in (b"{not json", b"[1, 2]", b"\xff\xfe"):
            with self.subTest(body=body):
                outcome = self.receiver.process(signed_request(body))
                self.assertEqual((outcome.response.status, outcome.reason), (400, "invalid_json"))

    def test_timestamp_problems_never_cost_a_key_vault_read(self) -> None:
        self.receiver.process(signed_request(batch_event("b")))  # loads the cache
        reads = self.secrets.reads
        self.clock.advance(31)  # a mismatch could refresh now; a stale timestamp must not
        stale = signed_request(batch_event("b"), timestamp=int(T0.timestamp()) - 3600)
        self.assertEqual(self.receiver.process(stale).reason, "timestamp_out_of_tolerance")
        self.assertEqual(self.secrets.reads, reads)

    def test_retry_after_is_a_whole_second_at_least(self) -> None:
        receiver = WebhookReceiver(
            self.settings,
            self.queue,
            self.secrets,
            self.clock,
            limiter=TokenBucket(5.0, 1, self.clock),
        )
        receiver.process(signed_request(batch_event("b")))
        limited = receiver.process(signed_request(batch_event("b"), webhook_id="m2"))
        self.assertEqual(limited.response.headers["retry-after"], "1")  # 0.2 s rounds up

    def test_event_without_id_keeps_the_webhook_id(self) -> None:
        event = batch_event("batch_1")
        del event["id"]
        self.receiver.process(signed_request(event, webhook_id="msg_9"))
        envelope = Envelope.from_json(self.queue.sent[0])
        self.assertIsNone(envelope.event_id)
        self.assertEqual(envelope.webhook_id, "msg_9")


class Harness(unittest.TestCase):
    def setUp(self, **overrides: str) -> None:
        self.settings = make_settings(**overrides)
        self.clock = clock()
        self.store = MemoryTableStore()
        self.registry = JobRegistry(self.store, self.settings, self.clock)
        self.control = ControlStore(self.store, "batchcontrol", "test", self.clock)
        self.provider = FakeProvider(self.clock)
        factory = FakeProviderFactory({"eus2": self.provider, "swc": FakeProvider(self.clock)})
        self.processor = EventProcessor(
            self.settings, self.registry, self.store, factory, MemoryQueue(), self.clock
        )
        self.reconciler = Reconciler(
            self.settings,
            self.registry,
            self.processor,
            self.control,
            factory,
            self.clock,
            holder="t1",
        )
        self.control.save_registration(
            RegistrationRecord(
                "eus2",
                "a",
                "wh_1",
                "batch-test-eus2-a",
                "https://x",
                "whsec-test-eus2-a",
                "v1",
                T0 - timedelta(days=1),
            )
        )

    def track(self, status: str = "in_progress", **job: object) -> str:
        batch_id = self.provider.add(status)
        snap = self.provider.retrieve_batch(batch_id)
        self.provider.calls["retrieve"] -= 1
        self.registry.create_job(
            JobRecord(
                "eus2",
                batch_id,
                "test",
                raw_status=status,
                webhook_covered=True,
                submitted_at=snap.created_at,
                **job,
            ).with_snapshot(snap)
        )  # type: ignore[arg-type]
        return batch_id

    def job(self, batch_id: str) -> JobRecord:
        stored = self.registry.load(JobKey("eus2", batch_id))
        assert stored is not None
        return stored.record

    def envelope(self, batch_id: str, event_id: str | None, webhook_id: str) -> Envelope:
        event = batch_event(batch_id)
        return Envelope(
            "test",
            "eus2",
            webhook_id,
            int(T0.timestamp()),
            self.clock.now(),
            event_id,
            "batch.completed",
            event["created_at"],
            batch_id,
            event,
        )


class ProcessorBookkeepingTests(Harness):
    def test_receipt_falls_back_to_the_webhook_id(self) -> None:
        batch_id = self.track(details_complete=True)
        self.assertEqual(
            self.processor.process_envelope(self.envelope(batch_id, None, "msg_7")).kind, "terminal"
        )
        self.assertEqual(
            self.processor.process_envelope(self.envelope(batch_id, None, "msg_7")).kind,
            "duplicate",
        )
        receipts = [e.row_key for e in self.registry.receipts(JobKey("eus2", batch_id))]
        self.assertEqual(receipts, ["rcpt:wh:msg_7"])

    def test_conflict_resolution_counts_as_a_status_call(self) -> None:
        batch_id = self.track(details_complete=True)
        self.processor.process_envelope(self.envelope(batch_id, "e1", "m1"))
        self.provider.set_status(batch_id, "completed")
        self.clock.advance(300)
        event = batch_event(batch_id, "batch.failed", "e2")
        self.processor.process_envelope(
            Envelope(
                "test",
                "eus2",
                "m2",
                int(T0.timestamp()),
                self.clock.now(),
                "e2",
                "batch.failed",
                event["created_at"],
                batch_id,
                event,
            )
        )
        self.assertEqual(self.job(batch_id).last_status_call_at, self.clock.now())


class DispatcherBookkeepingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.settings = make_settings(
            BATCH_OUTBOX_MAX_ATTEMPTS="2", BATCH_OUTBOX_RETRY_HORIZON_SECONDS="0"
        )
        self.clock = clock()
        self.registry = JobRegistry(MemoryTableStore(), self.settings, self.clock)
        job = JobRecord("eus2", "batch_1", "test").with_status("completed")
        event = NormalizedEvent.from_job(job, source="webhook", emitted_at=T0)
        self.registry.create_job(JobRecord("eus2", "batch_1", "test"))
        stored = self.registry.load(job.key)
        assert stored is not None
        self.registry.commit(
            job,
            etag=stored.etag,
            outbox=OutboxItem(
                event.idempotency_key, json.dumps(event.payload), "webhook", T0, next_attempt_at=T0
            ),
        )
        self.pk, self.key = self.registry.partition(job.key), event.idempotency_key

    def item(self) -> OutboxItem:
        stored = self.registry.load_outbox(self.pk, self.key)
        assert stored is not None
        return stored.item

    def test_the_claim_is_stored_before_the_send(self) -> None:
        seen: list[OutboxItem] = []

        class Peeking(RecordingDownstream):
            def deliver(inner, body: bytes, *, idempotency_key: str) -> DeliveryResult:  # noqa: N805
                seen.append(self.item())
                return DeliveryResult(200)

        OutboxDispatcher(self.settings, self.registry, Peeking(), self.clock).dispatch_due()
        self.assertEqual(
            (seen[0].attempts, seen[0].next_attempt_at), (1, T0 + timedelta(seconds=60))
        )
        self.assertIsNone(self.item().next_attempt_at)

    def test_quarantine_reason_and_bounded_error_text(self) -> None:
        long_error = "e" * 500
        cases = [
            ([DeliveryResult(400, error=long_error)], "permanent", 1),
            ([DeliveryResult(503, error="busy")] * 2, "attempts_exhausted", 2),
        ]
        for script, reason, attempts in cases:
            with self.subTest(reason=reason):
                self.setUp()
                dispatcher = OutboxDispatcher(
                    self.settings,
                    self.registry,
                    RecordingDownstream(script=list(script)),
                    self.clock,
                    rng=random.Random(1),
                )
                with self.assertLogs("foundry_batch.relay.dispatcher", logging.WARNING) as logs:
                    for _ in range(attempts):
                        dispatcher.dispatch_due()
                        self.clock.advance(3600)
                self.assertIn(f"reason={reason}", logs.output[-1])
                self.assertEqual(self.item().attempts, attempts)
                self.assertLessEqual(len(self.item().last_error or ""), 200)


class SweepStatsTests(Harness):
    def test_summary_counters_for_a_representative_sweep(self) -> None:
        finished = self.track()
        running = self.track()
        hidden = self.track()
        self.provider.order.remove(hidden)
        self.provider.add("completed")  # untracked noise
        self.provider.set_status(finished, "completed")
        self.clock.advance(120)
        stats = self.reconciler.run("eus2")
        self.assertEqual(
            (
                stats.open_tracked,
                stats.list_pages,
                stats.retrieves,
                stats.seen_in_list,
                stats.untracked_seen,
                stats.terminal_found,
                stats.jobs_updated,
            ),
            (3, 1, 1, 2, 1, 1, 1),
        )  # the hidden job's retrieve found no change ("stale")
        self.assertEqual(stats.results, {"terminal": 1, "stale": 1})
        self.assertTrue(stats.traversal_complete)
        self.assertEqual(self.control.get_state("sweep|eus2")["cursor"], "")
        self.assertIsNotNone(self.job(hidden).last_status_call_at)
        self.assertIsNone(self.job(running).last_status_call_at)  # unchanged: no write

    def test_unchanged_running_jobs_are_not_rewritten(self) -> None:
        self.track()
        self.reconciler.run("eus2")
        writes = self.store.submit_calls
        self.reconciler.run("eus2")
        # Only the lease acquire/release and the cursor checkpoint are written.
        self.assertLessEqual(self.store.submit_calls - writes, 3)

    def test_nothing_to_track_resets_the_cursor(self) -> None:
        self.control.put_state("sweep|eus2", {"cursor": "batch_x"})
        stats = self.reconciler.run("eus2")
        self.assertTrue(stats.traversal_complete)
        self.assertEqual(self.control.get_state("sweep|eus2")["cursor"], "")
        self.assertEqual(stats.list_pages, 0)

    def test_throttle_classification(self) -> None:
        self.track()
        for error, field in (
            (ProviderRetryableError("x", status_code=429), "status_429"),
            (ProviderRetryableError("x", status_code=500), "status_5xx"),
            (ProviderRetryableError("x", status_code=503), "status_5xx"),
            (ProviderRetryableError("x"), None),
        ):
            with self.subTest(status=error.status_code):
                self.provider.fail("list", error)
                stats = self.reconciler.run("eus2")
                self.assertTrue(stats.stopped_on_throttle)
                self.assertEqual(stats.status_429 + stats.status_5xx, 1 if field else 0)
                if field:
                    self.assertEqual(getattr(stats, field), 1)


class SweepBoundaryTests(Harness):
    def test_status_floor_is_inclusive(self) -> None:
        hidden = self.track(last_status_call_at=T0)
        self.provider.order.remove(hidden)
        self.clock.advance(59)
        self.assertEqual(self.reconciler.run("eus2").skipped_floor, 1)
        self.clock.advance(1)
        self.assertEqual(self.reconciler.run("eus2").retrieves, 1)

    def test_grace_and_long_running_boundaries(self) -> None:
        self.setUp(BATCH_MISSED_EVENT_GRACE_SECONDS="600")
        covered = self.track()
        self.provider.set_status(covered, "completed")  # provider final time = T0
        self.reconciler.run("eus2")
        self.clock.set(T0 + timedelta(seconds=599))
        self.reconciler.run("eus2")
        self.assertFalse(self.job(covered).event_accounted)
        self.clock.set(T0 + timedelta(seconds=600))
        self.reconciler.run("eus2")
        self.assertTrue(self.job(covered).missed_event)
        old = self.track()
        started = self.job(old).provider_created_at
        assert started is not None
        self.clock.set(started + timedelta(hours=24) - timedelta(seconds=1))
        self.reconciler.run("eus2")
        self.assertFalse(self.job(old).long_running_flagged)
        self.clock.set(started + timedelta(hours=24))
        self.reconciler.run("eus2")
        self.assertTrue(self.job(old).long_running_flagged)


if __name__ == "__main__":
    unittest.main()
