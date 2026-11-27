import unittest
from datetime import timedelta

from foundry_batch.adapters.memory import MemoryQueue, MemoryTableStore
from foundry_batch.control import ControlStore, RegistrationRecord
from foundry_batch.domain.models import JobKey, JobRecord
from foundry_batch.errors import ProviderRetryableError
from foundry_batch.processing.processor import EventProcessor
from foundry_batch.processing.registry import JobRegistry
from foundry_batch.reconcile.sweeper import Reconciler
from foundry_batch.webhook.envelope import Envelope
from tests.support import T0, FakeProvider, FakeProviderFactory, batch_event, clock, make_settings


class ReconcilerTests(unittest.TestCase):
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
                "https://x/api/webhooks/eus2",
                "whsec-test-eus2-a",
                "v1",
                T0 - timedelta(days=1),
            )
        )

    def track(self, status: str = "in_progress", offset: int = 0, **job: object) -> str:
        batch_id = self.provider.add(status, created_offset=offset)
        snap = self.provider.retrieve_batch(batch_id)
        self.provider.calls["retrieve"] -= 1
        fields = {
            "webhook_covered": True,
            "registration_id": "wh_1",
            "deployment": "gpt-batch",
            "submitted_at": snap.created_at,
            **job,
        }
        record = JobRecord("eus2", batch_id, "test", raw_status=status, **fields).with_snapshot(
            snap
        )  # type: ignore[arg-type]
        self.registry.create_job(record)
        return batch_id

    def job(self, batch_id: str) -> JobRecord:
        stored = self.registry.load(JobKey("eus2", batch_id))
        assert stored is not None
        return stored.record

    def test_tc_d03_receiver_off_all_final_states_converge(self) -> None:
        ids = [self.track() for _ in range(5)]
        for _ in range(20):
            self.provider.add("completed")  # untracked noise from other workloads
        for batch_id, status in zip(
            ids, ["completed", "failed", "cancelled", "expired", "in_progress"], strict=True
        ):
            self.provider.set_status(batch_id, status)
        self.clock.advance(900)
        with self.assertLogs("foundry_batch", "INFO") as logs:
            stats = self.reconciler.run("eus2")
        self.assertTrue(stats.traversal_complete)
        self.assertEqual(stats.terminal_found, 4)
        self.assertEqual(stats.retrieves, 0)
        self.assertEqual([self.job(i).terminal_source for i in ids[:4]], ["polling"] * 4)
        self.assertEqual(self.job(ids[4]).raw_status, "in_progress")
        self.assertTrue(any("reconcile.sweep_completed" in line for line in logs.output))
        # Next sweep has nothing new to do and never re-emits events.
        again = self.reconciler.run("eus2")
        self.assertEqual(again.terminal_found, 0)

    def test_tc_e02_counts_every_page_and_fallback_retrieve(self) -> None:
        self.setUp(BATCH_SWEEP_PAGE_SIZE="2")
        hidden = self.track()
        self.provider.order.remove(hidden)  # the list never returns it
        visible = self.track()
        for _ in range(5):
            self.provider.add()
        self.provider.set_status(hidden, "completed")
        self.provider.set_status(visible, "completed")
        self.clock.advance(120)
        stats = self.reconciler.run("eus2")
        self.assertEqual(stats.list_pages, 3)  # 6 listed jobs / page size 2
        self.assertEqual(stats.retrieves, 1)
        self.assertEqual(stats.calls, 4)
        self.assertEqual(self.job(hidden).raw_status, "completed")
        self.assertEqual(self.job(visible).raw_status, "completed")

    def test_no_open_jobs_means_no_calls(self) -> None:
        stats = self.reconciler.run("eus2")
        self.assertEqual(stats.calls, 0)
        self.assertTrue(stats.traversal_complete)

    def test_watermark_stops_newest_first_traversal_early(self) -> None:
        self.setUp(BATCH_SWEEP_PAGE_SIZE="2", BATCH_SWEEP_OVERLAP_SECONDS="0")
        for _ in range(6):
            self.provider.add(created_offset=-7200)  # older than any tracked job
        tracked = self.track(offset=0)
        self.provider.add(created_offset=10)
        self.clock.advance(120)
        stats = self.reconciler.run("eus2")
        self.assertEqual(stats.list_pages, 1)
        self.assertEqual(stats.seen_in_list, 1)
        self.assertEqual(self.job(tracked).raw_status, "in_progress")

    def test_tc_e07_interrupted_traversal_resumes_from_cursor(self) -> None:
        self.setUp(BATCH_SWEEP_PAGE_SIZE="1", BATCH_SWEEP_MAX_CALLS="2")
        old = self.track(offset=-100)
        for _ in range(3):
            self.provider.add(created_offset=-50)
        self.provider.set_status(old, "completed")
        self.clock.advance(120)
        first = self.reconciler.run("eus2")
        self.assertTrue(first.budget_exhausted)
        self.assertFalse(first.traversal_complete)
        self.assertNotEqual(self.control.get_state("sweep|eus2")["cursor"], "")
        second = self.reconciler.run("eus2")
        self.assertTrue(second.traversal_complete)
        self.assertEqual(self.job(old).raw_status, "completed")
        self.assertEqual(self.control.get_state("sweep|eus2")["cursor"], "")

    def test_unordered_list_never_stops_early(self) -> None:
        self.setUp(BATCH_SWEEP_PAGE_SIZE="2", BATCH_SWEEP_OVERLAP_SECONDS="0")
        self.provider.descending = False
        tracked = self.track(offset=0)
        for _ in range(3):
            self.provider.add(created_offset=-9000)
        self.provider.set_status(tracked, "completed")
        self.clock.advance(120)
        stats = self.reconciler.run("eus2")
        self.assertEqual(self.job(tracked).raw_status, "completed")
        self.assertTrue(stats.traversal_complete)

    def test_tc_e09_throttling_stops_the_sweep_and_keeps_cursor(self) -> None:
        self.track()
        self.provider.fail(
            "list", ProviderRetryableError("x", status_code=429, retry_after_seconds=30)
        )
        stats = self.reconciler.run("eus2")
        self.assertTrue(stats.stopped_on_throttle)
        self.assertEqual(stats.status_429, 1)
        hidden = self.track()
        self.provider.order.remove(hidden)
        self.provider.fail("retrieve", ProviderRetryableError("x", status_code=503))
        self.clock.advance(120)
        stats = self.reconciler.run("eus2")
        self.assertEqual(stats.status_5xx, 1)

    def test_fallback_respects_the_per_job_floor(self) -> None:
        hidden = self.track(last_status_call_at=T0)
        self.provider.order.remove(hidden)
        self.clock.advance(30)
        stats = self.reconciler.run("eus2")
        self.assertEqual((stats.retrieves, stats.skipped_floor), (0, 1))
        self.clock.advance(31)
        self.assertEqual(self.reconciler.run("eus2").retrieves, 1)

    def test_missing_job_and_incomplete_details(self) -> None:
        gone = self.track()
        self.provider.order.remove(gone)
        del self.provider.batches[gone]
        with self.assertLogs("foundry_batch", "WARNING"):
            stats = self.reconciler.run("eus2")
        self.assertEqual(stats.retrieves, 1)
        # A terminal job without details is completed by a retrieve.
        done = self.track()
        self.provider.set_status(done, "completed")
        stored = self.registry.load(JobKey("eus2", done))
        assert stored is not None
        self.registry.commit(stored.record.with_status("completed"), etag=stored.etag)
        self.clock.advance(120)
        self.reconciler.run("eus2")
        self.assertTrue(self.job(done).details_complete)
        self.assertEqual(self.job(done).request_completed, 3)

    def test_lease_serialises_sweeps(self) -> None:
        self.track()
        lease = self.control.acquire("sweep|eus2", "other-worker", 600)
        self.assertIsNotNone(lease)
        stats = self.reconciler.run("eus2")
        self.assertEqual(stats.skipped_reason, "lease_held")
        self.clock.advance(601)
        self.assertIsNone(self.reconciler.run("eus2").skipped_reason)

    def test_tc_e06_missed_event_classification_and_late_event(self) -> None:
        covered = self.track()
        uncovered = self.track(webhook_covered=False)
        for batch_id in (covered, uncovered):
            self.provider.set_status(batch_id, "completed")
        self.clock.advance(120)
        self.reconciler.run("eus2")  # reconciled before any event
        self.clock.advance(1500)  # grace runs from the provider's final timestamp (T0)
        self.assertEqual(self.reconciler.run("eus2").missed_classified, 0)  # within grace
        self.clock.advance(400)
        with self.assertLogs("foundry_batch", "WARNING") as logs:
            stats = self.reconciler.run("eus2")
        self.assertEqual(stats.missed_classified, 1)
        self.assertTrue(any("accounting.missed_event" in line for line in logs.output))
        self.assertTrue(self.job(covered).missed_event)
        self.assertFalse(self.job(uncovered).missed_event)
        self.assertEqual(self.reconciler.run("eus2").missed_classified, 0)  # no double count
        # The late event resolves the classification without a second downstream event.
        event = batch_event(covered)
        envelope = Envelope(
            "test",
            "eus2",
            "m1",
            int(T0.timestamp()),
            self.clock.now(),
            "evt_1",
            "batch.completed",
            event["created_at"],
            covered,
            event,
        )
        self.assertEqual(self.processor.process_envelope(envelope).kind, "duplicate")
        self.assertTrue(self.job(covered).late_event)

    def _polling_first_then_webhook(self, webhook_delay_s: int, sweep_delay_s: int) -> str:
        """Polling sees the final state; the webhook arrives later; a sweep runs after that."""
        batch_id = self.track()
        self.provider.set_status(batch_id, "completed")
        self.reconciler.run("eus2")
        received = self.clock.now() + timedelta(seconds=webhook_delay_s)
        self.clock.set(received)
        event = batch_event(batch_id)
        envelope = Envelope(
            "test",
            "eus2",
            "m1",
            int(received.timestamp()),
            received,
            "evt_1",
            "batch.completed",
            event["created_at"],
            batch_id,
            event,
        )
        self.assertEqual(self.processor.process_envelope(envelope).kind, "duplicate")
        self.clock.advance(sweep_delay_s)
        self.reconciler.run("eus2")
        return batch_id

    def test_webhook_after_grace_is_missed_even_if_it_lands_before_the_sweep(self) -> None:
        job = self.job(self._polling_first_then_webhook(webhook_delay_s=2400, sweep_delay_s=300))
        self.assertTrue(job.missed_event)
        self.assertTrue(job.late_event)
        self.assertTrue(job.event_accounted)

    def test_webhook_within_grace_is_not_missed_even_if_the_sweep_runs_later(self) -> None:
        job = self.job(self._polling_first_then_webhook(webhook_delay_s=600, sweep_delay_s=7200))
        self.assertFalse(job.missed_event)
        self.assertTrue(job.late_event)
        self.assertTrue(job.event_accounted)

    def test_webhook_received_before_polling_but_processed_after_is_not_late(self) -> None:
        batch_id = self.track()
        received = self.clock.now()  # the receiver accepted the event here...
        self.provider.set_status(batch_id, "completed")
        self.clock.advance(90)
        self.reconciler.run("eus2")  # ...polling commits the final state first...
        event = batch_event(batch_id)
        envelope = Envelope(
            "test",
            "eus2",
            "m1",
            int(received.timestamp()),
            received,
            "evt_1",
            "batch.completed",
            event["created_at"],
            batch_id,
            event,
        )
        self.clock.advance(30)  # ...and the deferred event is processed afterwards
        self.assertEqual(self.processor.process_envelope(envelope).kind, "duplicate")
        job = self.job(batch_id)
        self.assertFalse(job.late_event)
        self.assertEqual(job.first_webhook_at, received)

    def test_webhook_exactly_at_the_grace_boundary_is_not_missed(self) -> None:
        on_time = self.job(self._polling_first_then_webhook(webhook_delay_s=1800, sweep_delay_s=60))
        self.assertTrue(on_time.event_accounted)
        self.assertFalse(on_time.missed_event)
        self.assertIsNone(on_time.missed_event_at)
        self.clock.set(T0)
        late = self.job(self._polling_first_then_webhook(webhook_delay_s=1801, sweep_delay_s=60))
        self.assertTrue(late.missed_event)

    def test_missed_classification_records_when_it_was_made(self) -> None:
        covered = self.track()
        self.track(webhook_covered=False)
        for stored in self.registry.open_jobs("eus2"):
            self.provider.set_status(stored.record.batch_id, "completed")
        self.reconciler.run("eus2")
        self.clock.advance(1801)
        stats = self.reconciler.run("eus2")
        self.assertEqual((stats.accounted, stats.missed_classified), (2, 1))
        self.assertEqual(self.job(covered).missed_event_at, self.clock.now())

    def test_grace_runs_from_first_observation_without_a_provider_final_time(self) -> None:
        batch_id = self.track()
        self.provider.set_status(batch_id, "completed")
        self.provider.batches[batch_id].pop("completed_at")
        self.clock.advance(1000)
        self.reconciler.run("eus2")  # first observation: T0 + 1000
        self.assertIsNone(self.job(batch_id).provider_terminal_at)
        self.clock.advance(1500)  # 2500 s after the provider finished, 1500 s after we saw it
        self.assertEqual(self.reconciler.run("eus2").accounted, 0)
        self.clock.advance(400)
        self.assertEqual(self.reconciler.run("eus2").missed_classified, 1)

    def test_registration_removed_excludes_job_from_missed_denominator(self) -> None:
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
                active_until=T0,
            )
        )
        batch_id = self.track()
        self.clock.advance(60)
        self.provider.set_status(batch_id, "completed")
        self.reconciler.run("eus2")
        self.clock.advance(4000)
        self.assertEqual(self.reconciler.run("eus2").missed_classified, 0)

    def test_long_running_jobs_are_flagged_once(self) -> None:
        old = self.track(offset=-25 * 3600)
        with self.assertLogs("foundry_batch", "WARNING") as logs:
            stats = self.reconciler.run("eus2")
        self.assertEqual(stats.long_running_flagged, 1)
        self.assertTrue(any("job.over_24h" in line for line in logs.output))
        self.assertTrue(self.job(old).long_running_flagged)
        self.clock.advance(61)
        self.assertEqual(self.reconciler.run("eus2").long_running_flagged, 0)

    def test_failure_is_logged_and_lease_released(self) -> None:
        self.track()
        self.provider.fail("list", RuntimeError("bug"))
        with self.assertLogs("foundry_batch", "ERROR"), self.assertRaises(RuntimeError):
            self.reconciler.run("eus2")
        self.assertIsNotNone(self.control.acquire("sweep|eus2", "someone-else", 60))

    def test_run_all_covers_every_resource(self) -> None:
        self.assertEqual([s.resource_key for s in self.reconciler.run_all()], ["eus2", "swc"])


if __name__ == "__main__":
    unittest.main()
