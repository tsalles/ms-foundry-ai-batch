"""Reconciler traversal and budget behaviour, asserted on call counts (code review findings)."""

import unittest
from datetime import timedelta

from foundry_batch.adapters.memory import MemoryQueue, MemoryTableStore
from foundry_batch.control import ControlStore
from foundry_batch.domain.models import BatchSnapshot, JobKey, JobRecord
from foundry_batch.ports import BatchPage
from foundry_batch.processing.processor import EventProcessor
from foundry_batch.processing.registry import JobRegistry, StoredJob
from foundry_batch.reconcile.sweeper import Reconciler
from tests.support import T0, FakeProvider, FakeProviderFactory, clock, make_settings


class SlowProvider(FakeProvider):
    """Every list call takes ``seconds`` of wall time (drives the lease deadline)."""

    seconds = 0.0

    def list_batches(self, *, after: str | None, limit: int) -> BatchPage:
        self.clock.advance(self.seconds)
        return super().list_batches(after=after, limit=limit)


class Harness(unittest.TestCase):
    def setUp(self, **overrides: str) -> None:
        self.settings = make_settings(**overrides)
        self.clock = clock()
        self.store = MemoryTableStore()
        self.registry = JobRegistry(self.store, self.settings, self.clock)
        self.control = ControlStore(self.store, "batchcontrol", "test", self.clock)
        self.provider = SlowProvider(self.clock)
        factory = FakeProviderFactory({"eus2": self.provider, "swc": FakeProvider(self.clock)})
        processor = EventProcessor(
            self.settings, self.registry, self.store, factory, MemoryQueue(), self.clock
        )
        self.reconciler = Reconciler(
            self.settings, self.registry, processor, self.control, factory, self.clock, holder="t1"
        )

    def track(self, offset: int = 0, *, listed: bool = True, **job: object) -> str:
        batch_id = self.provider.add("in_progress", created_offset=offset)
        if not listed:
            self.provider.order.remove(batch_id)  # retrievable, but never returned by list
        snap = self.provider.retrieve_batch(batch_id)
        self.provider.calls["retrieve"] -= 1
        self.registry.create_job(
            JobRecord(
                "eus2",
                batch_id,
                "test",
                raw_status="in_progress",
                submitted_at=snap.created_at,
                **job,
            ).with_snapshot(snap)
        )  # type: ignore[arg-type]
        return batch_id

    def noise(self, count: int, offset: int) -> None:
        for _ in range(count):
            self.provider.add("completed", created_offset=offset)

    def job(self, batch_id: str) -> JobRecord:
        stored = self.registry.load(JobKey("eus2", batch_id))
        assert stored is not None
        return stored.record


class WatermarkTests(Harness):
    def setUp(self, **overrides: str) -> None:
        super().setUp(BATCH_SWEEP_PAGE_SIZE="5", BATCH_SWEEP_OVERLAP_SECONDS="600")

    def test_newest_first_traversal_stops_at_the_watermark(self) -> None:
        self.noise(20, offset=-86_400)  # old, untracked
        hidden = self.track(offset=0, listed=False)
        self.noise(4, offset=60)  # newer than the tracked job
        stats = self.reconciler.run("eus2")
        # Page 1: 4 new + 1 old proves newest-first order and passes the watermark -> stop
        # (a full traversal of the 24 listed items would take 5 pages).
        self.assertEqual(stats.list_pages, 1)
        self.assertTrue(stats.traversal_complete)
        self.assertEqual(stats.retrieves, 1)  # the hidden job came from the fallback
        self.assertEqual(self.job(hidden).last_status_call_at, T0)

    def test_oldest_first_listing_is_traversed_to_the_end(self) -> None:
        self.provider.descending = False
        self.noise(20, offset=-86_400)
        self.track(offset=0, listed=False)
        stats = self.reconciler.run("eus2")
        self.assertEqual(stats.list_pages, 4)  # all 20 listed items, 5 per page
        self.assertTrue(stats.traversal_complete)
        self.assertEqual(stats.retrieves, 1)

    def test_single_item_or_equal_timestamps_prove_nothing(self) -> None:
        old = T0 - timedelta(days=2)
        snap = BatchSnapshot("b1", "completed", {"created_at": old})
        same = BatchSnapshot("b2", "completed", {"created_at": old})
        watermark = T0 - timedelta(days=1)
        self.assertFalse(Reconciler._passed_watermark((snap,), watermark))
        self.assertFalse(Reconciler._passed_watermark((snap, same), watermark))
        newer = BatchSnapshot("b0", "completed", {"created_at": old + timedelta(seconds=1)})
        self.assertTrue(Reconciler._passed_watermark((newer, snap), watermark))
        # Continuity with the previous page counts as proof; a rise breaks it.
        self.assertTrue(Reconciler._passed_watermark((snap,), watermark, old + timedelta(1)))
        self.assertFalse(Reconciler._passed_watermark((newer,), watermark, old))
        self.assertFalse(Reconciler._passed_watermark((newer, snap), None))
        missing = BatchSnapshot("bx", "completed", {})
        self.assertFalse(Reconciler._passed_watermark((newer, missing), watermark))

    def test_unknown_creation_time_disables_the_early_stop(self) -> None:
        known = StoredJob(JobRecord("eus2", "a", "test", submitted_at=T0), "e1")
        unknown = StoredJob(JobRecord("eus2", "b", "test"), "e2")
        self.assertIsNone(self.reconciler._watermark([known, unknown]))
        self.assertIsNone(self.reconciler._watermark([]))
        self.assertEqual(self.reconciler._watermark([known]), T0 - timedelta(seconds=600))


class BudgetAndDeadlineTests(Harness):
    def test_multi_page_traversal_resumes_every_fallback_job_with_a_one_call_budget(self) -> None:
        self.setUp(BATCH_SWEEP_MAX_CALLS="1", BATCH_SWEEP_PAGE_SIZE="1")
        jobs = [self.track(listed=False) for _ in range(3)]
        for batch_id in jobs:
            self.provider.set_status(batch_id, "completed")
        self.noise(2, offset=0)
        for _ in range(8):
            stats = self.reconciler.run("eus2")
            self.assertLessEqual(stats.calls, 1)
            self.clock.advance(900)
        self.assertTrue(all(self.job(batch_id).outcome == "completed" for batch_id in jobs))
        self.assertEqual(self.provider.calls["retrieve"], 3)

    def test_fallback_survives_a_list_traversal_that_consumes_the_budget(self) -> None:
        self.setUp(BATCH_SWEEP_MAX_CALLS="1", BATCH_SWEEP_PAGE_SIZE="1")
        hidden = self.track(listed=False)
        self.provider.set_status(hidden, "completed")
        self.noise(1, offset=0)
        for _ in range(3):
            self.reconciler.run("eus2")
            self.clock.advance(900)
        self.assertEqual(self.job(hidden).outcome, "completed")
        self.assertEqual(self.provider.calls["retrieve"], 1)

    def test_fallback_budget_rotates_through_jobs(self) -> None:
        self.setUp(BATCH_SWEEP_MAX_CALLS="3")
        jobs = [self.track(listed=False) for _ in range(3)]
        first = self.reconciler.run("eus2")
        self.assertEqual((first.list_pages, first.retrieves, first.budget_exhausted), (1, 2, True))
        checked = {j for j in jobs if self.job(j).last_status_call_at is not None}
        self.assertEqual(len(checked), 2)
        self.clock.advance(120)
        self.reconciler.run("eus2")
        unchecked = [j for j in jobs if j not in checked]
        self.assertIsNotNone(self.job(unchecked[0]).last_status_call_at)  # not starved

    def test_sweep_stops_before_the_lease_can_expire(self) -> None:
        self.setUp(BATCH_SWEEP_PAGE_SIZE="1", BATCH_SWEEP_LEASE_SECONDS="600")
        self.provider.seconds = 100.0
        self.provider.descending = False
        self.noise(10, offset=-86_400)
        self.track(offset=0)
        stats = self.reconciler.run("eus2")
        self.assertTrue(stats.stopped_on_deadline)
        self.assertFalse(stats.budget_exhausted)
        self.assertEqual(stats.list_pages, 5)  # 5 x 100 s >= 0.8 x 600 s
        self.assertNotEqual(self.control.get_state("sweep|eus2")["cursor"], "")  # resumable
        self.clock.advance(60)
        again = self.reconciler.run("eus2")
        self.assertGreater(again.seen_in_list + again.list_pages, 0)

    def test_missing_cursor_ends_the_traversal(self) -> None:
        self.track(listed=False)

        def no_cursor(*, after: str | None, limit: int) -> BatchPage:
            return BatchPage(items=(), has_more=True, last_id=None)

        self.provider.list_batches = no_cursor  # type: ignore[method-assign]
        stats = self.reconciler.run("eus2")
        self.assertEqual(stats.list_pages, 1)
        self.assertTrue(stats.traversal_complete)
        self.assertEqual(stats.retrieves, 1)


class ResumedFallbackTests(Harness):
    """A resumed fallback retrieves only what the completed list traversal omitted (17 Nov)."""

    def _exhaust_budget_on_list(self) -> tuple[str, list[str]]:
        self.setUp(BATCH_SWEEP_MAX_CALLS="2", BATCH_SWEEP_PAGE_SIZE="5")
        hidden = self.track(listed=False)
        listed = [self.track() for _ in range(10)]
        self.provider.set_status(hidden, "completed")
        first = self.reconciler.run("eus2")
        self.assertEqual((first.list_pages, first.retrieves), (2, 0))
        self.assertEqual(self.control.get_state("sweep|eus2")["phase"], "fallback")
        self.clock.advance(900)
        return hidden, listed

    def test_resume_retrieves_only_jobs_the_list_omitted(self) -> None:
        hidden, listed = self._exhaust_budget_on_list()
        second = self.reconciler.run("eus2")
        self.assertEqual((second.list_pages, second.retrieves), (0, 1))
        self.assertEqual(self.job(hidden).outcome, "completed")
        self.assertTrue(all(self.job(b).last_status_call_at is None for b in listed))
        self.assertEqual(self.control.get_state("sweep|eus2")["phase"], "list")
        self.clock.advance(900)
        third = self.reconciler.run("eus2")
        self.assertEqual(third.list_pages, 2)  # the list phase is not starved

    def test_state_without_a_saved_set_falls_back_to_every_open_job(self) -> None:
        hidden, _ = self._exhaust_budget_on_list()
        state = self.control.get_state("sweep|eus2")
        state.pop("fallback_pending")
        self.control.put_state("sweep|eus2", state)
        second = self.reconciler.run("eus2")
        self.assertEqual(second.retrieves, 2)  # budget-bound walk over all open jobs
        self.assertEqual(self.job(hidden).outcome, "completed")

    def test_unreadable_saved_set_is_ignored(self) -> None:
        from foundry_batch.reconcile.sweeper import _load_pending

        for raw in (None, "", "not json", "{}", "[1, 2]"):
            self.assertIsNone(_load_pending(raw), raw)
        self.assertEqual(_load_pending('["a","b"]'), {"a", "b"})

    def test_oversized_set_is_not_saved_and_is_logged(self) -> None:
        from foundry_batch.reconcile import sweeper

        original = sweeper.MAX_PENDING_CHARS
        sweeper.MAX_PENDING_CHARS = 5
        try:
            with self.assertLogs("foundry_batch.reconcile.sweeper", level="WARNING") as logs:
                self._exhaust_budget_on_list()
            self.assertTrue(any("fallback_set_not_saved" in line for line in logs.output))
            self.assertNotIn("fallback_pending", self.control.get_state("sweep|eus2"))
        finally:
            sweeper.MAX_PENDING_CHARS = original


if __name__ == "__main__":
    unittest.main()
