"""Convergence across sweeps: interrupted traversals, cursor and job refusals, the lag signal.

Findings R1 and R3 of the 17 Nov full review, plus the sweeper branches a mutation run showed
were not pinned (resumed-fallback progress, watermark choice, update counting, deferrals).
"""

import unittest
from unittest import mock

from foundry_batch.errors import ProviderRequestError, ProviderRetryableError, RetryLater
from foundry_batch.ports import BatchPage
from foundry_batch.reconcile import sweeper
from tests.unit.test_reconciler_hardening import Harness


def throttle() -> ProviderRetryableError:
    return ProviderRetryableError("batch.list: HTTP 429", status_code=429, retry_after_seconds=10)


class InterruptedTraversalTests(Harness):
    """A traversal spread over several sweeps must not re-retrieve jobs it already listed."""

    def _listed_and_hidden(self, listed: int) -> tuple[str, list[str]]:
        hidden = self.track(listed=False)
        jobs = [self.track() for _ in range(listed)]
        self.provider.set_status(hidden, "completed")
        return hidden, jobs

    def test_budget_interrupted_traversal_retrieves_only_the_omitted_job(self) -> None:
        self.setUp(BATCH_SWEEP_MAX_CALLS="2", BATCH_SWEEP_PAGE_SIZE="5")
        hidden, listed = self._listed_and_hidden(20)
        sweeps = 0
        while self.job(hidden).outcome is None and sweeps < 12:
            self.reconciler.run("eus2")
            self.clock.advance(900)
            sweeps += 1
        self.assertEqual(self.job(hidden).outcome, "completed")
        self.assertEqual(self.provider.calls["retrieve"], 1)
        self.assertTrue(all(self.job(b).last_status_call_at is None for b in listed))
        self.assertLessEqual(sweeps, 4)  # 4 list pages over 2 sweeps, then the fallback

    def test_throttled_traversal_keeps_its_listed_jobs_for_the_next_sweep(self) -> None:
        self.setUp(BATCH_SWEEP_PAGE_SIZE="5")
        hidden, listed = self._listed_and_hidden(10)
        real = self.provider.list_batches
        calls = {"n": 0}

        def second_page_throttled(*, after: str | None, limit: int) -> BatchPage:
            calls["n"] += 1
            if calls["n"] == 2:
                raise throttle()
            return real(after=after, limit=limit)

        self.provider.list_batches = second_page_throttled  # type: ignore[method-assign]
        first = self.reconciler.run("eus2")
        self.assertTrue(first.stopped_on_throttle)
        state = self.control.get_state("sweep|eus2")
        self.assertNotEqual(state["cursor"], "")
        self.assertEqual(len(sweeper._load_pending(state["listed"]) or ()), 5)
        self.clock.advance(900)
        second = self.reconciler.run("eus2")
        self.assertTrue(second.traversal_complete)
        self.assertEqual(second.retrieves, 1)  # only the job the list never returned
        self.assertEqual(self.job(hidden).outcome, "completed")
        self.assertTrue(all(self.job(b).last_status_call_at is None for b in listed))

    def test_oversized_listed_set_is_logged_and_the_job_still_converges(self) -> None:
        self.setUp(BATCH_SWEEP_MAX_CALLS="2", BATCH_SWEEP_PAGE_SIZE="5")
        hidden, _ = self._listed_and_hidden(20)
        with mock.patch.object(sweeper, "MAX_PENDING_CHARS", 5):
            with self.assertLogs("foundry_batch.reconcile.sweeper", level="WARNING") as logs:
                self.reconciler.run("eus2")
            self.assertTrue(any("listed_set_not_saved" in line for line in logs.output))
            self.assertNotIn("listed", self.control.get_state("sweep|eus2"))
            for _ in range(12):
                self.clock.advance(900)
                self.reconciler.run("eus2")
        self.assertEqual(self.job(hidden).outcome, "completed")

    def test_fresh_traversal_ignores_a_listed_set_without_a_cursor(self) -> None:
        hidden, _ = self._listed_and_hidden(3)
        self.control.put_state("sweep|eus2", {"cursor": "", "listed": f'["{hidden}"]'})
        stats = self.reconciler.run("eus2")
        self.assertEqual(stats.retrieves, 1)  # the stale set did not hide the omitted job
        self.assertEqual(self.job(hidden).outcome, "completed")


class RefusalTests(Harness):
    def test_rejected_cursor_restarts_the_traversal(self) -> None:
        self.setUp(BATCH_SWEEP_MAX_CALLS="1", BATCH_SWEEP_PAGE_SIZE="2")
        hidden = self.track(listed=False)
        for _ in range(4):
            self.track()
        self.reconciler.run("eus2")  # one page, budget spent: cursor saved
        self.assertNotEqual(self.control.get_state("sweep|eus2")["cursor"], "")
        self.provider.fail("list", ProviderRequestError("invalid after", status_code=400))
        self.clock.advance(900)
        with self.assertLogs("foundry_batch.reconcile.sweeper", level="WARNING") as logs:
            stats = self.reconciler.run("eus2")
        self.assertTrue(stats.cursor_reset)
        self.assertTrue(any("reconcile.cursor_rejected" in line for line in logs.output))
        state = self.control.get_state("sweep|eus2")
        self.assertEqual(state["cursor"], "")
        self.assertNotIn("listed", state)
        self.provider.set_status(hidden, "completed")
        for _ in range(6):
            self.clock.advance(900)
            self.reconciler.run("eus2")
        self.assertEqual(self.job(hidden).outcome, "completed")

    def test_refused_first_page_fails_the_sweep_and_releases_the_lease(self) -> None:
        self.track()
        self.provider.fail("list", ProviderRequestError("forbidden", status_code=403))
        with self.assertLogs("foundry_batch", "ERROR"), self.assertRaises(ProviderRequestError):
            self.reconciler.run("eus2")
        self.assertEqual(self.reconciler.run("eus2").skipped_reason, None)  # lease released

    def test_one_refused_retrieve_does_not_stall_the_fallback(self) -> None:
        jobs = [self.track(listed=False) for _ in range(3)]
        for batch_id in jobs:
            self.provider.set_status(batch_id, "completed")
        self.provider.fail("retrieve", ProviderRequestError("forbidden", status_code=403))
        with self.assertLogs("foundry_batch.reconcile.sweeper", level="WARNING") as logs:
            stats = self.reconciler.run("eus2")
        self.assertEqual((stats.retrieves, stats.retrieve_failures), (3, 1))
        self.assertTrue(any("reconcile.retrieve_failed" in line for line in logs.output))
        self.assertEqual(sum(self.job(b).outcome == "completed" for b in jobs), 2)
        self.assertEqual(self.control.get_state("sweep|eus2")["phase"], "list")


class LagSignalTests(Harness):
    """US-5.2 AC6 / TC-E09: a lag that keeps growing while sweeps stop converging."""

    def test_lag_grows_across_throttled_sweeps_and_resets_on_completion(self) -> None:
        self.track()
        healthy = self.reconciler.run("eus2")
        self.assertEqual(healthy.last_complete_at, self.clock.now())
        self.assertEqual(healthy.lag_s, 0.0)
        lags = []
        for _ in range(3):
            self.clock.advance(900)
            self.provider.fail("list", throttle())
            with self.assertLogs("foundry_batch.reconcile.sweeper", level="INFO") as logs:
                lags.append(self.reconciler.run("eus2").lag_s)
            completed = [line for line in logs.output if "reconcile.sweep_completed" in line]
            self.assertTrue(any("lag_s" in line for line in completed))
            self.assertIn("last_complete_at", self.control.get_state("sweep|eus2"))
        self.assertEqual(lags, [900.0, 1800.0, 2700.0])
        self.clock.advance(900)
        self.assertEqual(self.reconciler.run("eus2").lag_s, 0.0)

    def test_lag_is_unknown_until_a_sweep_completes(self) -> None:
        self.track()
        self.provider.fail("list", throttle())
        stats = self.reconciler.run("eus2")
        self.assertIsNone(stats.last_complete_at)
        self.assertIsNone(stats.lag_s)
        self.assertNotIn("last_complete_at", self.control.get_state("sweep|eus2"))

    def test_a_resource_with_nothing_open_has_converged(self) -> None:
        stats = self.reconciler.run("eus2")
        self.assertEqual(stats.lag_s, 0.0)
        self.assertEqual(
            self.control.get_state("sweep|eus2")["last_complete_at"],
            "2026-11-20T12:00:00.000000Z",
        )


class SweeperBranchTests(Harness):
    """Branches a mutation run showed were reachable but not asserted."""

    def test_resumed_fallback_skips_jobs_it_already_retrieved(self) -> None:
        self.setUp(BATCH_SWEEP_MAX_CALLS="3", BATCH_SWEEP_PAGE_SIZE="5")
        hidden = [self.track(listed=False) for _ in range(3)]  # stay open after retrieval
        for _ in range(6):
            self.track()
        first = self.reconciler.run("eus2")
        self.assertEqual((first.list_pages, first.retrieves), (2, 1))
        self.clock.advance(900)
        second = self.reconciler.run("eus2")
        self.assertEqual((second.list_pages, second.retrieves), (0, 2))
        self.assertEqual(self.provider.calls["retrieve"], 3)  # no job retrieved twice
        self.assertTrue(all(self.job(b).last_status_call_at is not None for b in hidden))

    def test_watermark_follows_the_oldest_open_job(self) -> None:
        self.setUp(BATCH_SWEEP_PAGE_SIZE="2", BATCH_SWEEP_OVERLAP_SECONDS="600")
        self.noise(4, offset=-86_400)
        old = self.track(offset=-7_200)
        self.noise(3, offset=-3_600)
        self.track(offset=0)
        stats = self.reconciler.run("eus2")
        self.assertEqual(stats.retrieves, 0)  # the old job was reached by the list itself
        self.assertIsNone(self.job(old).last_status_call_at)
        self.assertGreaterEqual(stats.list_pages, 3)

    def test_update_and_terminal_counts(self) -> None:
        changed = self.track()
        self.track()  # unchanged and non-final: no write at all
        self.provider.set_status(changed, "completed")
        stats = self.reconciler.run("eus2")
        self.assertEqual((stats.jobs_updated, stats.terminal_found), (1, 1))
        self.assertEqual(stats.results, {"terminal": 1})

    def test_a_deferred_apply_is_logged_and_the_sweep_continues(self) -> None:
        jobs = [self.track() for _ in range(2)]
        for batch_id in jobs:
            self.provider.set_status(batch_id, "completed")
        processor = self.reconciler._processor
        with (
            mock.patch.object(
                processor, "apply", side_effect=RetryLater("status_call_floor", 60.0)
            ),
            self.assertLogs("foundry_batch.reconcile.sweeper", level="WARNING") as logs,
        ):
            stats = self.reconciler.run("eus2")
        self.assertEqual(sum("reconcile.apply_deferred" in line for line in logs.output), 2)
        self.assertTrue(stats.traversal_complete)
        self.assertEqual(stats.jobs_updated, 0)


if __name__ == "__main__":
    unittest.main()
