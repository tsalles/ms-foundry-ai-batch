import unittest
from datetime import timedelta

from foundry_batch.domain.models import BatchSnapshot, JobRecord, RequestCounts, TokenUsage
from foundry_batch.domain.status import BatchStatus, is_terminal, rank
from foundry_batch.processing.decision import DecisionKind, Observation, Source, decide
from tests.support import T0


def snap(status: str, **extra: int) -> BatchSnapshot:
    return BatchSnapshot.from_api(
        {
            "id": "b",
            "status": status,
            "created_at": 1_700_000_000,
            "request_counts": {"total": 2, "completed": 2, "failed": 0},
            **extra,
        }
    )


class DecisionTests(unittest.TestCase):
    job = JobRecord("eus2", "b", "test", raw_status="in_progress")

    def obs(self, status: str, source: Source = Source.WEBHOOK, **kw: object) -> Observation:
        return Observation(source=source, raw_status=status, observed_at=T0, **kw)  # type: ignore[arg-type]

    def test_first_terminal_emits_event_and_records_source(self) -> None:
        d = decide(
            self.job, self.obs("completed", snapshot=snap("completed", completed_at=1_700_000_500))
        )
        self.assertEqual(d.kind, DecisionKind.TERMINAL)
        self.assertTrue(d.emit_event)
        self.assertEqual(d.record.outcome, "completed")
        self.assertEqual(d.record.terminal_source, "webhook")
        self.assertTrue(d.record.details_complete)
        self.assertEqual(d.record.webhook_event_count, 1)
        self.assertEqual(d.record.first_webhook_at, T0)

    def test_progress_and_out_of_order_updates(self) -> None:
        d = decide(self.job, self.obs("finalizing", Source.POLLING))
        self.assertEqual((d.kind, d.record.raw_status), (DecisionKind.PROGRESS, "finalizing"))
        d = decide(self.job, self.obs("validating", Source.POLLING))
        self.assertEqual(d.kind, DecisionKind.STALE)
        self.assertEqual(d.record.raw_status, "in_progress")

    def test_duplicate_terminal_does_not_emit(self) -> None:
        done = self.job.with_status("completed")
        d = decide(done, self.obs("completed"))
        self.assertEqual(d.kind, DecisionKind.DUPLICATE)
        self.assertFalse(d.emit_event)
        self.assertEqual(
            decide(done, self.obs("in_progress", Source.POLLING)).kind, DecisionKind.STALE
        )

    def test_late_event_after_polling_is_marked(self) -> None:
        polled = JobRecord(
            "eus2", "b", "test", terminal_source="polling", missed_event=True
        ).with_status("completed")
        d = decide(polled, self.obs("completed"))
        self.assertTrue(d.late_event)
        self.assertTrue(d.record.late_event)

    def test_tc_d07_conflicting_terminal_needs_authoritative_retrieve(self) -> None:
        done = self.job.with_status("completed")
        d = decide(done, self.obs("failed"))
        self.assertEqual(d.kind, DecisionKind.CONFLICT)
        self.assertFalse(d.emit_event)
        d = decide(done, self.obs("failed", Source.POLLING, authoritative=True))
        self.assertEqual(d.kind, DecisionKind.CORRECTION)
        self.assertTrue(d.emit_event)
        self.assertEqual((d.record.raw_status, d.record.conflict_count), ("failed", 1))

    def test_status_call_updates_pacing_timestamp(self) -> None:
        d = decide(self.job, self.obs("in_progress", Source.POLLING, status_call_made=True))
        self.assertEqual(d.record.last_status_call_at, T0)
        later = decide(
            d.record, Observation(Source.POLLING, "in_progress", T0 + timedelta(seconds=5))
        )
        self.assertEqual(later.record.last_status_call_at, T0)


class MonotonicityTests(unittest.TestCase):
    """Equal-rank reads must never move a job backwards (counts or status)."""

    def polled(self, status: str, snapshot: BatchSnapshot | None = None) -> Observation:
        return Observation(Source.POLLING, status, T0, snapshot=snapshot)

    def test_stale_same_status_snapshot_never_lowers_counts(self) -> None:
        job = JobRecord(
            "eus2",
            "b",
            "test",
            raw_status="in_progress",
            request_total=100,
            request_completed=80,
            request_failed=1,
            output_tokens=900,
        )
        stale = BatchSnapshot(
            "b", "in_progress", {}, RequestCounts(100, 50, 0), usage=TokenUsage(10, 500)
        )
        d = decide(job, self.polled("in_progress", stale))
        self.assertEqual(d.kind, DecisionKind.STALE)
        record = d.record
        self.assertEqual((record.request_total, record.request_completed), (100, 80))
        self.assertEqual((record.request_failed, record.output_tokens), (1, 900))
        self.assertEqual(record.input_tokens, 10)  # a counter we did not know yet is filled in

    def test_newer_same_status_snapshot_raises_counts(self) -> None:
        job = JobRecord("eus2", "b", "test", raw_status="in_progress", request_completed=50)
        newer = BatchSnapshot("b", "in_progress", {}, RequestCounts(100, 80, 2))
        record = decide(job, self.polled("in_progress", newer)).record
        self.assertEqual((record.request_completed, record.request_failed), (80, 2))

    def test_status_change_takes_the_snapshot_counts(self) -> None:
        job = JobRecord("eus2", "b", "test", raw_status="in_progress", request_completed=80)
        final = BatchSnapshot("b", "finalizing", {}, RequestCounts(100, 79, 21))
        d = decide(job, self.polled("finalizing", final))
        self.assertEqual((d.kind, d.record.request_completed), (DecisionKind.PROGRESS, 79))

    def test_cancelling_never_goes_back_to_finalizing(self) -> None:
        job = JobRecord("eus2", "b", "test", raw_status="cancelling", request_completed=7)
        older = BatchSnapshot("b", "finalizing", {}, RequestCounts(10, 3, 0))
        d = decide(job, self.polled("finalizing", older))
        self.assertEqual((d.kind, d.record.raw_status), (DecisionKind.STALE, "cancelling"))
        self.assertEqual(d.record.request_completed, 7)  # the older snapshot is ignored
        again = decide(d.record, self.polled("cancelling"))
        self.assertEqual((again.kind, again.record.raw_status), (DecisionKind.STALE, "cancelling"))

    def test_finalizing_may_advance_to_cancelling_once(self) -> None:
        job = JobRecord("eus2", "b", "test", raw_status="finalizing")
        d = decide(job, self.polled("cancelling"))
        self.assertEqual((d.kind, d.record.raw_status), (DecisionKind.PROGRESS, "cancelling"))
        back = decide(d.record, self.polled("finalizing"))
        self.assertEqual((back.kind, back.record.raw_status), (DecisionKind.STALE, "cancelling"))

    def test_known_non_terminal_statuses_are_strictly_ordered(self) -> None:
        ranks = [rank(s) for s in BatchStatus if not is_terminal(s)]
        self.assertEqual(len(set(ranks)), len(ranks))
        self.assertGreater(rank("cancelling"), rank("finalizing"))

    def test_every_status_can_still_reach_any_final_state(self) -> None:
        for status in ("validating", "in_progress", "finalizing", "cancelling"):
            for final in ("completed", "failed", "expired", "cancelled"):
                with self.subTest(status=status, final=final):
                    job = JobRecord("eus2", "b", "test", raw_status=status)
                    d = decide(job, self.polled(final))
                    self.assertEqual((d.kind, d.record.raw_status), (DecisionKind.TERMINAL, final))

    def test_authoritative_correction_takes_the_final_counts(self) -> None:
        done = JobRecord(
            "eus2", "b", "test", request_total=3, request_completed=3, request_failed=0
        ).with_status("completed")
        failed = BatchSnapshot("b", "failed", {}, RequestCounts(3, 1, 2))
        obs = Observation(Source.POLLING, "failed", T0, snapshot=failed, authoritative=True)
        d = decide(done, obs)
        self.assertEqual(d.kind, DecisionKind.CORRECTION)
        self.assertEqual((d.record.request_completed, d.record.request_failed), (1, 2))


if __name__ == "__main__":
    unittest.main()
