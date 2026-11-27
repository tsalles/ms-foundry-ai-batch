import unittest
from dataclasses import replace
from datetime import timedelta

from foundry_batch.domain.models import JobRecord
from foundry_batch.metrics.api_pressure import ApiCallCounter, status_class
from foundry_batch.metrics.indicators import (
    STANDARD_WINDOWS,
    Distribution,
    compute_indicators,
    percentile,
)
from foundry_batch.metrics.lifecycle import LifecycleRecord
from foundry_batch.metrics.queued_tokens import (
    CharacterHeuristicEstimator,
    compare_estimates,
    queued_tokens,
)
from tests.support import T0

NO_OFFSET = timedelta(0)


def finished(
    batch_id: str,
    status: str,
    turnaround_h: float,
    *,
    deployment: str = "gpt-batch",
    region: str = "eastus2",
    total: int | None = 10,
    failed: int = 0,
    start_offset_s: float | None = 60,
    tokens: tuple[int, int] | None = None,
    end: timedelta = NO_OFFSET,
) -> JobRecord:
    terminal = T0 - end
    created = terminal - timedelta(hours=turnaround_h)
    stamps = {
        f"provider_{ {'completed': 'completed_at', 'failed': 'failed_at', 'expired': 'expired_at', 'cancelled': 'cancelled_at'}[status] }": terminal
    }
    return JobRecord(
        "eus2",
        batch_id,
        "test",
        deployment=deployment,
        region=region,
        model="gpt-x",
        provider_created_at=created,
        provider_in_progress_at=created + timedelta(seconds=start_offset_s)
        if start_offset_s is not None
        else None,
        provider_finalizing_at=terminal - timedelta(seconds=30) if status == "completed" else None,
        request_total=total,
        request_failed=failed,
        input_tokens=tokens[0] if tokens else None,
        output_tokens=tokens[1] if tokens else None,
        terminal_source="webhook",
        **stamps,  # type: ignore[arg-type]
    ).with_status(status)


class LifecycleTests(unittest.TestCase):
    def test_durations_and_normalisation(self) -> None:
        rec = LifecycleRecord.from_job(finished("b", "completed", 2, tokens=(600_000, 400_000)))
        self.assertEqual(rec.turnaround_s, 7200)
        self.assertEqual(rec.time_to_start_s, 60)
        self.assertEqual(rec.finalizing_s, 30)
        self.assertEqual(rec.per_request_s, 720)
        self.assertEqual(rec.per_million_tokens_s, 7200)
        self.assertEqual(rec.flags, ())
        props = rec.log_properties()
        self.assertEqual(props["turnaround_s"], 7200)
        self.assertEqual(props["outcome"], "completed")

    def test_tc_c08_missing_and_negative_values_are_flagged_not_zero(self) -> None:
        rec = LifecycleRecord.from_job(finished("b", "failed", 1, start_offset_s=None, total=0))
        self.assertIsNone(rec.time_to_start_s)
        self.assertIn("time_to_start_unavailable", rec.flags)
        self.assertIsNone(rec.per_request_s)
        self.assertIsNone(rec.per_million_tokens_s)
        weird = JobRecord(
            "eus2",
            "b",
            "test",
            provider_created_at=T0,
            provider_completed_at=T0 - timedelta(seconds=5),
        ).with_status("completed")
        self.assertIn("turnaround_negative", LifecycleRecord.from_job(weird).flags)
        observed = JobRecord(
            "eus2",
            "b",
            "test",
            provider_created_at=T0 - timedelta(hours=1),
            terminal_observed_at=T0,
        ).with_status("cancelled")
        rec = LifecycleRecord.from_job(observed)
        self.assertEqual((rec.turnaround_s, rec.terminal_at_provenance), (3600, "observed"))
        zero_tokens = finished("b", "completed", 1, tokens=(0, 0))
        self.assertIsNone(LifecycleRecord.from_job(zero_tokens).per_million_tokens_s)


class IndicatorTests(unittest.TestCase):
    def test_request_failure_rate_excludes_unavailable_counts_and_reports_coverage(self) -> None:
        unknown = replace(finished("unknown", "completed", 1), request_failed=None)
        known = finished("known", "completed", 1, total=10, failed=2)
        [result] = compute_indicators(
            [unknown, known], [], now=T0, window=timedelta(days=1), window_name="24h"
        )
        self.assertEqual(result.request_failure_rate, 0.2)
        self.assertEqual(result.request_failure_coverage, 0.5)
        [unavailable] = compute_indicators(
            [unknown], [], now=T0, window=timedelta(days=1), window_name="24h"
        )
        self.assertIsNone(unavailable.request_failure_rate)
        self.assertEqual(unavailable.request_failure_coverage, 0.0)

    def test_percentile_nearest_rank(self) -> None:
        values = list(range(1, 21))
        self.assertEqual(percentile(values, 50), 10)
        self.assertEqual(percentile(values, 95), 19)
        self.assertEqual(percentile(values, 100), 20)
        self.assertIsNone(percentile([], 50))
        with self.assertRaises(ValueError):
            percentile(values, 0)
        self.assertEqual(Distribution.of([]).count, 0)

    def test_tc_g01_g07_indicators_by_group_and_window(self) -> None:
        jobs = [finished(f"c{i}", "completed", i + 1, failed=1) for i in range(18)]
        jobs += [
            finished("f1", "failed", 2),
            finished("e1", "expired", 30),
            finished("x1", "cancelled", 1),
        ]
        jobs += [finished("old", "completed", 1, end=timedelta(days=10))]
        jobs += [finished("swc1", "completed", 1, region="swedencentral")]
        open_jobs = [
            JobRecord(
                "eus2",
                "o1",
                "test",
                deployment="gpt-batch",
                region="eastus2",
                model="gpt-x",
                provider_created_at=T0 - timedelta(hours=30),
            ),
            JobRecord(
                "eus2",
                "o2",
                "test",
                deployment="gpt-batch",
                region="eastus2",
                model="gpt-x",
                provider_created_at=T0 - timedelta(hours=1),
            ),
        ]
        result = compute_indicators(
            jobs, open_jobs, now=T0, window=timedelta(days=7), window_name="7d"
        )
        self.assertEqual(len(result), 2)
        eus = next(r for r in result if r.group.region == "eastus2")
        self.assertEqual(eus.terminal_jobs, 21)
        self.assertEqual((eus.completed, eus.failed, eus.expired, eus.cancelled), (18, 1, 1, 1))
        self.assertAlmostEqual(eus.completion_rate or 0, 18 / 21)
        self.assertAlmostEqual(eus.expiration_rate or 0, 1 / 21)
        self.assertAlmostEqual(eus.request_failure_rate or 0, 18 / 210)
        self.assertAlmostEqual(eus.share_above_24h or 0, 1 / 21)
        self.assertEqual(eus.unfinished_over_24h, 1)
        self.assertEqual(eus.turnaround.maximum, 30 * 3600)
        self.assertEqual(eus.turnaround.count, 21)
        self.assertEqual(eus.turnaround_coverage, 1.0)
        self.assertEqual(eus.per_million_tokens.count, 0)  # no usage: unavailable, not zero
        day = compute_indicators(jobs, [], now=T0, window=timedelta(hours=24), window_name="24h")
        self.assertEqual(sum(r.terminal_jobs for r in day), 22)

    def test_empty_denominators_are_unavailable(self) -> None:
        open_only = [JobRecord("eus2", "o1", "test", provider_created_at=T0 - timedelta(days=2))]
        [result] = compute_indicators(
            [], open_only, now=T0, window=timedelta(days=1), window_name="24h"
        )
        self.assertIsNone(result.completion_rate)
        self.assertIsNone(result.turnaround.p95)
        self.assertEqual(result.group.deployment, "unknown")
        self.assertEqual([w for w, _ in STANDARD_WINDOWS], ["24h", "7d", "30d"])

    def test_flags_for_observed_and_missing_times(self) -> None:
        observed = JobRecord("eus2", "b", "test", terminal_observed_at=T0).with_status("completed")
        [result] = compute_indicators(
            [observed], [], now=T0, window=timedelta(days=1), window_name="24h"
        )
        self.assertIn("observed_terminal_times_included", result.flags)
        self.assertIn("turnaround_missing_for_some_jobs", result.flags)


class QueuedTokenTests(unittest.TestCase):
    def test_estimator_reads_messages_and_responses_input(self) -> None:
        jsonl = (
            b'{"custom_id":"1","method":"POST","url":"/v1/chat/completions",'
            b'"body":{"model":"d","messages":[{"role":"user","content":"12345678"}]}}\n'
            b'{"custom_id":"2","method":"POST","url":"/v1/responses",'
            b'"body":{"model":"d","input":[{"role":"user","content":[{"type":"input_text","text":"abcdefgh"}]}]}}\n'
            b"not json\n\n"
        )
        estimator = CharacterHeuristicEstimator()
        self.assertEqual(estimator.estimate(jsonl), (2 + 4) + (2 + 4))

    def test_gauge_is_recomputed_and_never_negative(self) -> None:
        jobs = [
            JobRecord(
                "eus2", "a", "test", deployment="d", token_estimate=100, tokenizer="chars-per-4-v1"
            ),
            JobRecord("eus2", "b", "test", deployment="d", token_estimate=None),
            JobRecord("eus2", "c", "test", deployment="d", token_estimate=50).with_status(
                "completed"
            ),
        ]
        [gauge] = queued_tokens(jobs)
        self.assertEqual(
            (gauge.estimated_tokens, gauge.jobs, gauge.jobs_without_estimate), (100, 2, 1)
        )
        self.assertEqual(gauge.estimators, ("chars-per-4-v1",))
        self.assertEqual(queued_tokens([]), [])

    def test_tc_g02_estimate_accuracy_uses_complete_usage_only(self) -> None:
        jobs = [
            JobRecord("eus2", "a", "test", token_estimate=110, input_tokens=100).with_status(
                "completed"
            ),
            JobRecord("eus2", "b", "test", token_estimate=200, input_tokens=100).with_status(
                "completed"
            ),
            JobRecord("eus2", "c", "test", token_estimate=100, input_tokens=90).with_status(
                "failed"
            ),
            JobRecord("eus2", "d", "test", token_estimate=100).with_status("completed"),
        ]
        accuracy = compare_estimates(jobs, tolerance=0.15)
        self.assertEqual(
            (accuracy.compared_jobs, accuracy.within_tolerance, accuracy.excluded_without_usage),
            (2, 1, 2),
        )
        self.assertAlmostEqual(accuracy.mean_abs_error_ratio or 0, 0.55)
        self.assertIsNone(compare_estimates([], 0.1).mean_abs_error_ratio)


class ApiPressureTests(unittest.TestCase):
    def test_independent_workers_emit_calls_without_a_timer_flush(self) -> None:
        with self.assertLogs("foundry_batch", "INFO") as logs:
            ApiCallCounter().record("eus2", "batch.retrieve", 200)
            ApiCallCounter().record("swc", "batch.list", 429)
        calls = [record for record in logs.records if record.batch_event == "api.calls"]
        self.assertEqual(sum(record.count for record in calls), 2)
        self.assertEqual({record.resource_key for record in calls}, {"eus2", "swc"})

    def test_counts_by_operation_and_class(self) -> None:
        counter = ApiCallCounter()
        for code in (200, 200, 429, 503, None, 404):
            counter.record("eus2", "batch.retrieve", code)
        counter.record("eus2", "files.upload", 200)
        self.assertEqual(counter.total(operation_prefix="batch."), 6)
        self.assertEqual(counter.total(status="429"), 1)
        self.assertEqual(counter.total(status="5xx"), 1)
        self.assertEqual(counter.total(status="network"), 1)
        self.assertEqual(counter.total(resource_key="other"), 0)
        self.assertEqual(len(counter.snapshot()), 6)
        self.assertEqual(len(counter.reset()), 6)
        self.assertEqual(counter.total(), 0)
        self.assertEqual([status_class(c) for c in (201, 400)], ["2xx", "4xx"])


if __name__ == "__main__":
    unittest.main()
