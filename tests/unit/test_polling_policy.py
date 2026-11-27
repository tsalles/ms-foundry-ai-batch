import unittest

from foundry_batch.reconcile.polling_policy import (
    PollingPolicy,
    load_durations,
    reduction,
    simulate,
)


class PolicyTests(unittest.TestCase):
    policy = PollingPolicy()

    def test_tc_e03_interval_grows_to_cap_and_never_below_floor(self) -> None:
        intervals = [
            self.policy.next_interval(job_age_seconds=0, checks_done=n, webhook_covered=True)
            for n in range(8)
        ]
        self.assertEqual(intervals[:5], [300, 600, 1200, 2400, 3600])
        self.assertTrue(all(i >= 60 for i in intervals))
        self.assertEqual(max(intervals), 3600)

    def test_suspicion_tightens_after_p95_or_default_age(self) -> None:
        kwargs = {"checks_done": 5, "webhook_covered": True}
        self.assertEqual(
            self.policy.next_interval(
                job_age_seconds=3600, p95_turnaround_seconds=1800, samples=20, **kwargs
            ),
            300,
        )
        # Too few samples: the documented 24 h default applies, not an undefined P95.
        self.assertEqual(
            self.policy.next_interval(
                job_age_seconds=3600, p95_turnaround_seconds=1800, samples=5, **kwargs
            ),
            3600,
        )
        self.assertEqual(self.policy.next_interval(job_age_seconds=86_400, **kwargs), 300)

    def test_uncovered_jobs_use_the_baseline_interval(self) -> None:
        self.assertEqual(
            self.policy.next_interval(job_age_seconds=0, checks_done=3, webhook_covered=False), 60
        )

    def test_invalid_policies_are_rejected(self) -> None:
        with self.assertRaises(ValueError):
            PollingPolicy(floor_seconds=30)
        with self.assertRaises(ValueError):
            PollingPolicy(growth_factor=0.5)
        tight = PollingPolicy(first_check_seconds=10, suspicion_interval_seconds=1)
        self.assertEqual(
            tight.next_interval(job_age_seconds=0, checks_done=0, webhook_covered=True), 60
        )


class PolicyOverflowTests(unittest.TestCase):
    def test_huge_check_counts_stay_at_the_cap(self) -> None:
        policy = PollingPolicy()
        for checks in (64, 1023, 1024, 10_000):
            with self.subTest(checks=checks):
                interval = policy.next_interval(
                    job_age_seconds=0, checks_done=checks, webhook_covered=True
                )
                self.assertEqual(interval, policy.max_interval_seconds)

    def test_simulating_a_very_long_job_does_not_overflow(self) -> None:
        policy = PollingPolicy(default_suspicion_age_seconds=60 * 86_400)
        result = simulate([45 * 86_400.0], policy)
        self.assertEqual(result.jobs, 1)
        self.assertGreater(result.total_calls, 1_000)


class SimulationTests(unittest.TestCase):
    def test_reduction_against_fixed_60s_polling(self) -> None:
        durations = [3600.0, 4 * 3600.0, 8 * 3600.0, 20 * 3600.0]
        baseline = simulate(durations, None)
        adaptive = simulate(durations, PollingPolicy())
        self.assertEqual(baseline.total_calls, 60 + 240 + 480 + 1200)
        self.assertLess(adaptive.total_calls, baseline.total_calls)
        ratio = reduction(baseline, adaptive)
        assert ratio is not None
        self.assertGreater(ratio, 0.8)
        self.assertGreater(
            adaptive.max_detection_delay_seconds, baseline.max_detection_delay_seconds
        )

    def test_short_jobs_may_not_reach_80_percent(self) -> None:
        durations = [120.0, 240.0]
        ratio = reduction(simulate(durations, None), simulate(durations, PollingPolicy()))
        assert ratio is not None
        self.assertLess(ratio, 0.8)

    def test_edge_cases(self) -> None:
        empty = simulate([], PollingPolicy())
        self.assertEqual((empty.jobs, empty.calls_per_job), (0, 0.0))
        self.assertIsNone(reduction(empty, empty))
        with self.assertRaises(ValueError):
            simulate([-1.0], None)
        with self.assertRaises(ValueError):
            simulate([1.0], None, fixed_interval_seconds=10)
        self.assertEqual(simulate([0.0], None).total_calls, 1)
        self.assertEqual(
            simulate([5000.0], PollingPolicy(), webhook_covered=False).calls_per_job, 84
        )

    def test_load_durations(self) -> None:
        self.assertEqual(load_durations(["# header", "", "10", " 20.5 "]), [10.0, 20.5])


if __name__ == "__main__":
    unittest.main()
