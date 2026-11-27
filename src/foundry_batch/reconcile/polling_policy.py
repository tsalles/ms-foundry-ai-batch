"""Adaptive polling reference policy and its simulation (US-5.3).

The policy is a pure function of job age and coverage. It is a *reference* for Batch's
scheduled jobs and a simulation tool; by default it does not run as a second live poller next
to the reconciliation sweep (US-5.3 AC6).

Starting values (to be tuned with data): first check after 5 minutes, interval doubling up to
60 minutes for webhook-covered jobs, tighter checks once a job is older than the deployment's
P95 turnaround (or 24 h until at least 20 terminal jobs exist), and never under 60 seconds.
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Sequence
from dataclasses import dataclass

from foundry_batch.config import STATUS_CALL_FLOOR_SECONDS


@dataclass(frozen=True)
class PollingPolicy:
    first_check_seconds: float = 300.0
    growth_factor: float = 2.0
    max_interval_seconds: float = 3600.0
    suspicion_interval_seconds: float = 300.0
    uncovered_interval_seconds: float = 60.0
    min_samples_for_p95: int = 20
    default_suspicion_age_seconds: float = 24 * 3600.0
    floor_seconds: float = STATUS_CALL_FLOOR_SECONDS

    def __post_init__(self) -> None:
        if self.floor_seconds < STATUS_CALL_FLOOR_SECONDS:
            raise ValueError("the status-call floor cannot be below 60 seconds")
        if self.growth_factor < 1.0:
            raise ValueError("growth factor must be >= 1")

    def suspicion_age(self, p95_turnaround_seconds: float | None, samples: int) -> float:
        if p95_turnaround_seconds is None or samples < self.min_samples_for_p95:
            return self.default_suspicion_age_seconds
        return p95_turnaround_seconds

    def next_interval(
        self,
        *,
        job_age_seconds: float,
        checks_done: int,
        webhook_covered: bool,
        p95_turnaround_seconds: float | None = None,
        samples: int = 0,
    ) -> float:
        """Seconds until the next status check for a job that is not final yet."""
        if not webhook_covered:
            return max(self.floor_seconds, self.uncovered_interval_seconds)
        if job_age_seconds >= self.suspicion_age(p95_turnaround_seconds, samples):
            return max(self.floor_seconds, self.suspicion_interval_seconds)
        if checks_done == 0:
            return max(self.floor_seconds, self.first_check_seconds)
        # Cap the exponent: growth**1024 overflows a float, and 64 doublings already exceed
        # any max interval, so the cap never changes a result.
        interval = self.first_check_seconds * (self.growth_factor ** min(checks_done, 64))
        return max(self.floor_seconds, min(self.max_interval_seconds, interval))


@dataclass(frozen=True)
class SimulationResult:
    jobs: int
    total_calls: int
    calls_per_job: float
    max_detection_delay_seconds: float
    mean_detection_delay_seconds: float


def simulate(
    durations_seconds: Sequence[float],
    policy: PollingPolicy | None,
    *,
    webhook_covered: bool = True,
    fixed_interval_seconds: float = STATUS_CALL_FLOOR_SECONDS,
    p95_turnaround_seconds: float | None = None,
    samples: int = 0,
) -> SimulationResult:
    """Count status calls until each job's final state is observed.

    ``policy=None`` simulates fixed-interval polling (today's baseline). Detection delay is the
    time between the job's final state and the check that observes it. With webhooks the event
    normally arrives first; the simulation shows the cost of the safety net alone.
    """
    if fixed_interval_seconds < STATUS_CALL_FLOOR_SECONDS:
        raise ValueError("fixed interval cannot be below the 60-second floor")
    total = 0
    delays: list[float] = []
    for duration in durations_seconds:
        if not math.isfinite(duration) or duration < 0:
            raise ValueError("durations must be finite, non-negative numbers")
        t = 0.0
        checks = 0
        while True:
            if policy is None:
                step = fixed_interval_seconds
            else:
                step = policy.next_interval(
                    job_age_seconds=t,
                    checks_done=checks,
                    webhook_covered=webhook_covered,
                    p95_turnaround_seconds=p95_turnaround_seconds,
                    samples=samples,
                )
            t += step
            checks += 1
            if t >= duration:
                delays.append(t - duration)
                break
        total += checks
    jobs = len(durations_seconds)
    return SimulationResult(
        jobs=jobs,
        total_calls=total,
        calls_per_job=(total / jobs) if jobs else 0.0,
        max_detection_delay_seconds=max(delays) if delays else 0.0,
        mean_detection_delay_seconds=(sum(delays) / len(delays)) if delays else 0.0,
    )


def reduction(baseline: SimulationResult, candidate: SimulationResult) -> float | None:
    """Fractional reduction of status calls (0.8 means 80% fewer). ``None`` when undefined."""
    if baseline.total_calls == 0:
        return None
    return 1.0 - candidate.total_calls / baseline.total_calls


def load_durations(lines: Iterable[str]) -> list[float]:
    """Parse one duration in seconds per line, ignoring blanks and ``#`` comments."""
    values: list[float] = []
    for number, line in enumerate(lines, start=1):
        text = line.strip()
        if not text or text.startswith("#"):
            continue
        value = float(text)
        if not math.isfinite(value) or value < 0:
            raise ValueError(f"line {number}: duration must be finite and non-negative")
        values.append(value)
    return values
