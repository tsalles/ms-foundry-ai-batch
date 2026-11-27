"""Indicators per resource/deployment, region and model over a UTC window (US-6.2).

Metric conventions:

* windows select jobs by terminal time (provider time, else observed time, flagged);
* percentiles use the nearest-rank method on per-job values (each job weighs the same);
* rates divide by the number of terminal jobs in the window; an empty denominator gives
  ``None`` (unavailable), never 0;
* normalised turnaround includes queueing time; it is not per-request latency or throughput.
"""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from foundry_batch.domain.models import JobRecord
from foundry_batch.metrics.lifecycle import LifecycleRecord

DAY = 24 * 3600


def percentile(values: Sequence[float], pct: float) -> float | None:
    """Nearest-rank percentile; ``None`` for an empty sample."""
    if not values:
        return None
    if not 0 < pct <= 100:
        raise ValueError("percentile must be in (0, 100]")
    ordered = sorted(values)
    rank = max(1, math.ceil(pct / 100 * len(ordered)))
    return ordered[rank - 1]


def _rate(numerator: int, denominator: int) -> float | None:
    return None if denominator == 0 else numerator / denominator


@dataclass(frozen=True)
class GroupKey:
    resource_key: str
    deployment: str
    region: str
    model: str


@dataclass(frozen=True)
class Distribution:
    count: int
    p50: float | None
    p95: float | None
    maximum: float | None

    @classmethod
    def of(cls, values: Sequence[float]) -> Distribution:
        return cls(
            count=len(values),
            p50=percentile(values, 50),
            p95=percentile(values, 95),
            maximum=max(values) if values else None,
        )


@dataclass(frozen=True)
class IndicatorSet:
    group: GroupKey
    window: str
    terminal_jobs: int
    completed: int
    failed: int
    expired: int
    cancelled: int
    completion_rate: float | None
    failure_rate: float | None
    expiration_rate: float | None
    cancellation_rate: float | None
    request_failure_rate: float | None
    share_above_24h: float | None
    unfinished_over_24h: int
    turnaround: Distribution
    time_to_start: Distribution
    per_request: Distribution
    per_million_tokens: Distribution
    turnaround_coverage: float | None
    request_failure_coverage: float | None
    flags: tuple[str, ...] = field(default_factory=tuple)


def _group(job: JobRecord | LifecycleRecord) -> GroupKey:
    return GroupKey(
        resource_key=job.resource_key,
        deployment=job.deployment or "unknown",
        region=job.region or "unknown",
        model=job.model or "unknown",
    )


def compute_indicators(
    terminal_jobs: Iterable[JobRecord],
    open_jobs: Iterable[JobRecord],
    *,
    now: datetime,
    window: timedelta,
    window_name: str,
) -> list[IndicatorSet]:
    start = now - window
    records: dict[GroupKey, list[LifecycleRecord]] = defaultdict(list)
    for job in terminal_jobs:
        record = LifecycleRecord.from_job(job)
        if record.terminal_at is None or not start <= record.terminal_at <= now:
            continue
        records[_group(record)].append(record)

    unfinished: dict[GroupKey, int] = defaultdict(int)
    for job in open_jobs:
        started = job.provider_created_at or job.submitted_at
        if started is not None and (now - started).total_seconds() > DAY:
            unfinished[_group(job)] += 1

    result: list[IndicatorSet] = []
    for group in sorted(set(records) | set(unfinished), key=lambda g: tuple(vars(g).values())):
        items = records.get(group, [])
        n = len(items)
        by_status: dict[str, int] = defaultdict(int)
        for item in items:
            by_status[item.raw_status] += 1
        turnaround = [r.turnaround_s for r in items if r.turnaround_s is not None]
        flags: list[str] = []
        if any(r.terminal_at_provenance == "observed" for r in items):
            flags.append("observed_terminal_times_included")
        if n and len(turnaround) < n:
            flags.append("turnaround_missing_for_some_jobs")
        counts = [
            (record.requests_total, record.requests_failed)
            for record in items
            if record.requests_total is not None
            and record.requests_failed is not None
            and 0 <= record.requests_failed <= record.requests_total
        ]
        if len(counts) < n:
            flags.append("request_counts_missing_or_invalid")
        total_requests = sum(total for total, _ in counts)
        failed_requests = sum(failed for _, failed in counts)
        result.append(
            IndicatorSet(
                group=group,
                window=window_name,
                terminal_jobs=n,
                completed=by_status["completed"],
                failed=by_status["failed"],
                expired=by_status["expired"],
                cancelled=by_status["cancelled"],
                completion_rate=_rate(by_status["completed"], n),
                failure_rate=_rate(by_status["failed"], n),
                expiration_rate=_rate(by_status["expired"], n),
                cancellation_rate=_rate(by_status["cancelled"], n),
                request_failure_rate=_rate(failed_requests, total_requests),
                share_above_24h=_rate(sum(1 for t in turnaround if t > DAY), len(turnaround)),
                unfinished_over_24h=unfinished.get(group, 0),
                turnaround=Distribution.of(turnaround),
                time_to_start=Distribution.of(
                    [r.time_to_start_s for r in items if r.time_to_start_s is not None]
                ),
                per_request=Distribution.of(
                    [r.per_request_s for r in items if r.per_request_s is not None]
                ),
                per_million_tokens=Distribution.of(
                    [r.per_million_tokens_s for r in items if r.per_million_tokens_s is not None]
                ),
                turnaround_coverage=_rate(len(turnaround), n),
                request_failure_coverage=_rate(len(counts), n),
                flags=tuple(flags),
            )
        )
    return result


STANDARD_WINDOWS: tuple[tuple[str, timedelta], ...] = (
    ("24h", timedelta(hours=24)),
    ("7d", timedelta(days=7)),
    ("30d", timedelta(days=30)),
)
