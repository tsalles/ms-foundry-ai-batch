"""Queued-token estimate per deployment (US-6.3).

The gauge is recomputed from the registry on every read (sum over tracked non-terminal jobs),
so duplicate or late final states, restarts and backfills cannot double-decrement it or make
it negative. It is an *input-token estimate* for tracked jobs only, not the provider's quota.
"""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, Protocol

from foundry_batch.domain.models import JobRecord
from foundry_batch.observability import log_event

logger = logging.getLogger(__name__)


class TokenEstimator(Protocol):
    name: str

    def estimate(self, jsonl: bytes) -> int: ...


def _texts(value: Any) -> Iterable[str]:
    if isinstance(value, str):
        yield value
    elif isinstance(value, list):
        for item in value:
            yield from _texts(item)
    elif isinstance(value, dict):
        for key in ("content", "text", "input", "instructions"):
            if key in value:
                yield from _texts(value[key])


class CharacterHeuristicEstimator:
    """~4 characters per token plus a small per-message overhead. Cheap and dependency-free.

    Replace with a tokenizer matched to the pinned model (T-6.3.1) once chosen; the name is
    stored with every estimate so results stay comparable.
    """

    name = "chars-per-4-v1"

    def __init__(self, chars_per_token: float = 4.0, per_message_overhead: int = 4) -> None:
        self._ratio = chars_per_token
        self._overhead = per_message_overhead
        self.last_skipped_lines = 0

    def estimate(self, jsonl: bytes) -> int:
        total = 0
        skipped = 0
        for raw_line in jsonl.splitlines():
            line = raw_line.strip()
            if not line:
                continue
            try:
                request = json.loads(line)
            except ValueError:  # invalid JSON or invalid UTF-8: the provider rejects it too
                skipped += 1
                continue
            body = request.get("body", {}) if isinstance(request, dict) else {}
            messages = body.get("messages") if isinstance(body, dict) else None
            source = messages if messages is not None else body.get("input", "")
            chars = sum(len(t) for t in _texts(source))
            count = len(messages) if isinstance(messages, list) else 1
            total += int(chars / self._ratio) + self._overhead * count
        self.last_skipped_lines = skipped
        if skipped:
            log_event(
                logger,
                "estimate.skipped_lines",
                level=logging.WARNING,
                estimator=self.name,
                skipped_lines=skipped,
            )
        return total


@dataclass(frozen=True)
class QueuedTokens:
    resource_key: str
    deployment: str
    estimated_tokens: int
    jobs: int
    jobs_without_estimate: int
    estimators: tuple[str, ...]
    label: str = "estimate, tracked jobs only"


def queued_tokens(open_jobs: Iterable[JobRecord]) -> list[QueuedTokens]:
    sums: dict[tuple[str, str], int] = defaultdict(int)
    jobs: dict[tuple[str, str], int] = defaultdict(int)
    missing: dict[tuple[str, str], int] = defaultdict(int)
    estimators: dict[tuple[str, str], set[str]] = defaultdict(set)
    for job in open_jobs:
        if job.is_terminal:
            continue
        key = (job.resource_key, job.deployment or "unknown")
        jobs[key] += 1
        if job.token_estimate is None or job.token_estimate < 0:
            missing[key] += 1
            continue
        sums[key] += job.token_estimate
        if job.tokenizer:
            estimators[key].add(job.tokenizer)
    return [
        QueuedTokens(
            resource_key=k[0],
            deployment=k[1],
            estimated_tokens=sums[k],
            jobs=jobs[k],
            jobs_without_estimate=missing[k],
            estimators=tuple(sorted(estimators[k])),
        )
        for k in sorted(jobs)
    ]


@dataclass(frozen=True)
class EstimateAccuracy:
    compared_jobs: int
    within_tolerance: int
    mean_abs_error_ratio: float | None
    excluded_without_usage: int


def compare_estimates(terminal_jobs: Iterable[JobRecord], tolerance: float) -> EstimateAccuracy:
    """Compare estimates with reported input tokens on completed jobs with usable usage only."""
    errors: list[float] = []
    excluded = 0
    for job in terminal_jobs:
        if job.raw_status != "completed" or not job.input_tokens or job.token_estimate is None:
            excluded += 1
            continue
        errors.append(abs(job.token_estimate - job.input_tokens) / job.input_tokens)
    return EstimateAccuracy(
        compared_jobs=len(errors),
        within_tolerance=sum(1 for e in errors if e <= tolerance),
        mean_abs_error_ratio=(sum(errors) / len(errors)) if errors else None,
        excluded_without_usage=excluded,
    )
