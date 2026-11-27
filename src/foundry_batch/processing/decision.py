"""Pure lifecycle decision: given the stored job and a new observation, what changes?

Kept free of I/O so every race and ordering rule is unit-testable (US-3.3, US-5.2, US-5.5).
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from enum import StrEnum
from typing import Any

from foundry_batch.domain.models import BatchSnapshot, JobRecord
from foundry_batch.domain.status import is_terminal, outcome_for, rank


class Source(StrEnum):
    WEBHOOK = "webhook"
    POLLING = "polling"
    SUBMISSION = "submission"


class DecisionKind(StrEnum):
    TERMINAL = "terminal"  # first terminal state: write it and an outbox item
    PROGRESS = "progress"  # non-terminal advance (no downstream event)
    DUPLICATE = "duplicate"  # same terminal outcome seen again (maybe from another source)
    STALE = "stale"  # older/equal non-terminal state: nothing to change except metadata
    CONFLICT = "conflict"  # different terminal evidence: needs an authoritative retrieve
    CORRECTION = "correction"  # authoritative retrieve overrides a previous terminal state


@dataclass(frozen=True)
class Observation:
    source: Source
    raw_status: str
    observed_at: datetime
    snapshot: BatchSnapshot | None = None
    authoritative: bool = False  # True only for a direct provider retrieve, never a list page
    receipt_id: str | None = None
    event_id: str | None = None
    webhook_id: str | None = None
    status_call_made: bool = False
    #: When the webhook reached the receiver (``observed_at`` is the processing time, which a
    #: deferral can delay). Used for first-webhook and terminal-observation times.
    received_at: datetime | None = None

    @property
    def seen_at(self) -> datetime:
        return self.received_at or self.observed_at


@dataclass(frozen=True)
class Decision:
    kind: DecisionKind
    record: JobRecord
    emit_event: bool = False
    late_event: bool = False


#: Provider counters that only grow while a job stays in one status.
MONOTONIC_COUNTERS: tuple[str, ...] = (
    "request_total",
    "request_completed",
    "request_failed",
    "input_tokens",
    "output_tokens",
)


def decide(job: JobRecord, obs: Observation) -> Decision:
    """Return the new record and whether a downstream event must be emitted."""
    updated = job
    if obs.snapshot is not None and rank(obs.snapshot.status) >= rank(job.raw_status):
        # Never let an older snapshot (e.g. a list page read before the final state was
        # committed) overwrite newer counts or details.
        updated = _with_snapshot(updated, obs.snapshot)
    if obs.status_call_made:
        updated = replace(updated, last_status_call_at=obs.observed_at)
    if obs.source is Source.WEBHOOK:
        updated = replace(
            updated,
            first_webhook_at=updated.first_webhook_at or obs.seen_at,
            webhook_event_count=updated.webhook_event_count + 1,
        )

    new_terminal = is_terminal(obs.raw_status)
    if job.is_terminal:
        if not new_terminal:
            return Decision(DecisionKind.STALE, updated)
        same = outcome_for(obs.raw_status) is not None and (
            outcome_for(obs.raw_status) == outcome_for(job.raw_status)
            and obs.raw_status == job.raw_status
        )
        if same:
            late = (
                obs.source is Source.WEBHOOK
                and job.terminal_source == Source.POLLING.value
                and job.first_webhook_at is None
                and (job.terminal_observed_at is None or obs.seen_at >= job.terminal_observed_at)
            )
            if late:
                # Polling found the final state first; the event arrived afterwards. This
                # resolves any missed-event classification without double counting (US-5.5).
                updated = replace(updated, late_event=True)
            return Decision(DecisionKind.DUPLICATE, updated, late_event=late)
        if not obs.authoritative:
            return Decision(
                DecisionKind.CONFLICT,
                replace(updated, conflict_count=job.conflict_count + 1),
            )
        corrected = replace(
            updated.with_status(obs.raw_status),
            conflict_count=job.conflict_count + 1,
            terminal_observed_at=obs.seen_at,
            terminal_source=obs.source.value,
        )
        return Decision(DecisionKind.CORRECTION, corrected, emit_event=True)

    if new_terminal:
        terminal = replace(
            updated.with_status(obs.raw_status),
            terminal_observed_at=obs.seen_at,
            terminal_source=obs.source.value,
        )
        return Decision(DecisionKind.TERMINAL, terminal, emit_event=True)

    if rank(obs.raw_status) >= rank(job.raw_status) and obs.raw_status != job.raw_status:
        return Decision(DecisionKind.PROGRESS, replace(updated, raw_status=obs.raw_status))
    return Decision(DecisionKind.STALE, updated)


def _with_snapshot(job: JobRecord, snapshot: BatchSnapshot) -> JobRecord:
    """Copy a snapshot that is not older than the stored status.

    Within one status the provider's counters only grow, so a stale read of the same status
    (a list page read before a newer retrieve was committed) never lowers them. A status change,
    including an authoritative final-state correction, takes the snapshot's counters as they are.
    """
    updated = job.with_snapshot(snapshot)
    if snapshot.status == job.raw_status:
        counters: dict[str, Any] = {
            name: _highest(getattr(job, name), getattr(updated, name))
            for name in MONOTONIC_COUNTERS
        }
        updated = replace(updated, **counters)
    if is_terminal(snapshot.status):
        # A provider object in a final state carries the final timestamps and counts.
        updated = replace(updated, details_complete=True)
    return updated


def _highest(stored: int | None, observed: int | None) -> int | None:
    if stored is None or observed is None:
        return observed if stored is None else stored
    return max(stored, observed)
