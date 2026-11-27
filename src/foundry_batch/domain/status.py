"""Batch statuses, their ordering and the normalised outcome (US-3.4 AC2)."""

from __future__ import annotations

from enum import StrEnum


class BatchStatus(StrEnum):
    VALIDATING = "validating"
    FAILED = "failed"
    IN_PROGRESS = "in_progress"
    FINALIZING = "finalizing"
    COMPLETED = "completed"
    EXPIRED = "expired"
    CANCELLING = "cancelling"
    CANCELLED = "cancelled"


class Outcome(StrEnum):
    """Normalised final outcome. ``failed`` and ``expired`` share ``FAILED``; raw status is kept."""

    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


TERMINAL_STATUSES = frozenset(
    {BatchStatus.COMPLETED, BatchStatus.FAILED, BatchStatus.EXPIRED, BatchStatus.CANCELLED}
)

#: Lifecycle order for monotonic updates. Known non-terminal statuses are strictly ordered, so
#: two reads can never flip a job back and forth. Decision: ``cancelling`` ranks above
#: ``finalizing``. A cancel may race the provider's finalization (finalizing -> cancelling is
#: accepted), but a job that reached ``cancelling`` only leaves it for a final state, so a later
#: ``finalizing`` read is stale and ignored. Final states share the top rank; changes between
#: them follow the conflict and correction rules in ``processing.decision``.
_RANK = {
    BatchStatus.VALIDATING: 10,
    BatchStatus.IN_PROGRESS: 20,
    BatchStatus.FINALIZING: 30,
    BatchStatus.CANCELLING: 40,
    BatchStatus.COMPLETED: 100,
    BatchStatus.FAILED: 100,
    BatchStatus.EXPIRED: 100,
    BatchStatus.CANCELLED: 100,
}

_OUTCOME = {
    BatchStatus.COMPLETED: Outcome.COMPLETED,
    BatchStatus.FAILED: Outcome.FAILED,
    BatchStatus.EXPIRED: Outcome.FAILED,
    BatchStatus.CANCELLED: Outcome.CANCELLED,
}

EVENT_TYPE_TO_STATUS = {
    "batch.completed": BatchStatus.COMPLETED,
    "batch.failed": BatchStatus.FAILED,
    "batch.cancelled": BatchStatus.CANCELLED,
    "batch.expired": BatchStatus.EXPIRED,
}

#: Field on the provider batch object that holds the time a terminal status was reached.
TERMINAL_TIMESTAMP_FIELD = {
    BatchStatus.COMPLETED: "completed_at",
    BatchStatus.FAILED: "failed_at",
    BatchStatus.EXPIRED: "expired_at",
    BatchStatus.CANCELLED: "cancelled_at",
}


def parse_status(raw: object) -> BatchStatus | None:
    """Return the known status or ``None``. Unknown values are preserved by callers as text."""
    if not isinstance(raw, str):
        return None
    try:
        return BatchStatus(raw.strip().lower())
    except ValueError:
        return None


def is_terminal(raw: str | None) -> bool:
    status = parse_status(raw)
    return status in TERMINAL_STATUSES if status else False


def rank(raw: str | None) -> int:
    status = parse_status(raw)
    return _RANK[status] if status else 0


def outcome_for(raw: str | None) -> Outcome | None:
    status = parse_status(raw)
    return _OUTCOME.get(status) if status else None
