"""Authorised, audited replay (US-4.1 AC3 and AC5).

Replay never creates new business keys: it re-arms existing outbox items (quarantined, or
delivered within a time range) or moves poison-queue messages back to their source queue.
Every action is logged with the operator and the reason.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, replace
from datetime import datetime

from foundry_batch.clock import Clock
from foundry_batch.errors import ConcurrencyConflictError
from foundry_batch.observability import log_event
from foundry_batch.ports import QueueAdmin
from foundry_batch.processing.registry import (
    OUTBOX_DELIVERED,
    OUTBOX_PENDING,
    OUTBOX_QUARANTINED,
    JobRegistry,
    StoredOutbox,
)

logger = logging.getLogger(__name__)

#: Largest selection one replay may re-arm without an explicit override.
MAX_REPLAY_ITEMS = 1000


@dataclass(frozen=True)
class ReplayReport:
    selected: int
    rearmed: int
    conflicts: int
    dry_run: bool


class ReplayService:
    def __init__(self, registry: JobRegistry, clock: Clock) -> None:
        self._registry = registry
        self._clock = clock

    def replay_outbox(
        self,
        *,
        operator: str,
        reason: str,
        status: str = OUTBOX_QUARANTINED,
        since: datetime | None = None,
        until: datetime | None = None,
        dry_run: bool = True,
        max_items: int = MAX_REPLAY_ITEMS,
    ) -> ReplayReport:
        """Re-arm items in ``status`` whose relevant time falls in ``[since, until)``.

        Delivered items are selected by ``delivered_at`` (the downstream lost data received in
        that window), quarantined items by ``created_at``. Every page is read; a selection
        larger than ``max_items`` is refused instead of being truncated silently.
        """
        if not operator.strip() or not reason.strip():
            raise ValueError("replay needs an operator and a reason for the audit trail")
        if status not in (OUTBOX_QUARANTINED, OUTBOX_DELIVERED):
            raise ValueError("only quarantined or delivered items can be replayed")
        for bound in (since, until):
            if bound is not None and bound.tzinfo is None:
                raise ValueError("since/until must be timezone-aware")

        def moment(stored: StoredOutbox) -> datetime:
            if status == OUTBOX_DELIVERED and stored.item.delivered_at is not None:
                return stored.item.delivered_at
            return stored.item.created_at

        items = [
            s
            for s in self._registry.outbox_items(status, limit=None)
            if (since is None or moment(s) >= since) and (until is None or moment(s) < until)
        ]
        if len(items) > max_items:
            raise ValueError(
                f"{len(items)} items match; narrow the window or raise max_items ({max_items})"
            )
        rearmed = conflicts = 0
        now = self._clock.now()
        for stored in items:
            if dry_run:
                continue
            item = replace(
                stored.item,
                status=OUTBOX_PENDING,
                attempts=0,
                next_attempt_at=now,
                replay_count=stored.item.replay_count + 1,
            )
            try:
                self._registry.save_outbox(stored, item)
                rearmed += 1
            except ConcurrencyConflictError:
                conflicts += 1
        log_event(
            logger,
            "relay.replay_requested",
            level=logging.WARNING,
            operator=operator,
            reason=reason[:200],
            status=status,
            selected=len(items),
            rearmed=rearmed,
            conflicts=conflicts,
            dry_run=dry_run,
        )
        return ReplayReport(len(items), rearmed, conflicts, dry_run)


@dataclass(frozen=True)
class PoisonReplayReport:
    selected: int
    moved: int
    dry_run: bool


def replay_poison_queue(
    admin: QueueAdmin,
    *,
    poison_queue: str,
    target_queue: str,
    operator: str,
    reason: str,
    max_messages: int = 32,
    dry_run: bool = True,
) -> PoisonReplayReport:
    """Move messages from ``poison_queue`` back to ``target_queue`` (send first, then delete)."""
    if not operator.strip() or not reason.strip():
        raise ValueError("replay needs an operator and a reason for the audit trail")
    if dry_run:
        # Peek: a dry run must not hide the messages from the --execute run that follows.
        messages = admin.peek(poison_queue, max_messages=max_messages)
    else:
        messages = admin.receive(poison_queue, max_messages=max_messages, visibility_timeout=300)
    moved = 0
    for message in messages:
        if dry_run:
            continue
        admin.send_to(target_queue, message.content)
        admin.delete(poison_queue, message)
        moved += 1
    log_event(
        logger,
        "relay.poison_replayed",
        level=logging.WARNING,
        operator=operator,
        reason=reason[:200],
        queue=poison_queue,
        selected=len(messages),
        moved=moved,
        dry_run=dry_run,
    )
    return PoisonReplayReport(len(messages), moved, dry_run)
