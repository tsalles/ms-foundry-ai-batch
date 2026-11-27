"""Delivers outbox items downstream until acknowledged (US-4.1).

* An item is marked delivered only after a 2xx from the downstream.
* 408/425/429/5xx and network failures are retried with bounded exponential backoff, jitter and
  Retry-After; other 4xx are permanent and quarantined for triage.
* Every attempt reuses the item's idempotency key, so a timeout after the downstream committed
  is safe as long as the downstream deduplicates on that key (demo API does; Batch must agree).
"""

from __future__ import annotations

import json
import logging
import random
from dataclasses import dataclass, replace
from datetime import timedelta

from foundry_batch.clock import Clock
from foundry_batch.config import Settings
from foundry_batch.errors import ConcurrencyConflictError
from foundry_batch.observability import log_event
from foundry_batch.ports import DeliveryResult, Downstream
from foundry_batch.processing.registry import (
    OUTBOX_DELIVERED,
    OUTBOX_PENDING,
    OUTBOX_QUARANTINED,
    JobRegistry,
    OutboxItem,
    StoredOutbox,
)
from foundry_batch.relay.backoff import backoff_delay

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DispatchResult:
    status: str  # delivered | retry_scheduled | quarantined | skipped | not_found | conflict
    attempts: int = 0


class OutboxDispatcher:
    def __init__(
        self,
        settings: Settings,
        registry: JobRegistry,
        downstream: Downstream,
        clock: Clock,
        *,
        rng: random.Random | None = None,
    ) -> None:
        self._settings = settings
        self._registry = registry
        self._downstream = downstream
        self._clock = clock
        self._rng = rng or random.SystemRandom()

    def dispatch_nudge(self, message: str) -> DispatchResult:
        """Handle one relay-queue message ``{"pk": ..., "key": ...}``."""
        try:
            data = json.loads(message)
            partition, key = str(data["pk"]), str(data["key"])
        except (ValueError, KeyError, TypeError):
            log_event(logger, "relay.bad_nudge", level=logging.WARNING)
            return DispatchResult("skipped")
        stored = self._registry.load_outbox(partition, key)
        if stored is None:
            return DispatchResult("not_found")
        return self.dispatch(stored)

    def dispatch_due(self, limit: int = 100) -> list[DispatchResult]:
        """Timer path: deliver every pending item whose next attempt is due."""
        due = self._registry.due_outbox(self._clock.now(), limit)
        results: list[DispatchResult] = []
        for item in due:
            try:
                results.append(self.dispatch(item))
            except Exception as exc:  # noqa: BLE001 - one bad item must not stop the batch
                log_event(
                    logger,
                    "relay.dispatch_error",
                    level=logging.ERROR,
                    idempotency_key=item.item.idempotency_key,
                    error_type=type(exc).__name__,
                )
                results.append(DispatchResult("error", item.item.attempts))
        return results

    def dispatch(self, stored: StoredOutbox) -> DispatchResult:
        item = stored.item
        now = self._clock.now()
        if item.status != OUTBOX_PENDING:
            return DispatchResult("skipped", item.attempts)
        if item.next_attempt_at is not None and item.next_attempt_at > now:
            return DispatchResult("skipped", item.attempts)

        # Claim the attempt first (ETag) so two workers never send the same item concurrently;
        # the claim pushes next_attempt_at out by the per-attempt timeout window.
        claimed = replace(
            item,
            attempts=item.attempts + 1,
            next_attempt_at=now + timedelta(seconds=60),
        )
        try:
            etag = self._registry.save_outbox(stored, claimed)
        except ConcurrencyConflictError:
            return DispatchResult("conflict", item.attempts)
        stored = StoredOutbox(stored.partition_key, claimed, etag)

        try:
            result = self._downstream.deliver(
                item.event_json.encode("utf-8"), idempotency_key=item.idempotency_key
            )
        except Exception as exc:  # noqa: BLE001 - keep the attempt accounting intact
            result = DeliveryResult(status_code=None, error=f"deliver_{type(exc).__name__}")
        now = self._clock.now()
        if result.ok:
            final = replace(
                claimed,
                status=OUTBOX_DELIVERED,
                delivered_at=now,
                last_status_code=result.status_code,
                last_error=None,
                next_attempt_at=None,
            )
            self._save(stored, final)
            log_event(
                logger,
                "relay.delivered",
                idempotency_key=item.idempotency_key,
                attempts=claimed.attempts,
                status_code=result.status_code,
                source=item.source,
                latency_s=round((now - item.created_at).total_seconds(), 3),
            )
            return DispatchResult("delivered", claimed.attempts)

        permanent = not result.retryable
        # Transient failures are quarantined only after BOTH the attempt count and the retry
        # horizon: with full jitter, 12 attempts alone span about half an hour on average, so a
        # one-hour downstream outage (TC-D05) would otherwise need a manual replay.
        age = (now - item.created_at).total_seconds()
        exhausted = (
            claimed.attempts >= self._settings.outbox_max_attempts
            and age >= self._settings.outbox_retry_horizon_seconds
        )
        if permanent or exhausted:
            final = replace(
                claimed,
                status=OUTBOX_QUARANTINED,
                last_status_code=result.status_code,
                last_error=(result.error or "")[:200] or None,
                next_attempt_at=None,
            )
            self._save(stored, final)
            log_event(
                logger,
                "relay.quarantined",
                level=logging.ERROR,
                idempotency_key=item.idempotency_key,
                attempts=claimed.attempts,
                status_code=result.status_code,
                reason="permanent" if permanent else "attempts_exhausted",
            )
            return DispatchResult("quarantined", claimed.attempts)

        delay = backoff_delay(
            claimed.attempts,
            base_seconds=self._settings.outbox_base_delay_seconds,
            max_seconds=self._settings.outbox_max_delay_seconds,
            rng=self._rng,
            retry_after_seconds=result.retry_after_seconds,
        )
        retry = replace(
            claimed,
            next_attempt_at=now + timedelta(seconds=delay),
            last_status_code=result.status_code,
            last_error=(result.error or "")[:200] or None,
        )
        self._save(stored, retry)
        log_event(
            logger,
            "relay.retry_scheduled",
            level=logging.WARNING,
            idempotency_key=item.idempotency_key,
            attempts=claimed.attempts,
            status_code=result.status_code,
            delay_s=round(delay, 1),
        )
        return DispatchResult("retry_scheduled", claimed.attempts)

    def _save(self, stored: StoredOutbox, item: OutboxItem) -> None:
        try:
            self._registry.save_outbox(stored, item)
        except ConcurrencyConflictError:
            # Someone else (operator replay) changed the row after our claim. The downstream
            # call already happened with the stable key, so a later retry is harmless.
            log_event(
                logger,
                "relay.save_conflict",
                level=logging.WARNING,
                idempotency_key=item.idempotency_key,
            )
