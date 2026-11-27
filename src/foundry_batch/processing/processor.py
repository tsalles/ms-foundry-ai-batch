"""Applies observations (webhook envelopes or polling results) to the job registry.

One code path for every source (plan section 3.4 item 4): reconciliation, webhooks and replay
share the same transaction and idempotency key. Every change commits the job row, the
deduplication receipt and the outbox item in one atomic batch; the relay nudge is best effort
because the outbox timer delivers anything a lost nudge leaves behind.
"""

from __future__ import annotations

import contextlib
import json
import logging
import math
from dataclasses import dataclass, replace
from datetime import datetime

from foundry_batch.clock import Clock, to_iso
from foundry_batch.config import Settings
from foundry_batch.domain.contract import NormalizedEvent
from foundry_batch.domain.models import BatchSnapshot, JobKey, JobRecord, safe_key, utf16_prefix
from foundry_batch.domain.status import EVENT_TYPE_TO_STATUS, is_terminal
from foundry_batch.errors import (
    ConcurrencyConflictError,
    ProviderError,
    ProviderNotFoundError,
    ProviderRetryableError,
    QueueUnavailableError,
    RetryLater,
    StoreUnavailableError,
)
from foundry_batch.metrics.lifecycle import LifecycleRecord
from foundry_batch.observability import log_event
from foundry_batch.ports import BatchApiFactory, Entity, MessageQueue, TableStore, TxOp
from foundry_batch.processing.decision import (
    Decision,
    DecisionKind,
    Observation,
    Source,
    decide,
)
from foundry_batch.processing.registry import JobRegistry, OutboxItem, Receipt, StoredJob
from foundry_batch.webhook.envelope import Envelope, EnvelopeError

logger = logging.getLogger(__name__)

MAX_COMMIT_ATTEMPTS = 5
MAX_ENRICHMENT_ATTEMPTS = 3
MAX_DEFERRALS = 10
#: Longest single deferral. Azure caps a visibility delay at 7 days and below the message
#: time-to-live (14 days, see ``adapters.azure_queues``).
MAX_DEFERRAL_SECONDS = 24 * 3600
#: Azure Table limits counted in UTF-16 code units: string properties hold 64 KiB (32,768
#: units) and keys 1 KiB (512 units). Quarantined text is cut below them, never split mid-pair.
MAX_QUARANTINE_TEXT = 30_000
MAX_QUARANTINE_FIELD = 1_000
MAX_QUARANTINE_PARTITION_KEY = 200
MAX_QUARANTINE_ROW_KEY = 500
ENRICHMENT_RETRY = "enrichment_retry"
UNKNOWN_JOB_PENDING = "unknown_job_pending"
STORE_UNAVAILABLE = "store_unavailable"
#: Unknown-job back-off: 1, 3, 9, 27... minutes, capped by the remaining grace. The default
#: 30-minute grace takes 4 deferrals and the 6-hour maximum 6, leaving room in
#: ``MAX_DEFERRALS`` for pacing deferrals once the job is linked.
UNKNOWN_JOB_FIRST_DELAY_SECONDS = 60
UNKNOWN_JOB_BACKOFF_FACTOR = 3
#: Table outage back-off: 15 s doubling up to 10 minutes; ``MAX_DEFERRALS`` spans about 55 min.
STORE_RETRY_FIRST_DELAY_SECONDS = 15
STORE_RETRY_MAX_DELAY_SECONDS = 600


@dataclass(frozen=True)
class ProcessResult:
    kind: str
    job_key: JobKey | None = None
    outbox_key: str | None = None


@dataclass(frozen=True)
class StatusCallResult:
    """The one status call a message pass makes: the snapshot, or the provider error."""

    snapshot: BatchSnapshot | None = None
    error: ProviderError | None = None


def relay_nudge(partition_key: str, idempotency_key: str) -> str:
    return json.dumps({"pk": partition_key, "key": idempotency_key}, separators=(",", ":"))


def store_retry_delay(attempt: int) -> int:
    """Back-off before retrying a message after a Table outage (``attempt`` deferrals so far)."""
    growth: int = 2 ** min(attempt, MAX_DEFERRALS)
    return min(STORE_RETRY_FIRST_DELAY_SECONDS * growth, STORE_RETRY_MAX_DELAY_SECONDS)


def unknown_job_delay(attempt: int, remaining_seconds: float) -> float:
    """Back-off for an event whose job is not linked yet, never past the end of the grace."""
    growth: int = UNKNOWN_JOB_BACKOFF_FACTOR ** min(attempt, MAX_DEFERRALS)
    return min(UNKNOWN_JOB_FIRST_DELAY_SECONDS * growth, remaining_seconds)


class EventProcessor:
    def __init__(
        self,
        settings: Settings,
        registry: JobRegistry,
        store: TableStore,
        batch_apis: BatchApiFactory,
        relay_queue: MessageQueue,
        clock: Clock,
        *,
        incoming_queue: MessageQueue | None = None,
    ) -> None:
        self._settings = settings
        self._registry = registry
        self._store = store
        self._apis = batch_apis
        self._relay_queue = relay_queue
        self._incoming_queue = incoming_queue
        self._clock = clock

    # -- entry points ---------------------------------------------------------------------

    def retrieve_snapshot(self, key: JobKey) -> BatchSnapshot:
        self._registry.reserve_status_call(key)
        return self._apis.for_resource(key.resource_key).retrieve_batch(key.batch_id)

    def handle_queue_message(self, text: str) -> ProcessResult:
        """Queue-trigger entry point: defers with a visibility delay instead of failing.

        Pacing (``RetryLater``) and a Table outage (``StoreUnavailableError``, with back-off)
        are deferred: the envelope is re-enqueued with ``attempt + 1`` and the current message
        completes. After ``MAX_DEFERRALS`` the error propagates so the host retries and finally
        moves the message to the monitored poison queue. Raises ``QueueUnavailableError`` if the
        deferral cannot be enqueued (the host then redelivers the original message). An
        unparsable message cannot be deferred: if its quarantine write fails, the host retries.
        """
        try:
            envelope = Envelope.from_json(text)
        except EnvelopeError as exc:
            self._quarantine("unparsed", None, str(exc.args[0]), text)
            return ProcessResult("quarantined")
        try:
            return self.process_envelope(envelope)
        except RetryLater as exc:
            queue = self._deferral_queue(envelope)
            if queue is None:
                raise
            return self._defer(queue, envelope, exc.reason, exc.delay_seconds)
        except StoreUnavailableError:
            queue = self._deferral_queue(envelope)
            if queue is None:
                raise
            return self._defer(
                queue, envelope, STORE_UNAVAILABLE, store_retry_delay(envelope.attempt)
            )

    def process_message(self, text: str) -> ProcessResult:
        """Process one message without deferral handling (raises ``RetryLater``)."""
        try:
            envelope = Envelope.from_json(text)
        except EnvelopeError as exc:
            self._quarantine("unparsed", None, str(exc.args[0]), text)
            return ProcessResult("quarantined")
        return self.process_envelope(envelope)

    def process_envelope(self, envelope: Envelope) -> ProcessResult:
        status = EVENT_TYPE_TO_STATUS.get(envelope.event_type)
        reason = None
        if envelope.environment != self._settings.environment:
            reason = "wrong_environment"
        elif self._settings.resource(envelope.resource_key) is None:
            reason = "unknown_resource"
        elif status is None or envelope.batch_id is None:
            reason = "unsupported_event"
        if reason is not None or status is None or envelope.batch_id is None:
            self._quarantine(
                envelope.resource_key, envelope.batch_id, reason or "invalid", envelope
            )
            return ProcessResult("quarantined")

        key = JobKey(envelope.resource_key, envelope.batch_id)
        receipt = Receipt(
            receipt_id=f"wh:{envelope.event_id or envelope.webhook_id}",
            source=Source.WEBHOOK.value,
            received_at=envelope.received_at,
            event_type=envelope.event_type,
            webhook_id=envelope.webhook_id,
            event_id=envelope.event_id,
        )
        stored = self._registry.load(key)
        adopted: BatchSnapshot | None = None
        if stored is None:
            if self._settings.unknown_job_policy != "adopt":
                self._hold_unknown_job(key, envelope)
                return ProcessResult("quarantined", key)
            stored, adopted = self._adopt(key)
            if stored is None:
                self._quarantine(key.resource_key, key.batch_id, "adopt_failed", envelope)
                return ProcessResult("quarantined", key)

        if self._registry.has_receipt(key, receipt.receipt_id):
            log_event(
                logger,
                "processor.duplicate",
                resource_key=key.resource_key,
                batch_id=key.batch_id,
                receipt_id=receipt.receipt_id,
            )
            return ProcessResult("duplicate", key)

        retrieval: StatusCallResult | None
        if adopted is not None:
            retrieval, called = StatusCallResult(adopted), False  # just retrieved while adopting
        else:
            retrieval = self._enrich(stored, envelope.enrichment_failures)
            called = retrieval is not None
        snapshot = retrieval.snapshot if retrieval is not None else None
        obs = Observation(
            source=Source.WEBHOOK,
            raw_status=status.value,
            observed_at=self._clock.now(),
            snapshot=snapshot,
            authoritative=False,
            receipt_id=receipt.receipt_id,
            event_id=envelope.event_id,
            webhook_id=envelope.webhook_id,
            status_call_made=called,
            received_at=envelope.received_at,
        )
        if snapshot is not None and is_terminal(snapshot.status):
            # A final-state retrieve is authoritative: it settles any conflict without a second
            # call. If it disagrees with the signed event, trust the retrieve and record it.
            if snapshot.status != status:
                log_event(
                    logger,
                    "processor.event_status_mismatch",
                    level=logging.WARNING,
                    resource_key=key.resource_key,
                    batch_id=key.batch_id,
                    event_status=status.value,
                    retrieved_status=snapshot.status,
                )
            obs = replace(obs, raw_status=snapshot.status, authoritative=True)
        return self.apply(key, obs, receipt, status_call=retrieval)

    def apply(
        self,
        key: JobKey,
        obs: Observation,
        receipt: Receipt | None,
        *,
        status_call: StatusCallResult | None = None,
    ) -> ProcessResult:
        """Commit ``obs`` for ``key`` with optimistic concurrency, retrying on conflicts.

        ``status_call`` is the retrieve this pass already made (enrichment or adoption). Conflict
        resolution reuses it, so one message never books the status-call floor twice.
        """
        conflict_resolved = False
        for _ in range(MAX_COMMIT_ATTEMPTS):
            stored = self._registry.load(key)
            if stored is None:
                return ProcessResult("unknown_job", key)
            if receipt is not None and self._registry.has_receipt(key, receipt.receipt_id):
                return ProcessResult("duplicate", key)
            decision = decide(stored.record, obs)
            if decision.kind is DecisionKind.CONFLICT:
                obs = self._resolve_conflict(stored, obs, status_call)
                conflict_resolved = True
                decision = decide(stored.record, obs)
            if conflict_resolved and decision.kind is DecisionKind.DUPLICATE:
                # Stored state confirmed; keep an auditable count of the conflict (also after a
                # concurrency retry, where the resolved observation no longer conflicts).
                counted = replace(decision.record, conflict_count=stored.record.conflict_count + 1)
                decision = replace(decision, record=counted)
            try:
                return self._commit(stored, decision, obs, receipt)
            except ConcurrencyConflictError:
                continue
        raise RetryLater("concurrency_exhausted", 5)

    # -- internals ------------------------------------------------------------------------

    def _deferral_queue(self, envelope: Envelope) -> MessageQueue | None:
        return self._incoming_queue if envelope.attempt < MAX_DEFERRALS else None

    def _defer(
        self, queue: MessageQueue, envelope: Envelope, reason: str, delay_seconds: float
    ) -> ProcessResult:
        """Re-enqueue ``envelope`` invisibly for ``delay_seconds``; a failed send propagates."""
        deferred = replace(
            envelope,
            attempt=envelope.attempt + 1,
            enrichment_failures=envelope.enrichment_failures
            + (1 if reason == ENRICHMENT_RETRY else 0),
        )
        delay = max(1, min(math.ceil(delay_seconds), MAX_DEFERRAL_SECONDS))
        queue.send(deferred.to_json(), visibility_timeout=delay)
        log_event(
            logger,
            "processor.deferred",
            resource_key=envelope.resource_key,
            batch_id=envelope.batch_id,
            reason=reason,
            delay_s=delay,
            attempt=deferred.attempt,
        )
        return ProcessResult("deferred")

    def _hold_unknown_job(self, key: JobKey, envelope: Envelope) -> None:
        """Defer an event for a job the registry does not know yet; quarantine after the grace.

        A webhook can beat the submission that links its job (a fast validation failure, or a
        lost create response recovered after 15 minutes). Quarantining it at once would lose
        it to the webhook path and later count the job as a missed webhook. Raises
        ``RetryLater`` while the event is younger than ``unknown_job_grace_seconds`` and
        deferrals remain; otherwise quarantines it (ADR 0007).
        """
        age = (self._clock.now() - envelope.received_at).total_seconds()
        remaining = self._settings.unknown_job_grace_seconds - age
        if remaining > 0 and envelope.attempt < MAX_DEFERRALS:
            raise RetryLater(UNKNOWN_JOB_PENDING, unknown_job_delay(envelope.attempt, remaining))
        self._quarantine(key.resource_key, key.batch_id, "unknown_job", envelope)

    def _commit(
        self,
        stored: StoredJob,
        decision: Decision,
        obs: Observation,
        receipt: Receipt | None,
    ) -> ProcessResult:
        record = decision.record
        outbox: OutboxItem | None = None
        correction_of: str | None = None
        if decision.kind is DecisionKind.CORRECTION:
            correction_of = (
                stored.record.last_event_key
                or NormalizedEvent.from_job(
                    stored.record, source="polling", emitted_at=obs.observed_at
                ).idempotency_key
            )
        if decision.emit_event:
            now = self._clock.now()
            event = NormalizedEvent.from_job(
                record,
                source=obs.source.value,
                emitted_at=now,
                event_id=obs.event_id,
                webhook_id=obs.webhook_id,
                correction_of=correction_of,
            )
            outbox = OutboxItem(
                idempotency_key=event.idempotency_key,
                event_json=json.dumps(event.payload, separators=(",", ":")),
                source=obs.source.value,
                created_at=now,
                next_attempt_at=now,
                correction_of=correction_of,
            )
            # Chained corrections reference the event actually sent and never reuse a key.
            record = replace(record, last_event_key=event.idempotency_key)
        if receipt is None and outbox is None and record == stored.record:
            return ProcessResult("unchanged", record.key)

        self._registry.commit(record, etag=stored.etag, receipt=receipt, outbox=outbox)
        partition = self._registry.partition(record.key)
        log_event(
            logger,
            f"processor.{decision.kind.value}",
            resource_key=record.resource_key,
            batch_id=record.batch_id,
            source=obs.source.value,
            raw_status=record.raw_status,
            outcome=record.outcome,
            late_event=decision.late_event,
        )
        if record.is_terminal and (
            LifecycleRecord.from_job(record) != LifecycleRecord.from_job(stored.record)
        ):
            self._emit_lifecycle(record)
        if outbox is not None:
            try:
                self._relay_queue.send(relay_nudge(partition, outbox.idempotency_key))
            except QueueUnavailableError:
                log_event(
                    logger,
                    "processor.nudge_failed",
                    level=logging.WARNING,
                    resource_key=record.resource_key,
                    batch_id=record.batch_id,
                )
        outbox_key = outbox.idempotency_key if outbox else None
        return ProcessResult(decision.kind.value, record.key, outbox_key)

    def _retry_delay(self, error: ProviderRetryableError) -> float:
        """The next status call waits for the provider's hint and for the floor just booked."""
        return max(error.retry_after_seconds or 0, self._settings.status_call_floor_seconds)

    def _status_call(self, key: JobKey) -> StatusCallResult:
        """Reserve the floor and retrieve once; ``RetryLater`` means no call was made."""
        try:
            return StatusCallResult(snapshot=self.retrieve_snapshot(key))
        except ProviderError as exc:
            return StatusCallResult(error=exc)

    def _enrich(self, stored: StoredJob, failures: int) -> StatusCallResult | None:
        """One logical retrieve per event when details are missing, respecting the 60 s floor.

        Enrichment never blocks the signed final state for long: after
        ``MAX_ENRICHMENT_ATTEMPTS`` failed retrieves, or on any non-retryable provider error,
        the event is committed without details and the reconciler completes them later.
        Returns ``None`` when no call was needed, else the call's snapshot or error.
        """
        job = stored.record
        if job.details_complete:
            return None
        result = self._status_call(job.key)
        error = result.error
        if error is None:
            return result
        if isinstance(error, ProviderRetryableError):
            if failures + 1 < MAX_ENRICHMENT_ATTEMPTS:
                raise RetryLater(ENRICHMENT_RETRY, self._retry_delay(error)) from error
            reason = "attempts_exhausted"
        else:  # 404, 403 (RBAC), 400, 401: retrying will not help
            reason = "not_found" if isinstance(error, ProviderNotFoundError) else "non_retryable"
        log_event(
            logger,
            "processor.enrichment_failed",
            level=logging.WARNING,
            resource_key=job.resource_key,
            batch_id=job.batch_id,
            reason=reason,
            status_code=error.status_code,
            attempts=failures + 1,
        )
        return result

    def _resolve_conflict(
        self, stored: StoredJob, obs: Observation, prior: StatusCallResult | None
    ) -> Observation:
        """Authoritative answer for conflicting final states.

        Reuses ``prior`` (the status call this pass already made) and retrieves only when
        there was none. Raises ``RetryLater`` while the provider is throttled or unavailable.
        """
        job = stored.record
        result = prior if prior is not None else self._status_call(job.key)
        if isinstance(result.error, ProviderRetryableError):
            delay = self._retry_delay(result.error)
            raise RetryLater("conflict_resolution_pending", delay) from result.error
        snapshot = result.snapshot
        if snapshot is None:
            # Cannot be settled by the provider: keep the stored state (counted, audited).
            log_event(
                logger,
                "processor.conflict_unresolved",
                level=logging.WARNING,
                resource_key=job.resource_key,
                batch_id=job.batch_id,
                stored_status=job.raw_status,
                observed_status=obs.raw_status,
                status_code=result.error.status_code if result.error else None,
            )
            return replace(
                obs,
                raw_status=job.raw_status,
                snapshot=None,
                authoritative=True,
                status_call_made=True,
            )
        log_event(
            logger,
            "processor.conflict_resolved",
            level=logging.WARNING,
            resource_key=job.resource_key,
            batch_id=job.batch_id,
            stored_status=job.raw_status,
            observed_status=obs.raw_status,
            authoritative_status=snapshot.status,
        )
        return replace(
            obs,
            raw_status=snapshot.status,
            snapshot=snapshot,
            authoritative=True,
            status_call_made=True,
        )

    def _adopt(self, key: JobKey) -> tuple[StoredJob | None, BatchSnapshot | None]:
        """Policy ``adopt``: register a job we did not submit, inside an owned resource only."""
        resource = self._settings.resource(key.resource_key)
        try:
            snapshot = self.retrieve_snapshot(key)
        except ProviderRetryableError as exc:
            # Transient: defer the signed event instead of sending it to manual triage.
            raise RetryLater(
                "adopt_retry",
                max(exc.retry_after_seconds or 0, self._settings.status_call_floor_seconds),
            ) from exc
        except ProviderError:
            return None, None
        record = JobRecord(
            resource_key=key.resource_key,
            batch_id=key.batch_id,
            environment=self._settings.environment,
            region=resource.region if resource else None,
            submitted_at=snapshot.created_at,
            trusted=False,
            last_status_call_at=self._clock.now(),
        ).with_snapshot(snapshot)
        with contextlib.suppress(ConcurrencyConflictError):  # adopted concurrently: same record
            self._registry.create_job(record)
        log_event(
            logger,
            "processor.adopted",
            level=logging.WARNING,
            resource_key=key.resource_key,
            batch_id=key.batch_id,
        )
        return self._registry.load(key), snapshot

    def _quarantine(
        self, resource_key: str, batch_id: str | None, reason: str, payload: Envelope | str
    ) -> None:
        """Store the untrusted message for triage; it never creates job records or events."""
        now = self._clock.now()
        text = payload.to_json() if isinstance(payload, Envelope) else payload
        kept = utf16_prefix(text, MAX_QUARANTINE_TEXT)
        partition_key = safe_key(f"{self._settings.environment}|{resource_key}")
        row_key = safe_key(f"{to_iso(now)}|{batch_id or 'none'}|{reason}")
        entity = Entity(
            partition_key=utf16_prefix(partition_key, MAX_QUARANTINE_PARTITION_KEY),
            row_key=utf16_prefix(row_key, MAX_QUARANTINE_ROW_KEY),
            data={
                "reason": reason,
                "batch_id": utf16_prefix(batch_id or "", MAX_QUARANTINE_FIELD),
                "resource_key": utf16_prefix(resource_key, MAX_QUARANTINE_FIELD),
                "quarantined_at": to_iso(now),
                "envelope": kept,
                "envelope_truncated": len(kept) < len(text),
                "resolved": False,
            },
        )
        # A failed write propagates (deferred or retried); only a stored row is reported.
        self._store.submit(self._settings.table_quarantine, [TxOp("upsert", entity)])
        log_event(
            logger,
            "processor.quarantined",
            level=logging.WARNING,
            resource_key=resource_key,
            batch_id=batch_id,
            reason=reason,
        )

    def _emit_lifecycle(self, job: JobRecord) -> None:
        """Metadata-only lifecycle record for the workbook (US-6.1); see metrics.lifecycle."""
        lifecycle = LifecycleRecord.from_job(job)
        log_event(logger, "job.lifecycle", **lifecycle.log_properties())


def observation_from_snapshot(
    snapshot: BatchSnapshot, observed_at: datetime, *, status_call_made: bool
) -> Observation:
    """Only a direct retrieve can authorize a conflicting terminal correction."""
    return Observation(
        source=Source.POLLING,
        raw_status=snapshot.status,
        observed_at=observed_at,
        snapshot=snapshot,
        authoritative=status_call_made,
        receipt_id=f"poll:{snapshot.status}" if is_terminal(snapshot.status) else None,
        status_call_made=status_call_made,
    )
