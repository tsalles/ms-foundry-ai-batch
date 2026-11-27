"""Submission harness with crash-safe registry linking (US-5.1).

Order of operations: durable intent -> upload -> mark ``uploaded`` -> wait until the input file
is ``processed`` -> claim ``creating`` (ETag-conditional) -> create batch (with
``batch_correlation_id`` metadata) -> create the job row -> mark ``linked``. Any crash or
ambiguous create between provider acceptance and the job row is resolved by discovery (match on
the unique input file id or the correlation id metadata) before anything is resubmitted;
duplicate provider jobs are reported, not hidden. Use synthetic data only.

Intent statuses: ``intent`` -> ``uploaded`` -> ``creating`` -> ``linked``, or ``ambiguous``
(outcome unknown, ``recover`` resolves it), ``rejected`` (definitely not created), ``abandoned``
(no create call was made, so no batch can exist) and ``needs_operator`` (could not be resolved).
``uploaded`` means the create call was never made; ``recover`` resumes it once the file is
processed. The input file of a ``rejected`` or ``abandoned`` submission is deleted (best
effort; it also expires, see ``FoundryRestClient``).
"""

from __future__ import annotations

import json
import logging
import re
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

from foundry_batch.clock import Clock, from_iso, to_iso
from foundry_batch.config import Settings
from foundry_batch.control import ControlStore
from foundry_batch.domain.models import BatchSnapshot, JobRecord
from foundry_batch.domain.status import is_terminal
from foundry_batch.errors import (
    ConcurrencyConflictError,
    FoundryBatchError,
    ProviderAmbiguousError,
    ProviderError,
    ProviderRequestError,
)
from foundry_batch.foundry.files import (
    FILE_POLL_SECONDS,
    FILE_WAIT_TIMEOUT_SECONDS,
    FileApi,
    FileWait,
    wait_until_processed,
)
from foundry_batch.metrics.queued_tokens import TokenEstimator
from foundry_batch.observability import log_event
from foundry_batch.ports import BatchApi, BatchApiFactory, Entity, Query, TableStore, TxOp
from foundry_batch.processing.decision import Observation, Source
from foundry_batch.processing.registry import JobRegistry, Receipt

if TYPE_CHECKING:
    from foundry_batch.processing.processor import EventProcessor

logger = logging.getLogger(__name__)

MAX_DISCOVERY_PAGES = 10
#: How old an unfinished intent must be before ``recover`` touches it.
RECOVERY_MIN_AGE_SECONDS = 900
#: ``recover`` abandons an ``uploaded`` intent whose file is still not processed after this.
FILE_PENDING_MAX_AGE_SECONDS = 24 * 3600
#: Azure OpenAI batch input limits (per file).
MAX_REQUESTS_PER_FILE = 100_000
MAX_FILE_BYTES = 200 * 1024 * 1024
#: Safe as a Table RowKey and as a provider metadata value.
_CORRELATION_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


class SubmissionError(FoundryBatchError):
    pass


class SubmissionPending(FoundryBatchError):  # noqa: N818 - an outcome signal, not an error
    """Not finished and not definitely refused; ``recover`` will resolve it. Never resubmit.

    ``reason``: ``create_outcome_unknown`` (ambiguous create, batch not found yet),
    ``discovery_failed`` (ambiguous create, then the batch list failed), ``file_not_processed``
    (the input file was not processed in time; no create call was made),
    ``claimed_elsewhere`` (another process took over the create) or ``link_failed`` (the batch
    exists but its job row could not be completed).
    """

    def __init__(self, correlation_id: str, reason: str = "create_outcome_unknown") -> None:
        super().__init__(f"submission {correlation_id} is pending ({reason})")
        self.correlation_id = correlation_id
        self.reason = reason


@dataclass(frozen=True)
class InputSummary:
    lines: int
    deployment: str


def validate_input(
    jsonl: bytes,
    deployment: str,
    *,
    endpoint: str = "/chat/completions",
    max_lines: int = MAX_REQUESTS_PER_FILE,
    max_bytes: int = MAX_FILE_BYTES,
) -> InputSummary:
    """Check the batch file rules that otherwise fail late, on the service.

    Every line must be a JSON object with a unique ``custom_id``, ``method`` POST, a ``url``
    matching the batch endpoint (``/chat/completions`` or ``/v1/chat/completions``) and a body
    whose ``model`` is the deployment.
    """
    if len(jsonl) > max_bytes:
        raise SubmissionError(f"input file is larger than {max_bytes} bytes")
    accepted_urls = {endpoint, "/v1" + endpoint}
    lines = 0
    custom_ids: set[str] = set()
    for number, raw in enumerate(jsonl.splitlines(), start=1):
        if not raw.strip():
            continue
        try:
            item = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise SubmissionError(f"line {number}: not valid JSON") from exc
        if not isinstance(item, dict):
            raise SubmissionError(f"line {number}: not an object")
        custom_id = item.get("custom_id")
        if not isinstance(custom_id, str) or not custom_id:
            raise SubmissionError(f"line {number}: custom_id is required")
        if custom_id in custom_ids:
            raise SubmissionError(f"line {number}: duplicate custom_id")
        custom_ids.add(custom_id)
        body = item.get("body")
        if item.get("method") != "POST" or not isinstance(body, dict):
            raise SubmissionError(f"line {number}: method must be POST with a body")
        if item.get("url") not in accepted_urls:
            raise SubmissionError(f"line {number}: url must be {endpoint} for this batch")
        if body.get("model") != deployment:
            raise SubmissionError(f"line {number}: model must be the deployment '{deployment}'")
        lines += 1
    if lines == 0:
        raise SubmissionError("input file is empty")
    if lines > max_lines:
        raise SubmissionError(f"input file has more than {max_lines} requests")
    return InputSummary(lines=lines, deployment=deployment)


def _optional_int(value: object) -> int | None:
    """Unknown stays unknown: a missing estimate must not count as zero queued tokens."""
    if isinstance(value, bool) or value is None:
        return None
    try:
        return int(str(value))
    except ValueError:
        return None


class SubmissionService:
    def __init__(
        self,
        settings: Settings,
        store: TableStore,
        registry: JobRegistry,
        control: ControlStore,
        batch_apis: BatchApiFactory,
        estimator: TokenEstimator,
        clock: Clock,
        *,
        processor: EventProcessor | None = None,
        sleep: Callable[[float], None] = time.sleep,
        correlation_ids: Callable[[], str] | None = None,
        file_wait_timeout_s: float = FILE_WAIT_TIMEOUT_SECONDS,
        file_poll_s: float = FILE_POLL_SECONDS,
    ) -> None:
        self._settings = settings
        self._store = store
        self._registry = registry
        self._control = control
        self._apis = batch_apis
        self._estimator = estimator
        self._clock = clock
        self._processor = processor
        self._table = settings.table_submissions
        #: Waits between file status checks; must move ``clock`` (``ManualClock.advance``).
        self._sleep = sleep
        self._new_correlation_id = correlation_ids or (lambda: uuid.uuid4().hex)
        self._file_wait_timeout = file_wait_timeout_s
        self._file_poll = file_poll_s

    def submit(
        self,
        resource_key: str,
        deployment: str,
        jsonl: bytes,
        *,
        endpoint: str = "/chat/completions",
        correlation_id: str | None = None,
    ) -> JobRecord:
        resource = self._settings.resource(resource_key)
        if resource is None:
            raise SubmissionError(f"unknown resource {resource_key!r}")
        if resource.deployments and deployment not in resource.deployments:
            raise SubmissionError(f"deployment {deployment!r} is not configured for {resource_key}")
        validate_input(jsonl, deployment, endpoint=endpoint)
        correlation_id = correlation_id or self._new_correlation_id()
        if not _CORRELATION_ID.match(correlation_id):
            raise SubmissionError("correlation id: 1-128 letters, digits, '.', '_' or '-'")
        estimate = self._estimator.estimate(jsonl)
        intent = {
            "status": "intent",
            "resource_key": resource_key,
            "deployment": deployment,
            "endpoint": endpoint,
            "token_estimate": estimate,
            "tokenizer": self._estimator.name,
            "created_at": to_iso(self._clock.now()),
        }
        try:
            self._put(resource_key, correlation_id, intent, create=True)
        except ConcurrencyConflictError as exc:
            # A reused correlation id must never upload or create a second batch.
            existing = self._store.get(self._table, self._partition(resource_key), correlation_id)
            status = existing.data.get("status") if existing else "unknown"
            raise SubmissionError(
                f"correlation id {correlation_id} was already used (status {status}); "
                "use recover-submissions or a new id"
            ) from exc
        api = self._apis.for_resource(resource_key)
        file_id = api.upload_file(filename=f"{correlation_id}.jsonl", content=jsonl)
        uploaded = {**intent, "status": "uploaded", "file_id": file_id}
        etag = self._put(resource_key, correlation_id, uploaded)
        wait = self._wait_for_file(api, file_id, timeout_s=self._file_wait_timeout)
        if wait.outcome == "timeout":
            log_event(
                logger,
                "submission.file_pending",
                level=logging.WARNING,
                resource_key=resource_key,
                correlation_id=correlation_id,
                file_status=wait.status,
                polls=wait.polls,
            )
            raise SubmissionPending(correlation_id, "file_not_processed")
        if wait.outcome == "unusable":
            self._reject(api, resource_key, correlation_id, uploaded, reason=f"file_{wait.status}")
            raise SubmissionError(f"input file is '{wait.status}'; the batch was not created")
        entity = Entity(self._partition(resource_key), correlation_id, uploaded, etag)
        return self._create(api, resource_key, entity)

    def recover(
        self, resource_key: str, *, min_age_seconds: int = RECOVERY_MIN_AGE_SECONDS
    ) -> list[str]:
        """Resolve intents left behind by a crash, a slow file or an ambiguous create; returns
        the correlation ids linked.

        * ``creating``/``ambiguous``: discover the batch; link it, or mark ``needs_operator``;
        * ``uploaded``: no create call was made; once the file is processed, create and link
          (an unusable file is ``rejected``; one still pending after
          ``FILE_PENDING_MAX_AGE_SECONDS`` is ``abandoned``);
        * ``intent``: the process stopped before the upload was recorded, so no batch can exist:
          mark ``abandoned`` (the file, if one was uploaded, is unknown and simply expires).

        Rows younger than ``min_age_seconds`` are left alone: their submit may still be running
        (a large upload can take minutes). A provider failure leaves the row for the next run.
        """
        now = self._clock.now()
        resolved: list[str] = []
        query = Query(partition_key=self._partition(resource_key))
        entities = self._store.query(self._table, query)
        api = self._apis.for_resource(resource_key)
        for entity in entities:
            status = entity.data.get("status")
            if status not in ("intent", "uploaded", "creating", "ambiguous"):
                continue
            created = from_iso(entity.data.get("created_at"))
            if created is not None and now - created < timedelta(seconds=min_age_seconds):
                continue
            try:
                if self._recover_one(api, resource_key, entity, now):
                    resolved.append(entity.row_key)
            except ProviderError as exc:
                log_event(
                    logger,
                    "submission.recovery_deferred",
                    level=logging.WARNING,
                    resource_key=resource_key,
                    correlation_id=entity.row_key,
                    status_code=exc.status_code,
                    error_type=type(exc).__name__,
                )
        return resolved

    # -- internals ------------------------------------------------------------------------

    def _recover_one(self, api: BatchApi, resource_key: str, entity: Entity, now: datetime) -> bool:
        data = dict(entity.data)
        if data.get("status") == "intent":
            self._abandon(api, resource_key, entity.row_key, data)
            return False
        if data.get("status") == "uploaded":
            return self._resume(api, resource_key, entity, now)
        file_id = str(data.get("file_id") or "")
        snapshot = self._discover(api, file_id, entity.row_key)
        if snapshot is None:
            self._put(resource_key, entity.row_key, {**data, "status": "needs_operator"})
            log_event(
                logger,
                "submission.needs_operator",
                level=logging.ERROR,
                resource_key=resource_key,
                correlation_id=entity.row_key,
            )
            return False
        self._link(resource_key, entity.row_key, data, snapshot)
        return True

    def _resume(self, api: BatchApi, resource_key: str, entity: Entity, now: datetime) -> bool:
        """An ``uploaded`` intent: create the batch once the file is processed (one check)."""
        data = dict(entity.data)
        wait = self._wait_for_file(api, str(data.get("file_id") or ""), timeout_s=0.0)
        if wait.outcome == "processed":
            try:
                self._create(api, resource_key, entity)
            except (SubmissionPending, ProviderError):
                return False  # recorded as ambiguous or rejected, or another process holds it
            return True
        if wait.outcome == "unusable":
            self._reject(api, resource_key, entity.row_key, data, reason=f"file_{wait.status}")
            return False
        created = from_iso(data.get("created_at"))
        if created is not None and (now - created).total_seconds() >= FILE_PENDING_MAX_AGE_SECONDS:
            self._abandon(api, resource_key, entity.row_key, data)
        return False

    def _abandon(
        self, api: BatchApi, resource_key: str, correlation_id: str, data: dict[str, Any]
    ) -> None:
        """No create call was made, so no batch can exist."""
        abandoned = {**data, "status": "abandoned"}
        self._put(resource_key, correlation_id, abandoned)
        log_event(
            logger,
            "submission.abandoned",
            level=logging.WARNING,
            resource_key=resource_key,
            correlation_id=correlation_id,
        )
        self._discard_file(api, resource_key, correlation_id, abandoned)

    def _create(self, api: BatchApi, resource_key: str, stored: Entity) -> JobRecord:
        """Claim the intent (``uploaded`` -> ``creating``, ETag-conditional), create, link."""
        correlation_id = stored.row_key
        data = {**stored.data, "status": "creating"}
        try:
            self._store.submit(
                self._table,
                [TxOp("update", Entity(stored.partition_key, correlation_id, data, stored.etag))],
            )
        except ConcurrencyConflictError as exc:
            log_event(
                logger,
                "submission.claim_lost",
                level=logging.WARNING,
                resource_key=resource_key,
                correlation_id=correlation_id,
            )
            raise SubmissionPending(correlation_id, "claimed_elsewhere") from exc
        file_id = str(data["file_id"])
        try:
            snapshot = api.create_batch(
                input_file_id=file_id,
                endpoint=str(data.get("endpoint") or "/chat/completions"),
                metadata={"batch_correlation_id": correlation_id},
            )
        except ProviderAmbiguousError as exc:
            snapshot = self._discover_after_ambiguous(api, resource_key, correlation_id, data, exc)
        except ProviderError as exc:
            # Definitely not created (429, 503, 4xx): record it so recovery does not chase it
            # and the operator can resubmit with a new correlation id.
            self._reject(api, resource_key, correlation_id, data, status_code=exc.status_code)
            raise
        try:
            return self._link(resource_key, correlation_id, data, snapshot)
        except FoundryBatchError as exc:  # the batch exists: this must never look like a refusal
            log_event(
                logger,
                "submission.link_failed",
                level=logging.WARNING,
                resource_key=resource_key,
                correlation_id=correlation_id,
                batch_id=snapshot.batch_id,
                error_type=type(exc).__name__,
            )
            raise SubmissionPending(correlation_id, "link_failed") from exc

    def _discover_after_ambiguous(
        self,
        api: BatchApi,
        resource_key: str,
        correlation_id: str,
        data: dict[str, Any],
        cause: ProviderAmbiguousError,
    ) -> BatchSnapshot:
        """The create may have committed. Anything short of finding the batch is pending: a
        failed list (429, 5xx, network) proves nothing, so it must not look like a refusal."""
        ambiguous = {**data, "status": "ambiguous"}
        try:
            found = self._discover(api, str(data["file_id"]), correlation_id)
        except ProviderError as exc:
            self._put(resource_key, correlation_id, ambiguous)
            log_event(
                logger,
                "submission.discovery_failed",
                level=logging.WARNING,
                resource_key=resource_key,
                correlation_id=correlation_id,
                status_code=exc.status_code,
                error_type=type(exc).__name__,
            )
            raise SubmissionPending(correlation_id, "discovery_failed") from exc
        if found is None:
            self._put(resource_key, correlation_id, ambiguous)
            raise SubmissionPending(correlation_id) from cause
        return found

    def _wait_for_file(self, api: BatchApi, file_id: str, *, timeout_s: float) -> FileWait:
        if not isinstance(api, FileApi):  # a port double without the Files status API
            return FileWait("processed", "unchecked", 0)
        try:
            return wait_until_processed(
                api, file_id, self._clock, self._sleep, timeout_s=timeout_s, poll_s=self._file_poll
            )
        except ProviderRequestError as exc:  # e.g. 403: this file will not be usable by us
            return FileWait("unusable", f"unreadable_{exc.status_code or 0}", 0)

    def _reject(
        self,
        api: BatchApi,
        resource_key: str,
        correlation_id: str,
        data: dict[str, Any],
        *,
        status_code: int | None = None,
        reason: str | None = None,
    ) -> None:
        rejected = {**data, "status": "rejected", "error_status": status_code or 0}
        if reason:
            rejected["reason"] = reason
        self._put(resource_key, correlation_id, rejected)
        log_event(
            logger,
            "submission.rejected",
            level=logging.ERROR,
            resource_key=resource_key,
            correlation_id=correlation_id,
            status_code=status_code,
            reason=reason,
        )
        self._discard_file(api, resource_key, correlation_id, rejected)

    def _discard_file(
        self, api: BatchApi, resource_key: str, correlation_id: str, data: dict[str, Any]
    ) -> None:
        """Best effort: a finished submission's input file only counts against CON-B08."""
        file_id = str(data.get("file_id") or "")
        if not file_id or not isinstance(api, FileApi):
            return
        try:
            api.delete_file(file_id)
        except ProviderError as exc:
            log_event(
                logger,
                "submission.file_delete_failed",
                level=logging.WARNING,
                resource_key=resource_key,
                correlation_id=correlation_id,
                status_code=exc.status_code,
            )
            return
        self._put(resource_key, correlation_id, {**data, "file_deleted": True})

    def _link(
        self,
        resource_key: str,
        correlation_id: str,
        intent: dict[str, Any],
        snapshot: BatchSnapshot,
    ) -> JobRecord:
        """Create (or complete) the job row and mark the intent ``linked``.

        A batch that is already final when it is linked (found by discovery after an outage,
        or failed fast in validation) is registered as open and then applied as an observation,
        so its downstream event is emitted exactly like any other final state. Without a
        processor the reconciler emits it on its next sweep.
        """
        resource = self._settings.resource(resource_key)
        now = self._clock.now()
        registration = self._control.active_registration(resource_key, now)
        final = is_terminal(snapshot.status)
        record = JobRecord(
            resource_key=resource_key,
            batch_id=snapshot.batch_id,
            environment=self._settings.environment,
            correlation_id=correlation_id,
            deployment=str(intent.get("deployment")),
            region=resource.region if resource else None,
            input_file_id=str(intent.get("file_id") or "") or None,
            token_estimate=_optional_int(intent.get("token_estimate")),
            tokenizer=str(intent.get("tokenizer") or "") or None,
            submitted_at=now,
            webhook_covered=registration is not None,
            registration_id=registration.endpoint_id if registration else None,
            last_status_call_at=now,
        ).with_snapshot(snapshot)
        if not final:
            record = record.with_status(snapshot.status)
        try:
            self._registry.create_job(record)
        except ConcurrencyConflictError:
            record = self._complete_existing(record)
        if final and self._processor is not None:
            obs = Observation(
                source=Source.SUBMISSION,
                raw_status=snapshot.status,
                observed_at=now,
                snapshot=snapshot,
                authoritative=True,
                receipt_id=f"submission:{snapshot.status}",
            )
            receipt = Receipt(f"submission:{snapshot.status}", Source.SUBMISSION.value, now)
            self._processor.apply(record.key, obs, receipt)
        stored = self._registry.load(record.key)  # return exactly what was stored
        record = stored.record if stored is not None else record
        self._put(
            resource_key,
            correlation_id,
            {**intent, "status": "linked", "batch_id": snapshot.batch_id},
        )
        log_event(
            logger,
            "submission.linked",
            resource_key=resource_key,
            batch_id=snapshot.batch_id,
            correlation_id=correlation_id,
            raw_status=snapshot.status,
            webhook_covered=record.webhook_covered,
            token_estimate=record.token_estimate,
        )
        return record

    def _complete_existing(self, record: JobRecord) -> JobRecord:
        """The job row exists (adopted from a webhook, or a concurrent recover): add what only
        the submission knows, without overwriting anything already recorded."""
        for _ in range(3):
            stored = self._registry.load(record.key)
            if stored is None:
                return record
            existing = stored.record
            updates: dict[str, Any] = {
                name: getattr(record, name)
                for name in (
                    "correlation_id",
                    "deployment",
                    "input_file_id",
                    "token_estimate",
                    "tokenizer",
                    "submitted_at",
                )
                if getattr(existing, name) is None
            }
            if not existing.trusted:
                updates["trusted"] = True  # matched our own file or correlation id
            if not updates:
                return existing
            updated = self._registry.try_update_job(stored, replace(existing, **updates))
            if updated is not None:
                log_event(
                    logger,
                    "submission.linked_existing",
                    level=logging.WARNING,
                    resource_key=record.resource_key,
                    batch_id=record.batch_id,
                    fields=",".join(sorted(updates)),
                )
                return updated.record
        return record

    def _discover(self, api: BatchApi, file_id: str, correlation_id: str) -> BatchSnapshot | None:
        """Find our batch among the newest ``MAX_DISCOVERY_PAGES`` pages.

        A batch is ours if it uses our (unique) input file or carries our correlation id.
        Several matches mean duplicates were created: all are reported and the earliest wins.
        """
        matches: dict[str, BatchSnapshot] = {}
        after: str | None = None
        for _ in range(MAX_DISCOVERY_PAGES):
            page = api.list_batches(after=after, limit=100)
            for batch in page.items:
                same_file = bool(file_id) and batch.input_file_id == file_id
                same_id = batch.metadata.get("batch_correlation_id") == correlation_id
                if same_file or same_id:
                    matches[batch.batch_id] = batch
            if not page.has_more or not page.last_id:
                break
            after = page.last_id
        if len(matches) > 1:
            log_event(
                logger,
                "submission.duplicate_detected",
                level=logging.ERROR,
                file_id=file_id,
                correlation_id=correlation_id,
                batch_ids=",".join(sorted(matches)),
            )
        if not matches:
            return None
        return min(matches.values(), key=lambda b: (b.created_at is None, b.created_at, b.batch_id))

    def _partition(self, resource_key: str) -> str:
        return f"{self._settings.environment}|{resource_key}"

    def _put(
        self, resource_key: str, correlation_id: str, data: dict[str, Any], *, create: bool = False
    ) -> str | None:
        entity = Entity(self._partition(resource_key), correlation_id, data)
        etags = self._store.submit(self._table, [TxOp("create" if create else "upsert", entity)])
        return etags.get(correlation_id)
