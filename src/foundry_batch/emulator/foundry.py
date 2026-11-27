"""Wire-level emulator of the Azure OpenAI v1 batch, files and webhook-endpoint APIs.

Documented behaviour (Microsoft Learn, checked 16 Nov 2026) is reproduced: statuses and their
timestamps, newest-first list paging with ``has_more``/``last_id``, the input validation codes,
one-time signing secrets returned only on endpoint creation, Standard Webhooks signatures, the
four ``batch.*`` event types, input files that are tracked until ``processed`` (checked 17 Nov
2026) and their optional ``expires_after`` (14 to 30 days, which raises the file limit).

Undocumented behaviour is a *setting* with a labelled default (SP-2, SP-4, Q-P06): delivery
retry schedule, duplicates, dropped event types, delay and jitter, event payload fields,
endpoint limits, the status/code used when key authentication is disabled (SP-3), how long a
file stays ``pending`` and the refusal of a batch create on a file that is not ``processed``.
Capture real behaviour in the spikes and update the defaults; the contract tests then hold the
emulator to it.
"""

from __future__ import annotations

import base64
import email.parser
import email.policy
import hashlib
import itertools
import json
import random
import threading
from collections import Counter, deque
from collections.abc import Callable, Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
from typing import Any, Literal
from urllib.parse import urlsplit

from foundry_batch.clock import Clock, to_iso
from foundry_batch.config import BATCH_EVENT_TYPES
from foundry_batch.webhook.signature import sign_headers

Outcome = Literal["completed", "failed", "expired", "cancelled"]

TERMINAL = frozenset({"completed", "failed", "expired", "cancelled"})
EVENT_FOR_STATUS = {
    "completed": "batch.completed",
    "failed": "batch.failed",
    "expired": "batch.expired",
    "cancelled": "batch.cancelled",
}
STAMP_FOR_STATUS = {
    "in_progress": "in_progress_at",
    "finalizing": "finalizing_at",
    "completed": "completed_at",
    "failed": "failed_at",
    "expired": "expired_at",
    "cancelling": "cancelling_at",
    "cancelled": "cancelled_at",
}
MAX_REQUESTS_PER_FILE = 100_000  # CON-B08
MAX_INPUT_FILES = 500  # CON-B08 (without file expiration)
MAX_INPUT_FILES_WITH_EXPIRY = 10_000  # CON-B08 (expiration set)
#: ``expires_after.seconds`` accepted for batch input files (batch how-to: 14 to 30 days).
FILE_EXPIRY_RANGE_S = (14 * 24 * 3600, 30 * 24 * 3600)
FileOutcome = Literal["processed", "error"]


# ---------------------------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class LifecyclePlan:
    """How one accepted job moves through its statuses (seconds of emulated time)."""

    outcome: Outcome = "completed"
    validating_s: float = 30.0
    in_progress_s: float = 120.0
    finalizing_s: float = 30.0
    failed_requests: int = 0
    #: CON-B05: one documented sample has no ``in_progress_at`` on a completed job.
    omit_in_progress_at: bool = False


@dataclass(frozen=True)
class DeliveryPolicy:
    """Webhook delivery behaviour. Defaults are assumptions, not documented guarantees."""

    #: Delays between attempts after a failure (SP-2: the real schedule is undocumented).
    retry_schedule_s: tuple[float, ...] = (5.0, 30.0, 120.0, 600.0, 1800.0)
    #: Event types never sent, e.g. ``{"batch.failed"}`` (EVD-02: not reproduced so far).
    drop_event_types: frozenset[str] = frozenset()
    drop_probability: float = 0.0
    duplicate_probability: float = 0.0
    delay_s: float = 0.0
    #: Extra seeded delay per delivery, uniform in ``[0, jitter_s]``: reorders deliveries.
    jitter_s: float = 0.0
    #: Standard Webhooks: retries of one message keep the same ``webhook-id``.
    stable_webhook_id: bool = True
    seed: int = 0


@dataclass(frozen=True)
class Fault:
    """One injected failure for the next call of an operation.

    ``after_commit`` applies the operation first and then fails, like a gateway timeout after
    the backend committed. ``drop_connection`` loses the response at the network level.
    """

    status: int | None = 500
    code: str = "emulated_fault"
    retry_after: float | None = None
    after_commit: bool = False
    drop_connection: bool = False
    connect_timeout: bool = False  # never reached the service (no effect)


@dataclass(frozen=True)
class ChaosRule:
    """Random failures for an operation (seeded), e.g. Files API bursts of HTTP 500 (M2)."""

    probability: float
    fault: Fault


# ---------------------------------------------------------------------------------------------
# Wire types
# ---------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class WireRequest:
    method: str
    path: str  # relative to ``/openai/v1/``
    query: Mapping[str, str] = field(default_factory=dict)
    headers: Mapping[str, str] = field(default_factory=dict)
    body: bytes = b""


@dataclass(frozen=True)
class WireResponse:
    status: int
    body: bytes = b""
    headers: Mapping[str, str] = field(default_factory=dict)
    drop_connection: bool = False
    connect_timeout: bool = False

    @classmethod
    def json(cls, status: int, data: Mapping[str, Any], **headers: str) -> WireResponse:
        merged = {"content-type": "application/json", **headers}
        return cls(status, json.dumps(data).encode("utf-8"), merged)

    @classmethod
    def error(
        cls,
        status: int,
        code: str,
        retry_after: float | None = None,
        *,
        message: str | None = None,
    ) -> WireResponse:
        headers = {"retry-after": str(int(retry_after))} if retry_after is not None else {}
        body = {"error": {"code": code, "message": message or f"emulated {code}"}}
        return cls.json(status, body, **headers)


@dataclass(frozen=True)
class WebhookCall:
    url: str
    body: bytes
    headers: Mapping[str, str]
    endpoint_id: str
    resource_key: str


#: Sends one webhook; returns the HTTP status, or ``None`` for a network failure/timeout.
WebhookTransport = Callable[[WebhookCall], int | None]


@dataclass
class Delivery:
    """One webhook message to one endpoint, retried until 2xx or the schedule is exhausted."""

    delivery_id: str
    endpoint_id: str
    url: str
    event_id: str
    event_type: str
    batch_id: str
    body: bytes
    webhook_id: str
    due_at: datetime
    attempts: int = 0
    statuses: list[int | None] = field(default_factory=list)
    state: Literal["pending", "delivered", "abandoned"] = "pending"
    duplicate_of: str | None = None


@dataclass
class _Endpoint:
    endpoint_id: str
    name: str
    url: str
    event_types: tuple[str, ...]
    secret: str
    created_at: int


@dataclass
class _Batch:
    data: dict[str, Any]
    plan: LifecyclePlan
    seq: int
    input_tokens: int = 0
    next_status: str | None = None
    next_at: datetime | None = None
    hidden_from_list: bool = False
    #: When work started (kept even when ``in_progress_at`` is omitted, CON-B05).
    started_at: datetime | None = None
    #: Share of the work done when cancellation began (0 if it never started).
    done_at_cancel: float = 0.0


class FoundryEmulator:
    """One emulated Foundry resource. Thread-safe; time comes from the injected clock."""

    def __init__(
        self,
        resource_key: str,
        clock: Clock,
        *,
        deployments: Sequence[str] = ("gpt-batch",),
        delivery: DeliveryPolicy | None = None,
        transport: WebhookTransport | None = None,
        default_plan: LifecyclePlan | None = None,
        key_auth_disabled: bool = True,
        allow_http_webhook_urls: bool = False,
        max_endpoints: int = 10,
        auto_advance: bool = True,
        file_processing_s: float = 0.0,
    ) -> None:
        self.resource_key = resource_key
        self.clock = clock
        self.deployments = tuple(deployments)
        self.delivery = delivery or DeliveryPolicy()
        self.transport = transport
        self.default_plan = default_plan or LifecyclePlan()
        self.key_auth_disabled = key_auth_disabled
        self.allow_http_webhook_urls = allow_http_webhook_urls
        self.max_endpoints = max_endpoints
        #: Apply due lifecycle transitions before each API call, so time alone moves jobs on.
        self.auto_advance = auto_advance
        #: How long an uploaded file stays ``pending`` (assumption; real time depends on size).
        self.file_processing_s = file_processing_s
        self.calls: Counter[str] = Counter()
        self.deliveries: list[Delivery] = []
        #: (time, operation, status) per API call and webhook attempt: a replayable timeline.
        self.journal: list[tuple[str, ...]] = []
        self._lock = threading.RLock()
        self._batches: dict[str, _Batch] = {}
        self._files: dict[str, dict[str, Any]] = {}
        self._file_content: dict[str, bytes] = {}
        self._files_due: dict[str, tuple[datetime, FileOutcome]] = {}
        self._file_plans: deque[FileOutcome] = deque()
        self._endpoints: dict[str, _Endpoint] = {}
        self._faults: dict[str, deque[Fault]] = {}
        self._chaos: dict[str, ChaosRule] = {}
        self._plans: deque[LifecyclePlan] = deque()
        self._ids = itertools.count(1)
        # Seeded simulation randomness, not cryptography.
        self._rng = random.Random(self.delivery.seed)  # noqa: S311  # nosec B311

    # -- control surface (tests, local stack) -----------------------------------------------

    def inject(self, operation: str, *faults: Fault) -> None:
        with self._lock:
            self._faults.setdefault(operation, deque()).extend(faults)

    def chaos(self, operation: str, rule: ChaosRule | None) -> None:
        with self._lock:
            if rule is None:
                self._chaos.pop(operation, None)
            else:
                self._chaos[operation] = rule

    def plan_next(self, *plans: LifecyclePlan) -> None:
        """Plans for the next accepted jobs, in order; afterwards ``default_plan`` applies."""
        with self._lock:
            self._plans.extend(plans)

    def plan_files(self, *outcomes: FileOutcome) -> None:
        """Final statuses for the next uploads, in order; afterwards ``processed``."""
        with self._lock:
            self._file_plans.extend(outcomes)

    def file(self, file_id: str) -> dict[str, Any]:
        with self._lock:
            return dict(self._files[file_id])

    def file_ids(self) -> list[str]:
        with self._lock:
            return list(self._files)

    def hide_from_list(self, batch_id: str, hidden: bool = True) -> None:
        """Retrievable but omitted from list pages (exercises the reconciler fallback)."""
        with self._lock:
            self._batches[batch_id].hidden_from_list = hidden

    def batch(self, batch_id: str) -> dict[str, Any]:
        with self._lock:
            return deepcopy(self._batches[batch_id].data)

    def batch_ids(self) -> list[str]:
        with self._lock:
            return list(self._batches)

    def endpoint_secret(self, endpoint_id: str) -> str:
        """Test/local helper: the value the receiver must hold (never exposed over the API)."""
        with self._lock:
            return self._endpoints[endpoint_id].secret

    def tick(self) -> int:
        """Advance every job to the clock's time and attempt due webhook deliveries."""
        self.advance()
        return self.deliver_due()

    def advance(self) -> list[str]:
        """Apply scheduled transitions due by now; returns batch ids that became terminal."""
        now = self.clock.now()
        finished: list[str] = []
        with self._lock:
            self._settle_files(now)
            progressed = True
            while progressed:
                progressed = False
                for batch in sorted(self._batches.values(), key=lambda b: b.seq):
                    if batch.next_status and batch.next_at and batch.next_at <= now:
                        status = batch.next_status
                        self._set_status(batch, status, batch.next_at)
                        progressed = True
                        if status in TERMINAL:
                            finished.append(str(batch.data["id"]))
        return finished

    def deliver_due(self) -> int:
        """Send due deliveries through the transport (outside the lock). Returns attempts."""
        if self.transport is None:
            return 0
        now = self.clock.now()
        with self._lock:
            due = [d for d in self.deliveries if d.state == "pending" and d.due_at <= now]
            calls = []
            for delivery in due:
                endpoint = self._endpoints.get(delivery.endpoint_id)
                if endpoint is None:  # deleted endpoint: nothing more is sent
                    delivery.state = "abandoned"
                    continue
                webhook_id = (
                    delivery.webhook_id
                    if self.delivery.stable_webhook_id
                    else f"{delivery.webhook_id}-{delivery.attempts + 1}"
                )
                headers = sign_headers(
                    endpoint.secret, webhook_id, int(now.timestamp()), delivery.body
                )
                headers["content-type"] = "application/json"
                call = WebhookCall(
                    endpoint.url, delivery.body, headers, endpoint.endpoint_id, self.resource_key
                )
                calls.append((delivery, call))
        for delivery, call in calls:
            try:
                status = self.transport(call)
            except Exception:  # noqa: BLE001 - a failing transport is a failed attempt
                status = None
            with self._lock:
                delivery.attempts += 1
                delivery.statuses.append(status)
                self.journal.append(
                    (to_iso(now) or "", "webhook", delivery.delivery_id, str(status))
                )
                if status is not None and 200 <= status < 300:
                    delivery.state = "delivered"
                elif delivery.attempts > len(self.delivery.retry_schedule_s):
                    delivery.state = "abandoned"
                else:
                    wait = self.delivery.retry_schedule_s[delivery.attempts - 1]
                    delivery.due_at = now + timedelta(seconds=wait)
        return len(calls)

    def pending_deliveries(self) -> int:
        with self._lock:
            return sum(1 for d in self.deliveries if d.state == "pending")

    # -- wire entry point -------------------------------------------------------------------

    def handle(self, request: WireRequest) -> WireResponse:
        operation, handler = self._route(request.method.upper(), request.path)
        if handler is None:
            return WireResponse.error(404, "route_not_found")
        if self.auto_advance:
            self.advance()
        with self._lock:
            self.calls[operation] += 1
            response = self._dispatch(operation, handler, request)
            outcome = "connect_timeout" if response.connect_timeout else str(response.status)
            if response.drop_connection:
                outcome = "dropped"
            self.journal.append((to_iso(self.clock.now()) or "", operation, outcome))
            return response

    def _dispatch(
        self, operation: str, handler: Callable[[WireRequest], WireResponse], request: WireRequest
    ) -> WireResponse:
        denied = self._authenticate(request.headers)
        if denied is not None:
            return denied
        fault = self._next_fault(operation)
        if fault is not None and fault.connect_timeout:
            return WireResponse(0, connect_timeout=True)
        if fault is not None and not fault.after_commit:
            return self._fault_response(fault)
        try:
            response = handler(request)
        except _ApiError as exc:
            response = WireResponse.error(exc.status, exc.code, message=exc.message)
        if fault is not None:  # committed, then the response is lost or replaced
            return self._fault_response(fault)
        return response

    # -- routing ----------------------------------------------------------------------------

    def _route(
        self, method: str, path: str
    ) -> tuple[str, Callable[[WireRequest], WireResponse] | None]:
        parts = [p for p in path.strip("/").split("/") if p]
        if parts == ["batches"]:
            if method == "GET":
                return "batch.list", self._list_batches
            if method == "POST":
                return "batch.create", self._create_batch
        if len(parts) == 2 and parts[0] == "batches" and method == "GET":
            return "batch.retrieve", lambda r: self._retrieve_batch(parts[1])
        if len(parts) == 3 and parts[0] == "batches" and parts[2] == "cancel" and method == "POST":
            return "batch.cancel", lambda r: self._cancel_batch(parts[1])
        if parts == ["files"] and method == "POST":
            return "files.upload", self._upload_file
        if len(parts) == 2 and parts[0] == "files":
            if method == "GET":
                return "files.retrieve", lambda r: self._retrieve_file(parts[1])
            if method == "DELETE":
                return "files.delete", lambda r: self._delete_file(parts[1])
        if parts == ["dashboard", "webhook_endpoints"]:
            if method == "GET":
                return "webhooks.list", self._list_endpoints
            if method == "POST":
                return "webhooks.create", self._create_endpoint
        if len(parts) == 3 and parts[:2] == ["dashboard", "webhook_endpoints"]:
            if method == "POST":
                return "webhooks.update", lambda r: self._update_endpoint(parts[2], r)
            if method == "DELETE":
                return "webhooks.delete", lambda r: self._delete_endpoint(parts[2])
        return "unknown", None

    def _authenticate(self, headers: Mapping[str, str]) -> WireResponse | None:
        lowered = {k.lower(): v for k, v in headers.items()}
        bearer = lowered.get("authorization", "")
        if bearer.startswith("Bearer ") and bearer[7:].strip():
            return None
        if lowered.get("api-key"):
            if self.key_auth_disabled:
                # Assumed shape for a resource with local auth disabled; confirm in SP-3.
                return WireResponse.error(403, "AuthenticationTypeDisabled")
            return None
        return WireResponse.error(401, "unauthorized")

    def _next_fault(self, operation: str) -> Fault | None:
        queued = self._faults.get(operation)
        if queued:
            return queued.popleft()
        rule = self._chaos.get(operation)
        if rule is not None and self._rng.random() < rule.probability:
            return rule.fault
        return None

    @staticmethod
    def _fault_response(fault: Fault) -> WireResponse:
        if fault.drop_connection:
            return WireResponse(0, drop_connection=True)
        return WireResponse.error(fault.status or 500, fault.code, fault.retry_after)

    # -- batches ----------------------------------------------------------------------------

    def _list_batches(self, request: WireRequest) -> WireResponse:
        try:
            limit = max(1, min(int(request.query.get("limit", "20")), 100))
        except ValueError as exc:
            raise _ApiError(400, "invalid_limit") from exc
        visible = [b for b in self._batches.values() if not b.hidden_from_list]
        visible.sort(key=lambda b: (int(b.data["created_at"]), b.seq), reverse=True)
        ids = [str(b.data["id"]) for b in visible]
        after = request.query.get("after")
        start = ids.index(after) + 1 if after in ids else 0
        chunk = visible[start : start + limit]
        data = [json.loads(json.dumps(b.data)) for b in chunk]
        return WireResponse.json(
            200,
            {
                "object": "list",
                "data": data,
                "first_id": data[0]["id"] if data else None,
                "last_id": data[-1]["id"] if data else None,
                "has_more": start + limit < len(ids),
            },
        )

    def _retrieve_batch(self, batch_id: str) -> WireResponse:
        batch = self._batches.get(batch_id)
        if batch is None:
            raise _ApiError(404, "not_found")
        return WireResponse.json(200, batch.data)

    def _create_batch(self, request: WireRequest) -> WireResponse:
        body = _json_body(request)
        file_id = body.get("input_file_id")
        if not isinstance(file_id, str) or file_id not in self._files:
            raise _ApiError(400, "invalid_input_file")
        file_status = self._files[file_id]["status"]
        if file_status != "processed":  # assumed shape of the documented refusal; confirm (SP-1)
            raise _ApiError(
                400,
                "invalid_request",
                f"Only files with status 'Processed' are accepted (file is '{file_status}').",
            )
        if body.get("completion_window") != "24h":
            raise _ApiError(400, "invalid_completion_window")
        endpoint = body.get("endpoint")
        if not isinstance(endpoint, str) or not endpoint.startswith("/"):
            raise _ApiError(400, "invalid_endpoint")
        metadata = body.get("metadata") or {}
        if not isinstance(metadata, dict) or len(metadata) > 16:
            raise _ApiError(400, "invalid_metadata")
        now = self.clock.now()
        created = int(now.timestamp())
        content = self._file_content[file_id]
        errors, total, model, input_tokens = _validate_input(content, endpoint, self.deployments)
        batch_id = f"batch_{self.resource_key}_{next(self._ids):05d}"
        data: dict[str, Any] = {
            "id": batch_id,
            "object": "batch",
            "endpoint": endpoint,
            "input_file_id": file_id,
            "completion_window": "24h",
            "status": "validating",
            "created_at": created,
            "expires_at": created + 24 * 3600,
            "in_progress_at": None,
            "finalizing_at": None,
            "completed_at": None,
            "failed_at": None,
            "expired_at": None,
            "cancelling_at": None,
            "cancelled_at": None,
            "output_file_id": None,
            "error_file_id": None,
            "errors": None,
            "model": model,
            "request_counts": {"total": total, "completed": 0, "failed": 0},
            "metadata": {str(k): str(v) for k, v in metadata.items()},
        }
        plan = self._plans.popleft() if self._plans else self.default_plan
        batch = _Batch(data=data, plan=plan, seq=next(self._ids), input_tokens=input_tokens)
        if errors:
            data["errors"] = {"object": "list", "data": [{"code": c, "line": n} for c, n in errors]}
            self._schedule(batch, "failed", now + timedelta(seconds=plan.validating_s))
        elif plan.outcome == "failed":  # accepted file, failed later (e.g. service-side)
            self._schedule(batch, "failed", now + timedelta(seconds=plan.validating_s))
        else:
            self._schedule(batch, "in_progress", now + timedelta(seconds=plan.validating_s))
        self._batches[batch_id] = batch
        return WireResponse.json(200, batch.data)

    def _cancel_batch(self, batch_id: str) -> WireResponse:
        batch = self._batches.get(batch_id)
        if batch is None:
            raise _ApiError(404, "not_found")
        status = batch.data["status"]
        if status in TERMINAL:
            raise _ApiError(409, "batch_not_cancellable")
        if status != "cancelling":
            self._set_status(batch, "cancelling", self.clock.now())  # cancelled 60 s later
        return WireResponse.json(200, batch.data)

    # -- files ------------------------------------------------------------------------------

    def _upload_file(self, request: WireRequest) -> WireResponse:
        fields_, files = _multipart(request)
        expiry = _expires_after(fields_)
        expiring = sum(1 for f in self._files.values() if f.get("expires_at") is not None)
        if expiry is None and len(self._files) - expiring >= MAX_INPUT_FILES:
            raise _ApiError(400, "too_many_files")
        if expiry is not None and expiring >= MAX_INPUT_FILES_WITH_EXPIRY:
            raise _ApiError(400, "too_many_files")
        if fields_.get("purpose") != "batch":
            raise _ApiError(400, "invalid_purpose")
        if "file" not in files:
            raise _ApiError(400, "missing_file")
        filename, content = files["file"]
        now = self.clock.now()
        created = int(now.timestamp())
        outcome: FileOutcome = self._file_plans.popleft() if self._file_plans else "processed"
        file_id = f"file-{self.resource_key}-{next(self._ids):05d}"
        self._files[file_id] = {
            "id": file_id,
            "object": "file",
            "bytes": len(content),
            "filename": filename,
            "purpose": "batch",
            "status": "pending" if self.file_processing_s > 0 else outcome,
            "status_details": None,
            "created_at": created,
            "expires_at": created + expiry if expiry is not None else None,
        }
        self._file_content[file_id] = content
        if self.file_processing_s > 0:
            ready = now + timedelta(seconds=self.file_processing_s)
            self._files_due[file_id] = (ready, outcome)
        return WireResponse.json(200, self._files[file_id])

    def _retrieve_file(self, file_id: str) -> WireResponse:
        if file_id not in self._files:
            raise _ApiError(404, "not_found")
        return WireResponse.json(200, self._files[file_id])

    def _delete_file(self, file_id: str) -> WireResponse:
        if file_id not in self._files:
            raise _ApiError(404, "not_found")
        self._drop_file(file_id)
        return WireResponse.json(200, {"id": file_id, "object": "file", "deleted": True})

    def _settle_files(self, now: datetime) -> None:
        for file_id, (ready, outcome) in list(self._files_due.items()):
            if ready <= now:
                self._files[file_id]["status"] = outcome
                del self._files_due[file_id]
        stamp = int(now.timestamp())
        for file_id, data in list(self._files.items()):
            if data.get("expires_at") is not None and data["expires_at"] <= stamp:
                self._drop_file(file_id)

    def _drop_file(self, file_id: str) -> None:
        self._files.pop(file_id, None)
        self._file_content.pop(file_id, None)
        self._files_due.pop(file_id, None)

    def _schedule(self, batch: _Batch, status: str, at: datetime) -> None:
        batch.next_status, batch.next_at = status, at

    def _set_status(self, batch: _Batch, status: str, at: datetime) -> None:
        data = batch.data
        data["status"] = status
        stamp = STAMP_FOR_STATUS.get(status)
        if stamp and not (status == "in_progress" and batch.plan.omit_in_progress_at):
            data[stamp] = int(at.timestamp())
        if status == "cancelling":
            batch.done_at_cancel = self._share_done(batch, at)
        batch.next_status, batch.next_at = None, None
        plan = batch.plan
        if status == "in_progress":
            batch.started_at = at
            if plan.outcome == "expired":
                self._schedule(batch, "expired", at + timedelta(seconds=plan.in_progress_s))
            elif plan.outcome == "cancelled":
                self._schedule(batch, "cancelling", at + timedelta(seconds=plan.in_progress_s / 2))
            else:
                self._schedule(batch, "finalizing", at + timedelta(seconds=plan.in_progress_s))
        elif status == "finalizing":
            self._schedule(batch, "completed", at + timedelta(seconds=plan.finalizing_s))
        elif status == "cancelling":
            self._schedule(batch, "cancelled", at + timedelta(seconds=60))  # CON-B03: <=10 min
        if status in ("completed", "cancelled", "expired"):
            self._settle_counts(batch, status)
        if status in TERMINAL:
            self._emit(batch, status, at)

    @staticmethod
    def _share_done(batch: _Batch, at: datetime) -> float:
        """Share of the work done by ``at``: none before it started, else prorated over the
        planned ``in_progress_s`` (all of it once finalizing)."""
        if batch.started_at is None:
            return 0.0
        if batch.plan.in_progress_s <= 0:
            return 1.0
        elapsed = (at - batch.started_at).total_seconds()
        return min(1.0, elapsed / batch.plan.in_progress_s)

    def _settle_counts(self, batch: _Batch, status: str) -> None:
        data = batch.data
        total = int(data["request_counts"]["total"])
        started = batch.started_at is not None
        failed = min(batch.plan.failed_requests, total) if started else 0
        if status == "completed":
            done = total - failed
        elif status == "cancelled":  # prorated by the work done when cancellation began
            done = min(total - failed, int(total * batch.done_at_cancel))
        else:  # expired: the window ran out with part of the work done (assumed half)
            done = min(total - failed, total // 2) if started else 0
        data["request_counts"] = {"total": total, "completed": done, "failed": failed}
        data["output_file_id"] = f"file-out-{data['id']}" if done else None
        data["error_file_id"] = f"file-err-{data['id']}" if failed else None
        used = batch.input_tokens * done // max(total, 1)
        data["usage"] = {
            "input_tokens": used,
            "output_tokens": 20 * done,
            "total_tokens": used + 20 * done,
        }

    # -- webhook endpoints ------------------------------------------------------------------

    def _list_endpoints(self, request: WireRequest) -> WireResponse:
        data = [_endpoint_view(e) for e in self._endpoints.values()]
        return WireResponse.json(200, {"object": "list", "data": data})

    def _create_endpoint(self, request: WireRequest) -> WireResponse:
        name, url, events = self._endpoint_fields(_json_body(request))
        self._reject_duplicate_url(url)
        if len(self._endpoints) >= self.max_endpoints:
            raise _ApiError(400, "endpoint_limit_reached")  # real limit unknown (Q-P06)
        endpoint_id = f"wh_{self.resource_key}_{next(self._ids):04d}"
        seed = f"{self.resource_key}|{endpoint_id}|{self._rng.random()}".encode()
        secret = "whsec_" + base64.b64encode(hashlib.sha256(seed).digest()).decode()
        endpoint = _Endpoint(
            endpoint_id, name, url, events, secret, int(self.clock.now().timestamp())
        )
        self._endpoints[endpoint_id] = endpoint
        return WireResponse.json(200, {**_endpoint_view(endpoint), "signing_secret": secret})

    def _update_endpoint(self, endpoint_id: str, request: WireRequest) -> WireResponse:
        endpoint = self._endpoints.get(endpoint_id)
        if endpoint is None:
            raise _ApiError(404, "not_found")
        name, url, events = self._endpoint_fields(_json_body(request))
        self._reject_duplicate_url(url, except_id=endpoint_id)
        endpoint.name, endpoint.url, endpoint.event_types = name, url, events  # secret unchanged
        return WireResponse.json(200, _endpoint_view(endpoint))

    def _reject_duplicate_url(self, url: str, except_id: str | None = None) -> None:
        # Observed on the live provider (22 Nov 2026): one endpoint per URL in a project.
        if any(e.url == url and e.endpoint_id != except_id for e in self._endpoints.values()):
            raise _ApiError(400, "url_already_registered")

    def _delete_endpoint(self, endpoint_id: str) -> WireResponse:
        if self._endpoints.pop(endpoint_id, None) is None:
            raise _ApiError(404, "not_found")
        return WireResponse.json(200, {"id": endpoint_id, "deleted": True})

    def _endpoint_fields(self, body: Mapping[str, Any]) -> tuple[str, str, tuple[str, ...]]:
        name, url, events = body.get("name"), body.get("url"), body.get("event_types")
        if not isinstance(name, str) or not name:
            raise _ApiError(400, "invalid_name")
        if not isinstance(url, str) or not _acceptable_url(url, self.allow_http_webhook_urls):
            raise _ApiError(400, "invalid_url")
        if (
            not isinstance(events, list)
            or not events
            or not all(isinstance(e, str) and e in BATCH_EVENT_TYPES for e in events)
        ):
            raise _ApiError(400, "invalid_event_types")
        return name, url, tuple(events)

    # -- events -----------------------------------------------------------------------------

    def _emit(self, batch: _Batch, status: str, at: datetime) -> None:
        """Queue the event for the transition at ``at`` (its time, not when it was applied)."""
        event_type = EVENT_FOR_STATUS[status]
        policy = self.delivery
        if event_type in policy.drop_event_types:
            return
        event_id = f"evt_{self.resource_key}_{next(self._ids):06d}"
        # Provisional payload (SP-4): the receiver relies only on type and data.id.
        body = json.dumps(
            {
                "object": "event",
                "id": event_id,
                "type": event_type,
                "created_at": int(at.timestamp()),
                "data": {"id": batch.data["id"]},
            },
            separators=(",", ":"),
        ).encode("utf-8")
        for endpoint in list(self._endpoints.values()):
            if event_type not in endpoint.event_types:
                continue
            if policy.drop_probability and self._rng.random() < policy.drop_probability:
                continue
            delay = policy.delay_s
            if policy.jitter_s:
                delay += self._rng.uniform(0.0, policy.jitter_s)
            delivery = Delivery(
                delivery_id=f"dlv_{next(self._ids):06d}",
                endpoint_id=endpoint.endpoint_id,
                url=endpoint.url,
                event_id=event_id,
                event_type=event_type,
                batch_id=str(batch.data["id"]),
                body=body,
                webhook_id=f"msg_{event_id}_{endpoint.endpoint_id}",
                due_at=at + timedelta(seconds=delay),
            )
            self.deliveries.append(delivery)
            if policy.duplicate_probability and self._rng.random() < policy.duplicate_probability:
                self.deliveries.append(
                    replace(
                        delivery,
                        delivery_id=f"dlv_{next(self._ids):06d}",
                        statuses=[],
                        due_at=delivery.due_at + timedelta(seconds=1),
                        duplicate_of=delivery.delivery_id,
                    )
                )


# ---------------------------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------------------------


class _ApiError(Exception):
    def __init__(self, status: int, code: str, message: str | None = None) -> None:
        super().__init__(code)
        self.status = status
        self.code = code
        self.message = message


def _endpoint_view(endpoint: _Endpoint) -> dict[str, Any]:
    return {
        "id": endpoint.endpoint_id,
        "object": "webhook_endpoint",
        "name": endpoint.name,
        "url": endpoint.url,
        "event_types": list(endpoint.event_types),
        "created_at": endpoint.created_at,
    }


def _acceptable_url(url: str, allow_http: bool) -> bool:
    parts = urlsplit(url)
    if parts.scheme == "https" and parts.netloc:
        return True
    return allow_http and parts.scheme == "http" and parts.hostname in ("127.0.0.1", "localhost")


def _expires_after(fields_: Mapping[str, str]) -> int | None:
    """``expires_after`` from multipart fields (``.anchor``/``.seconds`` as in the Azure REST
    sample, or ``[anchor]``/``[seconds]`` as the OpenAI SDK sends them). ``None`` if absent."""
    anchor = fields_.get("expires_after.anchor", fields_.get("expires_after[anchor]"))
    seconds = fields_.get("expires_after.seconds", fields_.get("expires_after[seconds]"))
    if anchor is None and seconds is None:
        return None
    low, high = FILE_EXPIRY_RANGE_S
    value = int(seconds) if seconds is not None and seconds.isdigit() else -1
    if anchor != "created_at" or not low <= value <= high:
        raise _ApiError(400, "invalid_expires_after")
    return value


def _json_body(request: WireRequest) -> dict[str, Any]:
    try:
        data = json.loads(request.body or b"{}")
    except (ValueError, UnicodeDecodeError) as exc:
        raise _ApiError(400, "invalid_json") from exc
    if not isinstance(data, dict):
        raise _ApiError(400, "invalid_json")
    return data


def _multipart(request: WireRequest) -> tuple[dict[str, str], dict[str, tuple[str, bytes]]]:
    content_type = next((v for k, v in request.headers.items() if k.lower() == "content-type"), "")
    if not content_type.startswith("multipart/form-data"):
        raise _ApiError(400, "expected_multipart")
    raw = f"Content-Type: {content_type}\r\n\r\n".encode() + request.body
    message = email.parser.BytesParser(policy=email.policy.HTTP).parsebytes(raw)
    fields_: dict[str, str] = {}
    files: dict[str, tuple[str, bytes]] = {}
    for part in message.walk():
        if part.is_multipart():
            continue
        name = part.get_param("name", header="content-disposition")
        if not isinstance(name, str):
            continue
        payload = part.get_payload(decode=True)
        content = payload if isinstance(payload, bytes) else b""
        filename = part.get_filename()
        if filename:
            files[name] = (filename, content)
        else:
            fields_[name] = content.decode("utf-8", errors="replace")
    return fields_, files


def _validate_input(
    content: bytes, endpoint: str, deployments: Sequence[str]
) -> tuple[list[tuple[str, int]], int, str | None, int]:
    """CON-B11 validation codes for a JSONL input file. Returns errors, lines, model, tokens."""
    lines = [line for line in content.splitlines() if line.strip()]
    if not lines:
        return [("empty_file", 0)], 0, None, 0
    if len(lines) > MAX_REQUESTS_PER_FILE:
        return [("too_many_tasks", 0)], len(lines), None, 0
    errors: list[tuple[str, int]] = []
    ids: set[str] = set()
    models: set[str] = set()
    tokens = 0
    target = endpoint.removeprefix("/v1")
    for number, line in enumerate(lines, start=1):
        try:
            item = json.loads(line)
        except (ValueError, UnicodeDecodeError):
            errors.append(("invalid_json_line", number))
            continue
        if not isinstance(item, dict):
            errors.append(("invalid_request", number))
            continue
        custom_id = str(item.get("custom_id", ""))
        if custom_id in ids:
            errors.append(("duplicate_custom_id", number))
        ids.add(custom_id)
        if str(item.get("url", "")).removeprefix("/v1") != target:
            errors.append(("url_mismatch", number))
        body = item.get("body")
        model = body.get("model") if isinstance(body, dict) else None
        if not isinstance(model, str) or model not in deployments:
            errors.append(("model_not_found", number))
        else:
            models.add(model)
        tokens += max(1, len(json.dumps(body)) // 4)  # character heuristic, like the estimator
    if len(models) > 1:
        errors.append(("model_mismatch", 0))
    return errors, len(lines), (sorted(models)[0] if models else None), tokens
