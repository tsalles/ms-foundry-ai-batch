"""Demo downstream API for M1 (US-4.1 T-4.1.2).

Authenticates the relay's Standard Webhooks signature, durably deduplicates on the business
idempotency key and stores metadata only. It is the reference behaviour Batch's API must match
before exactly-one business effect can be claimed (US-4.1 AC5).
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable

from foundry_batch.clock import Clock, to_iso
from foundry_batch.domain.contract import validate_event
from foundry_batch.errors import ConcurrencyConflictError
from foundry_batch.observability import log_event
from foundry_batch.ports import Entity, TableStore, TxOp
from foundry_batch.webhook import signature
from foundry_batch.webhook.http import HttpRequest, HttpResponse

logger = logging.getLogger(__name__)

MAX_BODY_BYTES = 48 * 1024


class DemoDownstreamApi:
    def __init__(
        self,
        store: TableStore,
        table: str,
        secret_provider: Callable[[], str | None],
        clock: Clock,
        *,
        tolerance_seconds: int = 300,
    ) -> None:
        self._store = store
        self._table = table
        self._secret_provider = secret_provider
        self._clock = clock
        self._tolerance = tolerance_seconds

    def handle(self, request: HttpRequest) -> HttpResponse:
        if request.method.upper() != "POST":
            return HttpResponse.json(405, {"error": "method_not_allowed"}, {"allow": "POST"})
        if len(request.body) > MAX_BODY_BYTES:
            return HttpResponse.json(413, {"error": "body_too_large"})
        secret = self._secret_provider()
        if not secret:
            return HttpResponse.json(503, {"error": "not_ready"}, {"retry-after": "30"})
        try:
            verified = signature.verify(
                request.body,
                request.headers,
                [secret],
                now=self._clock.now(),
                tolerance_seconds=self._tolerance,
            )
        except signature.SignatureError as exc:
            log_event(logger, "demo.rejected", level=logging.WARNING, reason=exc.reason)
            return HttpResponse.json(401, {"error": exc.reason})
        try:
            event = json.loads(request.body)
        except json.JSONDecodeError:
            return HttpResponse.json(400, {"error": "invalid_json"})
        problems = validate_event(event) if isinstance(event, dict) else ["not_object"]
        if problems:
            return HttpResponse.json(422, {"error": "invalid_event", "fields": problems})
        key = str(event["idempotency_key"])
        if key != verified.webhook_id:
            return HttpResponse.json(422, {"error": "idempotency_key_mismatch"})
        entity = Entity(
            partition_key=str(event["environment"]),
            row_key=key,
            data={
                "job_id": event["job_id"],
                "outcome": event["outcome"],
                "raw_status": event["raw_status"],
                "source": event["source"],
                "is_correction": bool(event.get("is_correction")),
                "received_at": to_iso(self._clock.now()),
            },
        )
        try:
            self._store.submit(self._table, [TxOp("create", entity)])
        except ConcurrencyConflictError:
            log_event(logger, "demo.duplicate", idempotency_key=key)
            return HttpResponse.json(200, {"status": "duplicate"})
        log_event(
            logger,
            "demo.accepted",
            idempotency_key=key,
            outcome=event["outcome"],
            source=event["source"],
        )
        return HttpResponse.json(201, {"status": "accepted"})
