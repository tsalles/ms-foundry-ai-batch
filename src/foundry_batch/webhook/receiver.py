"""Public webhook receiver: verify raw bytes, durably enqueue, then acknowledge (US-3.1/3.2/3.5).

Contract with the provider:

* 2xx is returned only after the envelope is accepted by the durable queue;
* a queue failure or unknown enqueue outcome returns 503 so the provider can retry
  (duplicates are safe: the processor deduplicates);
* signature, timestamp and header problems return 4xx and nothing is queued;
* an unknown resource key is checked like a known one and answered 401, so callers cannot
  enumerate the configured resources;
* verified non-batch event types are acknowledged and ignored;
* the per-resource rate limit applies to verified deliveries only, so unauthenticated traffic
  cannot use up the provider's budget (edge protection belongs to the platform, D-02);
* our own configuration faults (no readable or no well-formed secret, secret store outage)
  return 503 + Retry-After, never 401, so the provider keeps the delivery.

Every 401 has the same body and so does every 503: the reason codes go to logs and metrics
only. No business rules run here (REQ-W04). Logs never carry secrets or bodies.
"""

from __future__ import annotations

import json
import logging
import math
import secrets as pysecrets
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import NoReturn

from foundry_batch.clock import Clock
from foundry_batch.config import BATCH_EVENT_TYPES, Settings
from foundry_batch.errors import QueueUnavailableError, SecretUnavailableError
from foundry_batch.observability import log_event
from foundry_batch.ports import MessageQueue, SecretReader
from foundry_batch.webhook import signature
from foundry_batch.webhook.envelope import Envelope, extract_batch_id
from foundry_batch.webhook.http import HttpRequest, HttpResponse
from foundry_batch.webhook.ratelimit import TokenBucket
from foundry_batch.webhook.secrets import SigningSecretResolver

logger = logging.getLogger(__name__)

_RETRY_AFTER_SECONDS = "30"
#: Azure Storage queue messages are limited to 64 KiB; keep a margin for XML escaping.
MAX_ENVELOPE_BYTES = 60 * 1024
_CONFIG_FAULTS = frozenset({"secret_malformed", "no_secret_configured"})
#: The only bodies an unauthenticated caller sees for 401 and 503.
UNAUTHORIZED_BODY: Mapping[str, str] = {"error": "unauthorized"}
RETRY_BODY: Mapping[str, str] = {"status": "retry"}
_UNLISTED = "unlisted"
#: Random per process: an unknown resource gets the same header, timestamp and HMAC checks as
#: a known one, then fails whatever it carries.
_UNLISTED_RESOURCE_SECRET = pysecrets.token_hex(32)


@dataclass(frozen=True)
class ReceiverOutcome:
    """Result for tests and metrics; the HTTP response is what the provider sees."""

    response: HttpResponse
    reason: str
    enqueued: bool = False


class WebhookReceiver:
    def __init__(
        self,
        settings: Settings,
        queue: MessageQueue,
        secrets: SecretReader | SigningSecretResolver,
        clock: Clock,
        *,
        limiter: TokenBucket | None = None,
    ) -> None:
        self._settings = settings
        self._queue = queue
        self._clock = clock
        self._secrets = (
            secrets
            if isinstance(secrets, SigningSecretResolver)
            else SigningSecretResolver(secrets, settings, clock)
        )
        self._limiter = limiter or TokenBucket(
            settings.receiver_rate_per_second, settings.receiver_burst, clock
        )

    def handle(self, request: HttpRequest) -> HttpResponse:
        return self.process(request).response

    def process(self, request: HttpRequest) -> ReceiverOutcome:  # flat guard list
        resource_key = request.route_params.get("resource_key", "")
        known = self._settings.resource(resource_key) is not None
        log_key = resource_key if known else _UNLISTED  # never log an arbitrary path segment
        if request.method.upper() != "POST":
            return self._reject(405, "method_not_allowed", log_key, {"allow": "POST"})

        declared = request.headers.get("content-length") or request.headers.get("Content-Length")
        if declared and declared.isdigit() and int(declared) > self._settings.max_body_bytes:
            return self._reject(413, "body_too_large", log_key)
        if len(request.body) > self._settings.max_body_bytes:
            return self._reject(413, "body_too_large", log_key)

        now = self._clock.now()
        try:
            verified = (
                self._authenticate(request, resource_key, now)
                if known
                else self._reject_unlisted(request, now)
            )
        except SecretUnavailableError:
            return self._reject(
                503,
                "secret_store_unavailable",
                log_key,
                {"retry-after": _RETRY_AFTER_SECONDS},
            )
        except signature.SignatureError as exc:
            if exc.reason in _CONFIG_FAULTS:
                return self._reject(503, exc.reason, log_key, {"retry-after": _RETRY_AFTER_SECONDS})
            status = 400 if exc.reason in ("missing_headers", "timestamp_malformed") else 401
            return self._reject(status, exc.reason, log_key)

        wait = self._limiter.try_acquire(resource_key)
        if wait > 0:
            retry_after = str(max(1, math.ceil(wait)))
            return self._reject(429, "rate_limited", resource_key, {"retry-after": retry_after})

        try:
            event = json.loads(request.body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return self._reject(400, "invalid_json", resource_key)
        if not isinstance(event, dict):
            return self._reject(400, "invalid_json", resource_key)

        event_type = event.get("type")
        if event_type not in BATCH_EVENT_TYPES:
            log_event(
                logger,
                "webhook.ignored",
                resource_key=resource_key,
                event_type=str(event_type)[:64],
                webhook_id=verified.webhook_id,
            )
            return ReceiverOutcome(HttpResponse.json(200, {"status": "ignored"}), "ignored")

        event_id = event.get("id") if isinstance(event.get("id"), str) else None
        created_at = event.get("created_at") if isinstance(event.get("created_at"), int) else None
        envelope = Envelope(
            environment=self._settings.environment,
            resource_key=resource_key,
            webhook_id=verified.webhook_id,
            delivery_timestamp=verified.timestamp,
            received_at=now,
            event_id=event_id,
            event_type=str(event_type),
            event_created_at=created_at,
            batch_id=extract_batch_id(event),
            event=event,
        )
        message = envelope.to_json()
        if len(message.encode("utf-8")) > MAX_ENVELOPE_BYTES:
            # Escaping can grow a body under the cap past the queue limit; never 503-loop on it.
            return self._reject(413, "envelope_too_large", resource_key)
        try:
            self._queue.send(message)
        except QueueUnavailableError:
            log_event(
                logger,
                "webhook.enqueue_failed",
                level=logging.ERROR,
                resource_key=resource_key,
                webhook_id=verified.webhook_id,
            )
            return ReceiverOutcome(
                HttpResponse.json(503, RETRY_BODY, {"retry-after": _RETRY_AFTER_SECONDS}),
                "enqueue_failed",
            )

        elapsed_ms = (self._clock.now() - now).total_seconds() * 1000
        delay = (now.timestamp() - created_at) if created_at else None
        log_event(
            logger,
            "webhook.accepted",
            resource_key=resource_key,
            event_type=str(event_type),
            webhook_id=verified.webhook_id,
            event_id=event_id,
            batch_id=envelope.batch_id,
            secret_slot=verified.secret_index,
            delivery_delay_s=round(delay, 3) if delay is not None else None,
            ack_ms=round(elapsed_ms, 2),
        )
        return ReceiverOutcome(HttpResponse.json(200, {"status": "accepted"}), "accepted", True)

    def _reject_unlisted(self, request: HttpRequest, now: datetime) -> NoReturn:
        """Fail an unknown resource key exactly where a wrong signature would fail.

        Header and timestamp problems keep their own status (as for a known resource); a
        well-formed delivery then fails as ``unknown_resource``, which is answered 401.
        """
        try:
            self._verify(request, (_UNLISTED_RESOURCE_SECRET,), now)
        except signature.SignatureError as exc:
            if exc.reason != "signature_mismatch":
                raise
        raise signature.SignatureError("unknown_resource")

    def _authenticate(
        self, request: HttpRequest, resource_key: str, now: datetime
    ) -> signature.VerifiedDelivery:
        """Verify with the cached secrets; on a mismatch, retry once with refreshed secrets."""
        secrets = self._secrets.secrets_for(resource_key)
        if not secrets:
            secrets = self._secrets.refresh_if_allowed(resource_key) or ()
        if not secrets:
            raise signature.SignatureError("no_secret_configured")
        try:
            return self._verify(request, secrets, now)
        except signature.SignatureError as exc:
            if exc.reason not in ("signature_mismatch", "secret_malformed"):
                raise
            refreshed = self._secrets.refresh_if_allowed(resource_key)
            if not refreshed or refreshed == secrets:
                raise
            return self._verify(request, refreshed, now)

    def _verify(
        self, request: HttpRequest, secrets: tuple[str, ...], now: datetime
    ) -> signature.VerifiedDelivery:
        return signature.verify(
            request.body,
            request.headers,
            secrets,
            now=now,
            tolerance_seconds=self._settings.timestamp_tolerance_seconds,
        )

    def _reject(
        self,
        status: int,
        reason: str,
        resource_key: str,
        headers: dict[str, str] | None = None,
    ) -> ReceiverOutcome:
        log_event(
            logger,
            "webhook.rejected",
            level=logging.WARNING,
            resource_key=resource_key or "none",
            reason=reason,
            status=status,
        )
        body = _public_body(status, reason)
        return ReceiverOutcome(HttpResponse.json(status, body, headers), reason)


def _public_body(status: int, reason: str) -> Mapping[str, str]:
    """What a caller may learn: authentication and our own faults are opaque."""
    if status == 401:
        return UNAUTHORIZED_BODY
    if status == 503:
        return RETRY_BODY
    return {"error": reason}
