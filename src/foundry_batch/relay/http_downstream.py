"""HTTP downstream client: signs each delivery with Standard Webhooks headers.

The relay signs with its own secret (``relay-signing-<env>`` in Key Vault), so the demo API and,
later, Batch can authenticate the relay with any Standard Webhooks library. ``webhook-id`` and
``Idempotency-Key`` both carry the stable business key.
"""

from __future__ import annotations

from collections.abc import Callable

import requests

from foundry_batch.clock import Clock
from foundry_batch.ports import DeliveryResult
from foundry_batch.relay.backoff import parse_retry_after
from foundry_batch.webhook.signature import sign_headers


class HttpDownstream:
    def __init__(
        self,
        url: str,
        secret_provider: Callable[[], str | None],
        clock: Clock,
        *,
        session: requests.Session | None = None,
        timeout_seconds: tuple[float, float] = (5.0, 20.0),
    ) -> None:
        if not url.startswith(("https://", "http://127.0.0.1", "http://localhost")):
            raise ValueError("downstream URL must use https")
        self._url = url
        self._secret_provider = secret_provider
        self._clock = clock
        self._session = session or requests.Session()
        self._timeout = timeout_seconds

    def deliver(self, body: bytes, *, idempotency_key: str) -> DeliveryResult:
        try:
            secret = self._secret_provider()
        except Exception as exc:  # noqa: BLE001 - Key Vault/credential outage is retryable
            return DeliveryResult(status_code=None, error=f"relay_secret_{type(exc).__name__}")
        if not secret:
            # Treated as retryable: the outbox keeps the item until the secret exists.
            return DeliveryResult(status_code=None, error="relay_secret_missing")
        timestamp = int(self._clock.now().timestamp())
        headers = sign_headers(secret, idempotency_key, timestamp, body)
        headers["content-type"] = "application/json"
        headers["idempotency-key"] = idempotency_key
        try:
            # Never follow redirects: requests would re-send the POST as a GET and a 200 from
            # the redirect target (e.g. a sign-in page) would count as delivered. A 3xx is a
            # permanent misconfiguration (DeliveryResult.retryable is False for it).
            response = self._session.post(
                self._url,
                data=body,
                headers=headers,
                timeout=self._timeout,
                allow_redirects=False,
            )
        except requests.Timeout:
            return DeliveryResult(status_code=None, error="timeout")
        except requests.RequestException as exc:
            return DeliveryResult(status_code=None, error=type(exc).__name__)
        retry_after = parse_retry_after(response.headers.get("retry-after"), now=self._clock.now())
        error = None if 200 <= response.status_code < 300 else f"http_{response.status_code}"
        return DeliveryResult(
            status_code=response.status_code, retry_after_seconds=retry_after, error=error
        )
