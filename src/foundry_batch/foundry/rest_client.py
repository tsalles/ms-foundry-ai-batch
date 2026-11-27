"""REST client for the Azure OpenAI v1 data plane used by the PoC.

Endpoints (Microsoft Learn, checked 16 Nov 2026):

* ``GET  /openai/v1/batches?limit=&after=`` (paginated, ``has_more``/``last_id``)
* ``GET  /openai/v1/batches/{id}``, ``POST /openai/v1/batches/{id}/cancel``
* ``POST /openai/v1/batches``, ``POST /openai/v1/files`` (purpose ``batch``)
* ``GET|DELETE /openai/v1/files/{id}`` (upload status, cleanup)
* ``GET|POST /openai/v1/dashboard/webhook_endpoints`` and ``POST|DELETE .../{id}``

Input files are uploaded with ``expires_after`` (anchor ``created_at``, 14 to 30 days per the
batch how-to, checked 17 Nov 2026): CON-B08 allows 500 input files per resource without an
expiration and 10,000 with one.

Errors are mapped to retryable, ambiguous (a POST that may have taken effect), not-found and
request errors. Only error *codes* are kept: provider messages can echo input content.
"""

from __future__ import annotations

import contextlib
import json
from collections.abc import Mapping, Sequence
from typing import Any
from urllib.parse import quote

import requests

from foundry_batch.clock import Clock, SystemClock
from foundry_batch.domain.models import BatchSnapshot
from foundry_batch.errors import (
    EndpointSecretMissingError,
    ProviderAmbiguousError,
    ProviderError,
    ProviderNotFoundError,
    ProviderRequestError,
    ProviderRetryableError,
)
from foundry_batch.foundry.auth import AuthProvider
from foundry_batch.foundry.files import ProviderFile
from foundry_batch.metrics.api_pressure import ApiCallCounter
from foundry_batch.ports import (
    BatchPage,
    CreatedWebhookEndpoint,
    WebhookEndpoint,
)
from foundry_batch.relay.backoff import parse_retry_after

#: Retryable 4xx statuses; every 5xx is retryable too.
_RETRYABLE_4XX = frozenset({408, 425, 429})
#: Shortest input-file expiration the batch how-to allows (14 days); the longest is 30 days.
INPUT_FILE_EXPIRY_SECONDS = 14 * 24 * 3600
INPUT_FILE_EXPIRY_MAX_SECONDS = 30 * 24 * 3600


def _retryable(status: int) -> bool:
    return status in _RETRYABLE_4XX or 500 <= status <= 599


def _ambiguous_on_write(status: int) -> bool:
    """A write answered with 408 or a 5xx other than 503 may have committed first (gateway
    timeouts): the outcome is unknown, so callers reconcile instead of retrying blindly
    (US-2.2, US-4.1 AC4). 503 means the service did not accept the request."""
    return status == 408 or (500 <= status <= 599 and status != 503)


class FoundryRestClient:
    def __init__(
        self,
        resource_key: str,
        endpoint: str,
        auth: AuthProvider,
        *,
        session: requests.Session | None = None,
        counter: ApiCallCounter | None = None,
        timeout_seconds: tuple[float, float] = (5.0, 30.0),
        clock: Clock | None = None,
        input_file_expiry_seconds: int | None = INPUT_FILE_EXPIRY_SECONDS,
    ) -> None:
        if not endpoint.startswith(("https://", "http://127.0.0.1", "http://localhost")):
            raise ValueError("endpoint must use https")
        if input_file_expiry_seconds is not None and not (
            INPUT_FILE_EXPIRY_SECONDS <= input_file_expiry_seconds <= INPUT_FILE_EXPIRY_MAX_SECONDS
        ):
            raise ValueError("input_file_expiry_seconds must be 14 to 30 days, or None")
        self.resource_key = resource_key
        self._base = endpoint.rstrip("/") + "/openai/v1/"
        self._auth = auth
        self._session = session or requests.Session()
        self.counter = counter or ApiCallCounter()
        self._timeout = timeout_seconds
        self._clock = clock or SystemClock()  # reference time for HTTP-date Retry-After
        self._file_expiry = input_file_expiry_seconds

    # -- batches --------------------------------------------------------------------------

    def list_batches(self, *, after: str | None, limit: int) -> BatchPage:
        params: dict[str, str | int] = {"limit": max(1, min(limit, 100))}
        if after:
            params["after"] = after
        data = self._request("GET", "batches", "batch.list", params=params)
        raw_items = data.get("data", [])
        items = tuple(
            BatchSnapshot.from_api(item)
            for item in raw_items
            if isinstance(item, Mapping) and item.get("id") and item.get("status")
        )
        last_id = data.get("last_id") or (items[-1].batch_id if items else None)
        return BatchPage(items=items, has_more=bool(data.get("has_more")), last_id=last_id)

    def retrieve_batch(self, batch_id: str) -> BatchSnapshot:
        data = self._request("GET", f"batches/{quote(batch_id, safe='')}", "batch.retrieve")
        return BatchSnapshot.from_api(data)

    def cancel_batch(self, batch_id: str) -> BatchSnapshot:
        path = f"batches/{quote(batch_id, safe='')}/cancel"
        return BatchSnapshot.from_api(self._request("POST", path, "batch.cancel"))

    def create_batch(
        self, *, input_file_id: str, endpoint: str, metadata: Mapping[str, str]
    ) -> BatchSnapshot:
        body = {
            "input_file_id": input_file_id,
            "endpoint": endpoint,
            "completion_window": "24h",
            "metadata": dict(metadata),
        }
        return BatchSnapshot.from_api(self._request("POST", "batches", "batch.create", body=body))

    def upload_file(self, *, filename: str, content: bytes) -> str:
        form = {"purpose": "batch"}
        if self._file_expiry is not None:
            form["expires_after.anchor"] = "created_at"
            form["expires_after.seconds"] = str(self._file_expiry)
        data = self._request(
            "POST",
            "files",
            "files.upload",
            files={"file": (filename, content, "application/jsonl")},
            form=form,
        )
        file_id = data.get("id")
        if not isinstance(file_id, str) or not file_id:
            raise ProviderRequestError("file upload returned no id")
        return file_id

    def retrieve_file(self, file_id: str) -> ProviderFile:
        data = self._request("GET", f"files/{quote(file_id, safe='')}", "files.retrieve")
        status, expires_at = data.get("status"), data.get("expires_at")
        return ProviderFile(
            file_id=str(data.get("id") or file_id),
            status=status if isinstance(status, str) and status else "unknown",
            expires_at=expires_at if isinstance(expires_at, int) else None,
        )

    def delete_file(self, file_id: str) -> None:
        """Idempotent: a file that is already gone counts as deleted."""
        with contextlib.suppress(ProviderNotFoundError):
            path = f"files/{quote(file_id, safe='')}"
            self._request("DELETE", path, "files.delete", allow_empty=True)

    # -- webhook endpoints ----------------------------------------------------------------

    def list_endpoints(self) -> list[WebhookEndpoint]:
        data = self._request("GET", "dashboard/webhook_endpoints", "webhooks.list")
        raw = data.get("data", [])
        return [_endpoint(item) for item in raw if isinstance(item, Mapping) and item.get("id")]

    def create_endpoint(
        self, *, name: str, url: str, event_types: Sequence[str]
    ) -> CreatedWebhookEndpoint:
        body = {"name": name, "url": url, "event_types": list(event_types)}
        data = self._request("POST", "dashboard/webhook_endpoints", "webhooks.create", body=body)
        secret = data.get("signing_secret")
        if not isinstance(secret, str) or not secret:
            # Without the one-time secret the endpoint is unusable; the caller compensates.
            raise EndpointSecretMissingError(
                "create endpoint returned no signing secret",
                endpoint_id=str(data.get("id") or ""),
            )
        return CreatedWebhookEndpoint(endpoint=_endpoint(data), signing_secret=secret)

    def update_endpoint(
        self, endpoint_id: str, *, name: str, url: str, event_types: Sequence[str]
    ) -> WebhookEndpoint:
        body = {"name": name, "url": url, "event_types": list(event_types)}
        path = f"dashboard/webhook_endpoints/{quote(endpoint_id, safe='')}"
        return _endpoint(self._request("POST", path, "webhooks.update", body=body))

    def delete_endpoint(self, endpoint_id: str) -> None:
        """Idempotent: an endpoint that is already gone counts as deleted."""
        path = f"dashboard/webhook_endpoints/{quote(endpoint_id, safe='')}"
        with contextlib.suppress(ProviderNotFoundError):
            self._request("DELETE", path, "webhooks.delete", allow_empty=True)

    # -- transport ------------------------------------------------------------------------

    def _request(  # one choke point for all calls
        self,
        method: str,
        path: str,
        operation: str,
        *,
        params: Mapping[str, str | int] | None = None,
        body: Mapping[str, Any] | None = None,
        files: Mapping[str, tuple[str, bytes, str]] | None = None,
        form: Mapping[str, str] | None = None,
        allow_empty: bool = False,
    ) -> dict[str, Any]:
        try:
            auth_headers = self._auth.headers()
        except ProviderError:
            raise
        except Exception as exc:
            raise ProviderRetryableError(f"{operation}: auth {type(exc).__name__}") from exc
        headers = {"accept": "application/json", **auth_headers}
        write = method != "GET"
        try:
            response = self._session.request(
                method,
                self._base + path,
                params=params,
                json=body,
                files=files,
                data=form,
                headers=headers,
                timeout=self._timeout,
            )
        except requests.RequestException as exc:
            self.counter.record(self.resource_key, operation, None)
            if write and not isinstance(exc, requests.ConnectTimeout):
                # Unless the connection was never made, the request may have reached the
                # service: the effect is unknown and the caller must reconcile.
                raise ProviderAmbiguousError(f"{operation}: {type(exc).__name__}") from exc
            raise ProviderRetryableError(f"{operation}: {type(exc).__name__}") from exc

        status = response.status_code
        self.counter.record(self.resource_key, operation, status)
        if 200 <= status < 300:
            # A write that returned 2xx took effect even if the body is unusable.
            unreadable = ProviderAmbiguousError if write else ProviderRetryableError
            if not response.content:
                if allow_empty:
                    return {}
                raise unreadable(f"{operation}: empty response", status_code=status)
            try:
                data = response.json()
            except (ValueError, json.JSONDecodeError) as exc:
                raise unreadable(f"{operation}: invalid JSON", status_code=status) from exc
            if isinstance(data, list):
                return {"data": data}
            return data if isinstance(data, dict) else {}

        code = _error_code(response)
        message = f"{operation}: HTTP {status} {code}"
        if status == 404:
            raise ProviderNotFoundError(message, status_code=status)
        if _retryable(status):
            retry_after = parse_retry_after(
                response.headers.get("retry-after"), now=self._clock.now()
            )
            if write and _ambiguous_on_write(status):
                raise ProviderAmbiguousError(
                    message, status_code=status, retry_after_seconds=retry_after
                )
            raise ProviderRetryableError(
                message, status_code=status, retry_after_seconds=retry_after
            )
        raise ProviderRequestError(message, status_code=status)


def _endpoint(data: Mapping[str, Any]) -> WebhookEndpoint:
    events = data.get("event_types") or []
    return WebhookEndpoint(
        endpoint_id=str(data.get("id", "")),
        name=str(data.get("name", "")),
        url=str(data.get("url", "")),
        event_types=tuple(str(e) for e in events) if isinstance(events, list) else (),
    )


def _error_code(response: requests.Response) -> str:
    try:
        data = response.json()
    except ValueError:
        return "unknown"
    error = data.get("error") if isinstance(data, dict) else None
    if isinstance(error, dict):
        return str(error.get("code") or error.get("type") or "unknown")[:64]
    # Batch input rejections arrive as {"errors": {"data": [{"code": ...}]}} (observed in the lab).
    errors = data.get("errors") if isinstance(data, dict) else None
    items = errors.get("data") if isinstance(errors, dict) else None
    if isinstance(items, list) and items and isinstance(items[0], dict):
        return str(items[0].get("code") or "unknown")[:64]
    return "unknown"


class FoundryClientFactory:
    """``BatchApiFactory`` over a fixed map of per-resource clients."""

    def __init__(self, clients: Mapping[str, FoundryRestClient]) -> None:
        self._clients = dict(clients)

    def for_resource(self, resource_key: str) -> FoundryRestClient:
        try:
            return self._clients[resource_key]
        except KeyError as exc:
            raise KeyError(f"no client for resource {resource_key!r}") from exc
