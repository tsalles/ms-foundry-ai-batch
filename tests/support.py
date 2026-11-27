"""Shared test doubles: a provider simulator, endpoint API fake, settings and signed requests."""

from __future__ import annotations

import base64
import itertools
import json
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any

from foundry_batch.clock import ManualClock
from foundry_batch.config import Settings
from foundry_batch.domain.models import BatchSnapshot
from foundry_batch.errors import ProviderRequestError
from foundry_batch.ports import BatchPage, CreatedWebhookEndpoint, WebhookEndpoint
from foundry_batch.webhook.http import HttpRequest
from foundry_batch.webhook.signature import sign_headers

T0 = datetime(2026, 11, 20, 12, 0, 0, tzinfo=UTC)
SECRET_A = "whsec_" + base64.b64encode(b"a" * 32).decode()
SECRET_B = "whsec_" + base64.b64encode(b"b" * 32).decode()
RELAY_SECRET = "whsec_" + base64.b64encode(b"r" * 32).decode()

RESOURCES = [
    {
        "key": "eus2",
        "endpoint": "https://res-eus2.openai.azure.com/",
        "region": "eastus2",
        "deployments": ["gpt-batch"],
    },
    {
        "key": "swc",
        "endpoint": "https://res-swc.openai.azure.com/",
        "region": "swedencentral",
        "deployments": ["gpt-batch"],
    },
]


def make_env(**overrides: str) -> dict[str, str]:
    env = {
        "BATCH_ENVIRONMENT": "test",
        "BATCH_RESOURCES_JSON": json.dumps(RESOURCES),
        "BATCH_KEY_VAULT_URI": "https://kv.vault.azure.net/",
        "WEBSITE_HOSTNAME": "func.example.net",
    }
    env.update(overrides)
    return env


def make_settings(**overrides: str) -> Settings:
    return Settings.from_env(make_env(**overrides))


def clock() -> ManualClock:
    return ManualClock(T0)


def signed_request(
    event: Mapping[str, Any] | bytes,
    *,
    secret: str = SECRET_A,
    resource_key: str = "eus2",
    webhook_id: str = "msg_1",
    timestamp: int | None = None,
    method: str = "POST",
) -> HttpRequest:
    body = event if isinstance(event, bytes) else json.dumps(event).encode()
    ts = int(T0.timestamp()) if timestamp is None else timestamp
    headers = sign_headers(secret, webhook_id, ts, body)
    headers["content-type"] = "application/json"
    return HttpRequest(
        method=method, body=body, headers=headers, route_params={"resource_key": resource_key}
    )


def batch_event(
    batch_id: str,
    event_type: str = "batch.completed",
    event_id: str = "evt_1",
    created_at: int | None = None,
) -> dict[str, Any]:
    return {
        "object": "event",
        "id": event_id,
        "type": event_type,
        "created_at": int(T0.timestamp()) - 5 if created_at is None else created_at,
        "data": {"id": batch_id},
    }


class FakeProvider:
    """In-memory Azure OpenAI batch service: newest-first paging, timestamps, fault injection."""

    def __init__(self, clock: ManualClock, resource_key: str = "eus2") -> None:
        self.clock = clock
        self.resource_key = resource_key
        self.batches: dict[str, dict[str, Any]] = {}
        self.order: list[str] = []
        self.calls: Counter[str] = Counter()
        self.faults: dict[str, list[Exception]] = {}
        self.files: dict[str, bytes] = {}
        self._ids = itertools.count(1)
        self.descending = True

    # -- test controls ----------------------------------------------------------------------

    def add(
        self,
        status: str = "validating",
        *,
        created_offset: int = 0,
        total: int = 3,
        batch_id: str | None = None,
        input_file_id: str | None = None,
        metadata: Mapping[str, str] | None = None,
    ) -> str:
        batch_id = batch_id or f"batch_{next(self._ids):04d}"
        created = int(self.clock.now().timestamp()) + created_offset
        self.batches[batch_id] = {
            "id": batch_id,
            "object": "batch",
            "status": "validating",
            "created_at": created,
            "input_file_id": input_file_id or f"file-{batch_id}",
            "request_counts": {"total": total, "completed": 0, "failed": 0},
            "metadata": dict(metadata or {}),
            "endpoint": "/chat/completions",
        }
        self.order.append(batch_id)
        if status != "validating":
            self.set_status(batch_id, status)
        return batch_id

    def set_status(self, batch_id: str, status: str, *, failed: int = 0) -> None:
        now = int(self.clock.now().timestamp())
        batch = self.batches[batch_id]
        batch["status"] = status
        if status in ("in_progress", "finalizing", "completed"):
            batch.setdefault("in_progress_at", now)
        if status in ("finalizing", "completed"):
            batch.setdefault("finalizing_at", now)
        stamp = {
            "completed": "completed_at",
            "failed": "failed_at",
            "expired": "expired_at",
            "cancelled": "cancelled_at",
            "cancelling": "cancelling_at",
        }.get(status)
        if stamp:
            batch[stamp] = now
        if status == "completed":
            total = batch["request_counts"]["total"]
            batch["request_counts"] = {
                "total": total,
                "completed": total - failed,
                "failed": failed,
            }

    def fail(self, operation: str, *errors: Exception) -> None:
        self.faults.setdefault(operation, []).extend(errors)

    def _maybe_fail(self, operation: str) -> None:
        self.calls[operation] += 1
        pending = self.faults.get(operation)
        if pending:
            raise pending.pop(0)

    # -- BatchApi -----------------------------------------------------------------------------

    def list_batches(self, *, after: str | None, limit: int) -> BatchPage:
        self._maybe_fail("list")
        ids = list(reversed(self.order)) if self.descending else list(self.order)
        start = ids.index(after) + 1 if after else 0
        chunk = ids[start : start + limit]
        items = tuple(BatchSnapshot.from_api(self.batches[i]) for i in chunk)
        return BatchPage(
            items=items, has_more=start + limit < len(ids), last_id=chunk[-1] if chunk else None
        )

    def retrieve_batch(self, batch_id: str) -> BatchSnapshot:
        self._maybe_fail("retrieve")
        from foundry_batch.errors import ProviderNotFoundError

        if batch_id not in self.batches:
            raise ProviderNotFoundError("not found", status_code=404)
        return BatchSnapshot.from_api(self.batches[batch_id])

    def cancel_batch(self, batch_id: str) -> BatchSnapshot:
        self._maybe_fail("cancel")
        self.set_status(batch_id, "cancelling")
        return BatchSnapshot.from_api(self.batches[batch_id])

    def upload_file(self, *, filename: str, content: bytes) -> str:
        self._maybe_fail("upload")
        file_id = f"file-{next(self._ids):04d}"
        self.files[file_id] = content
        return file_id

    def create_batch(
        self, *, input_file_id: str, endpoint: str, metadata: Mapping[str, str]
    ) -> BatchSnapshot:
        self.calls["create"] += 1
        pending = self.faults.get("create")
        error = pending.pop(0) if pending else None
        if error is not None and not getattr(error, "after_commit", False):
            raise error
        batch_id = self.add(input_file_id=input_file_id, metadata=metadata)
        if error is not None:
            raise error  # ambiguous: the provider accepted the job but the response was lost
        return BatchSnapshot.from_api(self.batches[batch_id])


class FakeProviderFactory:
    def __init__(self, providers: Mapping[str, FakeProvider]) -> None:
        self.providers = dict(providers)

    def for_resource(self, resource_key: str) -> FakeProvider:
        return self.providers[resource_key]


class FakeEndpoints:
    """Webhook endpoint management API double with one-time secrets and fault injection."""

    def __init__(self) -> None:
        self.endpoints: dict[str, WebhookEndpoint] = {}
        self.created_secrets: dict[str, str] = {}
        self.faults: dict[str, list[Exception]] = {}
        self._ids = itertools.count(1)
        self.calls: Counter[str] = Counter()

    def fail(self, operation: str, *errors: Exception) -> None:
        self.faults.setdefault(operation, []).extend(errors)

    def _maybe_fail(self, operation: str) -> Exception | None:
        self.calls[operation] += 1
        pending = self.faults.get(operation)
        return pending.pop(0) if pending else None

    def list_endpoints(self) -> list[WebhookEndpoint]:
        error = self._maybe_fail("list")
        if error:
            raise error
        return list(self.endpoints.values())

    def create_endpoint(
        self, *, name: str, url: str, event_types: Sequence[str]
    ) -> CreatedWebhookEndpoint:
        error = self._maybe_fail("create")
        if error is not None and not getattr(error, "after_commit", False):
            raise error
        self._reject_duplicate_url(url)
        endpoint_id = f"wh_{next(self._ids)}"
        secret = (
            "whsec_" + base64.b64encode(f"secret-{endpoint_id}".encode().ljust(32, b"x")).decode()
        )
        endpoint = WebhookEndpoint(endpoint_id, name, url, tuple(event_types))
        self.endpoints[endpoint_id] = endpoint
        self.created_secrets[endpoint_id] = secret
        if error is not None:
            raise error
        return CreatedWebhookEndpoint(endpoint, secret)

    def update_endpoint(
        self, endpoint_id: str, *, name: str, url: str, event_types: Sequence[str]
    ) -> WebhookEndpoint:
        error = self._maybe_fail("update")
        if error:
            raise error
        self._reject_duplicate_url(url, except_id=endpoint_id)
        endpoint = WebhookEndpoint(endpoint_id, name, url, tuple(event_types))
        self.endpoints[endpoint_id] = endpoint
        return endpoint

    def _reject_duplicate_url(self, url: str, except_id: str | None = None) -> None:
        # Observed on the live provider (22 Nov 2026): one endpoint per URL in a project.
        if any(e.url == url and e.endpoint_id != except_id for e in self.endpoints.values()):
            raise ProviderRequestError("webhooks.create: HTTP 400 url_already_registered")

    def delete_endpoint(self, endpoint_id: str) -> None:
        error = self._maybe_fail("delete")
        if error:
            raise error
        self.endpoints.pop(endpoint_id, None)


def ambiguous(exc: Exception) -> Exception:
    """Mark a fault as happening *after* the provider committed the operation."""
    exc.after_commit = True  # type: ignore[attr-defined]
    return exc
