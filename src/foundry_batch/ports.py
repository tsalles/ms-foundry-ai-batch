"""Protocols between the core and infrastructure. Adapters live in :mod:`foundry_batch.adapters`."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol

from foundry_batch.domain.models import BatchSnapshot

# ---------------------------------------------------------------------------------------------
# Table storage (Azure Tables semantics: per-partition atomic batches with ETag concurrency)
# ---------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Entity:
    partition_key: str
    row_key: str
    data: Mapping[str, Any]
    etag: str | None = None


@dataclass(frozen=True)
class TxOp:
    """One operation in an atomic batch. ``update``/``delete`` require ``etag`` unless forced."""

    kind: Literal["create", "update", "upsert", "delete"]
    entity: Entity
    force: bool = False


@dataclass(frozen=True)
class Query:
    """Small structured query that every adapter can translate (OData for Azure Tables)."""

    partition_key: str | None = None
    row_key_prefix: str | None = None
    equals: Mapping[str, str | int | bool] = field(default_factory=dict)
    less_or_equal: Mapping[str, str] = field(default_factory=dict)
    limit: int | None = None


class TableStore(Protocol):
    def get(self, table: str, partition_key: str, row_key: str) -> Entity | None: ...

    def query(self, table: str, query: Query) -> list[Entity]: ...

    def submit(self, table: str, ops: Sequence[TxOp]) -> dict[str, str]:
        """Apply ``ops`` atomically (same partition). Return new ETags by row key.

        Raises ``ConcurrencyConflictError`` when a create hits an existing row or an ETag does not
        match, and ``StoreUnavailableError`` when the outcome is unknown.
        """
        ...


# ---------------------------------------------------------------------------------------------
# Queues
# ---------------------------------------------------------------------------------------------


class MessageQueue(Protocol):
    def send(self, message: str, *, visibility_timeout: int | None = None) -> None:
        """Durably enqueue ``message`` or raise ``QueueUnavailableError`` (outcome unknown)."""
        ...


@dataclass(frozen=True)
class ReceivedMessage:
    message_id: str
    pop_receipt: str
    content: str
    dequeue_count: int = 0


class QueueAdmin(Protocol):
    def approximate_depth(self, queue: str) -> int: ...

    def receive(
        self, queue: str, *, max_messages: int, visibility_timeout: int
    ) -> list[ReceivedMessage]: ...

    def peek(self, queue: str, *, max_messages: int) -> list[ReceivedMessage]:
        """Read without hiding the messages or changing their dequeue count."""
        ...

    def delete(self, queue: str, message: ReceivedMessage) -> None: ...

    def send_to(self, queue: str, content: str) -> None: ...


# ---------------------------------------------------------------------------------------------
# Secrets
# ---------------------------------------------------------------------------------------------


class SecretReader(Protocol):
    def get_secret(self, name: str) -> str | None:
        """Return the enabled current value, ``None`` if absent or disabled."""
        ...


class SecretVersionReader(Protocol):
    def get_secret_version(self, name: str, version: str) -> str | None:
        """The exact version's value when it exists and is enabled, else ``None``."""
        ...


class SecretWriter(SecretReader, SecretVersionReader, Protocol):
    def set_secret(self, name: str, value: str, *, tags: Mapping[str, str]) -> str:
        """Store ``value`` and return the new version id (never log ``value``)."""
        ...

    def get_secret_tags(self, name: str) -> Mapping[str, str] | None: ...

    def disable_secret(self, name: str) -> None:
        """Disable the current version. Deleting would reserve the name under purge protection,
        blocking the next rotation into the same slot; a later ``set_secret`` adds a new
        enabled version."""
        ...


@dataclass(frozen=True)
class VaultRecord:
    """Non-sensitive description of one version in the client's vault (never the value)."""

    name: str
    version: str
    metadata: Mapping[str, str]
    enabled: bool


class ClientVault(Protocol):
    """Optional external secret store used as a synchronization destination.

    An adapter for AWS Secrets Manager, HashiCorp Vault or another product must map
    ``request_token`` to the product's idempotency control
    (Secrets Manager ``ClientRequestToken``, Vault KV ``cas``) and ``retire`` to its approved
    disable/tombstone operation, never to an immediate delete.
    """

    def put(self, name: str, value: str, *, request_token: str, metadata: Mapping[str, str]) -> str:
        """Write a new version; replaying the same token returns the same version."""
        ...

    def find(self, name: str, request_token: str) -> VaultRecord | None:
        """The version a token produced, to reconcile a write whose response was lost."""
        ...

    def current(self, name: str) -> VaultRecord | None:
        """The newest version written under ``name``, retired ones included (``enabled`` says
        which), so a writer can start above any history; ``None`` if nothing was written."""
        ...

    def read(self, name: str, version: str) -> str | None:
        """Readback for verification; ``None`` when absent or retired."""
        ...

    def retire(self, name: str, version: str) -> None:
        """Make a version unusable for verification. Idempotent."""
        ...


# ---------------------------------------------------------------------------------------------
# Azure OpenAI (Foundry) data plane
# ---------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class BatchPage:
    items: tuple[BatchSnapshot, ...]
    has_more: bool
    last_id: str | None


class BatchApi(Protocol):
    def list_batches(self, *, after: str | None, limit: int) -> BatchPage: ...

    def retrieve_batch(self, batch_id: str) -> BatchSnapshot: ...

    def cancel_batch(self, batch_id: str) -> BatchSnapshot: ...

    def upload_file(self, *, filename: str, content: bytes) -> str: ...

    def create_batch(
        self, *, input_file_id: str, endpoint: str, metadata: Mapping[str, str]
    ) -> BatchSnapshot: ...


class BatchApiFactory(Protocol):
    def for_resource(self, resource_key: str) -> BatchApi: ...


@dataclass(frozen=True)
class WebhookEndpoint:
    endpoint_id: str
    name: str
    url: str
    event_types: tuple[str, ...]


@dataclass(frozen=True)
class CreatedWebhookEndpoint:
    endpoint: WebhookEndpoint
    # repr=False keeps the one-time secret out of reprs, tracebacks and logs.
    signing_secret: str = field(repr=False)


class WebhookEndpointsApi(Protocol):
    def list_endpoints(self) -> list[WebhookEndpoint]: ...

    def create_endpoint(
        self, *, name: str, url: str, event_types: Sequence[str]
    ) -> CreatedWebhookEndpoint: ...

    def update_endpoint(
        self, endpoint_id: str, *, name: str, url: str, event_types: Sequence[str]
    ) -> WebhookEndpoint: ...

    def delete_endpoint(self, endpoint_id: str) -> None: ...


# ---------------------------------------------------------------------------------------------
# Downstream delivery
# ---------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class DeliveryResult:
    status_code: int | None
    retry_after_seconds: float | None = None
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.status_code is not None and 200 <= self.status_code < 300

    @property
    def retryable(self) -> bool:
        if self.status_code is None:
            return True  # network failure or timeout: the key makes a retry safe
        # 409: the Idempotency-Key draft answers "same key still being processed" with 409, so
        # an in-flight duplicate (our read timed out, the first request is still running) is
        # retried; the key makes the retry safe and a finished duplicate must answer 2xx.
        return self.status_code in (408, 409, 425, 429) or self.status_code >= 500


class Downstream(Protocol):
    def deliver(self, body: bytes, *, idempotency_key: str) -> DeliveryResult: ...
