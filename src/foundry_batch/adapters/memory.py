"""In-memory adapters with the same semantics as the Azure services they stand in for.

Used by unit tests, the local end-to-end harness and simulations. The table store reproduces
Azure Tables behaviour that the design relies on: atomic same-partition batches, create
conflicts, ETag-checked updates and new ETags on every write. The queues reproduce visibility
delays, message expiry (time-to-live), the message size limit and pop-receipt checks.
"""

from __future__ import annotations

import itertools
import math
import threading
from collections import deque
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from foundry_batch.errors import ConcurrencyConflictError, SecretUnavailableError
from foundry_batch.ports import (
    DeliveryResult,
    Entity,
    Query,
    ReceivedMessage,
    TxOp,
)


class MemoryTableStore:
    def __init__(self) -> None:
        self._tables: dict[str, dict[tuple[str, str], tuple[dict[str, Any], str]]] = {}
        self._etags = itertools.count(1)
        self._lock = threading.RLock()
        self.submit_calls = 0

    def _table(self, name: str) -> dict[tuple[str, str], tuple[dict[str, Any], str]]:
        return self._tables.setdefault(name, {})

    def get(self, table: str, partition_key: str, row_key: str) -> Entity | None:
        with self._lock:
            row = self._table(table).get((partition_key, row_key))
            if row is None:
                return None
            data, etag = row
            return Entity(partition_key, row_key, dict(data), etag)

    def query(self, table: str, query: Query) -> list[Entity]:
        with self._lock:
            result: list[Entity] = []
            for (pk, rk), (data, etag) in sorted(self._table(table).items()):
                if query.partition_key is not None and pk != query.partition_key:
                    continue
                if query.row_key_prefix is not None and not rk.startswith(query.row_key_prefix):
                    continue
                if any(data.get(k) != v for k, v in query.equals.items()):
                    continue
                if any(
                    not isinstance(data.get(k), str) or data[k] > v
                    for k, v in query.less_or_equal.items()
                ):
                    continue
                result.append(Entity(pk, rk, dict(data), etag))
                if query.limit is not None and len(result) >= query.limit:
                    break
            return result

    def submit(self, table: str, ops: Sequence[TxOp]) -> dict[str, str]:
        if not ops:
            return {}
        with self._lock:
            self.submit_calls += 1
            rows = self._table(table)
            partitions = {op.entity.partition_key for op in ops}
            if len(partitions) != 1:
                raise ValueError("a transaction must target a single partition")
            if len(ops) > MAX_BATCH_OPERATIONS:
                raise ValueError("a transaction holds at most 100 operations")
            if sum(_entity_size(op.entity) for op in ops) > MAX_BATCH_BYTES:
                raise ValueError("a transaction payload holds at most 4 MiB")
            seen: set[str] = set()
            for op in ops:
                _check_entity_limits(op.entity)
                key = (op.entity.partition_key, op.entity.row_key)
                if op.entity.row_key in seen:
                    raise ValueError("a transaction cannot touch the same row twice")
                seen.add(op.entity.row_key)
                existing = rows.get(key)
                if op.kind == "create" and existing is not None:
                    raise ConcurrencyConflictError(f"row exists: {op.entity.row_key}")
                if op.kind in ("update", "delete"):
                    if existing is None:
                        raise ConcurrencyConflictError(f"row missing: {op.entity.row_key}")
                    if not op.force and existing[1] != op.entity.etag:
                        raise ConcurrencyConflictError(f"etag mismatch: {op.entity.row_key}")
            etags: dict[str, str] = {}
            for op in ops:
                key = (op.entity.partition_key, op.entity.row_key)
                if op.kind == "delete":
                    rows.pop(key, None)
                    continue
                etag = f'W/"{next(self._etags)}"'
                rows[key] = (dict(op.entity.data), etag)
                etags[op.entity.row_key] = etag
            return etags

    def rows(self, table: str) -> list[Entity]:
        return self.query(table, Query())


#: Azure Storage Queue limits the double enforces. ``MESSAGE_TTL_SECONDS`` mirrors
#: ``azure_queues.MESSAGE_TTL_SECONDS`` (that module needs the SDK).
MAX_QUEUE_MESSAGE_BYTES = 64 * 1024
MAX_VISIBILITY_TIMEOUT_SECONDS = 7 * 24 * 3600
MESSAGE_TTL_SECONDS = 14 * 24 * 3600


@dataclass
class _QueuedMessage:
    message_id: str
    content: str
    visible_after: float = 0.0
    dequeue_count: int = 0
    expires_at: float = math.inf
    pop_receipt: str = ""


class MemoryQueue:
    """FIFO queue with visibility delays and expiry (time is supplied by the caller).

    Expiry is measured on the caller's clock (``now``, epoch seconds). A message sent before
    the caller supplied any time has no reference point and never expires.
    """

    def __init__(self, name: str = "queue") -> None:
        self.name = name
        self._messages: deque[_QueuedMessage] = deque()
        self._ids = itertools.count(1)
        self.sent: list[str] = []
        self._now = 0.0
        self._time_supplied = False

    @property
    def now(self) -> float:
        return self._now

    @now.setter
    def now(self, value: float) -> None:
        self._now = value
        self._time_supplied = True

    def send(self, message: str, *, visibility_timeout: int | None = None) -> None:
        delay = visibility_timeout or 0
        if len(message.encode("utf-8", "surrogatepass")) > MAX_QUEUE_MESSAGE_BYTES:
            raise ValueError("queue message exceeds 64 KiB (RequestBodyTooLarge)")
        if not 0 <= delay <= MAX_VISIBILITY_TIMEOUT_SECONDS:
            raise ValueError("visibility timeout must be 0 to 7 days (OutOfRangeQueryParameter)")
        msg = _QueuedMessage(
            message_id=str(next(self._ids)),
            content=message,
            visible_after=self.now + delay,
            expires_at=self.now + MESSAGE_TTL_SECONDS if self._time_supplied else math.inf,
        )
        self._messages.append(msg)
        self.sent.append(message)

    def live(self) -> list[_QueuedMessage]:
        """Messages that have not expired; expired ones are gone, as in the service."""
        for msg in [m for m in self._messages if m.expires_at <= self.now]:
            self._messages.remove(msg)
        return list(self._messages)

    def pop_visible(self) -> str | None:
        for msg in self.live():
            if msg.visible_after <= self.now:
                self._messages.remove(msg)
                return msg.content
        return None

    def drain(self) -> list[str]:
        out: list[str] = []
        while (item := self.pop_visible()) is not None:
            out.append(item)
        return out

    def __len__(self) -> int:
        return len(self.live())


class MemoryQueueAdmin:
    def __init__(self, queues: Mapping[str, MemoryQueue]) -> None:
        self.queues = dict(queues)
        self._receipts = itertools.count(1)

    def approximate_depth(self, queue: str) -> int:
        return len(self.queues[queue])

    def receive(
        self, queue: str, *, max_messages: int, visibility_timeout: int
    ) -> list[ReceivedMessage]:
        q = self.queues[queue]
        out: list[ReceivedMessage] = []
        visible = [m for m in q.live() if m.visible_after <= q.now]  # Azure semantics
        for msg in visible[:max_messages]:  # test double internals
            msg.dequeue_count += 1
            msg.visible_after = q.now + visibility_timeout
            msg.pop_receipt = str(next(self._receipts))
            out.append(
                ReceivedMessage(msg.message_id, msg.pop_receipt, msg.content, msg.dequeue_count)
            )
        return out

    def peek(self, queue: str, *, max_messages: int) -> list[ReceivedMessage]:
        q = self.queues[queue]
        visible = [m for m in q.live() if m.visible_after <= q.now]
        return [
            ReceivedMessage(m.message_id, "", m.content, m.dequeue_count)
            for m in visible[:max_messages]
        ]

    def delete(self, queue: str, message: ReceivedMessage) -> None:
        """Delete with the pop receipt of the latest receive, as the service requires."""
        q = self.queues[queue]
        for msg in q.live():
            matches = msg.message_id == message.message_id
            if matches and msg.pop_receipt and msg.pop_receipt == message.pop_receipt:
                q._messages.remove(msg)
                return
        raise LookupError(f"message {message.message_id} not found or pop receipt mismatch")

    def send_to(self, queue: str, content: str) -> None:
        self.queues[queue].send(content)


#: Azure Table limits the double enforces so tests catch writes the service would reject.
MAX_BATCH_OPERATIONS = 100
MAX_BATCH_BYTES = 4 * 1024 * 1024
MAX_KEY_BYTES = 1024
MAX_PROPERTY_BYTES = 64 * 1024  # strings are stored as UTF-16
MAX_ENTITY_BYTES = 1024 * 1024
MAX_CUSTOM_PROPERTIES = 252  # 255 including PartitionKey, RowKey and Timestamp
_FORBIDDEN_KEY_CHARS = frozenset("/\\#?")
_INT32 = range(-(2**31), 2**31)


def _utf16_bytes(text: str) -> int:
    return len(text.encode("utf-16-le", "surrogatepass"))


def _value_size(value: Any) -> int:
    """Stored size of one property value (Azure Tables size formula)."""
    if isinstance(value, str):
        return 4 + _utf16_bytes(value)
    if isinstance(value, bytes):
        return 4 + len(value)
    if isinstance(value, bool):
        return 1
    if isinstance(value, int):
        return 4 if value in _INT32 else 8
    return 8  # double, datetime


def _entity_size(entity: Entity) -> int:
    keys = _utf16_bytes(entity.partition_key) + _utf16_bytes(entity.row_key)
    props = sum(
        8 + _utf16_bytes(name) + _value_size(value)
        for name, value in entity.data.items()
        if value is not None  # the Azure adapter does not send null properties
    )
    return 4 + keys + props


def _check_entity_limits(entity: Entity) -> None:
    for name, key in (("PartitionKey", entity.partition_key), ("RowKey", entity.row_key)):
        if (
            _utf16_bytes(key) > MAX_KEY_BYTES
            or _FORBIDDEN_KEY_CHARS & set(key)
            or any(ord(ch) < 0x20 or 0x7F <= ord(ch) <= 0x9F for ch in key)
        ):
            raise ValueError(f"{name} is not a valid Azure Table key: {key[:60]!r}")
    for prop, value in entity.data.items():
        if isinstance(value, str) and _utf16_bytes(value) > MAX_PROPERTY_BYTES:
            raise ValueError(f"property {prop} exceeds 64 KiB (PropertyValueTooLarge)")
    if sum(1 for value in entity.data.values() if value is not None) > MAX_CUSTOM_PROPERTIES:
        raise ValueError("an entity holds at most 252 properties (TooManyProperties)")
    if _entity_size(entity) > MAX_ENTITY_BYTES:
        raise ValueError("an entity holds at most 1 MiB (EntityTooLarge)")


class MemorySecrets:
    """Key Vault double: versions, tags and enabled flags; failure injection for tests."""

    def __init__(self, values: Mapping[str, str] | None = None) -> None:
        self._values: dict[str, list[tuple[str, dict[str, str], bool]]] = {}
        self.reads = 0
        self.fail_writes = False
        self.fail_reads = False  # simulate a Key Vault outage (SecretUnavailableError)
        for name, value in (values or {}).items():
            self.set_secret(name, value, tags={})

    def get_secret(self, name: str) -> str | None:
        self.reads += 1
        if self.fail_reads:
            raise SecretUnavailableError("simulated Key Vault outage")
        versions = self._values.get(name)
        if not versions or not versions[-1][2]:
            return None
        return versions[-1][0]

    def set_secret(self, name: str, value: str, *, tags: Mapping[str, str]) -> str:
        if self.fail_writes:
            raise RuntimeError("simulated Key Vault failure")
        versions = self._values.setdefault(name, [])
        versions.append((value, dict(tags), True))
        return f"v{len(versions)}"

    def get_secret_version(self, name: str, version: str) -> str | None:
        self.reads += 1
        if self.fail_reads:
            raise SecretUnavailableError("simulated Key Vault outage")
        versions = self._values.get(name, [])
        index = int(version[1:]) - 1 if version[:1] == "v" and version[1:].isdigit() else -1
        if not 0 <= index < len(versions) or not versions[index][2]:
            return None
        return versions[index][0]

    def get_secret_tags(self, name: str) -> Mapping[str, str] | None:
        if self.fail_reads:
            raise SecretUnavailableError("simulated Key Vault outage")
        versions = self._values.get(name)
        if not versions or not versions[-1][2]:
            return None
        return dict(versions[-1][1])

    def disable_secret(self, name: str) -> None:
        versions = self._values.get(name)
        if versions:
            value, tags, _ = versions[-1]
            versions[-1] = (value, tags, False)

    def names(self) -> list[str]:
        return sorted(self._values)

    def values_for_test(self) -> list[str]:
        return [v for versions in self._values.values() for v, _, _ in versions]


@dataclass
class RecordingDownstream:
    """Downstream double that returns scripted results and records every attempt."""

    script: list[DeliveryResult] = field(default_factory=list)
    attempts: list[tuple[str, bytes]] = field(default_factory=list)
    default: DeliveryResult = field(default_factory=lambda: DeliveryResult(status_code=200))

    def deliver(self, body: bytes, *, idempotency_key: str) -> DeliveryResult:
        self.attempts.append((idempotency_key, body))
        return self.script.pop(0) if self.script else self.default

    @property
    def distinct_keys(self) -> set[str]:
        return {key for key, _ in self.attempts}
