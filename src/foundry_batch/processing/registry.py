"""Job registry on a table store: one partition per job holding the job row, deduplication
receipts and outbox items, so a lifecycle change, its receipt and its outbound intent commit
atomically (US-3.3 AC2, US-5.1 AC4, plan section 3.4).
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass, fields, replace
from datetime import UTC, datetime, timedelta
from typing import Any

from foundry_batch.clock import Clock, from_iso, to_iso
from foundry_batch.config import Settings
from foundry_batch.domain.models import JobKey, JobRecord, safe_key
from foundry_batch.errors import ConcurrencyConflictError, RetryLater
from foundry_batch.ports import Entity, Query, TableStore, TxOp

JOB_ROW = "job"
RECEIPT_PREFIX = "rcpt:"
OUTBOX_PREFIX = "outbox:"

OUTBOX_PENDING = "pending"
OUTBOX_DELIVERED = "delivered"
OUTBOX_QUARANTINED = "quarantined"

EPOCH = datetime.fromtimestamp(0, tz=UTC)


@dataclass(frozen=True)
class Receipt:
    receipt_id: str
    source: str
    received_at: datetime
    event_type: str | None = None
    webhook_id: str | None = None
    event_id: str | None = None

    @property
    def row_key(self) -> str:
        return RECEIPT_PREFIX + safe_key(self.receipt_id)

    def to_entity(self) -> dict[str, Any]:
        return {
            "kind": "receipt",
            "receipt_id": self.receipt_id,
            "source": self.source,
            "received_at": to_iso(self.received_at),
            "event_type": self.event_type,
            "webhook_id": self.webhook_id,
            "event_id": self.event_id,
        }


@dataclass(frozen=True)
class OutboxItem:
    idempotency_key: str
    event_json: str
    source: str
    created_at: datetime
    status: str = OUTBOX_PENDING
    attempts: int = 0
    next_attempt_at: datetime | None = None
    delivered_at: datetime | None = None
    last_status_code: int | None = None
    last_error: str | None = None
    correction_of: str | None = None
    replay_count: int = 0

    @property
    def row_key(self) -> str:
        return OUTBOX_PREFIX + self.idempotency_key

    def to_entity(self) -> dict[str, Any]:
        data: dict[str, Any] = {"kind": "outbox"}
        for f in fields(self):
            value = getattr(self, f.name)
            if value is None:
                continue
            data[f.name] = to_iso(value) if isinstance(value, datetime) else value
        return data

    @classmethod
    def from_entity(cls, data: Mapping[str, Any]) -> OutboxItem:
        return cls(
            idempotency_key=str(data["idempotency_key"]),
            event_json=str(data["event_json"]),
            source=str(data.get("source", "webhook")),
            created_at=from_iso(data.get("created_at")) or EPOCH,
            status=str(data.get("status", OUTBOX_PENDING)),
            attempts=int(data.get("attempts", 0)),
            next_attempt_at=from_iso(data.get("next_attempt_at")),
            delivered_at=from_iso(data.get("delivered_at")),
            last_status_code=(
                int(data["last_status_code"]) if data.get("last_status_code") is not None else None
            ),
            last_error=data.get("last_error"),
            correction_of=data.get("correction_of"),
            replay_count=int(data.get("replay_count", 0)),
        )

    def event(self) -> dict[str, Any]:
        loaded = json.loads(self.event_json)
        return loaded if isinstance(loaded, dict) else {}


@dataclass(frozen=True)
class StoredJob:
    record: JobRecord
    etag: str


@dataclass(frozen=True)
class StoredOutbox:
    partition_key: str
    item: OutboxItem
    etag: str


class JobRegistry:
    def __init__(self, store: TableStore, settings: Settings, clock: Clock) -> None:
        self._store = store
        self._settings = settings
        self._clock = clock
        self._table = settings.table_jobs

    @property
    def environment(self) -> str:
        return self._settings.environment

    def partition(self, key: JobKey) -> str:
        return key.partition_key(self._settings.environment)

    # -- reads ----------------------------------------------------------------------------

    def load(self, key: JobKey) -> StoredJob | None:
        entity = self._store.get(self._table, self.partition(key), JOB_ROW)
        if entity is None or entity.etag is None:
            return None
        return StoredJob(JobRecord.from_entity(entity.data), entity.etag)

    def has_receipt(self, key: JobKey, receipt_id: str) -> bool:
        row = RECEIPT_PREFIX + safe_key(receipt_id)
        return self._store.get(self._table, self.partition(key), row) is not None

    def receipts(self, key: JobKey) -> list[Entity]:
        return self._store.query(
            self._table, Query(partition_key=self.partition(key), row_key_prefix=RECEIPT_PREFIX)
        )

    def load_outbox(self, partition_key: str, idempotency_key: str) -> StoredOutbox | None:
        entity = self._store.get(self._table, partition_key, OUTBOX_PREFIX + idempotency_key)
        if entity is None or entity.etag is None:
            return None
        return StoredOutbox(partition_key, OutboxItem.from_entity(entity.data), entity.etag)

    def due_outbox(self, now: datetime, limit: int = 100) -> list[StoredOutbox]:
        entities = self._store.query(
            self._table,
            Query(
                row_key_prefix=OUTBOX_PREFIX,
                equals={"status": OUTBOX_PENDING, "environment": self._settings.environment},
                less_or_equal={"next_attempt_at": to_iso(now) or ""},
                limit=limit,
            ),
        )
        return [
            StoredOutbox(e.partition_key, OutboxItem.from_entity(e.data), e.etag or "")
            for e in entities
        ]

    def outbox_items(self, status: str, limit: int | None = None) -> list[StoredOutbox]:
        """Every outbox item in ``status`` (``limit=None`` reads all pages)."""
        entities = self._store.query(
            self._table,
            Query(
                row_key_prefix=OUTBOX_PREFIX,
                equals={"status": status, "environment": self._settings.environment},
                limit=limit,
            ),
        )
        return [
            StoredOutbox(e.partition_key, OutboxItem.from_entity(e.data), e.etag or "")
            for e in entities
        ]

    def open_jobs(self, resource_key: str | None = None) -> list[StoredJob]:
        equals: dict[str, str | int | bool] = {
            "is_open": True,
            "environment": self._settings.environment,
        }
        if resource_key:
            equals["resource_key"] = resource_key
        return self._jobs(equals)

    def terminal_jobs(
        self,
        resource_key: str | None = None,
        *,
        where: Mapping[str, str | int | bool] | None = None,
    ) -> list[StoredJob]:
        equals: dict[str, str | int | bool] = {
            "is_open": False,
            "environment": self._settings.environment,
            **(where or {}),
        }
        if resource_key:
            equals["resource_key"] = resource_key
        return self._jobs(equals)

    def _jobs(self, equals: dict[str, str | int | bool]) -> list[StoredJob]:
        entities = self._store.query(self._table, Query(row_key_prefix=JOB_ROW, equals=equals))
        return [
            StoredJob(JobRecord.from_entity(e.data), e.etag or "")
            for e in entities
            if e.row_key == JOB_ROW
        ]

    # -- writes ---------------------------------------------------------------------------

    def reserve_status_call(self, key: JobKey) -> None:
        for _ in range(5):
            now = self._clock.now()
            partition = self.partition(key)
            current = self._store.get(self._table, partition, "status-call")
            job = self.load(key)
            boundaries = [from_iso(current.data.get("next_allowed_at"))] if current else []
            if job is not None and job.record.last_status_call_at is not None:
                boundaries.append(
                    job.record.last_status_call_at
                    + timedelta(seconds=self._settings.status_call_floor_seconds)
                )
            for boundary in boundaries:
                if boundary is not None and boundary > now:
                    raise RetryLater("status_call_floor", (boundary - now).total_seconds())
            entity = Entity(
                partition,
                "status-call",
                {
                    "next_allowed_at": to_iso(
                        now + timedelta(seconds=self._settings.status_call_floor_seconds)
                    ),
                },
                current.etag if current else None,
            )
            try:
                self._store.submit(self._table, [TxOp("update" if current else "create", entity)])
            except ConcurrencyConflictError:
                continue
            return
        raise RetryLater("status_call_contention", 5)

    def _job_entity(self, record: JobRecord, etag: str | None = None) -> Entity:
        data = record.to_entity()
        data["kind"] = "job"
        data["is_open"] = record.outcome is None
        return Entity(self.partition(record.key), JOB_ROW, data, etag)

    def create_job(self, record: JobRecord) -> str:
        """Create the job row. Raises ``ConcurrencyConflictError`` if it already exists."""
        record = replace(record, updated_at=self._clock.now())
        etags = self._store.submit(self._table, [TxOp("create", self._job_entity(record))])
        return etags[JOB_ROW]

    def commit(
        self,
        record: JobRecord,
        *,
        etag: str | None,
        receipt: Receipt | None = None,
        outbox: OutboxItem | None = None,
    ) -> str:
        """Atomically write the job row (ETag-checked), a receipt and an outbox item.

        ``etag=None`` means the job row must not exist yet (create). A duplicate receipt or a
        concurrent writer raises ``ConcurrencyConflictError``; callers reload and decide again.
        """
        record = replace(record, updated_at=self._clock.now())
        partition = self.partition(record.key)
        ops = [
            TxOp("create", self._job_entity(record))
            if etag is None
            else TxOp("update", self._job_entity(record, etag))
        ]
        if receipt is not None:
            ops.append(TxOp("create", Entity(partition, receipt.row_key, receipt.to_entity())))
        if outbox is not None:
            data = outbox.to_entity()
            data["environment"] = self._settings.environment
            ops.append(TxOp("create", Entity(partition, outbox.row_key, data)))
        etags = self._store.submit(self._table, ops)
        return etags[JOB_ROW]

    def save_outbox(self, stored: StoredOutbox, item: OutboxItem) -> str:
        data = item.to_entity()
        data["environment"] = self._settings.environment
        entity = Entity(stored.partition_key, item.row_key, data, stored.etag)
        etags = self._store.submit(self._table, [TxOp("update", entity)])
        return etags[item.row_key]

    def try_update_job(self, stored: StoredJob, record: JobRecord) -> StoredJob | None:
        """ETag-checked single-row update; ``None`` when another writer got there first."""
        record = replace(record, updated_at=self._clock.now())
        try:
            etags = self._store.submit(
                self._table, [TxOp("update", self._job_entity(record, stored.etag))]
            )
        except ConcurrencyConflictError:
            return None
        return StoredJob(record, etags[JOB_ROW])
