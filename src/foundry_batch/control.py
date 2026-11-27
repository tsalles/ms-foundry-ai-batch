"""Control table: leases, sweep checkpoints and webhook registration metadata.

Leases serialise concurrent sweeps and registration runs per resource/environment using ETag
conditional writes, so no extra locking service is needed.
"""

from __future__ import annotations

import contextlib
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from foundry_batch.clock import Clock, from_iso, to_iso
from foundry_batch.errors import ConcurrencyConflictError
from foundry_batch.ports import Entity, Query, TableStore, TxOp


@dataclass(frozen=True)
class Lease:
    name: str
    holder: str
    expires_at: datetime
    etag: str


@dataclass(frozen=True)
class RegistrationRecord:
    """Metadata of one webhook endpoint registration. Never contains the secret value."""

    resource_key: str
    slot: str
    endpoint_id: str
    endpoint_name: str
    url: str
    secret_name: str
    secret_version: str
    active_from: datetime
    active_until: datetime | None = None

    def covers(self, moment: datetime) -> bool:
        return self.active_from <= moment and (
            self.active_until is None or moment < self.active_until
        )


class ControlStore:
    def __init__(self, store: TableStore, table: str, environment: str, clock: Clock) -> None:
        self._store = store
        self._table = table
        self._env = environment
        self._clock = clock

    # -- leases ---------------------------------------------------------------------------

    def acquire(self, name: str, holder: str, ttl_seconds: int) -> Lease | None:
        pk = f"{self._env}|lease"
        now = self._clock.now()
        expires = now + timedelta(seconds=ttl_seconds)
        data = {"holder": holder, "expires_at": to_iso(expires)}
        current = self._store.get(self._table, pk, name)
        try:
            if current is None:
                etags = self._store.submit(self._table, [TxOp("create", Entity(pk, name, data))])
            else:
                current_expiry = from_iso(current.data.get("expires_at"))
                held_by_other = current.data.get("holder") != holder
                if held_by_other and current_expiry is not None and current_expiry > now:
                    return None
                etags = self._store.submit(
                    self._table, [TxOp("update", Entity(pk, name, data, current.etag))]
                )
        except ConcurrencyConflictError:
            return None
        return Lease(name, holder, expires, etags[name])

    def release(self, lease: Lease) -> None:
        pk = f"{self._env}|lease"
        # Expired and taken over by another holder: nothing to release.
        with contextlib.suppress(ConcurrencyConflictError):
            self._store.submit(
                self._table, [TxOp("delete", Entity(pk, lease.name, {}, lease.etag))]
            )

    # -- key/value state ------------------------------------------------------------------

    def get_state(self, name: str) -> dict[str, Any]:
        entity = self._store.get(self._table, f"{self._env}|state", name)
        return dict(entity.data) if entity else {}

    def put_state(self, name: str, data: Mapping[str, Any]) -> None:
        entity = Entity(f"{self._env}|state", name, dict(data))
        self._store.submit(self._table, [TxOp("upsert", entity)])

    # -- registrations --------------------------------------------------------------------

    def save_registration(self, record: RegistrationRecord) -> None:
        data = {
            "resource_key": record.resource_key,
            "slot": record.slot,
            "endpoint_id": record.endpoint_id,
            "endpoint_name": record.endpoint_name,
            "url": record.url,
            "secret_name": record.secret_name,
            "secret_version": record.secret_version,
            "active_from": to_iso(record.active_from),
            "active_until": to_iso(record.active_until) or "",
        }
        entity = Entity(
            f"{self._env}|registration", f"{record.resource_key}|{record.endpoint_id}", data
        )
        self._store.submit(self._table, [TxOp("upsert", entity)])

    def registrations(self, resource_key: str | None = None) -> list[RegistrationRecord]:
        entities = self._store.query(self._table, Query(partition_key=f"{self._env}|registration"))
        records = [_registration(e.data) for e in entities]
        return [r for r in records if resource_key is None or r.resource_key == resource_key]

    def active_registration(self, resource_key: str, at: datetime) -> RegistrationRecord | None:
        candidates = [r for r in self.registrations(resource_key) if r.covers(at)]
        return max(candidates, key=lambda r: r.active_from) if candidates else None


def _registration(data: Mapping[str, Any]) -> RegistrationRecord:
    return RegistrationRecord(
        resource_key=str(data["resource_key"]),
        slot=str(data["slot"]),
        endpoint_id=str(data["endpoint_id"]),
        endpoint_name=str(data.get("endpoint_name", "")),
        url=str(data.get("url", "")),
        secret_name=str(data.get("secret_name", "")),
        secret_version=str(data.get("secret_version", "")),
        active_from=from_iso(data.get("active_from")) or datetime(1970, 1, 1, tzinfo=UTC),
        active_until=from_iso(data.get("active_until")),
    )
