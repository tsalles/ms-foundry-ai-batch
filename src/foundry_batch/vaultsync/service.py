"""One-way, versioned synchronization of webhook signing secrets to the client's vault.

An optional downstream integration can use a separate secret store without accessing the
source vault directly. This module implements product-agnostic, versioned synchronization:

* the source is the **exact** Key Vault version recorded at registration, never "latest";
* every write carries a deterministic request token, so replays and lost responses converge
  on one destination version (``ClientVault.find`` reconciles an ambiguous write);
* readiness requires a destination **readback** that matches the source;
* retirement propagates: a slot that stops being live is retired in the destination too;
* a periodic reconcile rebuilds intent from the registration records, so a lost trigger
  cannot leave a live endpoint unsynchronized;
* one worker per resource at a time (a lease in the control table), and a write that loses an
  ETag race is retired unless the ledger still tracks it;
* destination history from an earlier deployment (ledger lost) is written above, never under.

The ledger (control table) holds metadata only. Secret values exist in memory for one call and
never reach logs, events, queues or exceptions. Product, worker placement, identity and lag
limits must be defined for each deployment.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import uuid
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field, replace
from datetime import datetime
from typing import Any, Literal

from foundry_batch.clock import Clock, from_iso, to_iso
from foundry_batch.control import ControlStore, RegistrationRecord
from foundry_batch.errors import ConcurrencyConflictError, SecretUnavailableError
from foundry_batch.observability import log_event
from foundry_batch.ports import (
    ClientVault,
    Entity,
    Query,
    SecretVersionReader,
    TableStore,
    TxOp,
)

logger = logging.getLogger(__name__)

SyncState = Literal["pending", "synced", "blocked", "superseded", "retire_pending", "retired"]
DESTINATION_TEMPLATE = "batch-{environment}-{resource_key}-{slot}"
LEASE_SECONDS = 300


@dataclass(frozen=True)
class OwedRetirement:
    """An earlier generation of the slot still to be retired downstream (version or token)."""

    version: str | None
    token: str | None


@dataclass(frozen=True)
class SyncRecord:
    resource_key: str
    slot: str
    endpoint_id: str
    source_name: str
    source_version: str
    generation: int
    destination_name: str
    state: SyncState = "pending"
    destination_version: str | None = None
    attempts: int = 0
    last_error: str | None = None
    captured_at: datetime | None = None
    synced_at: datetime | None = None
    retired_at: datetime | None = None
    #: Earlier generations of the slot, oldest first, still to be retired downstream.
    owed: tuple[OwedRetirement, ...] = ()

    @property
    def previous_version(self) -> str | None:
        """The most recent owed generation's destination version (``None`` if none is owed)."""
        return self.owed[-1].version if self.owed else None

    @property
    def previous_token(self) -> str | None:
        return self.owed[-1].token if self.owed else None

    @property
    def converged(self) -> bool:
        """Nothing left to do: verified or retired downstream, no error, nothing owed."""
        return self.state in ("synced", "retired") and not self.owed and self.last_error is None

    @property
    def request_token(self) -> str:
        """Deterministic, 48 hex chars (fits Secrets Manager's 32-64 ClientRequestToken)."""
        parts = (self.resource_key, self.slot, self.endpoint_id, self.source_version)
        seed = "|".join((*parts, str(self.generation)))
        return hashlib.sha256(seed.encode("utf-8")).hexdigest()[:48]

    def to_entity(self) -> dict[str, Any]:
        return {
            "resource_key": self.resource_key,
            "slot": self.slot,
            "endpoint_id": self.endpoint_id,
            "source_name": self.source_name,
            "source_version": self.source_version,
            "generation": self.generation,
            "destination_name": self.destination_name,
            "state": self.state,
            "destination_version": self.destination_version or "",
            "attempts": self.attempts,
            "last_error": self.last_error or "",
            "captured_at": to_iso(self.captured_at) or "",
            "synced_at": to_iso(self.synced_at) or "",
            "retired_at": to_iso(self.retired_at) or "",
            "owed": json.dumps([[o.version or "", o.token or ""] for o in self.owed]),
        }

    @classmethod
    def from_entity(cls, data: dict[str, Any]) -> SyncRecord:
        state = str(data.get("state") or "pending")
        return cls(
            resource_key=str(data["resource_key"]),
            slot=str(data["slot"]),
            endpoint_id=str(data["endpoint_id"]),
            source_name=str(data["source_name"]),
            source_version=str(data["source_version"]),
            generation=int(data.get("generation") or 0),
            destination_name=str(data["destination_name"]),
            state=state if state in _STATES else "pending",  # type: ignore[arg-type]
            destination_version=_opt(data.get("destination_version")),
            attempts=int(data.get("attempts") or 0),
            last_error=_opt(data.get("last_error")),
            captured_at=from_iso(data.get("captured_at")),
            synced_at=from_iso(data.get("synced_at")),
            retired_at=from_iso(data.get("retired_at")),
            owed=_owed_from(data.get("owed")),
        )


_STATES = frozenset({"pending", "synced", "blocked", "superseded", "retire_pending", "retired"})


@dataclass
class SyncRunStats:
    synced: int = 0
    retired: int = 0
    blocked: int = 0
    failed: int = 0
    superseded: int = 0
    errors: list[str] = field(default_factory=list)
    #: Resources left alone because another worker holds their lease.
    skipped: list[str] = field(default_factory=list)


def ledger_records(
    store: TableStore, table: str, environment: str, resource_key: str | None
) -> list[tuple[SyncRecord, str | None]]:
    """Ledger rows of one resource (exact key, never a prefix of another key) or of all."""
    prefix = f"{resource_key}|" if resource_key else None
    query = Query(partition_key=_ledger_partition(environment), row_key_prefix=prefix)
    return [(SyncRecord.from_entity(dict(e.data)), e.etag) for e in store.query(table, query)]


class VaultSyncService:
    def __init__(
        self,
        environment: str,
        store: TableStore,
        table: str,
        source: SecretVersionReader,
        destination: ClientVault,
        clock: Clock,
        *,
        destination_template: str = DESTINATION_TEMPLATE,
        holder: str | None = None,
        lease_seconds: int = LEASE_SECONDS,
    ) -> None:
        self._env = environment
        self._store = store
        self._table = table
        self._source = source
        self._destination = destination
        self._clock = clock
        self._template = destination_template
        self._control = ControlStore(store, table, environment, clock)
        self._holder = holder or f"vaultsync-{uuid.uuid4().hex[:8]}"
        self._lease_seconds = lease_seconds

    # -- intent -----------------------------------------------------------------------------

    def record_capture(self, registration: RegistrationRecord) -> SyncRecord:
        """Intent to synchronize the exact captured version of one live registration."""
        current = self._load(registration.resource_key, registration.slot)
        record = current[0] if current else None
        if (
            record is not None
            and record.endpoint_id == registration.endpoint_id
            and record.source_version == registration.secret_version
        ):
            return record  # same capture: idempotent
        if (
            record is not None
            and record.captured_at is not None
            and (registration.active_from, registration.endpoint_id)
            < (record.captured_at, record.endpoint_id)
        ):
            _log_ignored(registration)
            return record
        if record is not None and record.endpoint_id == registration.endpoint_id:
            for current_registration in self._control.registrations(registration.resource_key):
                if (
                    current_registration.endpoint_id == registration.endpoint_id
                    and current_registration != registration
                ):
                    _log_ignored(registration)
                    return record
        new = SyncRecord(
            resource_key=registration.resource_key,
            slot=registration.slot,
            endpoint_id=registration.endpoint_id,
            source_name=registration.secret_name,
            source_version=registration.secret_version,
            generation=(record.generation + 1) if record else 1,
            destination_name=self._template.format(
                environment=self._env,
                resource_key=registration.resource_key,
                slot=registration.slot,
            ),
            captured_at=registration.active_from or self._clock.now(),
            owed=_owed_after(record),
        )
        if registration.secret_version in ("", "unknown"):
            new = replace(new, state="blocked", last_error="source_version_unknown")
        self._save(new, current[1] if current else None)
        return new

    def record_retirement(self, resource_key: str, slot: str, endpoint_id: str) -> bool:
        current = self._load(resource_key, slot)
        if current is None:
            return False
        record, etag = current
        if record.endpoint_id != endpoint_id or record.state in ("retire_pending", "retired"):
            return False
        self._save(replace(record, state="retire_pending"), etag)
        return True

    def reconcile(self, registrations: Sequence[RegistrationRecord]) -> int:
        """Rebuild intent from registration records (lost triggers, restarts). Returns changes.

        Retirements are applied before captures, so a slot reused for a new endpoint cannot
        hide the retirement of its previous generation.
        """
        changes = 0
        for registration in registrations:
            if registration.active_until is not None and self.record_retirement(
                registration.resource_key, registration.slot, registration.endpoint_id
            ):
                changes += 1
        for registration in _newest_active_per_slot(registrations):
            before = self._load(registration.resource_key, registration.slot)
            after = self.record_capture(registration)
            if before is None or before[0] != after:
                changes += 1
        return changes

    # -- work -------------------------------------------------------------------------------

    def run(
        self,
        resource_key: str | None = None,
        registrations: Sequence[RegistrationRecord] | None = None,
    ) -> SyncRunStats:
        """Under each resource lease, reconcile registrations and process pending work.

        Without an explicit snapshot, read the current registrations after acquiring the lease.
        """
        stats = SyncRunStats()
        discovery = (
            self._control.registrations() if resource_key is None and registrations is None else ()
        )
        for key in self._resource_keys(resource_key, registrations or discovery):
            lease = self._control.acquire(f"vaultsync|{key}", self._holder, self._lease_seconds)
            if lease is None:
                stats.skipped.append(key)
                log_event(logger, "vaultsync.skipped", resource_key=key, reason="lease_held")
                continue
            try:
                current = (
                    self._control.registrations(key)
                    if registrations is None
                    else [record for record in registrations if record.resource_key == key]
                )
                self._run_resource(key, current, stats)
            finally:
                self._control.release(lease)
        return stats

    def _resource_keys(
        self, resource_key: str | None, registrations: Sequence[RegistrationRecord]
    ) -> list[str]:
        if resource_key:
            return [resource_key]
        keys = {r.resource_key for r in registrations}
        keys.update(record.resource_key for record, _ in self._records(None))
        return sorted(keys)

    def _run_resource(
        self, resource_key: str, registrations: Sequence[RegistrationRecord], stats: SyncRunStats
    ) -> None:
        if registrations:
            self.reconcile(registrations)
        for record, etag in self._records(resource_key):
            try:
                if record.owed:
                    settled = self._retire_owed(record, etag, stats)
                    if settled is None:
                        continue  # retried next run, before the new generation is written
                    record, etag = settled
                if record.state in ("pending", "superseded"):
                    self._sync(record, etag, stats)
                elif record.state == "retire_pending":
                    self._retire(record, etag, stats)
                elif record.state == "synced":
                    self._check_verified(record, stats)
            except ConcurrencyConflictError:
                continue  # another worker moved it; the next run re-evaluates

    def status(self, resource_key: str | None = None) -> list[SyncRecord]:
        return [record for record, _ in self._records(resource_key)]

    def ready(self, resource_key: str, endpoint_ids: Iterable[str]) -> bool:
        """Reverify the exact active captures against the current destination versions."""
        wanted = set(endpoint_ids)
        if not wanted:
            return False
        active = {
            registration.endpoint_id: registration
            for registration in _newest_active_per_slot(self._control.registrations(resource_key))
        }
        records = {record.slot: record for record, _ in self._records(resource_key)}
        for endpoint_id in sorted(wanted):
            registration = active.get(endpoint_id)
            if registration is None:
                return False
            record = records.get(registration.slot)
            if (
                record is None
                or record.endpoint_id != endpoint_id
                or record.source_name != registration.secret_name
                or record.source_version != registration.secret_version
                or not self._verified(record)
            ):
                return False
        return True

    def _verified(self, record: SyncRecord) -> bool:
        version = record.destination_version
        if record.state != "synced" or record.owed or not version:
            return False
        current = self._destination.current(record.destination_name)
        if current is None or not current.enabled or current.version != version:
            return False
        value = self._source.get_secret_version(record.source_name, record.source_version)
        readback = self._destination.read(record.destination_name, version)
        return (
            value is not None
            and readback is not None
            and hmac.compare_digest(_digest(value), _digest(readback))
        )

    def _check_verified(self, record: SyncRecord, stats: SyncRunStats) -> None:
        try:
            if self._verified(record):
                return
        except Exception as exc:  # noqa: BLE001 - report verification failure without values
            reason = f"verification_{type(exc).__name__}"
        else:
            reason = "verification_drift"
        stats.failed += 1
        stats.errors.append(f"{record.resource_key}/{record.slot}: {reason}")
        log_event(
            logger,
            "vaultsync.verification_failed",
            level=logging.WARNING,
            resource_key=record.resource_key,
            slot=record.slot,
            endpoint_id=record.endpoint_id,
            reason=reason,
        )

    def _sync(self, record: SyncRecord, etag: str | None, stats: SyncRunStats) -> None:
        try:
            value = self._source.get_secret_version(record.source_name, record.source_version)
        except SecretUnavailableError:
            self._failed(record, etag, stats, "source_unavailable")
            return
        if value is None:
            # Disabled or deleted before synchronization: never substitute another version.
            blocked = replace(record, state="blocked", last_error="source_version_unavailable")
            self._save(blocked, etag)
            stats.blocked += 1
            log_event(
                logger,
                "vaultsync.blocked",
                level=logging.ERROR,
                resource_key=record.resource_key,
                slot=record.slot,
                endpoint_id=record.endpoint_id,
                reason="source_version_unavailable",
            )
            return
        try:
            prepared = self._prepare(record, etag, stats)
            if prepared is None:
                return  # superseded by a newer enabled generation
            record, etag, version = prepared
            name = record.destination_name
            if version is None:
                token, metadata = record.request_token, self._metadata(record)
                version = self._destination.put(name, value, request_token=token, metadata=metadata)
            readback = self._destination.read(name, version)
            current = self._destination.current(name)
        except ConcurrencyConflictError:
            raise
        except Exception as exc:  # noqa: BLE001 - any destination failure is retried next run
            self._failed(record, etag, stats, f"destination_{type(exc).__name__}")
            return
        matches = readback is not None and hmac.compare_digest(_digest(value), _digest(readback))
        del value, readback
        if not matches:
            self._failed(record, etag, stats, "readback_mismatch")
            return
        if current is None or not current.enabled or current.version != version:
            self._failed(record, etag, stats, "destination_version_not_current")
            return
        now = self._clock.now()
        synced = replace(
            record,
            state="synced",
            destination_version=version,
            synced_at=now,
            attempts=record.attempts + 1,
            last_error=None,
        )
        try:
            self._save(synced, etag)
        except ConcurrencyConflictError:
            self._after_lost_race(record, version, stats)
            raise
        stats.synced += 1
        lag = (now - record.captured_at).total_seconds() if record.captured_at else None
        log_event(
            logger,
            "vaultsync.synced",
            resource_key=record.resource_key,
            slot=record.slot,
            endpoint_id=record.endpoint_id,
            source_version=record.source_version,
            destination_version=version,
            generation=record.generation,
            lag_s=round(lag, 3) if lag is not None else None,
        )

    def _prepare(
        self, record: SyncRecord, etag: str | None, stats: SyncRunStats
    ) -> tuple[SyncRecord, str | None, str | None] | None:
        """Decide how to write: reuse a committed write of this token, give way to a newer
        enabled generation, or write above any older history. Returns the record to write
        (generation possibly raised and saved first, so a lost response stays findable), its
        ETag and the already committed version, if any."""
        name = record.destination_name
        existing = self._destination.find(name, record.request_token)
        if existing is not None:
            return record, etag, existing.version  # an earlier write committed; response lost
        newest = self._destination.current(name)
        generation = _generation(newest.metadata) if newest is not None else 0
        if newest is not None and newest.enabled and generation > record.generation:
            self._supersede(record, etag, stats)
            return None
        record = replace(record, state="pending") if record.state == "superseded" else record
        if newest is not None and generation >= record.generation:
            # History from an earlier deployment or capture (e.g. the ledger was lost while the
            # destination kept its versions): write above it so readers see the newest key.
            record = replace(record, generation=generation + 1)
            etag = self._save(record, etag)
        return record, etag, None

    def _supersede(self, record: SyncRecord, etag: str | None, stats: SyncRunStats) -> None:
        stats.superseded += 1
        if record.state == "superseded":
            return
        reason = "newer_generation_downstream"
        self._save(replace(record, state="superseded", last_error=reason), etag)
        log_event(
            logger,
            "vaultsync.superseded",
            level=logging.WARNING,
            resource_key=record.resource_key,
            slot=record.slot,
            endpoint_id=record.endpoint_id,
            generation=record.generation,
        )

    def _after_lost_race(self, record: SyncRecord, version: str, stats: SyncRunStats) -> None:
        """Our write committed but the ledger row moved on (e.g. a retirement processed while
        the put was in flight). Never leave that version enabled unless the ledger tracks it."""
        current = self._load(record.resource_key, record.slot)
        if current is not None and _tracks(current[0], record.request_token):
            return  # still this capture, or already owed: a later run converges via the token
        try:
            self._destination.retire(record.destination_name, version)
        except Exception as exc:  # noqa: BLE001 - kept as owed so the next run retires it
            if current is not None:
                owed = _append_owed(current[0].owed, OwedRetirement(version, record.request_token))
                self._save(replace(current[0], owed=owed), current[1])
            log_event(
                logger,
                "vaultsync.failed",
                level=logging.WARNING,
                resource_key=record.resource_key,
                slot=record.slot,
                endpoint_id=record.endpoint_id,
                reason=f"orphan_retire_{type(exc).__name__}",
            )
            return
        stats.retired += 1
        log_event(
            logger,
            "vaultsync.orphan_retired",
            level=logging.WARNING,
            resource_key=record.resource_key,
            slot=record.slot,
            endpoint_id=record.endpoint_id,
            destination_version=version,
        )

    def _retire(self, record: SyncRecord, etag: str | None, stats: SyncRunStats) -> None:
        try:
            self._retire_downstream(
                record.destination_name, record.destination_version, record.request_token
            )
        except Exception as exc:  # noqa: BLE001
            self._failed(record, etag, stats, f"destination_{type(exc).__name__}")
            return
        retired = replace(record, state="retired", retired_at=self._clock.now(), last_error=None)
        self._save(retired, etag)
        stats.retired += 1
        log_event(
            logger,
            "vaultsync.retired",
            resource_key=record.resource_key,
            slot=record.slot,
            endpoint_id=record.endpoint_id,
        )

    def _retire_owed(
        self, record: SyncRecord, etag: str | None, stats: SyncRunStats
    ) -> tuple[SyncRecord, str | None] | None:
        """Retire every owed generation, oldest first; keep what is left on a failure."""
        remaining = list(record.owed)
        for owed in record.owed:
            try:
                self._retire_downstream(record.destination_name, owed.version, owed.token)
            except Exception as exc:  # noqa: BLE001
                kept = replace(record, owed=tuple(remaining))
                self._failed(kept, etag, stats, f"destination_{type(exc).__name__}")
                return None
            remaining.remove(owed)
            stats.retired += 1
        settled = replace(record, owed=())
        if settled.state not in ("blocked", "superseded"):
            settled = replace(settled, last_error=None)  # an earlier owed-retire failure
        return settled, self._save(settled, etag)

    def _retire_downstream(self, name: str, version: str | None, token: str | None) -> None:
        if not version and token:  # never confirmed: retire a write that may have committed
            found = self._destination.find(name, token)
            version = found.version if found is not None else None
        if version:
            self._destination.retire(name, version)

    def _failed(
        self, record: SyncRecord, etag: str | None, stats: SyncRunStats, reason: str
    ) -> None:
        self._save(replace(record, attempts=record.attempts + 1, last_error=reason), etag)
        stats.failed += 1
        stats.errors.append(f"{record.resource_key}/{record.slot}: {reason}")
        log_event(
            logger,
            "vaultsync.failed",
            level=logging.WARNING,
            resource_key=record.resource_key,
            slot=record.slot,
            endpoint_id=record.endpoint_id,
            reason=reason,
            attempts=record.attempts + 1,
        )

    def _metadata(self, record: SyncRecord) -> dict[str, str]:
        return {
            "environment": self._env,
            "resource_key": record.resource_key,
            "slot": record.slot,
            "endpoint_id": record.endpoint_id,
            "source_version": record.source_version,
            "generation": str(record.generation),
        }

    # -- ledger -----------------------------------------------------------------------------

    def _partition(self) -> str:
        return _ledger_partition(self._env)

    def _load(self, resource_key: str, slot: str) -> tuple[SyncRecord, str | None] | None:
        entity = self._store.get(self._table, self._partition(), f"{resource_key}|{slot}")
        if entity is None:
            return None
        return SyncRecord.from_entity(dict(entity.data)), entity.etag

    def _records(self, resource_key: str | None) -> list[tuple[SyncRecord, str | None]]:
        return ledger_records(self._store, self._table, self._env, resource_key)

    def _save(self, record: SyncRecord, etag: str | None) -> str | None:
        """Conditional write; returns the new ETag."""
        row = f"{record.resource_key}|{record.slot}"
        entity = Entity(self._partition(), row, record.to_entity(), etag)
        kind: Literal["create", "update"] = "update" if etag is not None else "create"
        return self._store.submit(self._table, [TxOp(kind, entity)]).get(row)


def _ledger_partition(environment: str) -> str:
    return f"{environment}|vaultsync"


def _newest_active_per_slot(
    registrations: Sequence[RegistrationRecord],
) -> list[RegistrationRecord]:
    """One live registration per slot. A stale record left active (e.g. a retire interrupted
    after the endpoint was deleted) must not take the slot back from its successor."""
    newest: dict[tuple[str, str], RegistrationRecord] = {}
    for registration in registrations:
        if registration.active_until is not None:
            continue
        key = (registration.resource_key, registration.slot)
        held = newest.get(key)
        rank = (registration.active_from, registration.endpoint_id)
        if held is None or rank > (held.active_from, held.endpoint_id):
            if held is not None:
                _log_ignored(held)
            newest[key] = registration
        else:
            _log_ignored(registration)
    return list(newest.values())


def _log_ignored(registration: RegistrationRecord) -> None:
    log_event(
        logger,
        "vaultsync.stale_registration_ignored",
        level=logging.WARNING,
        resource_key=registration.resource_key,
        slot=registration.slot,
        endpoint_id=registration.endpoint_id,
    )


def _owed_after(record: SyncRecord | None) -> tuple[OwedRetirement, ...]:
    """What a new capture of the slot still owes downstream: everything ``record`` owed, plus
    ``record`` itself unless it is already retired (reused before a run saw the retirement)."""
    if record is None:
        return ()
    if record.state == "retired":
        return record.owed
    itself = OwedRetirement(record.destination_version, record.request_token)
    return _append_owed(record.owed, itself)


def _append_owed(
    owed: tuple[OwedRetirement, ...], item: OwedRetirement
) -> tuple[OwedRetirement, ...]:
    if any(o.token == item.token and o.version == item.version for o in owed):
        return owed
    return (*owed, item)


def _tracks(record: SyncRecord, token: str) -> bool:
    """The ledger still accounts for a write made with ``token``."""
    live = record.request_token == token and record.state != "retired"
    return live or any(o.token == token for o in record.owed)


def _owed_from(raw: object) -> tuple[OwedRetirement, ...]:
    try:
        items = json.loads(str(raw or "[]"))
    except json.JSONDecodeError:
        items = []
    if not isinstance(items, list):
        return ()
    return tuple(
        OwedRetirement(_opt(item[0]), _opt(item[1]))
        for item in items
        if isinstance(item, list) and len(item) == 2 and (item[0] or item[1])
    )


def _opt(value: object) -> str | None:
    text = str(value or "")
    return text or None


def _digest(value: str) -> bytes:
    return hashlib.sha256(value.encode("utf-8")).digest()


def _generation(metadata: Any) -> int:
    try:
        return int(metadata.get("generation", 0))
    except (TypeError, ValueError, AttributeError):
        return 0
