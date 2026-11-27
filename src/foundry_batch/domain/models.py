"""Immutable domain records: provider snapshots, job keys and the job registry record."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field, fields, replace
from datetime import datetime
from typing import Any

from foundry_batch.clock import from_epoch, from_iso, to_iso
from foundry_batch.domain.status import TERMINAL_TIMESTAMP_FIELD, outcome_for, parse_status

_KEY_UNSAFE = re.compile(r"[/\\#?\x00-\x1f\x7f-\x9f]")

PROVIDER_TIMESTAMP_FIELDS: tuple[str, ...] = (
    "created_at",
    "in_progress_at",
    "finalizing_at",
    "completed_at",
    "failed_at",
    "expired_at",
    "cancelling_at",
    "cancelled_at",
    "expires_at",
)


def safe_key(value: str) -> str:
    """Make text safe for Azure Table PartitionKey/RowKey (no / \\ # ? or control chars)."""
    return _KEY_UNSAFE.sub("_", value)


def utf16_prefix(value: str, max_units: int) -> str:
    """Longest prefix of ``value`` within ``max_units`` UTF-16 code units.

    Azure Tables measure keys and string properties in UTF-16, where a character outside the
    Basic Multilingual Plane (an emoji, say) takes two units. A pair is never split.
    """
    if len(value) * 2 <= max_units:
        return value
    units = 0
    for index, char in enumerate(value):
        units += 2 if ord(char) > 0xFFFF else 1
        if units > max_units:
            return value[:index]
    return value


@dataclass(frozen=True)
class JobKey:
    resource_key: str
    batch_id: str

    def __post_init__(self) -> None:
        if not self.resource_key or not self.batch_id:
            raise ValueError("JobKey needs a resource key and a batch id")

    def partition_key(self, environment: str) -> str:
        return safe_key(f"{environment}|{self.resource_key}|{self.batch_id}")

    @property
    def job_id(self) -> str:
        """Resource-scoped job identifier used in the event contract."""
        return f"{self.resource_key}:{self.batch_id}"


@dataclass(frozen=True)
class RequestCounts:
    total: int | None = None
    completed: int | None = None
    failed: int | None = None

    @classmethod
    def from_api(cls, data: object) -> RequestCounts:
        if not isinstance(data, Mapping):
            return cls()
        return cls(
            total=_opt_int(data.get("total")),
            completed=_opt_int(data.get("completed")),
            failed=_opt_int(data.get("failed")),
        )


@dataclass(frozen=True)
class TokenUsage:
    input_tokens: int | None = None
    output_tokens: int | None = None

    @classmethod
    def from_api(cls, data: object) -> TokenUsage | None:
        if not isinstance(data, Mapping):
            return None
        return cls(
            input_tokens=_opt_int(data.get("input_tokens")),
            output_tokens=_opt_int(data.get("output_tokens")),
        )


@dataclass(frozen=True)
class BatchSnapshot:
    """What the provider says about one batch at one point in time."""

    batch_id: str
    status: str
    timestamps: Mapping[str, datetime | None]
    request_counts: RequestCounts = RequestCounts()
    input_file_id: str | None = None
    output_file_id: str | None = None
    error_file_id: str | None = None
    endpoint: str | None = None
    model: str | None = None
    metadata: Mapping[str, str] = field(default_factory=dict)
    usage: TokenUsage | None = None
    error_codes: tuple[str, ...] = ()

    @classmethod
    def from_api(cls, data: Mapping[str, Any]) -> BatchSnapshot:
        batch_id = data.get("id")
        status = data.get("status")
        if not isinstance(batch_id, str) or not batch_id or not isinstance(status, str):
            raise ValueError("batch object needs string 'id' and 'status'")
        metadata_raw = data.get("metadata")
        metadata = (
            {str(k): str(v) for k, v in metadata_raw.items()}
            if isinstance(metadata_raw, Mapping)
            else {}
        )
        errors = data.get("errors")
        codes: list[str] = []
        if isinstance(errors, Mapping) and isinstance(errors.get("data"), list):
            # Codes only: messages can echo input lines, which must not reach logs or events.
            codes = [str(e.get("code")) for e in errors["data"] if isinstance(e, Mapping)]
        return cls(
            batch_id=batch_id,
            status=status.strip().lower(),
            timestamps={name: from_epoch(data.get(name)) for name in PROVIDER_TIMESTAMP_FIELDS},
            request_counts=RequestCounts.from_api(data.get("request_counts")),
            input_file_id=_opt_str(data.get("input_file_id")),
            output_file_id=_opt_str(data.get("output_file_id")),
            error_file_id=_opt_str(data.get("error_file_id")),
            endpoint=_opt_str(data.get("endpoint")),
            model=_opt_str(data.get("model")),
            metadata=metadata,
            usage=TokenUsage.from_api(data.get("usage")),
            error_codes=tuple(codes),
        )

    @property
    def created_at(self) -> datetime | None:
        return self.timestamps.get("created_at")

    @property
    def terminal_at(self) -> datetime | None:
        status = parse_status(self.status)
        name = TERMINAL_TIMESTAMP_FIELD.get(status) if status else None
        return self.timestamps.get(name) if name else None


@dataclass(frozen=True)
class JobRecord:
    """Registry row for one batch job (Table row key ``job``). Stand-in for Batch's store (D-08)."""

    resource_key: str
    batch_id: str
    environment: str
    raw_status: str = "unknown"
    outcome: str | None = None
    correlation_id: str | None = None
    deployment: str | None = None
    model: str | None = None
    region: str | None = None
    input_file_id: str | None = None
    output_file_id: str | None = None
    error_file_id: str | None = None
    token_estimate: int | None = None
    tokenizer: str | None = None
    request_total: int | None = None
    request_completed: int | None = None
    request_failed: int | None = None
    input_tokens: int | None = None
    output_tokens: int | None = None
    provider_created_at: datetime | None = None
    provider_in_progress_at: datetime | None = None
    provider_finalizing_at: datetime | None = None
    provider_completed_at: datetime | None = None
    provider_failed_at: datetime | None = None
    provider_expired_at: datetime | None = None
    provider_cancelling_at: datetime | None = None
    provider_cancelled_at: datetime | None = None
    submitted_at: datetime | None = None
    terminal_observed_at: datetime | None = None
    terminal_source: str | None = None
    first_webhook_at: datetime | None = None
    webhook_event_count: int = 0
    webhook_covered: bool = False
    registration_id: str | None = None
    last_status_call_at: datetime | None = None
    details_complete: bool = False
    missed_event: bool = False
    missed_event_at: datetime | None = None
    late_event: bool = False
    long_running_flagged: bool = False
    conflict_count: int = 0
    last_event_key: str | None = None  # idempotency key of the last downstream event
    event_accounted: bool = False  # missed/late webhook classification is final
    trusted: bool = True
    updated_at: datetime | None = None

    @property
    def key(self) -> JobKey:
        return JobKey(self.resource_key, self.batch_id)

    @property
    def is_terminal(self) -> bool:
        return self.outcome is not None

    @property
    def provider_terminal_at(self) -> datetime | None:
        status = parse_status(self.raw_status)
        name = TERMINAL_TIMESTAMP_FIELD.get(status) if status else None
        value = getattr(self, f"provider_{name}") if name else None
        return value if isinstance(value, datetime) else None

    def with_snapshot(self, snapshot: BatchSnapshot) -> JobRecord:
        """Copy provider facts from a snapshot without erasing known values with gaps."""
        updates: dict[str, Any] = {}
        for name in PROVIDER_TIMESTAMP_FIELDS:
            if name == "expires_at":
                continue
            timestamp = snapshot.timestamps.get(name)
            if timestamp is not None:
                updates[f"provider_{name}"] = timestamp
        counts = snapshot.request_counts
        for attr, value in (
            ("request_total", counts.total),
            ("request_completed", counts.completed),
            ("request_failed", counts.failed),
            ("output_file_id", snapshot.output_file_id),
            ("error_file_id", snapshot.error_file_id),
            ("input_file_id", snapshot.input_file_id),
            ("model", snapshot.model),
        ):
            if value is not None:
                updates[attr] = value
        if snapshot.usage is not None:
            if snapshot.usage.input_tokens is not None:
                updates["input_tokens"] = snapshot.usage.input_tokens
            if snapshot.usage.output_tokens is not None:
                updates["output_tokens"] = snapshot.usage.output_tokens
        return replace(self, **updates)

    def with_status(self, raw_status: str) -> JobRecord:
        outcome = outcome_for(raw_status)
        return replace(
            self,
            raw_status=raw_status,
            outcome=outcome.value if outcome else None,
        )

    def to_entity(self) -> dict[str, Any]:
        entity: dict[str, Any] = {}
        for f in fields(self):
            value = getattr(self, f.name)
            if value is None:
                continue
            entity[f.name] = to_iso(value) if isinstance(value, datetime) else value
        return entity

    @classmethod
    def from_entity(cls, entity: Mapping[str, Any]) -> JobRecord:
        kwargs: dict[str, Any] = {}
        for f in fields(cls):
            if f.name not in entity:
                continue
            value = entity[f.name]
            if f.type in ("datetime | None",):
                kwargs[f.name] = from_iso(value)
            else:
                kwargs[f.name] = value
        return cls(**kwargs)


def _opt_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return None


def _opt_str(value: object) -> str | None:
    return value if isinstance(value, str) and value else None
