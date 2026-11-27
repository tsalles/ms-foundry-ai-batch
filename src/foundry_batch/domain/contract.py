"""Normalised event contract v1 sent downstream (US-3.4, REQ-W02, REQ-W04).

The contract is provider-independent, metadata-only (no prompt/output contents) and carries a
stable idempotency key shared by the webhook, reconciliation and replay paths.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from foundry_batch.clock import to_iso
from foundry_batch.domain.models import JobRecord

SCHEMA_VERSION = "foundry.batch.event.v1"
SOURCES = frozenset({"webhook", "polling", "replay", "submission"})
OUTCOMES = frozenset({"completed", "failed", "cancelled"})

_TIMESTAMP_NAMES = (
    "created_at",
    "in_progress_at",
    "finalizing_at",
    "completed_at",
    "failed_at",
    "expired_at",
    "cancelling_at",
    "cancelled_at",
)


def idempotency_key(
    environment: str,
    resource_key: str,
    batch_id: str,
    outcome: str,
    *,
    correction_of: str | None = None,
    raw_status: str | None = None,
) -> str:
    """Stable business key: the same job outcome always yields the same key."""
    parts = [environment, resource_key, batch_id, outcome]
    if correction_of:
        parts += ["correction", correction_of, raw_status or ""]
    digest = hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()
    return digest[:40]


@dataclass(frozen=True)
class NormalizedEvent:
    idempotency_key: str
    payload: Mapping[str, Any]

    @classmethod
    def from_job(
        cls,
        job: JobRecord,
        *,
        source: str,
        emitted_at: datetime,
        event_id: str | None = None,
        webhook_id: str | None = None,
        correction_of: str | None = None,
    ) -> NormalizedEvent:
        if job.outcome is None:
            raise ValueError("only terminal jobs produce downstream events")
        if source not in SOURCES:
            raise ValueError(f"unknown source {source!r}")
        key = idempotency_key(
            job.environment,
            job.resource_key,
            job.batch_id,
            job.outcome,
            correction_of=correction_of,
            raw_status=job.raw_status,
        )
        timestamps = {name: to_iso(getattr(job, f"provider_{name}")) for name in _TIMESTAMP_NAMES}
        timestamps["terminal_at"] = to_iso(job.provider_terminal_at)
        payload: dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "idempotency_key": key,
            "environment": job.environment,
            "job_id": job.key.job_id,
            "resource_key": job.resource_key,
            "batch_id": job.batch_id,
            "region": job.region,
            "deployment": job.deployment,
            "model": job.model,
            "correlation_id": job.correlation_id,
            "raw_status": job.raw_status,
            "outcome": job.outcome,
            "timestamps": timestamps,
            "timestamp_provenance": "provider",
            "observed": {
                "terminal_observed_at": to_iso(job.terminal_observed_at),
                "first_webhook_at": to_iso(job.first_webhook_at),
                "emitted_at": to_iso(emitted_at),
            },
            "request_counts": {
                "total": job.request_total,
                "completed": job.request_completed,
                "failed": job.request_failed,
            },
            "output_file_id": job.output_file_id,
            "error_file_id": job.error_file_id,
            "source": source,
            "event_id": event_id,
            "webhook_id": webhook_id,
            "details_complete": job.details_complete,
            "is_correction": correction_of is not None,
            "correction_of": correction_of,
        }
        return cls(idempotency_key=key, payload=payload)


def validate_event(payload: Mapping[str, Any]) -> list[str]:
    """Light structural validation (no third-party schema library). Returns problems found."""
    problems: list[str] = []
    if payload.get("schema_version") != SCHEMA_VERSION:
        problems.append("schema_version")
    for name in ("idempotency_key", "environment", "job_id", "resource_key", "batch_id"):
        if not isinstance(payload.get(name), str) or not payload.get(name):
            problems.append(name)
    if payload.get("outcome") not in OUTCOMES:
        problems.append("outcome")
    if payload.get("source") not in SOURCES:
        problems.append("source")
    if not isinstance(payload.get("raw_status"), str):
        problems.append("raw_status")
    timestamps = payload.get("timestamps")
    if not isinstance(timestamps, Mapping):
        problems.append("timestamps")
    counts = payload.get("request_counts")
    if not isinstance(counts, Mapping):
        problems.append("request_counts")
    return problems
