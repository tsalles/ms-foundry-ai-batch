"""Per-job lifecycle record (US-6.1): durations derived from provider timestamps.

Missing or inconsistent timestamps never become zero: the value is ``None`` and a flag says
why (CON-B05, US-6.2 AC4).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from foundry_batch.clock import to_iso
from foundry_batch.domain.models import JobRecord


def _span(
    start: datetime | None, end: datetime | None, name: str, flags: list[str]
) -> float | None:
    if start is None or end is None:
        flags.append(f"{name}_unavailable")
        return None
    seconds = (end - start).total_seconds()
    if seconds < 0:
        flags.append(f"{name}_negative")
        return None
    return seconds


@dataclass(frozen=True)
class LifecycleRecord:
    resource_key: str
    batch_id: str
    deployment: str | None
    region: str | None
    model: str | None
    raw_status: str
    outcome: str | None
    source: str | None
    created_at: datetime | None
    terminal_at: datetime | None
    terminal_at_provenance: str
    turnaround_s: float | None
    time_to_start_s: float | None
    processing_s: float | None
    finalizing_s: float | None
    requests_total: int | None
    requests_failed: int | None
    input_tokens: int | None
    output_tokens: int | None
    flags: tuple[str, ...]

    @classmethod
    def from_job(cls, job: JobRecord) -> LifecycleRecord:
        flags: list[str] = []
        created = job.provider_created_at
        terminal = job.provider_terminal_at
        provenance = "provider"
        if terminal is None and job.terminal_observed_at is not None:
            terminal = job.terminal_observed_at
            provenance = "observed"
            flags.append("terminal_at_observed")
        turnaround = _span(created, terminal, "turnaround", flags)
        time_to_start = _span(created, job.provider_in_progress_at, "time_to_start", flags)
        processing = _span(
            job.provider_in_progress_at, job.provider_finalizing_at, "processing", flags
        )
        finalizing = (
            _span(job.provider_finalizing_at, job.provider_completed_at, "finalizing", flags)
            if job.raw_status == "completed"
            else None
        )
        return cls(
            resource_key=job.resource_key,
            batch_id=job.batch_id,
            deployment=job.deployment,
            region=job.region,
            model=job.model,
            raw_status=job.raw_status,
            outcome=job.outcome,
            source=job.terminal_source,
            created_at=created,
            terminal_at=terminal,
            terminal_at_provenance=provenance,
            turnaround_s=turnaround,
            time_to_start_s=time_to_start,
            processing_s=processing,
            finalizing_s=finalizing,
            requests_total=job.request_total,
            requests_failed=job.request_failed,
            input_tokens=job.input_tokens,
            output_tokens=job.output_tokens,
            flags=tuple(flags),
        )

    @property
    def per_request_s(self) -> float | None:
        if self.turnaround_s is None or not self.requests_total:
            return None
        return self.turnaround_s / self.requests_total

    @property
    def per_million_tokens_s(self) -> float | None:
        """Turnaround per million observed tokens; unavailable without complete usage."""
        if self.turnaround_s is None or self.input_tokens is None or self.output_tokens is None:
            return None
        tokens = self.input_tokens + self.output_tokens
        return None if tokens <= 0 else self.turnaround_s / tokens * 1_000_000

    def log_properties(self) -> dict[str, Any]:
        return {
            "resource_key": self.resource_key,
            "batch_id": self.batch_id,
            "deployment": self.deployment,
            "region": self.region,
            "model": self.model,
            "raw_status": self.raw_status,
            "outcome": self.outcome,
            "source": self.source,
            "terminal_at": to_iso(self.terminal_at),
            "terminal_at_provenance": self.terminal_at_provenance,
            "turnaround_s": self.turnaround_s,
            "time_to_start_s": self.time_to_start_s,
            "processing_s": self.processing_s,
            "finalizing_s": self.finalizing_s,
            "requests_total": self.requests_total,
            "requests_failed": self.requests_failed,
            "per_request_s": self.per_request_s,
            "flags": ",".join(self.flags),
        }
