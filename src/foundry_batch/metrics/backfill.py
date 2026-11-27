"""Historical backfill (US-6.8): page through past jobs and store lifecycle records.

Backfilled jobs are written as untrusted registry rows with ``terminal_source="backfill"``. They
feed indicators only: no outbox item, no downstream event. Existing rows are never overwritten.
The provider keeps batch records for up to 365 days after a final state.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from foundry_batch.clock import Clock
from foundry_batch.config import Settings
from foundry_batch.domain.models import JobRecord
from foundry_batch.domain.status import is_terminal
from foundry_batch.errors import ConcurrencyConflictError, ProviderRetryableError
from foundry_batch.observability import log_event
from foundry_batch.ports import BatchApiFactory
from foundry_batch.processing.registry import JobRegistry

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class BackfillReport:
    resource_key: str
    pages: int
    seen: int
    written: int
    skipped_existing: int
    skipped_open: int
    complete: bool


def backfill(
    settings: Settings,
    registry: JobRegistry,
    batch_apis: BatchApiFactory,
    clock: Clock,
    resource_key: str,
    *,
    max_pages: int = 50,
) -> BackfillReport:
    resource = settings.resource(resource_key)
    if resource is None:
        raise ValueError(f"unknown resource {resource_key!r}")
    api = batch_apis.for_resource(resource_key)
    pages = seen = written = existing = open_jobs = 0
    after: str | None = None
    complete = False
    while pages < max_pages:
        try:
            page = api.list_batches(after=after, limit=100)
        except ProviderRetryableError:
            break
        pages += 1
        for snapshot in page.items:
            seen += 1
            if not is_terminal(snapshot.status):
                open_jobs += 1
                continue
            record = (
                JobRecord(
                    resource_key=resource_key,
                    batch_id=snapshot.batch_id,
                    environment=settings.environment,
                    region=resource.region,
                    trusted=False,
                    details_complete=True,
                    terminal_source="backfill",
                    terminal_observed_at=clock.now(),
                )
                .with_snapshot(snapshot)
                .with_status(snapshot.status)
            )
            try:
                registry.create_job(record)
                written += 1
            except ConcurrencyConflictError:
                existing += 1
        if not page.has_more:
            complete = True
            break
        after = page.last_id
    report = BackfillReport(resource_key, pages, seen, written, existing, open_jobs, complete)
    log_event(
        logger,
        "backfill.completed",
        resource_key=resource_key,
        pages=pages,
        seen=seen,
        written=written,
        skipped_existing=existing,
        skipped_open=open_jobs,
        complete=complete,
    )
    return report
