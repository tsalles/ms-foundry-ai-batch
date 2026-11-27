"""Reconciliation sweep (US-5.2) and missed-event / long-running accounting (US-5.5, US-6.6).

Per resource and run:

1. take a lease so only one worker sweeps a resource;
2. continue (or start) a paged traversal of ``GET /batches`` until every open tracked job is
   covered, the call budget is used, or the provider says there is nothing more; the cursor and
   the tracked jobs listed so far are persisted so an interrupted traversal resumes without
   re-checking them (a cursor the provider rejects restarts the traversal);
3. apply every changed tracked job through the same processor path as webhooks;
4. at the end of a traversal, retrieve tracked jobs the list did not return, respecting the
   per-job 60-second floor and the budget; one refused job is logged and skipped;
5. classify missed events and flag long-running jobs, then log one summary record.

A full traversal is many HTTP calls; the summary reports pages and retrieves separately, plus
``lag_s``: seconds since the last sweep that finished traversal and fallback (the convergence
lag the sweep-lagging alert watches).
"""

from __future__ import annotations

import json
import logging
import uuid
from collections.abc import Iterable
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
from itertools import pairwise

from foundry_batch.clock import Clock, from_iso, to_iso
from foundry_batch.config import Settings
from foundry_batch.control import ControlStore
from foundry_batch.domain.models import BatchSnapshot, JobKey, JobRecord
from foundry_batch.domain.status import is_terminal
from foundry_batch.errors import (
    ProviderNotFoundError,
    ProviderRequestError,
    ProviderRetryableError,
    RetryLater,
)
from foundry_batch.observability import log_event
from foundry_batch.ports import BatchApiFactory
from foundry_batch.processing.decision import Source
from foundry_batch.processing.processor import EventProcessor, observation_from_snapshot
from foundry_batch.processing.registry import JobRegistry, Receipt, StoredJob

logger = logging.getLogger(__name__)


@dataclass
class SweepStats:
    resource_key: str
    started_at: datetime
    list_pages: int = 0
    retrieves: int = 0
    status_429: int = 0
    status_5xx: int = 0
    open_tracked: int = 0
    seen_in_list: int = 0
    untracked_seen: int = 0
    jobs_updated: int = 0
    terminal_found: int = 0
    skipped_floor: int = 0
    missed_classified: int = 0
    accounted: int = 0
    long_running_flagged: int = 0
    traversal_complete: bool = False
    budget_exhausted: bool = False
    stopped_on_throttle: bool = False
    stopped_on_deadline: bool = False
    skipped_reason: str | None = None
    retrieve_failures: int = 0
    cursor_reset: bool = False
    #: End of the last sweep that finished list traversal and fallback (None: never).
    last_complete_at: datetime | None = None
    #: Seconds from ``last_complete_at`` to the end of this sweep: the convergence lag.
    lag_s: float | None = None
    results: dict[str, int] = field(default_factory=dict)

    @property
    def calls(self) -> int:
        return self.list_pages + self.retrieves


class Reconciler:
    def __init__(  # explicit dependencies keep the class testable
        self,
        settings: Settings,
        registry: JobRegistry,
        processor: EventProcessor,
        control: ControlStore,
        batch_apis: BatchApiFactory,
        clock: Clock,
        *,
        holder: str | None = None,
    ) -> None:
        self._settings = settings
        self._registry = registry
        self._processor = processor
        self._control = control
        self._apis = batch_apis
        self._clock = clock
        self._holder = holder or f"sweeper-{uuid.uuid4().hex[:8]}"

    def run_all(self) -> list[SweepStats]:
        return [self.run(resource.key) for resource in self._settings.resources]

    def run(self, resource_key: str) -> SweepStats:
        stats = SweepStats(resource_key, self._clock.now())
        lease = self._control.acquire(
            f"sweep|{resource_key}", self._holder, self._settings.sweep_lease_seconds
        )
        if lease is None:
            stats.skipped_reason = "lease_held"
            log_event(
                logger, "reconcile.sweep_skipped", resource_key=resource_key, reason="lease_held"
            )
            return stats
        try:
            self._sweep(resource_key, stats)
            self._account(resource_key, stats)
        except Exception:
            log_event(
                logger,
                "reconcile.sweep_failed",
                level=logging.ERROR,
                resource_key=resource_key,
                list_pages=stats.list_pages,
                retrieves=stats.retrieves,
            )
            raise
        finally:
            self._control.release(lease)
        finished = self._clock.now()
        duration = (finished - stats.started_at).total_seconds()
        if stats.last_complete_at is not None:
            stats.lag_s = round((finished - stats.last_complete_at).total_seconds(), 3)
        log_event(
            logger,
            "reconcile.sweep_completed",
            resource_key=resource_key,
            list_pages=stats.list_pages,
            retrieves=stats.retrieves,
            status_429=stats.status_429,
            status_5xx=stats.status_5xx,
            open_tracked=stats.open_tracked,
            seen_in_list=stats.seen_in_list,
            jobs_updated=stats.jobs_updated,
            terminal_found=stats.terminal_found,
            skipped_floor=stats.skipped_floor,
            missed_classified=stats.missed_classified,
            long_running_flagged=stats.long_running_flagged,
            traversal_complete=stats.traversal_complete,
            budget_exhausted=stats.budget_exhausted,
            stopped_on_throttle=stats.stopped_on_throttle,
            stopped_on_deadline=stats.stopped_on_deadline,
            retrieve_failures=stats.retrieve_failures,
            cursor_reset=stats.cursor_reset,
            last_complete_at=to_iso(stats.last_complete_at) or "",
            lag_s=stats.lag_s,
            duration_s=round(duration, 3),
        )
        return stats

    # -- sweep ----------------------------------------------------------------------------

    def _sweep(self, resource_key: str, stats: SweepStats) -> None:
        open_jobs = self._registry.open_jobs(resource_key)
        incomplete = self._registry.terminal_jobs(resource_key, where={"details_complete": False})
        stats.open_tracked = len(open_jobs)
        tracked: dict[str, StoredJob] = {j.record.batch_id: j for j in open_jobs}
        state = self._control.get_state(f"sweep|{resource_key}")
        stats.last_complete_at = from_iso(state.get("last_complete_at"))
        if not tracked and not incomplete:
            stats.traversal_complete = True  # nothing to converge: counts as a complete sweep
            self._complete(resource_key, stats)
            return

        api = self._apis.for_resource(resource_key)
        resuming_fallback = state.get("phase") == "fallback"
        cursor: str | None = state.get("cursor") or None
        watermark = self._watermark(open_jobs)
        # Tracked jobs an interrupted traversal already listed in earlier sweeps (saved with
        # its cursor), so the fallback does not retrieve them again one by one.
        listed_before = _load_pending(state.get("listed")) if cursor else None
        seen: set[str] = (listed_before or set()) & set(tracked)
        budget = self._settings.sweep_max_calls
        previous_last: datetime | None = None

        while tracked and not resuming_fallback and stats.calls < budget:
            if self._past_deadline(stats):
                break
            try:
                page = api.list_batches(after=cursor, limit=self._settings.sweep_page_size)
            except ProviderRetryableError as exc:
                self._count_throttle(stats, exc)
                self._save_traversal(resource_key, stats, cursor, seen)
                return
            except ProviderRequestError as exc:
                if cursor is None:
                    raise  # the first page itself is refused (e.g. permissions): surface it
                # The provider no longer accepts the saved cursor: restart the traversal on
                # the next sweep instead of failing every sweep on the same cursor.
                stats.cursor_reset = True
                log_event(
                    logger,
                    "reconcile.cursor_rejected",
                    level=logging.WARNING,
                    resource_key=resource_key,
                    status_code=exc.status_code,
                )
                self._save_traversal(resource_key, stats, None, set())
                return
            stats.list_pages += 1
            for snapshot in page.items:
                stored = tracked.get(snapshot.batch_id)
                if stored is None:
                    stats.untracked_seen += 1
                    continue
                seen.add(snapshot.batch_id)
                stats.seen_in_list += 1
                self._apply(stored.record, snapshot, stats, status_call=False)
            cursor = page.last_id
            done = (
                not page.has_more
                or not page.last_id  # no cursor to continue from: treat as the end
                or self._passed_watermark(page.items, watermark, previous_last)
            )
            previous_last = page.items[-1].created_at if page.items else previous_last
            if done or seen >= set(tracked):
                cursor = None
                stats.traversal_complete = True
                break
        else:
            stats.traversal_complete = not tracked or resuming_fallback
        if not stats.traversal_complete:
            stats.budget_exhausted = not stats.stopped_on_deadline
            self._save_traversal(resource_key, stats, cursor, seen)
            return
        fallback = [j.record for j in open_jobs if j.record.batch_id not in seen]
        fallback += [j.record for j in incomplete]
        fallback.sort(key=lambda job: job.batch_id)
        fallback_after = ""
        if resuming_fallback:
            # Resume only the jobs the completed traversal actually omitted. Without the saved
            # set (older state, or too many to store) every open job is a candidate: correct,
            # but it spends status calls on jobs the list already returned.
            fallback_after = str(state.get("fallback_after") or "")
            pending = _load_pending(state.get("fallback_pending"))
            if pending is not None:
                fallback = [job for job in fallback if job.batch_id in pending]
        saved_pending = _dump_pending(job.batch_id for job in fallback)
        if saved_pending is None:
            log_event(
                logger,
                "reconcile.fallback_set_not_saved",
                level=logging.WARNING,
                resource_key=resource_key,
                jobs=len(fallback),
            )
        for job in fallback:
            if job.batch_id <= fallback_after:
                continue
            checkpoint: dict[str, str] = {
                "phase": "fallback",
                "cursor": "",
                "fallback_after": fallback_after,
            }
            if saved_pending is not None:
                checkpoint["fallback_pending"] = saved_pending
            self._put_state(resource_key, stats, checkpoint)
            if stats.calls >= budget:
                stats.budget_exhausted = True
                return
            if self._past_deadline(stats):
                return
            try:
                snapshot = self._processor.retrieve_snapshot(job.key)
            except RetryLater:
                stats.skipped_floor += 1
                fallback_after = job.batch_id
                continue
            except ProviderNotFoundError:
                stats.retrieves += 1
                log_event(
                    logger,
                    "reconcile.job_not_found",
                    level=logging.WARNING,
                    resource_key=resource_key,
                    batch_id=job.batch_id,
                )
                fallback_after = job.batch_id
                continue
            except ProviderRetryableError as exc:
                stats.retrieves += 1
                self._count_throttle(stats, exc)
                return
            except ProviderRequestError as exc:
                # One job the provider refuses (400/403) must not stall the whole fallback and
                # the accounting behind it: record it and move on.
                stats.retrieves += 1
                stats.retrieve_failures += 1
                log_event(
                    logger,
                    "reconcile.retrieve_failed",
                    level=logging.WARNING,
                    resource_key=resource_key,
                    batch_id=job.batch_id,
                    status_code=exc.status_code,
                )
                fallback_after = job.batch_id
                continue
            stats.retrieves += 1
            self._apply(job, snapshot, stats, status_call=True)
            fallback_after = job.batch_id
        self._complete(resource_key, stats)

    def _complete(self, resource_key: str, stats: SweepStats) -> None:
        """Record a sweep that finished list traversal and fallback: the lag resets here."""
        stats.last_complete_at = self._clock.now()
        self._put_state(resource_key, stats, {"phase": "list", "cursor": ""})

    def _save_traversal(
        self, resource_key: str, stats: SweepStats, cursor: str | None, seen: set[str]
    ) -> None:
        """Checkpoint an interrupted list traversal: its cursor and the jobs listed so far."""
        data = {"cursor": cursor or ""}
        if cursor:
            listed = _dump_pending(seen)
            if listed is None:
                log_event(
                    logger,
                    "reconcile.listed_set_not_saved",
                    level=logging.WARNING,
                    resource_key=resource_key,
                    jobs=len(seen),
                )
            else:
                data["listed"] = listed
        self._put_state(resource_key, stats, data)

    def _put_state(self, resource_key: str, stats: SweepStats, data: dict[str, str]) -> None:
        """Write the sweep checkpoint; every write keeps ``last_complete_at`` (the lag anchor)."""
        anchor = to_iso(stats.last_complete_at)
        self._control.put_state(
            f"sweep|{resource_key}", {**data, "last_complete_at": anchor} if anchor else data
        )

    def _apply(
        self, job: JobRecord, snapshot: BatchSnapshot, stats: SweepStats, *, status_call: bool
    ) -> None:
        if (
            not status_call
            and snapshot.status == job.raw_status
            and not is_terminal(snapshot.status)
        ):
            return  # unchanged non-terminal job: no write needed
        now = self._clock.now()
        obs = observation_from_snapshot(snapshot, now, status_call_made=status_call)
        receipt = None
        if obs.receipt_id and not job.is_terminal:
            receipt = Receipt(obs.receipt_id, Source.POLLING.value, now)
        try:
            result = self._processor.apply(JobKey(job.resource_key, job.batch_id), obs, receipt)
        except RetryLater as exc:
            log_event(
                logger,
                "reconcile.apply_deferred",
                level=logging.WARNING,
                resource_key=job.resource_key,
                batch_id=job.batch_id,
                reason=exc.reason,
            )
            return
        stats.results[result.kind] = stats.results.get(result.kind, 0) + 1
        if result.kind not in ("unchanged", "duplicate", "stale"):
            stats.jobs_updated += 1
        if result.kind == "terminal":
            stats.terminal_found += 1

    def _watermark(self, open_jobs: list[StoredJob]) -> datetime | None:
        created = [j.record.provider_created_at or j.record.submitted_at for j in open_jobs]
        known = [c for c in created if c is not None]
        if len(known) != len(created) or not known:
            return None  # unknown age: never stop early
        return min(known) - timedelta(seconds=self._settings.sweep_overlap_seconds)

    @staticmethod
    def _passed_watermark(
        items: tuple[BatchSnapshot, ...],
        watermark: datetime | None,
        previous_last: datetime | None = None,
    ) -> bool:
        """Stop early only on proven newest-first order that is already past the watermark.

        Proof needs non-increasing creation times across this page (continuing from the
        previous page's last item) with at least one strict decrease; a single item, or items
        sharing one timestamp, prove nothing about the order.
        """
        if watermark is None or not items:
            return False
        created = [s.created_at for s in items]
        if any(c is None for c in created):
            return False
        known = [c for c in created if c is not None]
        sequence = ([previous_last] if previous_last is not None else []) + known
        non_increasing = all(earlier >= later for earlier, later in pairwise(sequence))
        strictly_ordered = len(sequence) >= 2 and sequence[0] > sequence[-1]
        return non_increasing and strictly_ordered and known[-1] < watermark

    def _past_deadline(self, stats: SweepStats) -> bool:
        """Stop before the lease can expire so two workers never sweep one resource at once."""
        elapsed = (self._clock.now() - stats.started_at).total_seconds()
        if elapsed >= 0.8 * self._settings.sweep_lease_seconds:
            stats.stopped_on_deadline = True
            return True
        return False

    @staticmethod
    def _count_throttle(stats: SweepStats, exc: ProviderRetryableError) -> None:
        stats.stopped_on_throttle = True
        if exc.status_code == 429:
            stats.status_429 += 1
        elif exc.status_code is not None and exc.status_code >= 500:
            stats.status_5xx += 1

    # -- accounting -----------------------------------------------------------------------

    def _account(self, resource_key: str, stats: SweepStats) -> None:
        """Classify each polling-detected final state once: webhook within grace, or missed.

        The grace runs from the provider's final timestamp (else our first observation), so the
        result does not depend on when the sweep happens to run. A webhook that arrives after
        the grace still counts as missed (and is also flagged ``late_event`` by the processor).
        """
        now = self._clock.now()
        grace = timedelta(seconds=self._settings.missed_event_grace_seconds)
        candidates = self._registry.terminal_jobs(
            resource_key,
            where={"terminal_source": Source.POLLING.value, "event_accounted": False},
        )
        for stored in candidates:
            job = stored.record
            if job.event_accounted or job.terminal_source != Source.POLLING.value:
                continue
            reference = job.provider_terminal_at or job.terminal_observed_at
            if reference is None:
                continue
            arrived = job.first_webhook_at
            if arrived is None and now - reference < grace:
                continue  # still inside the grace period
            missed = (
                job.webhook_covered
                and (arrived is None or arrived - reference > grace)
                and self._covered_at(job)
            )
            updated = self._registry.try_update_job(
                stored,
                replace(
                    job,
                    event_accounted=True,
                    missed_event=missed,
                    missed_event_at=now if missed else None,
                ),
            )
            if updated is None:
                continue  # another writer changed the job; the next sweep re-evaluates it
            stats.accounted += 1
            if missed:
                stats.missed_classified += 1
                log_event(
                    logger,
                    "accounting.missed_event",
                    level=logging.WARNING,
                    resource_key=resource_key,
                    batch_id=job.batch_id,
                    raw_status=job.raw_status,
                    registration_id=job.registration_id,
                    webhook_arrived=arrived is not None,
                )
        threshold = timedelta(seconds=self._settings.long_running_threshold_seconds)
        for stored in self._registry.open_jobs(resource_key):
            job = stored.record
            started = job.provider_created_at or job.submitted_at
            if job.long_running_flagged or started is None or now - started < threshold:
                continue
            updated = self._registry.try_update_job(stored, replace(job, long_running_flagged=True))
            if updated is not None:
                stats.long_running_flagged += 1
                log_event(
                    logger,
                    "job.over_24h",
                    level=logging.WARNING,
                    resource_key=resource_key,
                    batch_id=job.batch_id,
                    age_h=round((now - started).total_seconds() / 3600, 2),
                    raw_status=job.raw_status,
                )

    def _covered_at(self, job: JobRecord) -> bool:
        moment = job.provider_terminal_at or job.terminal_observed_at
        if moment is None:
            return False
        registration = self._control.active_registration(job.resource_key, moment)
        return registration is not None


#: Azure Table string properties hold 32K UTF-16 characters; stay well below that.
MAX_PENDING_CHARS = 24_000


def _dump_pending(batch_ids: Iterable[str]) -> str | None:
    """JSON list of the fallback candidates, or ``None`` when it would not fit one property."""
    text = json.dumps(sorted(batch_ids), separators=(",", ":"))
    return text if len(text) <= MAX_PENDING_CHARS else None


def _load_pending(raw: object) -> set[str] | None:
    if not isinstance(raw, str) or not raw:
        return None
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return None
    if not isinstance(data, list) or not all(isinstance(item, str) for item in data):
        return None
    return set(data)
