"""Processor resilience: one status call per pass, unknown-job grace, Table outages, UTF-16."""

import unittest
from collections.abc import Sequence
from datetime import timedelta
from typing import Any
from unittest import mock

from foundry_batch.adapters.memory import MemoryQueue, MemoryTableStore
from foundry_batch.control import ControlStore, RegistrationRecord
from foundry_batch.domain.models import JobKey, JobRecord
from foundry_batch.errors import (
    ConcurrencyConflictError,
    ConfigurationError,
    ProviderRequestError,
    ProviderRetryableError,
    QueueUnavailableError,
    RetryLater,
    StoreUnavailableError,
)
from foundry_batch.ports import Entity, Query, TxOp
from foundry_batch.processing.processor import (
    MAX_DEFERRALS,
    MAX_QUARANTINE_TEXT,
    STORE_RETRY_MAX_DELAY_SECONDS,
    EventProcessor,
    ProcessResult,
    store_retry_delay,
)
from foundry_batch.processing.registry import JobRegistry
from foundry_batch.reconcile.sweeper import Reconciler
from foundry_batch.webhook.envelope import Envelope
from tests.support import T0, FakeProvider, FakeProviderFactory, batch_event, clock, make_settings


def envelope(
    batch_id: str,
    event_type: str = "batch.completed",
    *,
    event_id: str = "evt_1",
    attempt: int = 0,
    enrichment_failures: int = 0,
) -> Envelope:
    event = batch_event(batch_id, event_type, event_id)
    return Envelope(
        "test",
        "eus2",
        f"msg_{event_id}",
        int(T0.timestamp()),
        T0,
        event_id,
        event_type,
        event["created_at"],
        batch_id,
        event,
        attempt=attempt,
        enrichment_failures=enrichment_failures,
    )


class FlakyStore(MemoryTableStore):
    """Table store that can be taken down (every read and write fails) per table."""

    def __init__(self) -> None:
        super().__init__()
        self.down: set[str] = set()

    def get(self, table: str, partition_key: str, row_key: str) -> Entity | None:
        if table in self.down:
            raise StoreUnavailableError(f"get {table}: ServiceRequestError")
        return super().get(table, partition_key, row_key)

    def submit(self, table: str, ops: Sequence[TxOp]) -> dict[str, str]:
        if table in self.down:
            raise StoreUnavailableError(f"{table}: ServiceRequestError")
        return super().submit(table, ops)


class FailingQueue:
    def send(self, message: str, *, visibility_timeout: int | None = None) -> None:
        raise QueueUnavailableError("down")


class Harness(unittest.TestCase):
    def setUp(self, **settings: str) -> None:
        self.settings = make_settings(**settings)
        self.clock = clock()
        self.store = FlakyStore()
        self.registry = JobRegistry(self.store, self.settings, self.clock)
        self.provider = FakeProvider(self.clock)
        self.factory = FakeProviderFactory({"eus2": self.provider, "swc": FakeProvider(self.clock)})
        self.relay = MemoryQueue("relay")
        self.incoming = MemoryQueue("incoming")
        self.processor = EventProcessor(
            self.settings,
            self.registry,
            self.store,
            self.factory,
            self.relay,
            self.clock,
            incoming_queue=self.incoming,
        )

    def job(self, batch_id: str) -> JobRecord:
        stored = self.registry.load(JobKey("eus2", batch_id))
        assert stored is not None
        return stored.record

    def quarantine(self) -> list[dict[str, Any]]:
        return [dict(e.data) for e in self.store.query("batchquarantine", Query())]

    def deferred(self) -> Envelope:
        return Envelope.from_json(self.incoming.sent[-1])

    def last_delay(self) -> float:
        return self.incoming._messages[-1].visible_after - self.incoming.now


class SingleStatusCallPerPassTests(Harness):
    """A conflicting final event for a final job whose details are missing (N1)."""

    def final_job_without_details(self, provider_status: str) -> str:
        batch_id = self.provider.add("in_progress")
        self.provider.set_status(batch_id, provider_status)
        self.registry.create_job(JobRecord("eus2", batch_id, "test").with_status("completed"))
        return batch_id

    def test_failed_enrichment_call_settles_the_conflict_in_the_same_pass(self) -> None:
        batch_id = self.final_job_without_details("failed")
        self.provider.fail("retrieve", ProviderRequestError("forbidden", status_code=403))
        message = envelope(batch_id, "batch.failed", event_id="evt_conflict").to_json()
        with self.assertLogs("foundry_batch", "WARNING") as logs:
            result = self.processor.handle_queue_message(message)
        self.assertEqual(result.kind, "duplicate")
        self.assertEqual(self.provider.calls["retrieve"], 1)
        self.assertEqual(self.incoming.sent, [])  # settled, not deferred
        job = self.job(batch_id)
        self.assertEqual((job.raw_status, job.conflict_count), ("completed", 1))
        self.assertTrue(any("processor.conflict_unresolved" in line for line in logs.output))

    def test_lagging_enrichment_snapshot_is_reused_for_the_conflict(self) -> None:
        batch_id = self.final_job_without_details("in_progress")  # provider read lags
        result = self.processor.handle_queue_message(
            envelope(batch_id, "batch.failed", event_id="evt_conflict").to_json()
        )
        self.assertEqual(result.kind, "stale")
        self.assertEqual(self.provider.calls["retrieve"], 1)
        self.assertEqual(self.incoming.sent, [])
        self.assertEqual(self.job(batch_id).raw_status, "completed")

    def test_terminal_enrichment_snapshot_settles_without_a_second_call(self) -> None:
        batch_id = self.final_job_without_details("failed")
        result = self.processor.handle_queue_message(
            envelope(batch_id, "batch.failed", event_id="evt_conflict").to_json()
        )
        self.assertEqual(result.kind, "correction")
        self.assertEqual(self.provider.calls["retrieve"], 1)

    def test_throttled_exhausted_enrichment_defers_the_conflict_past_the_floor(self) -> None:
        self.setUp(BATCH_STATUS_CALL_FLOOR_SECONDS="120")
        batch_id = self.final_job_without_details("failed")
        self.provider.fail("retrieve", ProviderRetryableError("busy", status_code=429))
        last_try = envelope(batch_id, "batch.failed", enrichment_failures=2)
        with self.assertRaises(RetryLater) as ctx:
            self.processor.process_envelope(last_try)
        self.assertEqual(ctx.exception.reason, "conflict_resolution_pending")
        self.assertEqual(ctx.exception.delay_seconds, 120)  # the floor it just booked
        self.assertEqual(self.provider.calls["retrieve"], 1)
        self.clock.advance(120)
        self.processor.process_envelope(last_try)  # next pass: the floor has passed
        self.assertEqual(self.provider.calls["retrieve"], 2)

    def test_retry_after_longer_than_the_floor_is_honoured_for_the_conflict(self) -> None:
        batch_id = self.final_job_without_details("failed")
        self.provider.fail(
            "retrieve", ProviderRetryableError("busy", status_code=429, retry_after_seconds=90)
        )
        last_try = envelope(batch_id, "batch.failed", enrichment_failures=2)
        with self.assertRaises(RetryLater) as ctx:
            self.processor.process_envelope(last_try)
        self.assertEqual(ctx.exception.delay_seconds, 90)

    def test_conflict_count_survives_a_concurrency_retry(self) -> None:
        batch_id = self.provider.add("completed")
        self.registry.create_job(
            JobRecord("eus2", batch_id, "test", details_complete=True).with_status("completed")
        )
        original = self.registry.commit
        attempts: list[int] = []

        def raced(*args: Any, **kwargs: Any) -> str:
            attempts.append(1)
            if len(attempts) == 1:
                raise ConcurrencyConflictError("another writer")
            return original(*args, **kwargs)

        with mock.patch.object(self.registry, "commit", raced):
            result = self.processor.process_envelope(envelope(batch_id, "batch.failed"))
        self.assertEqual(result.kind, "duplicate")
        self.assertEqual(len(attempts), 2)
        self.assertEqual(self.provider.calls["retrieve"], 1)
        self.assertEqual(self.job(batch_id).conflict_count, 1)


class UnknownJobGraceTests(Harness):
    """A webhook that reaches the processor before its job is linked (N2)."""

    def run_until_settled(self, message: str) -> tuple[ProcessResult, list[int]]:
        delays: list[int] = []
        for _ in range(MAX_DEFERRALS + 1):
            result = self.processor.handle_queue_message(message)
            if result.kind != "deferred":
                return result, delays
            delays.append(int(self.last_delay()))
            self.clock.advance(delays[-1])
            message = self.incoming.sent[-1]
        raise AssertionError("the event was still deferred after MAX_DEFERRALS passes")

    def test_event_that_beats_submission_linking_is_held_then_processed(self) -> None:
        batch_id = self.provider.add("validating")
        self.provider.set_status(batch_id, "failed")  # fast validation failure
        message = envelope(batch_id, "batch.failed").to_json()
        with self.assertLogs("foundry_batch", "INFO") as logs:
            first = self.processor.handle_queue_message(message)
        self.assertEqual(first.kind, "deferred")
        self.assertTrue(any("reason=unknown_job_pending" in line for line in logs.output))
        self.assertEqual(self.quarantine(), [])
        self.clock.advance(2)  # submission links the job from the create response
        self.registry.create_job(
            JobRecord("eus2", batch_id, "test", raw_status="validating", webhook_covered=True)
        )
        self.clock.advance(58)
        result = self.processor.handle_queue_message(self.incoming.sent[-1])
        self.assertEqual(result.kind, "terminal")
        job = self.job(batch_id)
        self.assertEqual((job.terminal_source, job.first_webhook_at), ("webhook", T0))
        self.assertEqual(self.quarantine(), [])

    def test_held_event_is_not_counted_as_a_missed_webhook(self) -> None:
        control = ControlStore(self.store, "batchcontrol", "test", self.clock)
        control.save_registration(
            RegistrationRecord("eus2", "a", "wh_1", "n", "u", "sec", "v1", T0 - timedelta(days=1))
        )
        reconciler = Reconciler(
            self.settings,
            self.registry,
            self.processor,
            control,
            self.factory,
            self.clock,
            holder="t1",
        )
        batch_id = self.provider.add("validating")
        self.provider.set_status(batch_id, "failed")
        self.processor.handle_queue_message(envelope(batch_id, "batch.failed").to_json())
        self.clock.advance(2)
        self.registry.create_job(
            JobRecord(
                "eus2",
                batch_id,
                "test",
                raw_status="validating",
                webhook_covered=True,
                registration_id="wh_1",
                submitted_at=T0,
            )
        )
        self.clock.advance(58)
        self.processor.handle_queue_message(self.incoming.sent[-1])
        self.clock.advance(15 * 60)
        reconciler.run_all()
        self.clock.advance(self.settings.missed_event_grace_seconds + 60)
        reconciler.run_all()
        job = self.job(batch_id)
        self.assertFalse(job.missed_event)
        self.assertEqual((job.terminal_source, job.webhook_event_count), ("webhook", 1))

    def test_event_for_a_job_never_linked_is_quarantined_after_the_grace(self) -> None:
        result, delays = self.run_until_settled(envelope("batch_never").to_json())
        self.assertEqual(result.kind, "quarantined")
        self.assertEqual(delays, [60, 180, 540, 1020])  # 1, 3, 9 min, then the rest of 30 min
        self.assertEqual(self.clock.now() - T0, timedelta(seconds=1800))
        self.assertEqual([q["reason"] for q in self.quarantine()], ["unknown_job"])

    def test_backoff_spans_the_longest_configurable_grace_within_the_cap(self) -> None:
        with self.assertRaises(ConfigurationError):
            make_settings(BATCH_UNKNOWN_JOB_GRACE_SECONDS="21601")
        self.setUp(BATCH_UNKNOWN_JOB_GRACE_SECONDS="21600")
        result, delays = self.run_until_settled(envelope("batch_never").to_json())
        self.assertEqual(result.kind, "quarantined")
        self.assertEqual(sum(delays), 21600)
        # Room is left for the three enrichment attempts and a floor deferral after linking.
        self.assertLessEqual(len(delays), MAX_DEFERRALS - 4)

    def test_grace_setting_is_range_validated(self) -> None:
        self.assertEqual(make_settings().unknown_job_grace_seconds, 1800)
        self.assertEqual(
            make_settings(BATCH_UNKNOWN_JOB_GRACE_SECONDS="0").unknown_job_grace_seconds, 0
        )
        for bad in ("-1", "21601", "soon"):
            with self.subTest(bad=bad), self.assertRaises(ConfigurationError):
                make_settings(BATCH_UNKNOWN_JOB_GRACE_SECONDS=bad)

    def test_zero_grace_quarantines_at_once(self) -> None:
        self.setUp(BATCH_UNKNOWN_JOB_GRACE_SECONDS="0")
        result = self.processor.handle_queue_message(envelope("batch_never").to_json())
        self.assertEqual(result.kind, "quarantined")
        self.assertEqual(self.incoming.sent, [])

    def test_no_deferrals_left_quarantines_instead_of_poisoning(self) -> None:
        message = envelope("batch_never", attempt=MAX_DEFERRALS).to_json()
        self.assertEqual(self.processor.handle_queue_message(message).kind, "quarantined")

    def test_replayed_old_event_is_quarantined_without_waiting(self) -> None:
        self.clock.advance(3 * 3600)  # e.g. moved back from the poison queue hours later
        result = self.processor.handle_queue_message(envelope("batch_never").to_json())
        self.assertEqual(result.kind, "quarantined")


class TableOutageTests(Harness):
    """A short Tables outage is deferred with back-off, not handed to the host (N4)."""

    def tracked(self) -> str:
        batch_id = self.provider.add("in_progress")
        self.provider.set_status(batch_id, "completed")
        self.registry.create_job(JobRecord("eus2", batch_id, "test", raw_status="in_progress"))
        return batch_id

    def test_outage_is_deferred_with_backoff_then_processed_once(self) -> None:
        batch_id = self.tracked()
        self.store.down.add("batchjobs")
        message = envelope(batch_id).to_json()
        delays = []
        with self.assertLogs("foundry_batch", "INFO") as logs:
            for _ in range(3):
                self.assertEqual(self.processor.handle_queue_message(message).kind, "deferred")
                delays.append(self.last_delay())
                message = self.incoming.sent[-1]
        self.assertEqual(delays, [15, 30, 60])
        self.assertEqual(self.deferred().attempt, 3)
        self.assertTrue(any("reason=store_unavailable" in line for line in logs.output))
        self.store.down.clear()
        self.assertEqual(self.processor.handle_queue_message(message).kind, "terminal")
        self.assertEqual(self.job(batch_id).outcome, "completed")

    def test_backoff_doubles_up_to_a_cap_and_spans_tens_of_minutes(self) -> None:
        delays = [store_retry_delay(attempt) for attempt in range(MAX_DEFERRALS)]
        self.assertEqual(delays[:4], [15, 30, 60, 120])
        self.assertEqual(max(delays), STORE_RETRY_MAX_DELAY_SECONDS)
        self.assertGreaterEqual(sum(delays), 45 * 60)

    def test_outage_at_the_deferral_cap_goes_to_the_host(self) -> None:
        batch_id = self.tracked()
        self.store.down.add("batchjobs")
        with self.assertRaises(StoreUnavailableError):
            self.processor.handle_queue_message(envelope(batch_id, attempt=MAX_DEFERRALS).to_json())

    def test_failed_deferral_send_goes_to_the_host(self) -> None:
        processor = EventProcessor(
            self.settings,
            self.registry,
            self.store,
            self.factory,
            self.relay,
            self.clock,
            incoming_queue=FailingQueue(),
        )
        message = envelope(self.tracked()).to_json()
        self.store.down.add("batchjobs")
        with self.assertRaises(QueueUnavailableError):
            processor.handle_queue_message(message)

    def test_quarantine_write_failure_is_deferred_and_not_reported_as_quarantined(self) -> None:
        self.store.down.add("batchquarantine")
        message = envelope("b", event_type="batch.paused").to_json()
        with self.assertLogs("foundry_batch", "INFO") as logs:
            self.assertEqual(self.processor.handle_queue_message(message).kind, "deferred")
        self.assertFalse(any("processor.quarantined" in line for line in logs.output))
        self.store.down.clear()
        with self.assertLogs("foundry_batch", "WARNING") as logs:
            result = self.processor.handle_queue_message(self.incoming.sent[-1])
        self.assertEqual(result.kind, "quarantined")
        self.assertTrue(any("processor.quarantined" in line for line in logs.output))


class QuarantineUtf16Tests(Harness):
    """Quarantine rows stay within Azure's UTF-16 limits whatever the text (N7)."""

    @staticmethod
    def utf16_units(text: str) -> int:
        return len(text.encode("utf-16-le", "surrogatepass")) // 2

    def test_astral_characters_are_truncated_by_utf16_units(self) -> None:
        text = "\U0001f600" * 20_000  # 40,000 UTF-16 units in 20,000 characters
        self.assertEqual(self.processor.handle_queue_message(text).kind, "quarantined")
        [row] = self.quarantine()
        self.assertEqual(self.utf16_units(row["envelope"]), MAX_QUARANTINE_TEXT)
        self.assertEqual(row["envelope"], "\U0001f600" * (MAX_QUARANTINE_TEXT // 2))
        self.assertTrue(row["envelope_truncated"])

    def test_odd_limit_never_splits_a_surrogate_pair(self) -> None:
        text = "x" + "\U0001f600" * 20_000
        self.processor.handle_queue_message(text)
        [row] = self.quarantine()
        self.assertEqual(self.utf16_units(row["envelope"]), MAX_QUARANTINE_TEXT - 1)
        self.assertTrue(row["envelope"].endswith("\U0001f600"))

    def test_astral_batch_id_keeps_keys_and_fields_within_limits(self) -> None:
        self.clock.advance(self.settings.unknown_job_grace_seconds)
        batch_id = "\U0001f600" * 600  # 1,200 UTF-16 units
        result = self.processor.handle_queue_message(envelope(batch_id).to_json())
        self.assertEqual(result.kind, "quarantined")
        [entity] = self.store.query("batchquarantine", Query())
        self.assertLessEqual(self.utf16_units(entity.row_key), 512)
        self.assertLessEqual(self.utf16_units(entity.data["batch_id"]), 1_000)

    def test_short_text_is_kept_whole(self) -> None:
        self.processor.handle_queue_message("\U0001f600 not json")
        [row] = self.quarantine()
        self.assertEqual(row["envelope"], "\U0001f600 not json")
        self.assertFalse(row["envelope_truncated"])

    def test_memory_store_rejects_what_azure_rejects(self) -> None:
        too_long = Entity("p", "r", {"text": "\U0001f600" * 16_385})  # 32,770 units
        with self.assertRaises(ValueError):
            self.store.submit("t", [TxOp("create", too_long)])


if __name__ == "__main__":
    unittest.main()
