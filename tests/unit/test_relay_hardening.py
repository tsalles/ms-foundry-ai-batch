"""Relay and replay edge cases found in the code review."""

import json
import random
import threading
import unittest
from datetime import timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import ClassVar

from foundry_batch.adapters.memory import MemoryTableStore, RecordingDownstream
from foundry_batch.domain.contract import NormalizedEvent
from foundry_batch.domain.models import JobRecord
from foundry_batch.errors import StoreUnavailableError
from foundry_batch.ports import DeliveryResult
from foundry_batch.processing.registry import (
    OUTBOX_DELIVERED,
    OUTBOX_QUARANTINED,
    JobRegistry,
    OutboxItem,
    StoredOutbox,
)
from foundry_batch.relay.dispatcher import DispatchResult, OutboxDispatcher
from foundry_batch.relay.http_downstream import HttpDownstream
from foundry_batch.relay.replay import ReplayService
from tests.support import RELAY_SECRET, T0, clock, make_settings


class _Redirecting(BaseHTTPRequestHandler):
    seen: ClassVar[list[tuple[str, str]]] = []

    def do_POST(self) -> None:
        type(self).seen.append(("POST", self.path))
        self.rfile.read(int(self.headers.get("content-length", 0) or 0))
        self.send_response(302)
        self.send_header("location", "/login")
        self.send_header("content-length", "0")
        self.end_headers()

    def do_GET(self) -> None:
        type(self).seen.append(("GET", self.path))
        self.send_response(200)
        self.send_header("content-length", "2")
        self.end_headers()
        self.wfile.write(b"ok")

    def log_message(self, *args: object) -> None:
        return


class HttpDownstreamTests(unittest.TestCase):
    def test_redirect_is_not_followed_and_not_delivered(self) -> None:
        server = ThreadingHTTPServer(("127.0.0.1", 0), _Redirecting)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        url = f"http://127.0.0.1:{server.server_address[1]}/events"
        result = HttpDownstream(url, lambda: RELAY_SECRET, clock()).deliver(
            b"{}", idempotency_key="k1"
        )
        self.assertEqual(result.status_code, 302)
        self.assertFalse(result.ok)
        self.assertFalse(result.retryable)  # misconfiguration: quarantine, do not loop
        self.assertEqual(result.error, "http_302")
        self.assertEqual(_Redirecting.seen, [("POST", "/events")])

    def test_secret_provider_failure_is_a_retryable_result(self) -> None:
        def broken() -> str:
            raise StoreUnavailableError("kv down")

        result = HttpDownstream("https://example.invalid/e", broken, clock()).deliver(
            b"{}", idempotency_key="k1"
        )
        self.assertIsNone(result.status_code)
        self.assertTrue(result.retryable)
        self.assertEqual(result.error, "relay_secret_StoreUnavailableError")

    def test_retry_classification(self) -> None:
        for code, retryable in (
            (408, True),
            (425, True),
            (429, True),
            (500, True),
            (503, True),
            (301, False),
            (400, False),
            (401, False),
            (409, True),  # in-flight duplicate (Idempotency-Key draft): retry, the key is stable
            (410, False),
            (422, False),
            (501, True),
            (507, True),
        ):
            with self.subTest(code=code):
                self.assertEqual(DeliveryResult(code).retryable, retryable)
                self.assertFalse(DeliveryResult(code).ok)
        self.assertTrue(DeliveryResult(204).ok)


class RaisingDownstream:
    def __init__(self) -> None:
        self.calls = 0

    def deliver(self, body: bytes, *, idempotency_key: str) -> DeliveryResult:
        self.calls += 1
        raise RuntimeError("bug in the downstream adapter")


class _OutboxHarness(unittest.TestCase):
    def setUp(self) -> None:
        self.settings = make_settings(
            BATCH_OUTBOX_MAX_ATTEMPTS="3", BATCH_OUTBOX_RETRY_HORIZON_SECONDS="0"
        )
        self.clock = clock()
        self.store = MemoryTableStore()
        self.registry = JobRegistry(self.store, self.settings, self.clock)

    def add_item(self, batch_id: str) -> tuple[str, str]:
        job = JobRecord("eus2", batch_id, "test").with_status("completed")
        event = NormalizedEvent.from_job(job, source="webhook", emitted_at=T0)
        self.registry.create_job(JobRecord("eus2", batch_id, "test"))
        stored = self.registry.load(job.key)
        assert stored is not None
        self.registry.commit(
            job,
            etag=stored.etag,
            outbox=OutboxItem(
                event.idempotency_key, json.dumps(event.payload), "webhook", T0, next_attempt_at=T0
            ),
        )
        return self.registry.partition(job.key), event.idempotency_key

    def item(self, pk: str, key: str) -> OutboxItem:
        stored = self.registry.load_outbox(pk, key)
        assert stored is not None
        return stored.item

    def dispatcher(self, downstream: object) -> OutboxDispatcher:
        return OutboxDispatcher(
            self.settings,
            self.registry,
            downstream,  # type: ignore[arg-type]
            self.clock,
            rng=random.Random(3),
        )


class DispatcherHardeningTests(_OutboxHarness):
    def test_adapter_exception_keeps_attempt_accounting_and_reaches_quarantine(self) -> None:
        pk, key = self.add_item("batch_1")
        downstream = RaisingDownstream()
        dispatcher = self.dispatcher(downstream)
        results = []
        for _ in range(5):
            results += [r.status for r in dispatcher.dispatch_due()]
            self.clock.advance(3600)
        self.assertEqual(results, ["retry_scheduled", "retry_scheduled", "quarantined"])
        self.assertEqual(self.item(pk, key).attempts, 3)
        self.assertEqual(self.item(pk, key).last_error, "deliver_RuntimeError")
        self.assertEqual(downstream.calls, 3)

    def test_one_failing_item_does_not_stop_the_timer_batch(self) -> None:
        first = self.add_item("batch_1")
        second = self.add_item("batch_2")
        downstream = RecordingDownstream()
        dispatcher = self.dispatcher(downstream)
        original = dispatcher.dispatch

        def flaky(stored: StoredOutbox) -> DispatchResult:
            if stored.item.idempotency_key == first[1]:
                raise StoreUnavailableError("table timeout")
            return original(stored)

        dispatcher.dispatch = flaky  # type: ignore[method-assign]
        with self.assertLogs("foundry_batch.relay.dispatcher", "ERROR") as logs:
            statuses = sorted(r.status for r in dispatcher.dispatch_due())
        self.assertEqual(statuses, ["delivered", "error"])
        self.assertEqual(self.item(*second).status, OUTBOX_DELIVERED)
        self.assertIn("error_type=StoreUnavailableError", logs.output[0])

    def test_crashed_claim_expires_and_another_worker_delivers(self) -> None:
        pk, key = self.add_item("batch_1")
        stored = self.registry.load_outbox(pk, key)
        assert stored is not None
        # A worker claimed the item (attempt 1, +60 s) and died before sending.
        claimed = OutboxItem(
            **{**stored.item.__dict__, "attempts": 1, "next_attempt_at": T0 + timedelta(seconds=60)}
        )
        self.registry.save_outbox(stored, claimed)
        downstream = RecordingDownstream()
        dispatcher = self.dispatcher(downstream)
        self.assertEqual(dispatcher.dispatch_due(), [])
        self.clock.advance(61)
        self.assertEqual([r.status for r in dispatcher.dispatch_due()], ["delivered"])
        self.assertEqual(self.item(pk, key).attempts, 2)
        self.assertEqual(len(downstream.attempts), 1)

    def test_two_workers_with_the_same_snapshot_send_once(self) -> None:
        pk, key = self.add_item("batch_1")
        stored = self.registry.load_outbox(pk, key)
        assert stored is not None
        downstream = RecordingDownstream()
        first, second = self.dispatcher(downstream), self.dispatcher(downstream)
        self.assertEqual(first.dispatch(stored).status, "delivered")
        self.assertEqual(second.dispatch(stored).status, "conflict")
        self.assertEqual(len(downstream.attempts), 1)

    def test_permanent_3xx_is_quarantined_without_retry(self) -> None:
        pk, key = self.add_item("batch_1")
        downstream = RecordingDownstream(script=[DeliveryResult(302, error="http_302")])
        self.assertEqual(self.dispatcher(downstream).dispatch_due()[0].status, "quarantined")
        self.assertEqual(self.item(pk, key).status, OUTBOX_QUARANTINED)
        self.assertEqual(self.item(pk, key).last_status_code, 302)

    def test_retry_after_from_downstream_is_honoured(self) -> None:
        pk, key = self.add_item("batch_1")
        downstream = RecordingDownstream(script=[DeliveryResult(429, retry_after_seconds=1800)])
        self.dispatcher(downstream).dispatch_due()
        due = self.item(pk, key).next_attempt_at
        assert due is not None
        self.assertGreaterEqual((due - T0).total_seconds(), 1800)


class ReplayHardeningTests(_OutboxHarness):
    def deliver_all(self) -> None:
        self.dispatcher(RecordingDownstream()).dispatch_due(limit=5000)

    def test_delivered_items_are_selected_by_delivery_time(self) -> None:
        old = self.add_item("batch_old")  # created at T0, delivered later
        self.clock.advance(7200)
        self.deliver_all()
        self.assertEqual(self.item(*old).status, OUTBOX_DELIVERED)
        report = ReplayService(self.registry, self.clock).replay_outbox(
            operator="op",
            reason="downstream lost a day",
            status=OUTBOX_DELIVERED,
            since=T0 + timedelta(hours=1),
            until=T0 + timedelta(hours=3),
            dry_run=False,
        )
        self.assertEqual((report.selected, report.rearmed), (1, 1))
        self.assertEqual(self.item(*old).replay_count, 1)
        self.assertEqual(self.item(*old).attempts, 0)

    def test_every_page_is_read_and_large_selections_are_refused(self) -> None:
        for n in range(1001):
            self.add_item(f"batch_{n:04d}")
        self.deliver_all()
        self.clock.advance(3600)
        recent = self.add_item("batch_zzzz_recent")
        self.deliver_all()
        service = ReplayService(self.registry, self.clock)
        report = service.replay_outbox(
            operator="op",
            reason="r",
            status=OUTBOX_DELIVERED,
            since=self.clock.now() - timedelta(minutes=5),
        )
        self.assertEqual(report.selected, 1)
        self.assertEqual(self.item(*recent).status, OUTBOX_DELIVERED)  # dry run
        with self.assertRaisesRegex(ValueError, "narrow the window"):
            service.replay_outbox(operator="op", reason="r", status=OUTBOX_DELIVERED)

    def test_naive_bounds_and_bad_requests_are_rejected(self) -> None:
        service = ReplayService(self.registry, self.clock)
        with self.assertRaises(ValueError):
            service.replay_outbox(operator="op", reason="r", since=T0.replace(tzinfo=None))
        with self.assertRaises(ValueError):
            service.replay_outbox(operator="op", reason="r", status="pending")
        with self.assertRaises(ValueError):
            service.replay_outbox(operator=" ", reason="r")


if __name__ == "__main__":
    unittest.main()
