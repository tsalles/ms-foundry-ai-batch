import json
import random
import threading
import unittest
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import ClassVar

from foundry_batch.adapters.memory import (
    MemoryQueue,
    MemoryQueueAdmin,
    MemoryTableStore,
    RecordingDownstream,
)
from foundry_batch.domain.contract import NormalizedEvent
from foundry_batch.domain.models import JobRecord
from foundry_batch.errors import ConcurrencyConflictError
from foundry_batch.ports import DeliveryResult
from foundry_batch.processing.registry import JobRegistry, OutboxItem
from foundry_batch.relay.backoff import MAX_RETRY_AFTER_SECONDS, backoff_delay, parse_retry_after
from foundry_batch.relay.demo_api import DemoDownstreamApi
from foundry_batch.relay.dispatcher import OutboxDispatcher
from foundry_batch.relay.http_downstream import HttpDownstream
from foundry_batch.relay.replay import ReplayService, replay_poison_queue
from foundry_batch.webhook.http import HttpRequest
from foundry_batch.webhook.signature import sign_headers, verify
from tests.support import RELAY_SECRET, T0, clock, make_settings


class BackoffTests(unittest.TestCase):
    def test_full_jitter_is_bounded_by_the_exponential_ceiling(self) -> None:
        rng = random.Random(1)
        for n in range(1, 10):
            ceiling = min(60, 5 * 2 ** (n - 1))
            samples = [
                backoff_delay(n, base_seconds=5, max_seconds=60, rng=rng) for _ in range(400)
            ]
            self.assertTrue(all(0 <= d <= ceiling for d in samples), n)
            # Full jitter spans the whole range, not just its upper half.
            self.assertLess(min(samples), ceiling * 0.1, n)
            self.assertGreater(max(samples), ceiling * 0.9, n)
        self.assertLessEqual(backoff_delay(10_000, base_seconds=5, max_seconds=60, rng=rng), 60)
        with self.assertRaises(ValueError):
            backoff_delay(0, base_seconds=1, max_seconds=1)

    def test_retry_after_is_a_floor_even_above_the_maximum(self) -> None:
        rng = random.Random(2)
        self.assertGreaterEqual(
            backoff_delay(1, base_seconds=5, max_seconds=60, rng=rng, retry_after_seconds=45), 45
        )
        self.assertEqual(
            backoff_delay(1, base_seconds=5, max_seconds=60, rng=rng, retry_after_seconds=999), 999
        )
        self.assertEqual(
            backoff_delay(1, base_seconds=5, max_seconds=60, rng=rng, retry_after_seconds=10**9),
            MAX_RETRY_AFTER_SECONDS,
        )
        self.assertLessEqual(
            backoff_delay(1, base_seconds=5, max_seconds=60, rng=rng, retry_after_seconds=0), 5
        )

    def test_retry_after_parsing(self) -> None:
        self.assertEqual(parse_retry_after("12"), 12)
        self.assertEqual(
            parse_retry_after(format_datetime(T0 + timedelta(seconds=30), usegmt=True), now=T0), 30
        )
        self.assertEqual(parse_retry_after("Wed, 21 Oct 2015 07:28:00", now=T0), 0)
        self.assertIsNone(parse_retry_after("soon"))
        self.assertIsNone(parse_retry_after(None))
        self.assertEqual(
            parse_retry_after(
                "Wed, 21 Oct 2015 07:28:30 GMT", now=datetime(2015, 10, 21, 7, 28, tzinfo=UTC)
            ),
            30,
        )

    def test_retry_after_accepts_only_ascii_digits_and_caps_absurd_values(self) -> None:
        for odd in ("\u00b2", "\u0661\u0662", "12.5", "-5", "1e3"):
            with self.subTest(value=odd):
                self.assertIsNone(parse_retry_after(odd))
        self.assertEqual(parse_retry_after("9" * 400), MAX_RETRY_AFTER_SECONDS)
        just_over = str(int(MAX_RETRY_AFTER_SECONDS) + 1)
        self.assertEqual(parse_retry_after(just_over), MAX_RETRY_AFTER_SECONDS)
        far = format_datetime(T0 + timedelta(days=30), usegmt=True)
        self.assertEqual(parse_retry_after(far, now=T0), MAX_RETRY_AFTER_SECONDS)


class DispatcherTests(unittest.TestCase):
    def setUp(self) -> None:
        self.settings = make_settings(
            BATCH_OUTBOX_MAX_ATTEMPTS="3", BATCH_OUTBOX_RETRY_HORIZON_SECONDS="0"
        )
        self.clock = clock()
        self.store = MemoryTableStore()
        self.registry = JobRegistry(self.store, self.settings, self.clock)
        self.downstream = RecordingDownstream()
        self.dispatcher = OutboxDispatcher(
            self.settings, self.registry, self.downstream, self.clock, rng=random.Random(7)
        )
        job = JobRecord("eus2", "batch_1", "test").with_status("completed")
        event = NormalizedEvent.from_job(job, source="webhook", emitted_at=T0)
        self.key = event.idempotency_key
        self.registry.create_job(JobRecord("eus2", "batch_1", "test"))
        stored = self.registry.load(job.key)
        assert stored is not None
        self.registry.commit(
            job,
            etag=stored.etag,
            outbox=OutboxItem(
                self.key, json.dumps(event.payload), "webhook", T0, next_attempt_at=T0
            ),
        )
        self.pk = self.registry.partition(job.key)

    def item(self) -> OutboxItem:
        stored = self.registry.load_outbox(self.pk, self.key)
        assert stored is not None
        return stored.item

    def nudge(self) -> str:
        return json.dumps({"pk": self.pk, "key": self.key})

    def test_delivered_only_after_2xx(self) -> None:
        result = self.dispatcher.dispatch_nudge(self.nudge())
        self.assertEqual(result.status, "delivered")
        self.assertEqual(self.item().status, "delivered")
        self.assertEqual(self.dispatcher.dispatch_nudge(self.nudge()).status, "skipped")
        self.assertEqual(len(self.downstream.attempts), 1)

    def test_tc_d05_outage_keeps_item_then_delivers_with_same_key(self) -> None:
        self.downstream.script = [
            DeliveryResult(None, error="timeout"),
            DeliveryResult(503, retry_after_seconds=120),
        ]
        self.assertEqual(self.dispatcher.dispatch_nudge(self.nudge()).status, "retry_scheduled")
        self.assertEqual(self.dispatcher.dispatch_due(), [])  # not due yet
        self.clock.advance(10)
        self.assertEqual([r.status for r in self.dispatcher.dispatch_due()], ["retry_scheduled"])
        self.assertGreaterEqual(
            (self.item().next_attempt_at - self.clock.now()).total_seconds(), 120
        )
        self.clock.advance(121)
        self.assertEqual([r.status for r in self.dispatcher.dispatch_due()], ["delivered"])
        self.assertEqual(self.downstream.distinct_keys, {self.key})
        self.assertEqual(len(self.downstream.attempts), 3)

    def test_permanent_4xx_is_quarantined(self) -> None:
        self.downstream.script = [DeliveryResult(400, error="http_400")]
        with self.assertLogs("foundry_batch", "ERROR"):
            self.assertEqual(self.dispatcher.dispatch_nudge(self.nudge()).status, "quarantined")
        self.assertEqual(self.item().status, "quarantined")

    def test_attempts_exhausted_is_quarantined(self) -> None:
        self.downstream.default = DeliveryResult(500)
        statuses = []
        for _ in range(3):
            statuses.append(self.dispatcher.dispatch_nudge(self.nudge()).status)
            self.clock.advance(3600)
        self.assertEqual(statuses, ["retry_scheduled", "retry_scheduled", "quarantined"])

    def test_transient_failures_are_retried_until_the_horizon(self) -> None:
        settings = make_settings(
            BATCH_OUTBOX_MAX_ATTEMPTS="3", BATCH_OUTBOX_RETRY_HORIZON_SECONDS="7200"
        )
        dispatcher = OutboxDispatcher(
            settings, self.registry, self.downstream, self.clock, rng=random.Random(7)
        )
        self.downstream.default = DeliveryResult(503)
        statuses = []
        for _ in range(4):  # more attempts than the cap, still inside two hours
            statuses.append(dispatcher.dispatch_nudge(self.nudge()).status)
            self.clock.advance(1200)
        self.assertEqual(statuses, ["retry_scheduled"] * 4)
        self.clock.advance(3600)  # now past the horizon
        with self.assertLogs("foundry_batch", "ERROR"):
            self.assertEqual(dispatcher.dispatch_nudge(self.nudge()).status, "quarantined")

    def test_quarantine_starts_exactly_at_the_horizon(self) -> None:
        settings = make_settings(
            BATCH_OUTBOX_MAX_ATTEMPTS="1", BATCH_OUTBOX_RETRY_HORIZON_SECONDS="600"
        )
        dispatcher = OutboxDispatcher(
            settings, self.registry, self.downstream, self.clock, rng=random.Random(7)
        )
        self.downstream.default = DeliveryResult(503)
        self.clock.set(T0 + timedelta(seconds=600))  # first attempt, item exactly 600 s old
        with self.assertLogs("foundry_batch", "ERROR"):
            self.assertEqual(dispatcher.dispatch_nudge(self.nudge()).status, "quarantined")

    def test_tc_d05_one_hour_outage_recovers_without_replay_by_default(self) -> None:
        dispatcher = OutboxDispatcher(
            make_settings(), self.registry, self.downstream, self.clock, rng=random.Random(3)
        )
        self.downstream.default = DeliveryResult(503)
        self.assertEqual(dispatcher.dispatch_nudge(self.nudge()).status, "retry_scheduled")
        for _ in range(60):  # one hour of the one-minute outbox timer
            self.clock.advance(60)
            for result in dispatcher.dispatch_due():
                self.assertNotEqual(result.status, "quarantined")
        self.downstream.default = DeliveryResult(201)
        self.clock.advance(900)  # the longest capped backoff
        self.assertEqual([r.status for r in dispatcher.dispatch_due()], ["delivered"])
        self.assertEqual(self.downstream.distinct_keys, {self.key})

    def test_claim_prevents_parallel_sends(self) -> None:
        stored = self.registry.load_outbox(self.pk, self.key)
        assert stored is not None
        self.dispatcher.dispatch(stored)  # delivered, ETag changed
        # A second worker holding the stale copy loses the ETag claim and sends nothing.
        self.assertEqual(self.dispatcher.dispatch(stored).status, "conflict")
        self.assertEqual(len(self.downstream.attempts), 1)
        fresh = self.registry.load_outbox(self.pk, self.key)
        assert fresh is not None

        class Raced(JobRegistry):
            def save_outbox(self, *args: object, **kw: object) -> str:  # type: ignore[override]
                raise ConcurrencyConflictError("x")

        raced = OutboxDispatcher(
            self.settings, Raced(self.store, self.settings, self.clock), self.downstream, self.clock
        )
        pending = fresh.__class__(
            fresh.partition_key,
            fresh.item.__class__(**{**fresh.item.__dict__, "status": "pending"}),
            fresh.etag,
        )
        self.assertEqual(raced.dispatch(pending).status, "conflict")

    def test_bad_and_unknown_nudges(self) -> None:
        with self.assertLogs("foundry_batch", "WARNING"):
            self.assertEqual(self.dispatcher.dispatch_nudge("{bad").status, "skipped")
        self.assertEqual(
            self.dispatcher.dispatch_nudge(json.dumps({"pk": "x", "key": "y"})).status, "not_found"
        )

    def test_future_item_is_not_sent(self) -> None:
        stored = self.registry.load_outbox(self.pk, self.key)
        assert stored is not None
        future = stored.__class__(
            stored.partition_key,
            stored.item.__class__(
                **{**stored.item.__dict__, "next_attempt_at": T0 + timedelta(hours=1)}
            ),
            stored.etag,
        )
        self.assertEqual(self.dispatcher.dispatch(future).status, "skipped")

    def test_save_conflict_after_send_is_logged_not_raised(self) -> None:
        calls = {"n": 0}

        class SecondSaveConflicts(JobRegistry):
            def save_outbox(self, stored, item):  # type: ignore[no-untyped-def,override]
                calls["n"] += 1
                if calls["n"] == 2:
                    raise ConcurrencyConflictError("operator replay")
                return super().save_outbox(stored, item)

        dispatcher = OutboxDispatcher(
            self.settings,
            SecondSaveConflicts(self.store, self.settings, self.clock),
            self.downstream,
            self.clock,
        )
        with self.assertLogs("foundry_batch", "WARNING"):
            self.assertEqual(dispatcher.dispatch_nudge(self.nudge()).status, "delivered")

    def test_tc_d06_replay_rearms_quarantined_items_with_audit(self) -> None:
        self.downstream.script = [DeliveryResult(400)]
        self.dispatcher.dispatch_nudge(self.nudge())
        service = ReplayService(self.registry, self.clock)
        with self.assertRaises(ValueError):
            service.replay_outbox(operator="", reason="x")
        with self.assertRaises(ValueError):
            service.replay_outbox(operator="op", reason="x", status="pending")
        dry = service.replay_outbox(operator="operator", reason="fixed auth", dry_run=True)
        self.assertEqual((dry.selected, dry.rearmed), (1, 0))
        with self.assertLogs("foundry_batch", "WARNING") as logs:
            report = service.replay_outbox(
                operator="operator",
                reason="fixed auth",
                since=T0 - timedelta(hours=1),
                dry_run=False,
            )
        self.assertEqual(report.rearmed, 1)
        self.assertIn("operator=operator", logs.output[0])
        self.assertEqual(self.item().replay_count, 1)
        self.assertEqual(self.dispatcher.dispatch_nudge(self.nudge()).status, "delivered")
        self.assertEqual(self.downstream.distinct_keys, {self.key})

    def test_poison_queue_replay(self) -> None:
        source, poison = MemoryQueue("q"), MemoryQueue("q-poison")
        poison.send("m1")
        poison.send("m2")
        admin = MemoryQueueAdmin({"q": source, "q-poison": poison})
        with self.assertRaises(ValueError):
            replay_poison_queue(
                admin, poison_queue="q-poison", target_queue="q", operator="", reason=""
            )
        dry = replay_poison_queue(
            admin, poison_queue="q-poison", target_queue="q", operator="op", reason="r"
        )
        self.assertEqual((dry.selected, dry.moved, dry.dry_run), (2, 0, True))
        # The dry run peeked: the messages stay visible for the --execute run right after it.
        report = replay_poison_queue(
            admin,
            poison_queue="q-poison",
            target_queue="q",
            operator="op",
            reason="r",
            dry_run=False,
        )
        self.assertEqual((report.selected, report.moved), (2, 2))
        self.assertEqual(source.drain(), ["m1", "m2"])
        self.assertEqual(admin.approximate_depth("q-poison"), 0)


class _Handler(BaseHTTPRequestHandler):
    responses: ClassVar[list[tuple[int, dict[str, str]]]] = []
    received: ClassVar[list[tuple[dict[str, str], bytes]]] = []

    def do_POST(self) -> None:
        length = int(self.headers.get("content-length", 0))
        body = self.rfile.read(length)
        type(self).received.append(({k.lower(): v for k, v in self.headers.items()}, body))
        status, headers = type(self).responses.pop(0) if type(self).responses else (200, {})
        self.send_response(status)
        for k, v in headers.items():
            self.send_header(k, v)
        self.send_header("content-length", "0")
        self.end_headers()

    def log_message(self, *args: object) -> None:
        return


class HttpDownstreamTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        threading.Thread(target=cls.server.serve_forever, daemon=True).start()
        cls.url = f"http://127.0.0.1:{cls.server.server_address[1]}/events"

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.shutdown()
        cls.server.server_close()

    def setUp(self) -> None:
        _Handler.responses, _Handler.received = [], []
        self.clock = clock()

    def test_signed_delivery_with_idempotency_key(self) -> None:
        client = HttpDownstream(self.url, lambda: RELAY_SECRET, self.clock)
        result = client.deliver(b'{"a":1}', idempotency_key="key-1")
        self.assertTrue(result.ok)
        headers, body = _Handler.received[0]
        self.assertEqual(headers["idempotency-key"], "key-1")
        verified = verify(body, headers, [RELAY_SECRET], now=T0)
        self.assertEqual(verified.webhook_id, "key-1")

    def test_status_mapping_and_retry_after(self) -> None:
        _Handler.responses = [(429, {"retry-after": "7"}), (404, {})]
        client = HttpDownstream(self.url, lambda: RELAY_SECRET, self.clock)
        throttled = client.deliver(b"{}", idempotency_key="k")
        self.assertEqual((throttled.status_code, throttled.retry_after_seconds), (429, 7))
        self.assertTrue(throttled.retryable)
        missing = client.deliver(b"{}", idempotency_key="k")
        self.assertFalse(missing.retryable)
        self.assertEqual(missing.error, "http_404")

    def test_network_failures_and_missing_secret_are_retryable(self) -> None:
        self.assertEqual(
            HttpDownstream(self.url, lambda: None, self.clock)
            .deliver(b"{}", idempotency_key="k")
            .error,
            "relay_secret_missing",
        )
        closed = HttpDownstream(
            "http://127.0.0.1:9/x", lambda: RELAY_SECRET, self.clock, timeout_seconds=(0.2, 0.2)
        )
        result = closed.deliver(b"{}", idempotency_key="k")
        self.assertIsNone(result.status_code)
        self.assertTrue(result.retryable)
        with self.assertRaises(ValueError):
            HttpDownstream("http://insecure.example", lambda: RELAY_SECRET, self.clock)

    def test_timeout_is_reported(self) -> None:
        import requests

        class SlowSession(requests.Session):
            def post(self, *a, **k):  # type: ignore[no-untyped-def,override]
                raise requests.Timeout("slow")

        client = HttpDownstream(self.url, lambda: RELAY_SECRET, self.clock, session=SlowSession())
        self.assertEqual(client.deliver(b"{}", idempotency_key="k").error, "timeout")


class DemoApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = clock()
        self.store = MemoryTableStore()
        self.api = DemoDownstreamApi(self.store, "demoreceipts", lambda: RELAY_SECRET, self.clock)
        job = JobRecord("eus2", "batch_1", "test").with_status("completed")
        self.event = NormalizedEvent.from_job(job, source="webhook", emitted_at=T0)

    def request(
        self,
        body: bytes | None = None,
        key: str | None = None,
        secret: str = RELAY_SECRET,
        method: str = "POST",
    ) -> HttpRequest:
        body = json.dumps(self.event.payload).encode() if body is None else body
        headers = sign_headers(secret, key or self.event.idempotency_key, int(T0.timestamp()), body)
        return HttpRequest(method, body, headers)

    def test_accepts_once_then_reports_duplicate(self) -> None:
        self.assertEqual(self.api.handle(self.request()).status, 201)
        duplicate = self.api.handle(self.request())
        self.assertEqual(
            (duplicate.status, json.loads(duplicate.body)["status"]), (200, "duplicate")
        )
        [row] = self.store.rows("demoreceipts")
        self.assertNotIn("event_json", row.data)

    def test_tc_b10_rejects_unauthenticated_and_invalid_calls(self) -> None:
        other = "whsec_" + "b3RoZXJzZWNyZXRvdGhlcnNlY3JldG90aGVyc2VjcmV0"
        self.assertEqual(self.api.handle(self.request(secret=other)).status, 401)
        self.assertEqual(self.api.handle(self.request(method="GET")).status, 405)
        self.assertEqual(self.api.handle(self.request(b"{x")).status, 400)
        self.assertEqual(self.api.handle(self.request(b'{"a":1}')).status, 422)
        self.assertEqual(self.api.handle(self.request(key="other")).status, 422)
        self.assertEqual(self.api.handle(self.request(b" " * 60_000)).status, 413)
        not_ready = DemoDownstreamApi(self.store, "demoreceipts", lambda: None, self.clock)
        self.assertEqual(not_ready.handle(self.request()).status, 503)


if __name__ == "__main__":
    unittest.main()
