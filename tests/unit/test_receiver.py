import json
import unittest

from foundry_batch.adapters.memory import MemoryQueue, MemorySecrets
from foundry_batch.errors import QueueUnavailableError
from foundry_batch.webhook.envelope import Envelope
from foundry_batch.webhook.http import HttpRequest
from foundry_batch.webhook.ratelimit import TokenBucket
from foundry_batch.webhook.receiver import (
    RETRY_BODY,
    UNAUTHORIZED_BODY,
    ReceiverOutcome,
    WebhookReceiver,
)
from tests.support import SECRET_A, SECRET_B, T0, batch_event, clock, make_settings, signed_request


class FailingQueue:
    def send(self, message: str, *, visibility_timeout: int | None = None) -> None:
        raise QueueUnavailableError("down")


def logged_rejection(
    case: unittest.TestCase, receiver: WebhookReceiver, request: HttpRequest
) -> tuple[ReceiverOutcome, str, str]:
    """Process ``request`` and return the outcome with the logged reason and resource key."""
    with case.assertLogs("foundry_batch.webhook.receiver", "WARNING") as logs:
        outcome = receiver.process(request)
    [record] = [r for r in logs.records if getattr(r, "batch_event", "") == "webhook.rejected"]
    return outcome, str(record.reason), str(record.resource_key)


def body(outcome: ReceiverOutcome) -> object:
    return json.loads(outcome.response.body)


class ReceiverTests(unittest.TestCase):
    def setUp(self) -> None:
        self.settings = make_settings()
        self.clock = clock()
        self.queue = MemoryQueue()
        self.secrets = MemorySecrets({"whsec-test-eus2-a": SECRET_A})
        self.receiver = WebhookReceiver(self.settings, self.queue, self.secrets, self.clock)

    def test_tc_b01_valid_event_is_queued_before_200(self) -> None:
        with self.assertLogs("foundry_batch.webhook.receiver", "INFO") as logs:
            outcome = self.receiver.process(signed_request(batch_event("batch_1")))
        self.assertEqual(outcome.response.status, 200)
        self.assertTrue(outcome.enqueued)
        envelope = Envelope.from_json(self.queue.sent[0])
        self.assertEqual(
            (envelope.resource_key, envelope.batch_id, envelope.event_type),
            ("eus2", "batch_1", "batch.completed"),
        )
        self.assertEqual(envelope.received_at, T0)
        self.assertIn("webhook.accepted", logs.output[0])
        self.assertIn("delivery_delay_s=5.0", logs.output[0])

    def test_tc_b02_b03_tampered_or_wrong_secret_is_401_and_not_queued(self) -> None:
        request = signed_request(batch_event("batch_1"))
        tampered = HttpRequest("POST", request.body + b" ", request.headers, request.route_params)
        other = signed_request(batch_event("batch_1"), secret=SECRET_B)
        for name, req in (("tampered", tampered), ("other resource secret", other)):
            with self.subTest(name):
                outcome, reason, _ = logged_rejection(self, self.receiver, req)
                self.assertEqual(outcome.response.status, 401)
                self.assertEqual(body(outcome), UNAUTHORIZED_BODY)
                self.assertEqual(reason, "signature_mismatch")
        self.assertEqual(len(self.queue), 0)

    def test_tc_b04_missing_header_is_400(self) -> None:
        request = signed_request(batch_event("batch_1"))
        headers = dict(request.headers)
        del headers["webhook-signature"]
        outcome = self.receiver.process(
            HttpRequest("POST", request.body, headers, request.route_params)
        )
        self.assertEqual((outcome.response.status, outcome.reason), (400, "missing_headers"))

    def test_tc_b05_b09_old_and_future_timestamps_are_rejected(self) -> None:
        now = int(T0.timestamp())
        for ts in (now - 3600, now + 3600):
            with self.subTest(ts=ts):
                request = signed_request(batch_event("b"), timestamp=ts)
                outcome, reason, _ = logged_rejection(self, self.receiver, request)
                self.assertEqual(outcome.response.status, 401)
                self.assertEqual(body(outcome), UNAUTHORIZED_BODY)
                self.assertEqual(reason, "timestamp_out_of_tolerance")
        self.assertEqual(len(self.queue), 0)

    def test_tc_b06_only_post_is_allowed(self) -> None:
        outcome = self.receiver.process(signed_request(batch_event("b"), method="GET"))
        self.assertEqual(outcome.response.status, 405)
        self.assertEqual(outcome.response.headers["allow"], "POST")

    def test_tc_b07_signed_malformed_json_and_oversized_body(self) -> None:
        outcome = self.receiver.process(signed_request(b"{not json"))
        self.assertEqual((outcome.response.status, outcome.reason), (400, "invalid_json"))
        outcome = self.receiver.process(signed_request(b"[1, 2]"))
        self.assertEqual(outcome.reason, "invalid_json")
        big = signed_request(b"{" + b" " * (self.settings.max_body_bytes + 1) + b"}")
        self.assertEqual(self.receiver.process(big).response.status, 413)
        declared = signed_request(batch_event("b"))
        headers = dict(declared.headers, **{"content-length": "999999"})
        outcome = self.receiver.process(
            HttpRequest("POST", declared.body, headers, declared.route_params)
        )
        self.assertEqual(outcome.reason, "body_too_large")
        self.assertEqual(len(self.queue), 0)

    def test_tc_b08_non_batch_event_is_acknowledged_and_ignored(self) -> None:
        event = {"object": "event", "id": "evt_9", "type": "response.completed", "data": {}}
        outcome = self.receiver.process(signed_request(event))
        self.assertEqual((outcome.response.status, outcome.reason), (200, "ignored"))
        self.assertEqual(len(self.queue), 0)

    def test_tc_b10_rate_limit_returns_429_with_retry_after(self) -> None:
        receiver = WebhookReceiver(
            self.settings,
            self.queue,
            self.secrets,
            self.clock,
            limiter=TokenBucket(1.0, 1, self.clock),
        )
        self.assertEqual(receiver.process(signed_request(batch_event("b"))).response.status, 200)
        limited = receiver.process(signed_request(batch_event("b")))
        self.assertEqual(limited.response.status, 429)
        self.assertEqual(limited.response.headers["retry-after"], "1")
        self.clock.advance(1)
        self.assertEqual(receiver.process(signed_request(batch_event("b"))).response.status, 200)

    def test_unknown_resource_is_answered_like_a_bad_signature(self) -> None:
        known = signed_request(batch_event("b"), secret=SECRET_B)
        unknown = signed_request(batch_event("b"), resource_key="nope")
        known_outcome, known_reason, _ = logged_rejection(self, self.receiver, known)
        outcome, reason, logged_key = logged_rejection(self, self.receiver, unknown)
        self.assertEqual(outcome.response.status, known_outcome.response.status)
        self.assertEqual(outcome.response.body, known_outcome.response.body)
        self.assertEqual(body(outcome), UNAUTHORIZED_BODY)
        self.assertEqual((reason, known_reason), ("unknown_resource", "signature_mismatch"))
        self.assertEqual(logged_key, "unlisted")  # the caller's path segment is never logged
        self.assertEqual(len(self.queue), 0)

    def test_unknown_resource_cannot_be_told_apart_by_headers_size_or_timestamp(self) -> None:
        def variants(resource_key: str) -> list[HttpRequest]:
            signed = signed_request(batch_event("b"), resource_key=resource_key, secret=SECRET_B)
            route = signed.route_params
            unsigned = {k: v for k, v in signed.headers.items() if k != "webhook-signature"}
            bad_timestamp = dict(signed.headers, **{"webhook-timestamp": "x"})
            big = b"{" + b" " * (self.settings.max_body_bytes + 1) + b"}"
            old = int(T0.timestamp()) - 3600
            return [
                HttpRequest("POST", signed.body, unsigned, route),
                HttpRequest("POST", signed.body, bad_timestamp, route),
                HttpRequest("POST", big, signed.headers, route),
                signed_request(batch_event("b"), resource_key=resource_key, timestamp=old),
                HttpRequest("GET", signed.body, signed.headers, route),
            ]

        for known, unknown in zip(variants("eus2"), variants("nope"), strict=True):
            with self.subTest(body_size=len(known.body), headers=sorted(known.headers)):
                expected = self.receiver.process(known).response
                actual = self.receiver.process(unknown).response
                self.assertEqual((actual.status, actual.body), (expected.status, expected.body))
                self.assertNotEqual(actual.status, 200)

    def test_tc_d08_queue_failure_returns_503_and_never_200(self) -> None:
        receiver = WebhookReceiver(self.settings, FailingQueue(), self.secrets, self.clock)
        outcome = receiver.process(signed_request(batch_event("b")))
        self.assertEqual(outcome.response.status, 503)
        self.assertEqual(body(outcome), RETRY_BODY)
        self.assertEqual(outcome.response.headers["retry-after"], "30")
        self.assertFalse(outcome.enqueued)

    def test_no_secret_yet_is_retryable_503(self) -> None:
        receiver = WebhookReceiver(self.settings, self.queue, MemorySecrets(), self.clock)
        outcome, reason, _ = logged_rejection(self, receiver, signed_request(batch_event("b")))
        self.assertEqual(outcome.response.status, 503)
        self.assertEqual(body(outcome), RETRY_BODY)  # the reason stays in logs and metrics
        self.assertEqual(outcome.response.headers["retry-after"], "30")
        self.assertEqual(reason, "no_secret_configured")

    def test_rotation_accepts_both_slots(self) -> None:
        self.secrets.set_secret("whsec-test-eus2-b", SECRET_B, tags={})
        receiver = WebhookReceiver(self.settings, self.queue, self.secrets, self.clock)
        for secret in (SECRET_A, SECRET_B):
            with self.subTest(secret=secret[:10]):
                request = signed_request(batch_event("b"), secret=secret)
                self.assertEqual(receiver.process(request).response.status, 200)

    def test_new_secret_is_picked_up_by_a_throttled_refresh(self) -> None:
        self.assertEqual(
            self.receiver.process(signed_request(batch_event("b"))).response.status, 200
        )
        self.secrets.set_secret("whsec-test-eus2-b", SECRET_B, tags={})
        reads_before = self.secrets.reads
        # Within the refresh window the cache is not reloaded on a mismatch.
        self.assertEqual(
            self.receiver.process(
                signed_request(batch_event("b"), secret=SECRET_B)
            ).response.status,
            401,
        )
        self.assertEqual(self.secrets.reads, reads_before)
        self.clock.advance(31)
        self.assertEqual(
            self.receiver.process(
                signed_request(batch_event("b"), secret=SECRET_B)
            ).response.status,
            200,
        )

    def test_refresh_that_still_fails_is_401(self) -> None:
        self.clock.advance(31)
        self.secrets.set_secret("whsec-test-eus2-b", SECRET_B, tags={})
        self.receiver = WebhookReceiver(self.settings, self.queue, self.secrets, self.clock)
        self.receiver.process(signed_request(batch_event("b")))  # warm cache
        self.clock.advance(31)
        self.secrets.set_secret("whsec-test-eus2-a", "whsec_" + "Zm9v" * 8, tags={})
        outcome = self.receiver.process(
            signed_request(batch_event("b"), secret="whsec_" + "YmFy" * 8)
        )
        self.assertEqual(outcome.response.status, 401)

    def test_logs_never_contain_secret_or_body(self) -> None:
        with self.assertLogs("foundry_batch", "DEBUG") as logs:
            self.receiver.process(signed_request(batch_event("batch_secret_body_marker")))
            self.receiver.process(signed_request(batch_event("b"), secret=SECRET_B))
        text = "\n".join(logs.output)
        self.assertNotIn(SECRET_A, text)
        self.assertNotIn(SECRET_B, text)
        self.assertNotIn('"data"', text)

    def test_response_body_is_json(self) -> None:
        outcome = self.receiver.process(signed_request(batch_event("b")))
        self.assertEqual(json.loads(outcome.response.body), {"status": "accepted"})
        self.assertEqual(self.receiver.handle(signed_request(batch_event("b"))).status, 200)


if __name__ == "__main__":
    unittest.main()
