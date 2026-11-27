"""Receiver edge cases found in the code review: ordering, configuration faults, limits."""

import json
import unittest

from foundry_batch.adapters.memory import MemoryQueue, MemorySecrets
from foundry_batch.webhook.envelope import Envelope
from foundry_batch.webhook.http import HttpRequest
from foundry_batch.webhook.ratelimit import TokenBucket
from foundry_batch.webhook.receiver import (
    MAX_ENVELOPE_BYTES,
    RETRY_BODY,
    UNAUTHORIZED_BODY,
    WebhookReceiver,
)
from foundry_batch.webhook.secrets import SigningSecretResolver
from tests.support import SECRET_A, SECRET_B, batch_event, clock, make_settings, signed_request
from tests.unit.test_receiver import body, logged_rejection


class ReceiverHardeningTests(unittest.TestCase):
    def setUp(self) -> None:
        self.settings = make_settings()
        self.clock = clock()
        self.queue = MemoryQueue()
        self.secrets = MemorySecrets({"whsec-test-eus2-a": SECRET_A})

    def receiver(self, **kwargs: object) -> WebhookReceiver:
        return WebhookReceiver(self.settings, self.queue, self.secrets, self.clock, **kwargs)  # type: ignore[arg-type]

    def test_unauthenticated_traffic_cannot_use_up_the_rate_limit(self) -> None:
        receiver = self.receiver(limiter=TokenBucket(1.0, 1, self.clock))
        forged = signed_request(batch_event("batch_1"), secret=SECRET_B)
        for _ in range(20):
            self.assertEqual(receiver.process(forged).response.status, 401)
        genuine = receiver.process(signed_request(batch_event("batch_1")))
        self.assertEqual(genuine.response.status, 200)
        limited = receiver.process(signed_request(batch_event("batch_1"), webhook_id="msg_2"))
        self.assertEqual(limited.response.status, 429)
        self.assertEqual(limited.response.headers["retry-after"], "1")
        self.assertEqual(len(self.queue), 1)

    def test_non_ascii_signature_is_a_401_not_a_crash(self) -> None:
        request = signed_request(batch_event("batch_1"))
        headers = dict(request.headers)
        headers["webhook-signature"] = "v1,\u00e9t\u00e9"
        outcome, reason, _ = logged_rejection(
            self, self.receiver(), HttpRequest("POST", request.body, headers, request.route_params)
        )
        self.assertEqual((outcome.response.status, reason), (401, "signature_mismatch"))
        self.assertEqual(body(outcome), UNAUTHORIZED_BODY)

    def test_malformed_slot_does_not_block_the_valid_slot(self) -> None:
        self.secrets.set_secret("whsec-test-eus2-a", "whsec_!!not-base64!!", tags={})
        self.secrets.set_secret("whsec-test-eus2-b", SECRET_B, tags={})
        outcome = self.receiver().process(signed_request(batch_event("batch_1"), secret=SECRET_B))
        self.assertEqual(outcome.response.status, 200)

    def test_only_malformed_secrets_is_our_fault_503(self) -> None:
        self.secrets.set_secret("whsec-test-eus2-a", "whsec_!!not-base64!!", tags={})
        request = signed_request(batch_event("batch_1"))
        outcome, reason, _ = logged_rejection(self, self.receiver(), request)
        self.assertEqual((outcome.response.status, reason), (503, "secret_malformed"))
        self.assertEqual(body(outcome), RETRY_BODY)  # no configuration detail for callers
        self.assertEqual(outcome.response.headers["retry-after"], "30")
        self.assertEqual(len(self.queue), 0)

    def test_secret_store_outage_without_cache_is_503(self) -> None:
        self.secrets.fail_reads = True
        request = signed_request(batch_event("batch_1"))
        outcome, reason, _ = logged_rejection(self, self.receiver(), request)
        self.assertEqual((outcome.response.status, reason), (503, "secret_store_unavailable"))
        self.assertEqual(body(outcome), RETRY_BODY)
        self.assertEqual(outcome.response.headers["retry-after"], "30")
        self.assertEqual(len(self.queue), 0)

    def test_secret_store_outage_serves_cached_secrets(self) -> None:
        receiver = self.receiver()
        self.assertEqual(receiver.process(signed_request(batch_event("b"))).response.status, 200)
        self.secrets.fail_reads = True
        self.clock.advance(self.settings.secret_cache_ttl_seconds + 1)  # cache expired
        with self.assertLogs("foundry_batch.webhook.secrets", "WARNING") as logs:
            outcome = receiver.process(
                signed_request(
                    batch_event("b"),
                    webhook_id="msg_2",
                    timestamp=int(self.clock.now().timestamp()),
                )
            )
        self.assertEqual(outcome.response.status, 200)
        self.assertIn("webhook.secrets_stale", logs.output[0])

    def test_envelope_over_the_queue_limit_is_413_not_a_503_loop(self) -> None:
        self.settings = make_settings(BATCH_MAX_BODY_BYTES="49152")
        # Non-ASCII text grows six-fold when the envelope JSON escapes it.
        event = batch_event("batch_1") | {"note": "\u00e9" * 20_000}
        body = json.dumps(event, ensure_ascii=False).encode("utf-8")
        self.assertLessEqual(len(body), 49152)
        outcome = self.receiver().process(signed_request(body))
        self.assertEqual((outcome.response.status, outcome.reason), (413, "envelope_too_large"))
        self.assertEqual(len(self.queue), 0)

    def test_envelope_size_guard_allows_the_body_cap(self) -> None:
        self.settings = make_settings(BATCH_MAX_BODY_BYTES="49152")
        event = batch_event("batch_1") | {"note": "x" * 45_000}
        outcome = self.receiver().process(signed_request(event))
        self.assertEqual(outcome.response.status, 200)
        self.assertLess(len(self.queue.sent[0].encode()), MAX_ENVELOPE_BYTES)
        self.assertEqual(Envelope.from_json(self.queue.sent[0]).batch_id, "batch_1")


class SecretResolverTests(unittest.TestCase):
    def setUp(self) -> None:
        self.settings = make_settings()
        self.clock = clock()
        self.secrets = MemorySecrets({"whsec-test-eus2-a": SECRET_A})
        self.resolver = SigningSecretResolver(self.secrets, self.settings, self.clock)

    def test_cache_ttl_and_throttled_refresh(self) -> None:
        self.assertEqual(self.resolver.secrets_for("eus2"), (SECRET_A,))
        reads = self.secrets.reads
        self.assertEqual(self.resolver.secrets_for("eus2"), (SECRET_A,))
        self.assertEqual(self.secrets.reads, reads)  # served from cache
        self.assertIsNone(self.resolver.refresh_if_allowed("eus2"))  # < 30 s since the load
        self.clock.advance(30)
        self.assertEqual(self.resolver.refresh_if_allowed("eus2"), (SECRET_A,))
        self.assertGreater(self.secrets.reads, reads)
        reads = self.secrets.reads
        self.clock.advance(self.settings.secret_cache_ttl_seconds)
        self.resolver.secrets_for("eus2")
        self.assertGreater(self.secrets.reads, reads)  # TTL expired: reloaded

    def test_outage_retries_at_most_every_30_seconds(self) -> None:
        self.resolver.secrets_for("eus2")
        self.secrets.fail_reads = True
        self.clock.advance(self.settings.secret_cache_ttl_seconds)
        with self.assertLogs("foundry_batch.webhook.secrets", "WARNING"):
            self.assertEqual(self.resolver.secrets_for("eus2"), (SECRET_A,))
        reads = self.secrets.reads
        self.clock.advance(10)
        self.assertEqual(self.resolver.secrets_for("eus2"), (SECRET_A,))
        self.assertIsNone(self.resolver.refresh_if_allowed("eus2"))
        self.assertEqual(self.secrets.reads, reads)  # no Key Vault hammering
        self.clock.advance(20)
        self.secrets.fail_reads = False
        self.secrets.set_secret("whsec-test-eus2-b", SECRET_B, tags={})
        self.assertEqual(self.resolver.secrets_for("eus2"), (SECRET_A, SECRET_B))

    def test_unknown_resource_has_no_secrets(self) -> None:
        self.assertEqual(self.resolver.secrets_for("nope"), ())


if __name__ == "__main__":
    unittest.main()
