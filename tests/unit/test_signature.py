import base64
import hashlib
import hmac
import unittest
from datetime import timedelta

from foundry_batch.webhook import signature
from tests.support import SECRET_A, SECRET_B, T0


def _manual(secret: str, msg_id: str, ts: str, body: bytes) -> str:
    key = base64.b64decode(secret.removeprefix("whsec_"))
    return base64.b64encode(
        hmac.new(key, f"{msg_id}.{ts}.".encode() + body, hashlib.sha256).digest()
    ).decode()


class SignatureTests(unittest.TestCase):
    body = b'{"type":"batch.completed","data":{"id":"batch_1"}}'

    def headers(self, secret: str = SECRET_A, ts: int | None = None) -> dict[str, str]:
        return signature.sign_headers(
            secret, "msg_1", int(T0.timestamp()) if ts is None else ts, self.body
        )

    def test_known_answer_matches_the_standard_webhooks_algorithm(self) -> None:
        ts = str(int(T0.timestamp()))
        self.assertEqual(
            signature.compute_signature(SECRET_A, "msg_1", ts, self.body),
            _manual(SECRET_A, "msg_1", ts, self.body),
        )

    def test_valid_signature_is_accepted(self) -> None:
        result = signature.verify(self.body, self.headers(), [SECRET_A], now=T0)
        self.assertEqual(result.webhook_id, "msg_1")
        self.assertEqual(result.secret_index, 0)

    def test_header_names_are_case_insensitive(self) -> None:
        upper = {k.upper(): v for k, v in self.headers().items()}
        self.assertEqual(signature.verify(self.body, upper, [SECRET_A], now=T0).webhook_id, "msg_1")

    def test_any_secret_and_any_signature_value_may_match(self) -> None:
        headers = self.headers(SECRET_B)
        headers["webhook-signature"] = "v1,AAAA " + headers["webhook-signature"]
        result = signature.verify(self.body, headers, [SECRET_A, SECRET_B], now=T0)
        self.assertEqual(result.secret_index, 1)

    def test_signature_without_version_prefix_is_accepted_like_the_sdk(self) -> None:
        headers = self.headers()
        headers["webhook-signature"] = headers["webhook-signature"].removeprefix("v1,")
        signature.verify(self.body, headers, [SECRET_A], now=T0)

    def test_raw_secret_without_prefix(self) -> None:
        headers = signature.sign_headers("plain-secret", "m", int(T0.timestamp()), self.body)
        signature.verify(self.body, headers, ["plain-secret"], now=T0)

    def test_rejections_have_stable_reason_codes(self) -> None:
        ts = int(T0.timestamp())
        cases = [
            ("tampered", self.body + b" ", self.headers(), [SECRET_A], "signature_mismatch"),
            ("wrong secret", self.body, self.headers(SECRET_B), [SECRET_A], "signature_mismatch"),
            (
                "too old",
                self.body,
                self.headers(ts=ts - 301),
                [SECRET_A],
                "timestamp_out_of_tolerance",
            ),
            (
                "too new",
                self.body,
                self.headers(ts=ts + 301),
                [SECRET_A],
                "timestamp_out_of_tolerance",
            ),
            ("no secret", self.body, self.headers(), [], "no_secret_configured"),
        ]
        for name, body, headers, secrets, reason in cases:
            with self.subTest(name), self.assertRaises(signature.SignatureError) as ctx:
                signature.verify(body, headers, secrets, now=T0)
            self.assertEqual(ctx.exception.reason, reason)

    def test_each_missing_header_is_rejected(self) -> None:
        for header in ("webhook-id", "webhook-timestamp", "webhook-signature"):
            headers = self.headers()
            del headers[header]
            with self.subTest(header), self.assertRaises(signature.SignatureError) as ctx:
                signature.verify(self.body, headers, [SECRET_A], now=T0)
            self.assertEqual(ctx.exception.reason, "missing_headers")

    def test_malformed_timestamp_and_secret(self) -> None:
        headers = self.headers()
        headers["webhook-timestamp"] = "yesterday"
        with self.assertRaises(signature.SignatureError) as ctx:
            signature.verify(self.body, headers, [SECRET_A], now=T0)
        self.assertEqual(ctx.exception.reason, "timestamp_malformed")
        with self.assertRaises(signature.SignatureError) as ctx:
            signature.decode_secret("whsec_***")
        self.assertEqual(ctx.exception.reason, "secret_malformed")

    def test_tolerance_boundary_is_inclusive(self) -> None:
        ts = int(T0.timestamp())
        signature.verify(self.body, self.headers(ts=ts - 300), [SECRET_A], now=T0)
        signature.verify(self.body, self.headers(ts=ts + 300), [SECRET_A], now=T0)
        signature.verify(
            self.body,
            self.headers(ts=ts - 60),
            [SECRET_A],
            now=T0 + timedelta(seconds=0),
            tolerance_seconds=60,
        )

    def test_lower_headers_accepts_pairs(self) -> None:
        self.assertEqual(signature.lower_headers([("A", "1")]), {"a": "1"})

    def test_secret_is_usable_only_when_it_decodes_to_a_key(self) -> None:
        self.assertTrue(signature.secret_is_usable(SECRET_A))
        self.assertTrue(signature.secret_is_usable("raw-utf8-secret"))
        for unusable in ("whsec_***", "whsec_not*base64", "whsec_", ""):
            with self.subTest(secret=unusable):
                self.assertFalse(signature.secret_is_usable(unusable))


if __name__ == "__main__":
    unittest.main()
