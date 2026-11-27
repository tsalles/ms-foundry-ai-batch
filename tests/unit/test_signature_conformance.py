"""Cross-check against the official OpenAI SDK verifier when it is installed (dev extra)."""

import time
import unittest

from foundry_batch.webhook import signature
from tests.support import SECRET_A

try:
    import openai
except ImportError:  # pragma: no cover - exercised in CI where the dev extra is installed
    openai = None


@unittest.skipIf(openai is None, "openai SDK not installed")
class SdkConformanceTests(unittest.TestCase):
    def test_sdk_accepts_our_signatures_and_rejects_tampering(self) -> None:
        assert openai is not None
        client = openai.OpenAI(api_key="placeholder", webhook_secret=SECRET_A)
        body = b'{"object":"event","id":"evt_1","type":"batch.completed","data":{"id":"b1"}}'
        headers = signature.sign_headers(SECRET_A, "msg_1", int(time.time()), body)
        client.webhooks.verify_signature(body, headers)  # does not raise
        with self.assertRaises(openai.InvalidWebhookSignatureError):
            client.webhooks.verify_signature(body + b" ", headers)


if __name__ == "__main__":
    unittest.main()
