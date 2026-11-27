import json
import logging
import unittest

from foundry_batch.observability import REDACTED, JsonFormatter, event_properties, log_event


class ObservabilityTests(unittest.TestCase):
    def test_sensitive_keys_are_redacted(self) -> None:
        props = event_properties(
            "x",
            {
                "signing_secret": "s",
                "api_key": "k",
                "body": "b",
                "Authorization": "a",
                "token": "t",
                "queued_tokens": 5,
                "max_body_bytes": 10,
                "reason": "ok",
            },
        )
        for key in ("signing_secret", "api_key", "body", "Authorization", "token"):
            self.assertEqual(props[key], REDACTED)
        self.assertEqual(props["queued_tokens"], 5)
        self.assertEqual(props["max_body_bytes"], 10)
        self.assertEqual(props["batch_event"], "x")

    def test_reserved_log_record_names_are_renamed_and_long_text_truncated(self) -> None:
        props = event_properties("x", {"name": "n", "message": "m", "detail": "y" * 600})
        self.assertEqual(props["name_"], "n")
        self.assertEqual(props["message_"], "m")
        self.assertTrue(props["detail"].endswith("..."))

    def test_json_formatter_outputs_properties(self) -> None:
        logger = logging.getLogger("foundry_batch.test_json")
        with self.assertLogs(logger, "INFO") as logs:
            log_event(logger, "demo.event", resource_key="eus2")
        line = JsonFormatter().format(logs.records[0])
        data = json.loads(line)
        self.assertEqual(data["batch_event"], "demo.event")
        self.assertEqual(data["resource_key"], "eus2")
        plain = logging.LogRecord("n", logging.INFO, "f", 1, "hello %s", ("x",), None)
        self.assertEqual(json.loads(JsonFormatter().format(plain))["message"], "hello x")
        try:
            raise RuntimeError("boom")
        except RuntimeError:
            import sys

            record = logging.LogRecord("n", logging.ERROR, "f", 1, "err", None, sys.exc_info())
        self.assertIn("RuntimeError", json.loads(JsonFormatter().format(record))["exception"])


if __name__ == "__main__":
    unittest.main()
