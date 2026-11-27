import json
import unittest
from datetime import datetime, timedelta

from foundry_batch.clock import ManualClock, from_epoch, from_iso, to_iso
from foundry_batch.domain.contract import NormalizedEvent, idempotency_key, validate_event
from foundry_batch.domain.models import BatchSnapshot, JobKey, JobRecord, safe_key, utf16_prefix
from foundry_batch.domain.status import outcome_for, parse_status, rank
from foundry_batch.webhook.envelope import Envelope, EnvelopeError, extract_batch_id
from tests.support import T0


class ClockTests(unittest.TestCase):
    def test_conversions(self) -> None:
        self.assertEqual(from_epoch(int(T0.timestamp())), T0)
        self.assertEqual(from_epoch(str(int(T0.timestamp()))), T0)
        for bad in (None, 0, -5, True, "x", 1.5j):
            self.assertIsNone(from_epoch(bad))
        self.assertEqual(from_iso(to_iso(T0)), T0)
        self.assertIsNone(from_iso("2026-01-01T00:00:00"))  # naive is rejected
        self.assertIsNone(from_iso("garbage"))
        self.assertIsNone(from_iso(None))
        self.assertIsNone(to_iso(None))

    def test_iso_text_sorts_chronologically(self) -> None:
        a, b = T0, T0 + timedelta(microseconds=500_000)
        self.assertLess(to_iso(a), to_iso(b))

    def test_manual_clock(self) -> None:
        c = ManualClock(T0)
        c.advance(10)
        self.assertEqual(c.now(), T0 + timedelta(seconds=10))
        c.set(T0)
        self.assertEqual(c.now(), T0)
        with self.assertRaises(ValueError):
            ManualClock(datetime(2026, 1, 1))


class StatusTests(unittest.TestCase):
    def test_failed_and_expired_share_outcome_but_keep_raw_status(self) -> None:
        self.assertEqual(outcome_for("failed"), outcome_for("expired"))
        self.assertEqual(outcome_for("COMPLETED").value, "completed")
        self.assertIsNone(outcome_for("in_progress"))
        self.assertIsNone(parse_status(5))
        self.assertEqual(rank("mystery"), 0)


class SnapshotTests(unittest.TestCase):
    def test_from_api_handles_missing_timestamps_and_keeps_error_codes_only(self) -> None:
        snap = BatchSnapshot.from_api(
            {
                "id": "batch_1",
                "status": "Failed",
                "created_at": 1_700_000_000,
                "in_progress_at": None,
                "failed_at": 1_700_000_100,
                "errors": {
                    "data": [{"code": "invalid_json_line", "message": "line 3: secret text"}]
                },
                "request_counts": {"total": 3, "completed": 1.0, "failed": True},
                "metadata": {"batch_correlation_id": "c1"},
                "usage": {"input_tokens": 10, "output_tokens": 5},
            }
        )
        self.assertEqual(snap.status, "failed")
        self.assertIsNone(snap.timestamps["in_progress_at"])
        self.assertEqual(snap.terminal_at, from_epoch(1_700_000_100))
        self.assertEqual(snap.error_codes, ("invalid_json_line",))
        self.assertEqual(snap.request_counts.completed, 1)
        self.assertIsNone(snap.request_counts.failed)
        self.assertEqual(snap.metadata["batch_correlation_id"], "c1")
        self.assertEqual(snap.usage.input_tokens if snap.usage else None, 10)
        with self.assertRaises(ValueError):
            BatchSnapshot.from_api({"status": "completed"})

    def test_unknown_status_has_no_terminal_time(self) -> None:
        self.assertIsNone(BatchSnapshot.from_api({"id": "b", "status": "new"}).terminal_at)


class JobRecordTests(unittest.TestCase):
    def test_entity_round_trip(self) -> None:
        job = JobRecord(
            "eus2",
            "batch_1",
            "test",
            raw_status="completed",
            outcome="completed",
            provider_created_at=T0,
            webhook_covered=True,
            token_estimate=12,
        )
        again = JobRecord.from_entity(job.to_entity())
        self.assertEqual(again, job)

    def test_with_snapshot_does_not_erase_known_values(self) -> None:
        job = JobRecord("eus2", "b", "test", provider_created_at=T0, request_total=3)
        snap = BatchSnapshot.from_api({"id": "b", "status": "in_progress"})
        self.assertEqual(job.with_snapshot(snap).provider_created_at, T0)
        self.assertEqual(job.with_snapshot(snap).request_total, 3)

    def test_keys(self) -> None:
        key = JobKey("eus2", "batch/1#x")
        self.assertEqual(key.partition_key("test"), "test|eus2|batch_1_x")
        self.assertEqual(key.job_id, "eus2:batch/1#x")
        self.assertEqual(safe_key("a?b\\c"), "a_b_c")
        with self.assertRaises(ValueError):
            JobKey("", "b")

    def test_utf16_prefix_counts_code_units_and_never_splits_a_pair(self) -> None:
        emoji = "\U0001f600"  # two UTF-16 code units
        self.assertEqual(utf16_prefix("abc", 3), "abc")
        self.assertEqual(utf16_prefix("abcd", 3), "abc")
        self.assertEqual(utf16_prefix(emoji * 3, 4), emoji * 2)
        self.assertEqual(utf16_prefix(emoji * 3, 5), emoji * 2)  # no half pair
        self.assertEqual(utf16_prefix("a" + emoji, 2), "a")
        self.assertEqual(utf16_prefix("", 0), "")
        long_text = "\u00e9" * 10 + emoji * 10
        kept = utf16_prefix(long_text, 21)
        self.assertEqual(kept, "\u00e9" * 10 + emoji * 5)
        self.assertLessEqual(len(kept.encode("utf-16-le")) // 2, 21)


class ContractTests(unittest.TestCase):
    def job(self, status: str = "completed") -> JobRecord:
        return JobRecord("eus2", "batch_1", "test").with_status(status)

    def test_idempotency_key_is_stable_across_sources(self) -> None:
        a = NormalizedEvent.from_job(self.job(), source="webhook", emitted_at=T0)
        b = NormalizedEvent.from_job(self.job(), source="polling", emitted_at=T0 + timedelta(1))
        self.assertEqual(a.idempotency_key, b.idempotency_key)
        self.assertEqual(a.idempotency_key, idempotency_key("test", "eus2", "batch_1", "completed"))
        corrected = idempotency_key(
            "test", "eus2", "batch_1", "failed", correction_of="k", raw_status="failed"
        )
        self.assertNotEqual(corrected, a.idempotency_key)

    def test_payload_is_valid_and_metadata_only(self) -> None:
        event = NormalizedEvent.from_job(self.job("expired"), source="polling", emitted_at=T0)
        self.assertEqual(validate_event(event.payload), [])
        self.assertEqual(event.payload["outcome"], "failed")
        self.assertEqual(event.payload["raw_status"], "expired")
        self.assertEqual(event.payload["job_id"], "eus2:batch_1")
        self.assertNotIn("output", json.dumps(event.payload).replace("output_file_id", ""))

    def test_non_terminal_and_bad_source_are_rejected(self) -> None:
        with self.assertRaises(ValueError):
            NormalizedEvent.from_job(self.job("in_progress"), source="webhook", emitted_at=T0)
        with self.assertRaises(ValueError):
            NormalizedEvent.from_job(self.job(), source="carrier-pigeon", emitted_at=T0)

    def test_validate_reports_each_problem(self) -> None:
        problems = validate_event({"schema_version": "v0", "outcome": "maybe", "source": "x"})
        for field in (
            "schema_version",
            "idempotency_key",
            "outcome",
            "source",
            "raw_status",
            "timestamps",
            "request_counts",
        ):
            self.assertIn(field, problems)


class EnvelopeTests(unittest.TestCase):
    def envelope(self) -> Envelope:
        return Envelope(
            "test",
            "eus2",
            "msg_1",
            1,
            T0,
            "evt_1",
            "batch.completed",
            2,
            "b1",
            {"data": {"id": "b1"}},
        )

    def test_round_trip(self) -> None:
        self.assertEqual(Envelope.from_json(self.envelope().to_json()), self.envelope())

    def test_rejects_untrusted_shapes(self) -> None:
        good = json.loads(self.envelope().to_json())
        cases = {
            "not json": "{",
            "schema": json.dumps({**good, "schema": "other"}),
            "fields": json.dumps({**good, "resource_key": ""}),
            "event": json.dumps({**good, "event": []}),
        }
        for name, text in cases.items():
            with self.subTest(name), self.assertRaises(EnvelopeError):
                Envelope.from_json(text)

    def test_optional_fields_default_safely(self) -> None:
        data = json.loads(self.envelope().to_json())
        data.update(
            batch_id="", event_id=5, event_created_at="x", delivery_timestamp=None, attempt="2"
        )
        envelope = Envelope.from_json(json.dumps(data))
        self.assertIsNone(envelope.batch_id)
        self.assertIsNone(envelope.event_id)
        self.assertEqual(envelope.attempt, 0)

    def test_extract_batch_id(self) -> None:
        self.assertEqual(extract_batch_id({"data": {"batch_id": "b2"}}), "b2")
        self.assertIsNone(extract_batch_id({"data": "x"}))
        self.assertIsNone(extract_batch_id({"data": {}}))


class SchemaFileTests(unittest.TestCase):
    """The published JSON schema and the producer must not drift apart."""

    def test_payload_matches_published_schema_keys(self) -> None:
        from pathlib import Path

        schema_path = (
            Path(__file__).resolve().parents[2] / "docs/schemas/batch-event-v1.schema.json"
        )
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
        job = JobRecord("eus2", "b1", "test").with_status("completed")
        payload = NormalizedEvent.from_job(job, source="webhook", emitted_at=T0).payload
        self.assertEqual(set(payload), set(schema["properties"]))
        self.assertTrue(set(schema["required"]) <= set(payload))
        self.assertEqual(
            set(payload["timestamps"]), set(schema["properties"]["timestamps"]["properties"])
        )
        self.assertEqual(schema["properties"]["schema_version"]["const"], payload["schema_version"])


if __name__ == "__main__":
    unittest.main()
