import logging
import unittest

from foundry_batch.adapters.memory import MemoryTableStore
from foundry_batch.domain.models import JobKey, JobRecord
from foundry_batch.errors import ProviderRetryableError
from foundry_batch.metrics.api_pressure import ApiCallCounter, log_api_pressure
from foundry_batch.metrics.backfill import backfill
from foundry_batch.processing.registry import JobRegistry
from tests.support import FakeProvider, FakeProviderFactory, clock, make_settings


class BackfillTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = clock()
        self.settings = make_settings()
        self.registry = JobRegistry(MemoryTableStore(), self.settings, self.clock)
        self.provider = FakeProvider(self.clock)
        self.factory = FakeProviderFactory({"eus2": self.provider})

    def test_tc_g06_writes_untrusted_terminal_records_once(self) -> None:
        done = [self.provider.add("completed") for _ in range(150)]
        self.provider.add("in_progress")
        existing = done[0]
        self.registry.create_job(JobRecord("eus2", existing, "test"))
        report = backfill(self.settings, self.registry, self.factory, self.clock, "eus2")
        self.assertEqual(
            (
                report.pages,
                report.seen,
                report.written,
                report.skipped_existing,
                report.skipped_open,
                report.complete,
            ),
            (2, 151, 149, 1, 1, True),
        )
        stored = self.registry.load(JobKey("eus2", done[5]))
        assert stored is not None
        self.assertFalse(stored.record.trusted)
        self.assertEqual(stored.record.terminal_source, "backfill")
        self.assertEqual(self.registry.outbox_items("pending"), [])

    def test_stops_on_throttling_and_rejects_unknown_resource(self) -> None:
        self.provider.fail("list", ProviderRetryableError("x", status_code=429))
        report = backfill(self.settings, self.registry, self.factory, self.clock, "eus2")
        self.assertEqual((report.pages, report.complete), (0, False))
        with self.assertRaises(ValueError):
            backfill(self.settings, self.registry, self.factory, self.clock, "nope")
        for _ in range(3):
            self.provider.add("completed")
        partial = backfill(
            self.settings, self.registry, self.factory, self.clock, "eus2", max_pages=1
        )
        self.assertTrue(partial.complete)


class ApiPressureLogTests(unittest.TestCase):
    def test_emits_one_event_per_key_and_resets(self) -> None:
        counter = ApiCallCounter()
        counter.record("eus2", "batch.list", 200)
        counter.record("eus2", "batch.list", 429)
        logger = logging.getLogger("foundry_batch.test_pressure")
        with self.assertLogs(logger, "INFO") as logs:
            self.assertEqual(log_api_pressure(counter, logger), 2)
        self.assertTrue(all("api.calls" in line for line in logs.output))
        self.assertEqual(counter.total(), 0)


if __name__ == "__main__":
    unittest.main()
