import unittest

from foundry_batch.adapters.memory import (
    MESSAGE_TTL_SECONDS,
    MemoryQueue,
    MemoryQueueAdmin,
    MemoryTableStore,
)
from foundry_batch.domain.models import JobKey, JobRecord
from foundry_batch.errors import ConcurrencyConflictError
from foundry_batch.ports import Entity, Query, ReceivedMessage, TxOp
from foundry_batch.processing.registry import JobRegistry, OutboxItem, Receipt
from tests.support import T0, clock, make_settings


class MemoryStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.store = MemoryTableStore()

    def test_create_conflict_and_etag_checks(self) -> None:
        etags = self.store.submit("t", [TxOp("create", Entity("p", "r", {"v": 1}))])
        with self.assertRaises(ConcurrencyConflictError):
            self.store.submit("t", [TxOp("create", Entity("p", "r", {"v": 2}))])
        with self.assertRaises(ConcurrencyConflictError):
            self.store.submit("t", [TxOp("update", Entity("p", "r", {"v": 2}, 'W/"old"'))])
        new = self.store.submit("t", [TxOp("update", Entity("p", "r", {"v": 2}, etags["r"]))])
        self.assertNotEqual(new["r"], etags["r"])
        self.store.submit("t", [TxOp("update", Entity("p", "r", {"v": 3}), force=True)])
        self.assertEqual(self.store.get("t", "p", "r").data["v"], 3)  # type: ignore[union-attr]

    def test_batch_is_atomic(self) -> None:
        self.store.submit("t", [TxOp("create", Entity("p", "dup", {}))])
        with self.assertRaises(ConcurrencyConflictError):
            self.store.submit(
                "t",
                [TxOp("create", Entity("p", "new", {})), TxOp("create", Entity("p", "dup", {}))],
            )
        self.assertIsNone(self.store.get("t", "p", "new"))

    def test_batch_rules(self) -> None:
        with self.assertRaises(ValueError):
            self.store.submit(
                "t", [TxOp("create", Entity("p1", "a", {})), TxOp("create", Entity("p2", "a", {}))]
            )
        with self.assertRaises(ValueError):
            self.store.submit(
                "t", [TxOp("upsert", Entity("p", "a", {})), TxOp("upsert", Entity("p", "a", {}))]
            )
        with self.assertRaises(ConcurrencyConflictError):
            self.store.submit("t", [TxOp("delete", Entity("p", "missing", {}, "x"))])
        self.assertEqual(self.store.submit("t", []), {})

    def test_delete_and_query(self) -> None:
        etags = self.store.submit(
            "t",
            [
                TxOp("create", Entity("p", "a1", {"s": "x", "n": "2026"})),
                TxOp("create", Entity("p", "b1", {"s": "y", "n": "2025"})),
            ],
        )
        self.assertEqual(len(self.store.query("t", Query(row_key_prefix="a"))), 1)
        self.assertEqual(len(self.store.query("t", Query(equals={"s": "y"}))), 1)
        self.assertEqual(len(self.store.query("t", Query(less_or_equal={"n": "2025"}))), 1)
        self.assertEqual(len(self.store.query("t", Query(partition_key="other"))), 0)
        self.assertEqual(len(self.store.query("t", Query(limit=1))), 1)
        self.store.submit("t", [TxOp("delete", Entity("p", "a1", {}, etags["a1"]))])
        self.assertEqual(len(self.store.rows("t")), 1)

    def test_entity_and_transaction_size_limits(self) -> None:
        big = "x" * 30_000  # 60,000 bytes of UTF-16: a legal property

        def create(row: str, data: dict[str, object]) -> list[TxOp]:
            return [TxOp("create", Entity("p", row, data))]

        with self.assertRaises(ValueError):  # 18 x 60 KB > 1 MiB
            self.store.submit("t", create("e", {f"s{i}": big for i in range(18)}))
        with self.assertRaises(ValueError):  # 255 including the keys and Timestamp
            self.store.submit("t", create("e", {f"n{i}": i for i in range(253)}))
        self.store.submit("t", create("ok", {f"n{i}": i for i in range(252)}))
        rows = [TxOp("create", Entity("p", f"r{i}", {"a": big, "b": big})) for i in range(40)]
        with self.assertRaises(ValueError):  # 40 x 120 KB > 4 MiB
            self.store.submit("t", rows)
        self.store.submit("t", rows[:30])
        self.assertIsNone(self.store.get("t", "p", "e"))


class MemoryQueueTests(unittest.TestCase):
    def setUp(self) -> None:
        self.queue = MemoryQueue("q")
        self.admin = MemoryQueueAdmin({"q": self.queue})

    def test_messages_expire_after_the_time_to_live(self) -> None:
        self.queue.now = 1_000.0
        self.queue.send("a")
        self.queue.send("b", visibility_timeout=60)
        self.queue.now += MESSAGE_TTL_SECONDS - 1
        self.assertEqual(len(self.queue), 2)
        self.assertEqual(self.admin.approximate_depth("q"), 2)
        self.queue.now += 1
        self.assertEqual(len(self.queue), 0)
        self.assertEqual(self.admin.receive("q", max_messages=5, visibility_timeout=30), [])
        self.assertEqual(self.admin.peek("q", max_messages=5), [])
        self.assertIsNone(self.queue.pop_visible())

    def test_message_sent_before_any_time_was_supplied_does_not_expire(self) -> None:
        self.queue.send("early")
        self.queue.now = 2_000_000_000.0
        self.assertEqual(self.queue.pop_visible(), "early")

    def test_service_limits_on_send(self) -> None:
        with self.assertRaises(ValueError):
            self.queue.send("x" * (64 * 1024 + 1))
        with self.assertRaises(ValueError):
            self.queue.send("x", visibility_timeout=7 * 24 * 3600 + 1)
        self.queue.send("x" * 64 * 1024, visibility_timeout=7 * 24 * 3600)
        self.assertEqual(len(self.queue), 1)

    def test_delete_needs_the_latest_pop_receipt(self) -> None:
        self.queue.send("m")
        [first] = self.admin.receive("q", max_messages=1, visibility_timeout=0)
        [second] = self.admin.receive("q", max_messages=1, visibility_timeout=30)
        with self.assertRaises(LookupError):
            self.admin.delete("q", first)  # stale receipt
        [peeked] = [ReceivedMessage(first.message_id, "", "m")]
        with self.assertRaises(LookupError):
            self.admin.delete("q", peeked)  # peek gives no receipt
        self.admin.delete("q", second)
        self.assertEqual(len(self.queue), 0)
        with self.assertRaises(LookupError):
            self.admin.delete("q", second)  # already gone


class RegistryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.store = MemoryTableStore()
        self.clock = clock()
        self.registry = JobRegistry(self.store, make_settings(), self.clock)
        self.job = JobRecord("eus2", "batch_1", "test", raw_status="in_progress")
        self.registry.create_job(self.job)

    def test_tc_d09_commit_is_all_or_nothing(self) -> None:
        stored = self.registry.load(self.job.key)
        assert stored is not None
        receipt = Receipt("wh:evt_1", "webhook", T0)
        terminal = self.job.with_status("completed")
        outbox = OutboxItem("k1", "{}", "webhook", T0, next_attempt_at=T0)
        self.registry.commit(terminal, etag=stored.etag, receipt=receipt, outbox=outbox)
        stored2 = self.registry.load(self.job.key)
        assert stored2 is not None
        # A second commit with the same receipt fails and changes nothing.
        with self.assertRaises(ConcurrencyConflictError):
            self.registry.commit(
                terminal.with_status("failed"),
                etag=stored2.etag,
                receipt=receipt,
                outbox=OutboxItem("k2", "{}", "webhook", T0),
            )
        after = self.registry.load(self.job.key)
        assert after is not None
        self.assertEqual(after.record.raw_status, "completed")
        self.assertIsNone(self.registry.load_outbox(self.registry.partition(self.job.key), "k2"))
        self.assertTrue(self.registry.has_receipt(self.job.key, "wh:evt_1"))
        self.assertEqual(len(self.registry.receipts(self.job.key)), 1)

    def test_stale_etag_conflicts(self) -> None:
        stored = self.registry.load(self.job.key)
        assert stored is not None
        self.registry.commit(self.job.with_status("finalizing"), etag=stored.etag)
        with self.assertRaises(ConcurrencyConflictError):
            self.registry.commit(self.job.with_status("completed"), etag=stored.etag)
        self.assertIsNone(self.registry.try_update_job(stored, self.job))

    def test_queries_by_state(self) -> None:
        other = JobRecord("swc", "batch_2", "test").with_status("completed")
        self.registry.create_job(other)
        self.assertEqual([j.record.batch_id for j in self.registry.open_jobs()], ["batch_1"])
        self.assertEqual([j.record.batch_id for j in self.registry.open_jobs("swc")], [])
        self.assertEqual(
            [j.record.batch_id for j in self.registry.terminal_jobs("swc")], ["batch_2"]
        )
        self.assertEqual(self.registry.terminal_jobs(where={"details_complete": True}), [])
        self.assertIsNone(self.registry.load(JobKey("eus2", "missing")))

    def test_outbox_round_trip_and_due_query(self) -> None:
        stored = self.registry.load(self.job.key)
        assert stored is not None
        item = OutboxItem("k1", '{"a": 1}', "webhook", T0, next_attempt_at=T0)
        self.registry.commit(self.job.with_status("completed"), etag=stored.etag, outbox=item)
        due = self.registry.due_outbox(T0)
        self.assertEqual([d.item.idempotency_key for d in due], ["k1"])
        self.assertEqual(due[0].item.event(), {"a": 1})
        self.assertEqual(self.registry.outbox_items("pending")[0].item, item)
        self.assertEqual(self.registry.due_outbox(T0.replace(year=2025)), [])


if __name__ == "__main__":
    unittest.main()
