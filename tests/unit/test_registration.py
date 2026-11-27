import unittest
from collections.abc import Sequence
from datetime import timedelta
from unittest import mock

from foundry_batch.adapters.memory import MemorySecrets, MemoryTableStore
from foundry_batch.control import ControlStore, RegistrationRecord
from foundry_batch.errors import (
    CompensationFailedError,
    ProviderAmbiguousError,
    StoreUnavailableError,
)
from foundry_batch.registration.registrar import RegistrationError, WebhookRegistrar, slot_url
from tests.support import T0, FakeEndpoints, ambiguous, clock, make_settings

URL = "https://func.example.net/api/webhooks/eus2"


class RegistrarTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = clock()
        self.settings = make_settings()
        self.store = MemoryTableStore()
        self.control = ControlStore(self.store, "batchcontrol", "test", self.clock)
        self.api = FakeEndpoints()
        self.secrets = MemorySecrets()
        self.registrar = WebhookRegistrar(
            self.settings, lambda _: self.api, self.secrets, self.control, self.clock, holder="r1"
        )

    def test_tc_a01_create_captures_secret_in_key_vault_only(self) -> None:
        with self.assertLogs("foundry_batch", "INFO") as logs:
            [outcome] = self.registrar.ensure("eus2", URL)
        self.assertEqual((outcome.action, outcome.slot, outcome.healthy), ("created", "a", True))
        [endpoint] = self.api.endpoints.values()
        self.assertEqual(endpoint.name, "batch-test-eus2-a")
        self.assertEqual(
            set(endpoint.event_types),
            {"batch.completed", "batch.failed", "batch.cancelled", "batch.expired"},
        )
        secret = self.api.created_secrets[endpoint.endpoint_id]
        self.assertEqual(self.secrets.get_secret("whsec-test-eus2-a"), secret)
        self.assertEqual(
            self.secrets.get_secret_tags("whsec-test-eus2-a")["endpoint_id"], endpoint.endpoint_id
        )
        # TC-A03: never in logs, outcome or registration metadata.
        self.assertNotIn(secret, "\n".join(logs.output))
        self.assertNotIn(secret, repr(outcome))
        self.assertNotIn(secret, repr(self.store.rows("batchcontrol")))
        [record] = self.control.registrations("eus2")
        self.assertEqual((record.endpoint_id, record.secret_version), (endpoint.endpoint_id, "v1"))
        self.assertTrue(record.covers(T0))

    def test_tc_a02_rerun_creates_no_duplicate(self) -> None:
        self.registrar.ensure("eus2", URL)
        [outcome] = self.registrar.ensure("eus2", URL)
        self.assertEqual(outcome.action, "unchanged")
        self.assertEqual(len(self.api.endpoints), 1)
        self.assertEqual(self.api.calls["create"], 1)

    def test_tc_a04_changed_url_is_an_update(self) -> None:
        self.registrar.ensure("eus2", URL)
        new_url = "https://func2.example.net/api/webhooks/eus2"
        [outcome] = self.registrar.ensure("eus2", new_url)
        self.assertEqual(outcome.action, "updated")
        self.assertEqual(next(iter(self.api.endpoints.values())).url, new_url)
        self.assertTrue(outcome.healthy)

    def test_tc_a08_secret_write_failure_compensates(self) -> None:
        self.secrets.fail_writes = True
        with self.assertLogs("foundry_batch", "WARNING"), self.assertRaises(RegistrationError):
            self.registrar.ensure("eus2", URL)
        self.assertEqual(self.api.endpoints, {})
        self.assertEqual(self.control.registrations(), [])

    def test_tc_a08_failed_compensation_is_a_hard_error(self) -> None:
        self.secrets.fail_writes = True
        self.api.fail("delete", RuntimeError("delete failed"))
        with (
            self.assertLogs("foundry_batch", "ERROR") as logs,
            self.assertRaises(CompensationFailedError),
        ):
            self.registrar.ensure("eus2", URL)
        self.assertTrue(any("registration.compensation_failed" in line for line in logs.output))

    def test_tc_a08_ambiguous_create_is_cleaned_before_retry(self) -> None:
        self.api.fail("create", ambiguous(ProviderAmbiguousError("timeout")))
        with self.assertLogs("foundry_batch", "WARNING"):
            [outcome] = self.registrar.ensure("eus2", URL)
        self.assertEqual(len(self.api.endpoints), 1)
        self.assertEqual(self.api.calls["create"], 2)
        self.assertEqual(
            self.secrets.get_secret_tags("whsec-test-eus2-a")["endpoint_id"], outcome.endpoint_id
        )

    def test_tc_a09_concurrent_runs_are_serialised(self) -> None:
        self.control.acquire("register|eus2", "other-run", 300)
        with self.assertRaises(RegistrationError):
            self.registrar.ensure("eus2", URL)
        self.clock.advance(301)
        self.registrar.ensure("eus2", URL)

    def test_repair_disables_an_enabled_mismatched_secret(self) -> None:
        self.registrar.ensure("eus2", URL)
        name = "whsec-test-eus2-a"
        value = self.secrets.get_secret(name)
        assert value is not None
        self.secrets.set_secret(name, value, tags={"endpoint_id": "wrong-endpoint"})
        self.registrar.ensure("eus2", URL, repair=True)
        self.assertIsNone(self.secrets.get_secret(name))
        self.assertTrue(all(item.healthy for item in self.registrar.health("eus2")))

    def test_rerun_finishes_secret_cleanup_after_endpoint_deletion(self) -> None:
        self.registrar.ensure("eus2", URL)
        name = "whsec-test-eus2-a"
        value = self.secrets.get_secret(name)
        assert value is not None
        self.secrets.set_secret(name, value, tags={"endpoint_id": "wrong-endpoint"})
        with (
            mock.patch.object(self.secrets, "disable_secret", side_effect=RuntimeError("outage")),
            self.assertRaises(RuntimeError),
        ):
            self.registrar.ensure("eus2", URL, repair=True)
        self.registrar.ensure("eus2", URL, repair=True)
        self.assertIsNone(self.secrets.get_secret(name))
        self.assertEqual(len(self.api.endpoints), 1)

    def test_two_ambiguous_creates_are_both_compensated(self) -> None:
        self.api.fail(
            "create",
            ambiguous(ProviderAmbiguousError("first response lost")),
            ambiguous(ProviderAmbiguousError("second response lost")),
        )
        with self.assertRaises(RegistrationError):
            self.registrar.ensure("eus2", URL)
        self.assertEqual(self.api.endpoints, {})
        self.assertEqual(self.control.registrations(), [])

    def test_unhealthy_endpoint_is_reported_then_repaired(self) -> None:
        self.registrar.ensure("eus2", URL)
        self.secrets.disable_secret("whsec-test-eus2-a")
        [outcome] = self.registrar.ensure("eus2", URL)
        self.assertFalse(outcome.healthy)
        [repaired] = self.registrar.ensure("eus2", URL, repair=True)
        self.assertEqual(
            (repaired.action, repaired.slot, repaired.healthy), ("repaired", "b", True)
        )
        self.assertEqual([e.name for e in self.api.endpoints.values()], ["batch-test-eus2-b"])
        records = {r.slot: r for r in self.control.registrations("eus2")}
        self.assertIsNotNone(records["a"].active_until)

    def test_tc_a05_slots_register_distinct_urls(self) -> None:
        # The live provider answers 400 to a second endpoint with the same URL (run L1).
        self.registrar.ensure("eus2", URL)
        rotated = self.registrar.rotate("eus2", URL)
        urls = {e.name: e.url for e in self.api.endpoints.values()}
        self.assertEqual(urls, {"batch-test-eus2-a": URL, "batch-test-eus2-b": URL + "?slot=b"})
        [again_a, again_b] = self.registrar.ensure("eus2", URL)
        self.assertEqual((again_a.action, again_b.action), ("unchanged", "unchanged"))
        self.assertEqual(rotated.slot, "b")
        self.assertEqual(slot_url(URL + "?x=1", "b"), URL + "?x=1&slot=b")

    def test_tc_a05_rotation_then_retire(self) -> None:
        self.registrar.ensure("eus2", URL)
        self.clock.advance(60)
        rotated = self.registrar.rotate("eus2", URL)
        self.assertEqual(rotated.slot, "b")
        self.assertEqual(len(self.api.endpoints), 2)
        self.assertTrue(all(h.healthy for h in self.registrar.health("eus2")))
        with self.assertRaises(RegistrationError):
            self.registrar.rotate("eus2", URL)
        self.registrar.retire("eus2", "a")
        self.assertIsNone(self.secrets.get_secret("whsec-test-eus2-a"))
        self.assertEqual([e.name for e in self.api.endpoints.values()], ["batch-test-eus2-b"])
        with self.assertRaises(RegistrationError):
            self.registrar.retire("eus2", "b")  # the only healthy endpoint
        # Rotating back into the disabled slot works (purge protection keeps the name).
        self.registrar.rotate("eus2", URL)
        self.assertIsNotNone(self.secrets.get_secret("whsec-test-eus2-a"))

    def test_tc_a07_remove_all_for_teardown(self) -> None:
        self.registrar.ensure("eus2", URL)
        outcomes = self.registrar.remove_all("eus2")
        self.assertEqual([o.action for o in outcomes], ["retired", "retired"])
        self.assertEqual(self.api.endpoints, {})
        self.assertTrue(all(r.active_until is not None for r in self.control.registrations()))
        self.assertIsNone(self.control.active_registration("eus2", T0 + timedelta(hours=1)))

    def test_input_validation(self) -> None:
        with self.assertRaises(RegistrationError):
            self.registrar.ensure("nope", URL)
        with self.assertRaises(RegistrationError):
            self.registrar.ensure("eus2", "http://insecure")
        with self.assertRaises(RegistrationError):
            self.registrar.retire("eus2", "c")
        self.api.create_endpoint(name="batch-test-eus2-a", url=URL, event_types=[])
        self.api.create_endpoint(name="batch-test-eus2-a", url=URL + "?other", event_types=[])
        with self.assertRaises(RegistrationError):
            self.registrar.ensure("eus2", URL)

    def test_repair_with_both_slots_unhealthy_fails(self) -> None:
        self.registrar.ensure("eus2", URL)
        self.registrar.rotate("eus2", URL)
        self.secrets.disable_secret("whsec-test-eus2-a")
        self.secrets.disable_secret("whsec-test-eus2-b")
        with self.assertRaises(RegistrationError) as ctx:
            self.registrar.ensure("eus2", URL, repair=True)
        self.assertEqual(ctx.exception.reason, "no_free_slot")
        self.assertEqual(len(self.api.endpoints), 2)

    def test_repair_keeps_a_healthy_other_slot_as_the_replacement(self) -> None:
        self.registrar.ensure("eus2", URL)
        rotated = self.registrar.rotate("eus2", URL)
        self.secrets.disable_secret("whsec-test-eus2-a")
        [outcome] = self.registrar.ensure("eus2", URL, repair=True)
        self.assertEqual(
            (outcome.action, outcome.slot, outcome.endpoint_id, outcome.healthy),
            ("repaired", "b", rotated.endpoint_id, True),
        )
        self.assertEqual(list(self.api.endpoints), [rotated.endpoint_id])
        self.assertEqual(self.api.calls["create"], 2)  # nothing new was created


class GatedRepairAndRetireTests(unittest.TestCase):
    """REQ-I03 (ADR 0009): an endpoint is removed only once the client vault verifies the key
    that remains, whether through ``retire`` or ``ensure --repair``."""

    def setUp(self) -> None:
        self.clock = clock()
        self.control = ControlStore(MemoryTableStore(), "batchcontrol", "test", self.clock)
        self.api = FakeEndpoints()
        self.secrets = MemorySecrets()
        self.gate_calls: list[tuple[str, list[str]]] = []
        self.gate_open = False
        self.registrar = WebhookRegistrar(
            make_settings(),
            lambda _: self.api,
            self.secrets,
            self.control,
            self.clock,
            holder="r1",
            sync_gate=self.gate,
        )

    def gate(self, resource_key: str, endpoint_ids: Sequence[str]) -> bool:
        self.gate_calls.append((resource_key, list(endpoint_ids)))
        return self.gate_open

    def break_slot_a(self) -> str:
        [outcome] = self.registrar.ensure("eus2", URL)
        value = self.secrets.get_secret("whsec-test-eus2-a")
        assert value is not None and outcome.endpoint_id is not None
        self.secrets.set_secret("whsec-test-eus2-a", value, tags={"endpoint_id": "stale"})
        return outcome.endpoint_id

    def active(self) -> dict[str, str]:
        return {
            r.endpoint_id: r.slot
            for r in self.control.registrations("eus2")
            if r.active_until is None
        }

    def test_repair_is_pending_until_the_replacement_is_verified(self) -> None:
        old = self.break_slot_a()
        with self.assertLogs("foundry_batch", "WARNING") as logs:
            created, pending = self.registrar.ensure("eus2", URL, repair=True)
        self.assertEqual((created.action, created.slot, created.healthy), ("created", "b", True))
        self.assertEqual(
            (pending.action, pending.slot, pending.endpoint_id, pending.healthy),
            ("repair_pending", "a", old, False),
        )
        self.assertIn("registration.repair_pending", "\n".join(logs.output))
        self.assertEqual(self.gate_calls, [("eus2", [created.endpoint_id])])
        self.assertIn(old, self.api.endpoints)  # still the key the client vault verifies
        self.assertIsNotNone(self.secrets.get_secret("whsec-test-eus2-a"))
        self.assertEqual(self.active(), {old: "a", created.endpoint_id: "b"})

        # A rerun before vault sync changes nothing and creates nothing.
        kept, still = self.registrar.ensure("eus2", URL, repair=True)
        self.assertEqual(
            (kept.action, kept.slot, still.action), ("unchanged", "b", "repair_pending")
        )
        self.assertEqual(self.api.calls["create"], 2)

        self.gate_open = True
        [repaired] = self.registrar.ensure("eus2", URL, repair=True)
        self.assertEqual(
            (repaired.action, repaired.slot, repaired.endpoint_id, repaired.healthy),
            ("repaired", "b", created.endpoint_id, True),
        )
        self.assertNotIn(old, self.api.endpoints)
        self.assertIsNone(self.secrets.get_secret("whsec-test-eus2-a"))
        self.assertEqual(self.active(), {created.endpoint_id: "b"})

    def test_forced_repair_skips_the_gate(self) -> None:
        old = self.break_slot_a()
        [repaired] = self.registrar.ensure("eus2", URL, repair=True, force=True)
        self.assertEqual((repaired.action, repaired.slot), ("repaired", "b"))
        self.assertNotIn(old, self.api.endpoints)
        self.assertEqual(self.gate_calls, [])
        self.assertEqual(self.active(), {repaired.endpoint_id: "b"})  # the new one stays covered

    def test_unreadable_ledger_refuses_retire_and_repair(self) -> None:
        def broken(resource_key: str, endpoint_ids: Sequence[str]) -> bool:
            raise StoreUnavailableError("ledger down")

        old = self.break_slot_a()
        self.registrar.sync_gate = broken
        with self.assertLogs("foundry_batch", "ERROR"), self.assertRaises(RegistrationError) as ctx:
            self.registrar.ensure("eus2", URL, repair=True)
        self.assertEqual(ctx.exception.reason, "vault_sync_verification_unavailable")
        self.assertIn(old, self.api.endpoints)
        with self.assertLogs("foundry_batch", "ERROR"), self.assertRaises(RegistrationError) as ctx:
            self.registrar.retire("eus2", "a")
        self.assertEqual(ctx.exception.reason, "vault_sync_verification_unavailable")
        self.assertEqual(len(self.api.endpoints), 2)

    def test_retire_refused_by_the_gate_names_the_reason(self) -> None:
        self.registrar.ensure("eus2", URL)
        rotated = self.registrar.rotate("eus2", URL)
        with self.assertRaises(RegistrationError) as ctx:
            self.registrar.retire("eus2", "a")
        self.assertEqual(ctx.exception.reason, "vault_sync_gate")
        self.assertEqual(self.gate_calls, [("eus2", [rotated.endpoint_id])])
        self.assertEqual(len(self.api.endpoints), 2)


class StaleCoverageTests(unittest.TestCase):
    """An endpoint deleted without its record being closed (crash, storage blip) must not stay
    'active': the client vault would keep its key enabled and a reused slot would have two."""

    def setUp(self) -> None:
        self.clock = clock()
        self.control = ControlStore(MemoryTableStore(), "batchcontrol", "test", self.clock)
        self.api = FakeEndpoints()
        self.secrets = MemorySecrets()
        self.registrar = WebhookRegistrar(
            make_settings(),
            lambda _: self.api,
            self.secrets,
            self.control,
            self.clock,
            holder="r1",
        )
        [first] = self.registrar.ensure("eus2", URL)
        self.registrar.rotate("eus2", URL)
        self.old = first.endpoint_id or ""

    def old_record(self) -> RegistrationRecord:
        return next(r for r in self.control.registrations("eus2") if r.endpoint_id == self.old)

    def test_rerun_of_an_interrupted_retire_ends_the_coverage(self) -> None:
        real = self.control.save_registration
        with (
            mock.patch.object(
                self.control, "save_registration", side_effect=StoreUnavailableError("blip")
            ),
            self.assertRaises(StoreUnavailableError),
        ):
            self.registrar.retire("eus2", "a")
        self.assertNotIn(self.old, self.api.endpoints)  # deleted before the failure
        self.assertIsNone(self.old_record().active_until)
        self.control.save_registration = real  # type: ignore[method-assign]
        self.clock.advance(30)
        self.assertEqual(self.registrar.retire("eus2", "a").action, "retired")
        self.assertEqual(self.old_record().active_until, T0 + timedelta(seconds=30))
        self.assertIsNone(self.secrets.get_secret("whsec-test-eus2-a"))

    def test_rotate_closes_a_deleted_endpoints_record_before_reusing_its_slot(self) -> None:
        self.api.delete_endpoint(self.old)  # retire interrupted right after the delete
        rotated = self.registrar.rotate("eus2", URL)
        self.assertEqual(rotated.slot, "a")
        self.assertIsNotNone(self.old_record().active_until)
        active_a = [
            r.endpoint_id
            for r in self.control.registrations("eus2")
            if r.slot == "a" and r.active_until is None
        ]
        self.assertEqual(active_a, [rotated.endpoint_id])


class RefusalReasonTests(unittest.TestCase):
    """Stable ``reason`` codes: the CLI prints them for operators and automation."""

    def setUp(self) -> None:
        self.clock = clock()
        self.control = ControlStore(MemoryTableStore(), "batchcontrol", "test", self.clock)
        self.api = FakeEndpoints()
        self.registrar = WebhookRegistrar(
            make_settings(),
            lambda _: self.api,
            MemorySecrets(),
            self.control,
            self.clock,
            holder="r1",
        )

    def reason(self, call: object) -> str:
        with self.assertRaises(RegistrationError) as ctx:
            call()  # type: ignore[operator]
        return ctx.exception.reason

    def test_reason_codes(self) -> None:
        self.assertEqual(
            self.reason(lambda: self.registrar.ensure("nope", URL)), "unknown_resource"
        )
        self.assertEqual(
            self.reason(lambda: self.registrar.ensure("eus2", "http://x")), "invalid_url"
        )
        self.assertEqual(self.reason(lambda: self.registrar.retire("eus2", "c")), "unknown_slot")
        self.registrar.ensure("eus2", URL)
        self.assertEqual(
            self.reason(lambda: self.registrar.retire("eus2", "a")), "only_healthy_endpoint"
        )
        self.registrar.rotate("eus2", URL)
        self.assertEqual(self.reason(lambda: self.registrar.rotate("eus2", URL)), "slots_in_use")
        self.control.acquire("register|eus2", "other-run", 300)
        self.assertEqual(self.reason(lambda: self.registrar.ensure("eus2", URL)), "lease_held")
        self.clock.advance(301)
        self.api.create_endpoint(name="batch-test-eus2-a", url=URL + "?dup", event_types=[])
        self.assertEqual(
            self.reason(lambda: self.registrar.ensure("eus2", URL)), "duplicate_endpoints"
        )

    def test_compensated_create_reason(self) -> None:
        self.api.fail(
            "create", ambiguous(ProviderAmbiguousError("1")), ambiguous(ProviderAmbiguousError("2"))
        )
        self.assertEqual(
            self.reason(lambda: self.registrar.ensure("eus2", URL)), "create_compensated"
        )


class ControlStoreTests(unittest.TestCase):
    def test_lease_semantics_and_state(self) -> None:
        c = clock()
        control = ControlStore(MemoryTableStore(), "batchcontrol", "test", c)
        lease = control.acquire("x", "a", 60)
        assert lease is not None
        self.assertIsNone(control.acquire("x", "b", 60))
        self.assertIsNotNone(control.acquire("x", "a", 60))  # renew by the holder
        c.advance(61)
        taken = control.acquire("x", "b", 60)
        assert taken is not None
        control.release(lease)  # stale release is ignored
        self.assertIsNone(control.acquire("x", "a", 60))
        control.release(taken)
        self.assertIsNotNone(control.acquire("x", "a", 60))
        control.put_state("s", {"cursor": "abc"})
        self.assertEqual(control.get_state("s"), {"cursor": "abc"})
        self.assertEqual(control.get_state("missing"), {})


if __name__ == "__main__":
    unittest.main()
