"""US-2.4 / TC-I04 locally: Azure capture, versioned sync, readback, rotation, retirement."""

import json
import unittest
from collections.abc import Mapping
from dataclasses import replace
from datetime import timedelta
from unittest import mock

from foundry_batch.adapters.memory import MemorySecrets, MemoryTableStore
from foundry_batch.control import ControlStore, RegistrationRecord
from foundry_batch.emulator.client_vault import ClientVaultUnavailableError, EmulatedClientVault
from foundry_batch.errors import ConcurrencyConflictError, StoreUnavailableError
from foundry_batch.ports import TxOp
from foundry_batch.vaultsync.service import (
    OwedRetirement,
    SyncRecord,
    VaultSyncService,
    _generation,
)
from tests.support import SECRET_A, SECRET_B, T0, clock


class Harness(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = clock()
        self.store = MemoryTableStore()
        self.secrets = MemorySecrets()
        self.vault = EmulatedClientVault()
        self.sync = self.worker("w1")
        self.control = ControlStore(self.store, "batchcontrol", "test", self.clock)

    def worker(self, holder: str, lease_seconds: int = 300) -> VaultSyncService:
        return VaultSyncService(
            "test",
            self.store,
            "batchcontrol",
            self.secrets,
            self.vault,
            self.clock,
            holder=holder,
            lease_seconds=lease_seconds,
        )

    def capture(
        self,
        slot: str = "a",
        endpoint: str = "wh_1",
        value: str = SECRET_A,
        resource: str = "eus2",
        at: timedelta = timedelta(),
    ) -> RegistrationRecord:
        version = self.secrets.set_secret(f"whsec-test-{resource}-{slot}", value, tags={})
        record = RegistrationRecord(
            resource,
            slot,
            endpoint,
            f"batch-test-{resource}-{slot}",
            "https://x/h",
            f"whsec-test-{resource}-{slot}",
            version,
            T0 + at,
        )
        self.control.save_registration(record)
        return record

    def end(self, registration: RegistrationRecord) -> None:
        self.control.save_registration(replace(registration, active_until=T0 + timedelta(hours=1)))

    def regs(self, resource: str = "eus2") -> list[RegistrationRecord]:
        return self.control.registrations(resource)

    def record(self, slot: str = "a") -> SyncRecord:
        return next(r for r in self.sync.status("eus2") if r.slot == slot)

    def enabled(self, name: str = "batch-test-eus2-a") -> list[str]:
        return [v.version for v in self.vault._secrets.get(name, []) if v.enabled]

    def overwrite(self, record: SyncRecord) -> None:
        """Write a ledger row as another worker would (unconditional)."""
        entity = self.store.get(
            "batchcontrol", "test|vaultsync", f"{record.resource_key}|{record.slot}"
        )
        assert entity is not None
        self.store.submit(
            "batchcontrol", [TxOp("update", replace(entity, data=record.to_entity()), force=True)]
        )


class SyncTests(Harness):
    def test_capture_sync_readback_and_metadata_only_ledger(self) -> None:
        reg = self.capture()
        with self.assertLogs("foundry_batch.vaultsync.service", "INFO") as logs:
            stats = self.sync.run("eus2", self.regs())
        self.assertEqual(stats.synced, 1)
        record = self.record()
        self.assertEqual(
            (record.state, record.source_version, record.generation),
            ("synced", reg.secret_version, 1),
        )
        self.assertEqual(
            self.vault.read(record.destination_name, record.destination_version or ""), SECRET_A
        )
        self.assertEqual(
            self.vault.current(record.destination_name).metadata["endpoint_id"], "wh_1"
        )  # type: ignore[union-attr]
        self.assertTrue(self.sync.ready("eus2", ["wh_1"]))
        self.assertFalse(self.sync.ready("eus2", []))
        ledger = json.dumps([dict(e.data) for e in self.store.rows("batchcontrol")])
        self.assertNotIn(SECRET_A, ledger)
        self.assertNotIn(SECRET_A, "\n".join(logs.output))
        self.assertEqual(self.sync.run("eus2", self.regs()).synced, 0)  # idempotent
        self.assertEqual(self.vault.writes, 1)

    def test_exact_source_version_never_latest(self) -> None:
        self.capture()
        self.secrets.set_secret("whsec-test-eus2-a", SECRET_B, tags={})  # a later rotation
        self.sync.run("eus2", self.regs())
        record = self.record()
        self.assertEqual(
            self.vault.read(record.destination_name, record.destination_version or ""), SECRET_A
        )

    def test_lost_write_response_converges_on_one_version(self) -> None:
        self.capture()
        self.vault.lose_next_put_response = True
        first = self.sync.run("eus2", self.regs())
        self.assertEqual((first.failed, self.record().state), (1, "pending"))
        self.assertEqual(self.record().last_error, "destination_ClientVaultUnavailableError")
        self.assertEqual(self.sync.run("eus2").synced, 1)
        self.assertEqual(self.vault.writes, 1)

    def test_outages_retry_and_blocked_versions_are_not_substituted(self) -> None:
        self.capture()
        self.secrets.fail_reads = True
        self.assertEqual(self.sync.run("eus2", self.regs()).errors, ["eus2/a: source_unavailable"])
        self.secrets.fail_reads = False
        self.vault.unavailable = True
        self.assertEqual(self.sync.run("eus2").failed, 1)
        self.vault.unavailable = False
        self.secrets.disable_secret("whsec-test-eus2-a")
        with self.assertLogs("foundry_batch.vaultsync.service", "ERROR"):
            self.assertEqual(self.sync.run("eus2").blocked, 1)
        self.assertEqual((self.record().state, self.record().attempts), ("blocked", 2))
        self.assertEqual(self.vault.names(), [])

    def test_unknown_source_version_is_blocked_at_capture(self) -> None:
        reg = self.capture()
        self.control.save_registration(replace(reg, secret_version="unknown"))
        self.sync.run("eus2", self.regs())
        self.assertEqual(
            (self.record().state, self.record().last_error), ("blocked", "source_version_unknown")
        )

    def test_readback_mismatch_is_never_ready(self) -> None:
        self.capture()
        original = self.vault.read
        self.vault.read = lambda name, version: "tampered"  # type: ignore[method-assign]
        self.assertEqual(self.sync.run("eus2", self.regs()).errors, ["eus2/a: readback_mismatch"])
        self.assertFalse(self.sync.ready("eus2", ["wh_1"]))
        self.vault.read = original  # type: ignore[method-assign]
        self.assertEqual(self.sync.run("eus2").synced, 1)

    def test_newer_destination_generation_supersedes(self) -> None:
        self.capture()
        self.vault.put(
            "batch-test-eus2-a", "x", request_token="other", metadata={"generation": "9"}
        )
        self.assertEqual(self.sync.run("eus2", self.regs()).superseded, 1)
        self.assertEqual(self.record().state, "superseded")
        self.assertEqual((_generation({"generation": "x"}), _generation(None)), (0, 0))


class RotationTests(Harness):
    def test_recovered_write_is_unsettled_when_no_longer_current(self) -> None:
        registration = self.capture()
        self.vault.lose_next_put_response = True
        self.assertEqual(self.sync.run("eus2").failed, 1)
        original = self.record()
        self.vault.put(
            original.destination_name,
            SECRET_B,
            request_token="unrelated",
            metadata={"generation": "2"},
        )

        stats = self.sync.run("eus2")

        self.assertEqual(stats.errors, ["eus2/a: destination_version_not_current"])
        self.assertFalse(self.record().converged)
        self.assertFalse(self.sync.ready("eus2", [registration.endpoint_id]))

    def test_retired_destination_version_is_not_ready(self) -> None:
        registration = self.capture()
        self.sync.run("eus2")
        synced = self.record()
        assert synced.destination_version is not None
        self.assertTrue(self.sync.ready("eus2", [registration.endpoint_id]))

        self.vault.retire(synced.destination_name, synced.destination_version)

        self.assertFalse(self.sync.ready("eus2", [registration.endpoint_id]))
        self.assertEqual(self.sync.run("eus2").errors, ["eus2/a: verification_drift"])
        self.assertEqual(self.record(), synced)

    def test_changed_source_version_is_not_ready_before_sync(self) -> None:
        registration = self.capture()
        self.sync.run("eus2")
        replacement = self.capture(value=SECRET_B)
        self.assertNotEqual(replacement.secret_version, registration.secret_version)

        self.assertFalse(self.sync.ready("eus2", [registration.endpoint_id]))

    def test_delayed_snapshot_cannot_undo_recapture_of_same_endpoint(self) -> None:
        original = self.capture()
        self.sync.run("eus2")
        replacement = self.capture(value=SECRET_B)
        self.sync.run("eus2")
        verified = self.record()
        self.assertEqual(verified.source_version, replacement.secret_version)

        self.sync.run("eus2", [original])

        self.assertEqual(self.record(), verified)
        self.assertTrue(self.sync.ready("eus2", [replacement.endpoint_id]))

    def test_delayed_snapshot_cannot_replace_newer_capture(self) -> None:
        old = self.capture()
        stale = self.regs()
        self.sync.run("eus2", stale)
        self.end(old)
        replacement = self.capture(endpoint="wh_2", value=SECRET_B, at=timedelta(minutes=1))
        self.sync.run("eus2", self.regs())
        verified = self.record()

        self.sync.run("eus2", stale)

        self.assertEqual(self.record(), verified)
        self.assertTrue(self.sync.ready("eus2", [replacement.endpoint_id]))
        self.assertEqual(self.enabled(), [verified.destination_version])

    def test_rotation_then_retirement_propagates(self) -> None:
        a = self.capture("a", "wh_1", SECRET_A)
        self.sync.run("eus2", self.regs())
        self.capture("b", "wh_2", SECRET_B)
        self.sync.run("eus2", self.regs())
        self.assertTrue(self.sync.ready("eus2", ["wh_1", "wh_2"]))
        self.control.save_registration(replace(a, active_until=T0 + timedelta(hours=1)))
        self.assertEqual(self.sync.run("eus2", self.regs()).retired, 1)
        old = self.record("a")
        self.assertEqual(old.state, "retired")
        self.assertIsNone(self.vault.read(old.destination_name, old.destination_version or ""))
        self.assertTrue(self.sync.ready("eus2", ["wh_2"]))
        self.assertFalse(self.sync.record_retirement("eus2", "a", "wh_1"))  # already retired
        self.assertFalse(self.sync.record_retirement("eus2", "a", "wh_other"))
        self.assertFalse(self.sync.record_retirement("eus2", "z", "wh_1"))

    def test_retired_before_sync_retires_a_possibly_committed_write(self) -> None:
        a = self.capture()
        self.vault.lose_next_put_response = True
        self.sync.run("eus2", self.regs())
        self.control.save_registration(replace(a, active_until=T0))
        self.sync.run("eus2", self.regs())
        name = self.record().destination_name
        self.assertEqual(self.record().state, "retired")
        self.assertFalse(self.vault.current(name).enabled)  # type: ignore[union-attr]
        self.vault.unavailable = True
        self.sync.record_retirement("eus2", "a", "wh_1")  # no-op on retired

    def test_slot_reuse_retires_the_previous_generation_first(self) -> None:
        a = self.capture("a", "wh_1", SECRET_A)
        self.sync.run("eus2", self.regs())
        first = self.record("a")
        # Retired and reused before any sync run saw the retirement (a lost trigger).
        self.control.save_registration(replace(a, active_until=T0))
        self.sync.record_capture(self.capture("a", "wh_3", SECRET_B))
        self.vault.unavailable = True
        self.assertEqual(self.sync.run("eus2").failed, 1)  # previous must go first
        self.vault.unavailable = False
        stats = self.sync.run("eus2")
        self.assertEqual((stats.retired, stats.synced), (1, 1))
        self.assertIsNone(self.vault.read(first.destination_name, first.destination_version or ""))
        new = self.record("a")
        self.assertEqual((new.endpoint_id, new.generation, new.previous_version), ("wh_3", 2, None))
        self.assertTrue(self.sync.ready("eus2", ["wh_3"]))

    def test_carried_previous_survives_another_reuse(self) -> None:
        self.capture("a", "wh_1")
        self.sync.run("eus2", self.regs())
        self.sync.record_capture(self.capture("a", "wh_2", SECRET_B))
        record = self.record("a")
        retired = replace(record, state="retired")
        entity = self.store.get("batchcontrol", "test|vaultsync", "eus2|a")
        self.store.submit(
            "batchcontrol",
            [
                __import__("foundry_batch.ports", fromlist=["TxOp"]).TxOp(
                    "update", replace(entity, data=retired.to_entity())
                )
            ],
        )  # type: ignore[arg-type]
        third = self.sync.record_capture(self.capture("a", "wh_4", SECRET_A))
        self.assertEqual(third.previous_version, record.previous_version)

    def test_retirement_failure_is_retried(self) -> None:
        a = self.capture()
        self.sync.run("eus2", self.regs())
        self.control.save_registration(replace(a, active_until=T0))
        self.vault.unavailable = True
        self.assertEqual(self.sync.run("eus2", self.regs()).failed, 1)
        self.assertEqual(self.record().state, "retire_pending")
        self.vault.unavailable = False
        self.assertEqual(self.sync.run("eus2").retired, 1)

    def test_concurrent_writer_is_skipped(self) -> None:
        self.capture()
        self.sync.reconcile(self.regs())
        original = self.sync._save

        def conflict(record: SyncRecord, etag: str | None) -> None:
            raise ConcurrencyConflictError("moved")

        self.sync._save = conflict  # type: ignore[method-assign]
        self.assertEqual(self.sync.run("eus2").synced, 0)
        self.sync._save = original  # type: ignore[method-assign]
        self.assertEqual(self.sync.run("eus2").synced, 1)


class ClientVaultAndMemoryTests(unittest.TestCase):
    def test_emulated_vault_semantics(self) -> None:
        vault = EmulatedClientVault()
        version = vault.put("n", "s3cr3t-value", request_token="t", metadata={"a": "b"})
        self.assertEqual(vault.put("n", "s3cr3t-value", request_token="t", metadata={}), version)
        with self.assertRaises(ValueError):
            vault.put("n", "other", request_token="t", metadata={})
        self.assertIsNone(vault.find("n", "missing"))
        self.assertIsNone(vault.current("none"))
        self.assertIsNone(vault.read("n", "nope"))
        vault.retire("n", version)
        self.assertIsNone(vault.read("n", version))
        self.assertNotIn("s3cr3t", repr(vault._secrets["n"][0]))  # value hidden from reprs
        vault.unavailable = True
        for call in (
            lambda: vault.put("n", "v", request_token="x", metadata={}),
            lambda: vault.current("n"),
            lambda: vault.retire("n", version),
        ):
            with self.assertRaises(ClientVaultUnavailableError):
                call()

    def test_memory_secrets_exact_versions(self) -> None:
        secrets = MemorySecrets()
        secrets.set_secret("s", "one", tags={})
        secrets.set_secret("s", "two", tags={})
        self.assertEqual(
            (secrets.get_secret_version("s", "v1"), secrets.get_secret_version("s", "v2")),
            ("one", "two"),
        )
        for bad in ("v3", "x1", "v", "v0"):
            self.assertIsNone(secrets.get_secret_version("s", bad))
        secrets.disable_secret("s")
        self.assertIsNone(secrets.get_secret_version("s", "v2"))

    def test_record_round_trip_and_unknown_state(self) -> None:
        record = SyncRecord("eus2", "a", "wh", "src", "v1", 1, "dst", captured_at=T0)
        self.assertEqual(SyncRecord.from_entity(record.to_entity()), record)
        self.assertEqual(len(record.request_token), 48)
        data = {**record.to_entity(), "state": "weird"}
        self.assertEqual(SyncRecord.from_entity(data).state, "pending")


class RegistrarGateTests(unittest.TestCase):
    def test_retire_waits_for_the_client_vault_unless_forced(self) -> None:
        from foundry_batch.emulator.local import LocalStack
        from foundry_batch.registration.registrar import RegistrationError

        stack = LocalStack()
        stack.register_webhooks()
        stack.registrar.rotate("eus2", stack.receiver_url("eus2"))
        calls: list[tuple[str, list[str]]] = []

        def gate(resource: str, endpoints: list[str]) -> bool:
            calls.append((resource, list(endpoints)))
            return False

        stack.registrar.sync_gate = gate
        with self.assertRaises(RegistrationError):
            stack.registrar.retire("eus2", "a")
        self.assertEqual(calls[0][0], "eus2")
        self.assertEqual(len(calls[0][1]), 1)
        self.assertEqual(stack.registrar.retire("eus2", "a", force=True).action, "retired")


class GenerationTests(Harness):
    """S7: retired destination history never blocks a capture, and superseded recovers."""

    def test_equal_generation_downstream_is_not_superseded(self) -> None:
        self.capture()
        self.vault.put(
            "batch-test-eus2-a", "x", request_token="other", metadata={"generation": "1"}
        )
        stats = self.sync.run("eus2", self.regs())
        self.assertEqual((stats.superseded, stats.synced), (0, 1))
        record = self.record()
        self.assertEqual((record.state, record.generation), ("synced", 2))
        self.assertEqual(
            self.vault.current(record.destination_name).version, record.destination_version
        )  # type: ignore[union-attr]

    def test_superseded_recovers_once_the_newer_generation_is_retired(self) -> None:
        self.capture()
        newer = self.vault.put(
            "batch-test-eus2-a", "x", request_token="o", metadata={"generation": "9"}
        )
        self.sync.run("eus2", self.regs())
        self.assertEqual(
            (self.record().state, self.record().last_error),
            ("superseded", "newer_generation_downstream"),
        )
        self.assertFalse(self.record().converged)
        before = self.store.get("batchcontrol", "test|vaultsync", "eus2|a")
        self.assertEqual(self.sync.run("eus2").superseded, 1)  # re-evaluated, still superseded
        self.assertEqual(
            self.store.get("batchcontrol", "test|vaultsync", "eus2|a"), before
        )  # no rewrite
        self.vault.retire("batch-test-eus2-a", newer)
        stats = self.sync.run("eus2")
        self.assertEqual((stats.synced, stats.superseded), (1, 0))
        record = self.record()
        self.assertEqual((record.state, record.generation, record.last_error), ("synced", 10, None))
        self.assertTrue(self.sync.ready("eus2", ["wh_1"]))

    def test_redeploy_with_retired_history_writes_above_it(self) -> None:
        name = "batch-test-eus2-a"
        for generation in ("1", "2"):
            old = self.vault.put(
                name, "old", request_token=f"t{generation}", metadata={"generation": generation}
            )
            self.vault.retire(name, old)
        self.capture("a", "wh_new")  # fresh ledger, generation restarts at 1
        self.assertEqual(self.sync.run("eus2", self.regs()).synced, 1)
        record = self.record()
        self.assertEqual((record.state, record.generation), ("synced", 3))
        self.assertEqual(self.vault.current(name).metadata["generation"], "3")  # type: ignore[union-attr]
        self.assertTrue(self.sync.ready("eus2", ["wh_new"]))

    def test_raised_generation_survives_a_crash_right_after_the_write(self) -> None:
        name = "batch-test-eus2-a"
        old = self.vault.put(name, "old", request_token="t", metadata={"generation": "4"})
        self.vault.retire(name, old)
        self.capture()
        real = self.vault.put

        def put_then_crash(*args: object, **kwargs: object) -> str:
            real(*args, **kwargs)  # type: ignore[arg-type]
            raise Crash

        self.vault.put = put_then_crash  # type: ignore[method-assign]
        with self.assertRaises(Crash):
            self.sync.run("eus2", self.regs())
        del self.vault.put
        self.assertEqual(self.record().generation, 5)  # saved before the write
        self.assertEqual(self.sync.run("eus2").synced, 1)  # found by token, not superseded
        self.assertEqual(len(self.vault._secrets[name]), 2)

    def test_failed_recovery_from_superseded_is_pending_not_superseded(self) -> None:
        self.capture()
        newer = self.vault.put(
            "batch-test-eus2-a", "x", request_token="o", metadata={"generation": "3"}
        )
        self.sync.run("eus2", self.regs())
        self.vault.retire("batch-test-eus2-a", newer)
        self.vault.lose_next_put_response = True
        self.assertEqual(self.sync.run("eus2").failed, 1)
        record = self.record()
        self.assertEqual((record.state, record.generation), ("pending", 4))
        self.assertEqual(self.sync.run("eus2").synced, 1)

    def test_raised_generation_is_saved_before_the_write(self) -> None:
        name = "batch-test-eus2-a"
        old = self.vault.put(name, "old", request_token="t", metadata={"generation": "4"})
        self.vault.retire(name, old)
        self.capture()
        self.vault.lose_next_put_response = True
        self.assertEqual(self.sync.run("eus2", self.regs()).failed, 1)
        self.assertEqual((self.record().state, self.record().generation), ("pending", 5))
        self.assertEqual(self.sync.run("eus2").synced, 1)
        self.assertEqual(
            len(self.vault._secrets[name]), 2
        )  # the lost write was found, not repeated


class Crash(BaseException):
    """A process crash: escapes every ``except Exception``."""


class OwedTests(Harness):
    """S8: every generation a reused slot still owes is retired downstream."""

    def test_two_reuses_before_a_successful_run_retire_both_owed_generations(self) -> None:
        r1 = self.capture("a", "wh_1", SECRET_A)
        self.sync.run("eus2", self.regs())
        dv1 = self.record().destination_version or ""
        self.end(r1)
        self.vault.unavailable = True
        r2 = self.capture("a", "wh_2", SECRET_B)
        self.sync.run("eus2", self.regs())
        self.end(r2)
        self.capture("a", "wh_3", SECRET_A)
        self.sync.run("eus2", self.regs())
        self.assertEqual([o.version for o in self.record().owed], [dv1, None])
        self.vault.unavailable = False
        stats = self.sync.run("eus2", self.regs())
        self.assertEqual((stats.retired, stats.synced), (2, 1))
        self.assertIsNone(self.vault.read("batch-test-eus2-a", dv1))
        record = self.record()
        self.assertEqual((record.endpoint_id, record.owed, record.converged), ("wh_3", (), True))

    def test_owed_write_known_only_by_token_is_found_and_retired(self) -> None:
        r1 = self.capture("a", "wh_1", SECRET_A)
        self.vault.lose_next_put_response = True  # committed; the ledger never learns the version
        self.sync.run("eus2", self.regs())
        committed = self.enabled()
        self.end(r1)
        self.sync.record_capture(self.capture("a", "wh_2", SECRET_B))
        self.assertEqual(self.record().owed[0].version, None)
        self.assertEqual(self.sync.run("eus2").retired, 1)
        self.assertEqual(len(committed), 1)
        self.assertIsNone(self.vault.read("batch-test-eus2-a", committed[0]))

    def test_partial_owed_failure_keeps_the_rest_and_recovers(self) -> None:
        self.capture()
        self.sync.reconcile(self.regs())
        dv_a = self.vault.put("batch-test-eus2-a", "a", request_token="ta", metadata={})
        dv_b = self.vault.put("batch-test-eus2-a", "b", request_token="tb", metadata={})
        owed = (OwedRetirement(dv_a, "ta"), OwedRetirement(dv_b, "tb"))
        self.overwrite(replace(self.record(), owed=owed))
        real, calls = self.vault.retire, []

        def flaky(name: str, version: str) -> None:
            calls.append(version)
            if version == dv_b and calls.count(dv_b) == 1:
                raise ClientVaultUnavailableError("blip")
            real(name, version)

        self.vault.retire = flaky  # type: ignore[method-assign]
        stats = self.sync.run("eus2")
        self.assertEqual((stats.retired, stats.failed, stats.synced), (1, 1, 0))
        record = self.record()
        self.assertEqual(
            (record.owed, record.last_error),
            ((OwedRetirement(dv_b, "tb"),), "destination_ClientVaultUnavailableError"),
        )
        stats = self.sync.run("eus2")
        self.assertEqual((stats.retired, stats.synced), (1, 1))
        self.assertEqual((self.record().owed, self.record().last_error), ((), None))
        self.assertEqual(calls, [dv_a, dv_b, dv_b])

    def test_owed_on_a_blocked_record_keeps_the_block_reason(self) -> None:
        reg = self.capture()
        self.sync.run("eus2", self.regs())
        self.control.save_registration(replace(reg, endpoint_id="wh_2", secret_version="unknown"))
        self.end(reg)
        self.sync.run("eus2", self.regs())
        record = self.record()
        self.assertEqual(
            (record.state, record.last_error, record.owed),
            ("blocked", "source_version_unknown", ()),
        )

    def test_retire_downstream_uses_the_version_else_the_token(self) -> None:
        name = "batch-test-eus2-a"
        by_token = self.vault.put(name, "x", request_token="tok", metadata={})
        recorded = self.vault.put(name, "y", request_token="other", metadata={})
        self.sync._retire_downstream(name, recorded, "tok")  # a confirmed version wins
        self.assertEqual(self.enabled(), [by_token])
        self.sync._retire_downstream(name, None, "tok")  # never confirmed: found by token
        self.assertEqual(self.enabled(), [])
        self.vault.unavailable = True
        self.sync._retire_downstream(name, None, None)  # nothing to look up, no call made

    def test_record_round_trip_with_owed_and_damaged_owed(self) -> None:
        record = SyncRecord(
            "eus2",
            "a",
            "wh",
            "src",
            "v1",
            2,
            "dst",
            owed=(OwedRetirement("dv1", "t1"), OwedRetirement(None, "t2")),
        )
        self.assertEqual(SyncRecord.from_entity(record.to_entity()), record)
        self.assertEqual((record.previous_version, record.previous_token), (None, "t2"))
        for damaged in ("not json", "{}", json.dumps([["", ""], ["x"], "y"])):
            with self.subTest(damaged=damaged):
                self.assertEqual(
                    SyncRecord.from_entity({**record.to_entity(), "owed": damaged}).owed, ()
                )


class LeaseTests(Harness):
    """S5: one worker per resource; a write that loses a race is never left enabled."""

    def put_hook(self, during: object) -> None:
        real = self.vault.put

        def put(name: str, value: str, *, request_token: str, metadata: Mapping[str, str]) -> str:
            during()  # type: ignore[operator]
            return real(name, value, request_token=request_token, metadata=metadata)

        self.vault.put = put  # type: ignore[method-assign]

    def test_a_retirement_waits_for_the_worker_holding_the_lease(self) -> None:
        reg = self.capture()
        self.sync.reconcile(self.regs())
        other = self.worker("w2")
        seen: list[tuple[list[str], int]] = []

        def during() -> None:
            self.end(reg)
            stats = other.run("eus2", self.regs())
            seen.append((stats.skipped, stats.retired))

        self.put_hook(during)
        with self.assertLogs("foundry_batch.vaultsync.service", "INFO") as logs:
            self.assertEqual(self.sync.run("eus2").synced, 1)
        self.assertEqual(seen, [(["eus2"], 0)])
        self.assertIn("vaultsync.skipped", "\n".join(logs.output))
        self.assertEqual(other.run("eus2", self.regs()).retired, 1)  # lease released
        self.assertEqual((self.record().state, self.enabled()), ("retired", []))

    def test_lost_race_after_the_put_retires_the_written_version(self) -> None:
        reg = self.capture()
        self.sync = self.worker("w1", lease_seconds=60)
        self.sync.reconcile(self.regs())
        other = self.worker("w2")
        seen: list[int] = []

        def during() -> None:  # w1's lease expires while its put is in flight
            self.clock.advance(61)
            self.end(reg)
            seen.append(other.run("eus2", self.regs()).retired)

        self.put_hook(during)
        with self.assertLogs("foundry_batch.vaultsync.service", "WARNING") as logs:
            stats = self.sync.run("eus2")
        self.assertEqual(seen, [1])  # w2 took over and retired before the write committed
        self.assertEqual((stats.synced, stats.retired), (0, 1))
        self.assertIn("vaultsync.orphan_retired", "\n".join(logs.output))
        self.assertEqual((self.record().state, self.enabled()), ("retired", []))

    def test_lost_race_keeps_a_write_the_ledger_still_tracks(self) -> None:
        self.capture()
        self.sync.reconcile(self.regs())
        self.put_hook(lambda: self.overwrite(replace(self.record(), attempts=7)))
        self.assertEqual(self.sync.run("eus2").synced, 0)
        self.assertEqual(len(self.enabled()), 1)  # same capture: found by its token next run
        self.assertEqual(self.sync.run("eus2").synced, 1)
        self.assertEqual(self.vault.writes, 1)

    def test_lost_race_retire_failure_is_kept_as_owed(self) -> None:
        reg = self.capture()
        self.sync.reconcile(self.regs())

        def during() -> None:
            self.end(reg)
            self.overwrite(replace(self.record(), state="retired"))
            self.vault.retire = mock.Mock(side_effect=ClientVaultUnavailableError("down"))  # type: ignore[method-assign]

        self.put_hook(during)
        with self.assertLogs("foundry_batch.vaultsync.service", "WARNING") as logs:
            self.sync.run("eus2")
        self.assertIn("orphan_retire_ClientVaultUnavailableError", "\n".join(logs.output))
        [owed] = self.record().owed
        self.assertEqual(owed.token, self.record().request_token)
        self.assertEqual(self.sync.run("eus2").failed, 1)  # still down: kept, error recorded
        self.assertEqual(self.record().last_error, "destination_ClientVaultUnavailableError")
        del self.vault.retire  # back to the real method
        self.assertEqual(self.sync.run("eus2").retired, 1)
        self.assertEqual(self.enabled(), [])
        self.assertTrue(self.record().converged)  # the owed-retire error is cleared

    def test_lease_is_released_when_a_run_fails(self) -> None:
        self.capture()
        with (
            mock.patch.object(self.sync, "_run_resource", side_effect=StoreUnavailableError("x")),
            self.assertRaises(StoreUnavailableError),
        ):
            self.sync.run("eus2", self.regs())
        self.assertEqual(self.worker("w2").run("eus2", self.regs()).skipped, [])

    def test_run_without_a_resource_takes_one_lease_per_resource(self) -> None:
        self.capture("a", "wh_1", resource="eus2")
        self.capture("a", "wh_9", resource="swc")
        ControlStore(self.store, "batchcontrol", "test", self.clock).acquire(
            "vaultsync|swc", "w9", 300
        )
        stats = self.sync.run(None, [*self.regs("eus2"), *self.regs("swc")])
        self.assertEqual((stats.synced, stats.skipped), (1, ["swc"]))
        self.assertEqual([r.resource_key for r in self.sync.status()], ["eus2"])
        self.clock.advance(301)
        self.assertEqual(self.sync.run(None, self.regs("swc")).synced, 1)
        self.capture("b", "wh_5", SECRET_B)
        self.sync.reconcile(self.regs())  # intent recorded, then the trigger was lost
        self.assertEqual(self.sync.run().synced, 1)  # keys also come from the ledger


class ReconcileTests(Harness):
    def test_reconcile_counts_each_change(self) -> None:
        a = self.capture("a", "wh_1")
        self.assertEqual(self.sync.reconcile(self.regs()), 1)
        self.assertEqual(self.sync.reconcile(self.regs()), 0)
        ended_b = self.capture("b", "wh_2")
        self.end(ended_b)  # ended before any capture: no ledger row is created for it
        self.assertEqual(self.sync.reconcile(self.regs()), 0)
        self.assertEqual([r.slot for r in self.sync.status("eus2")], ["a"])
        self.end(a)
        self.assertEqual(self.sync.reconcile(self.regs()), 1)
        self.assertEqual(self.record().state, "retire_pending")
        self.assertEqual(self.sync.reconcile(self.regs()), 0)
        self.capture("a", "wh_3")  # reuse: no retirement left to record, one new capture
        self.assertEqual(self.sync.reconcile(self.regs()), 1)
        a3 = next(r for r in self.regs() if r.endpoint_id == "wh_3")
        self.end(a3)
        self.capture("a", "wh_4")  # retirement and capture in one pass
        self.assertEqual(self.sync.reconcile(self.regs()), 2)

    def test_a_stale_active_registration_never_takes_the_slot_back(self) -> None:
        self.capture("a", "wh_1", SECRET_A)
        self.sync.run("eus2", self.regs())
        old = self.record()
        self.capture("a", "wh_3", SECRET_B, at=timedelta(hours=1))  # wh_1's record never ended
        for order in (list, lambda regs: list(reversed(regs))):
            with (
                self.subTest(order=order),
                self.assertLogs("foundry_batch.vaultsync.service", "WARNING") as logs,
            ):
                for _ in range(3):
                    self.sync.run("eus2", order(self.regs()))
                record = self.record()
                self.assertEqual(
                    (record.endpoint_id, record.generation, record.state), ("wh_3", 2, "synced")
                )
                self.assertIn("vaultsync.stale_registration_ignored", "\n".join(logs.output))
        self.assertIsNone(self.vault.read(old.destination_name, old.destination_version or ""))
        self.assertEqual(len(self.enabled()), 1)

    def test_same_start_time_is_decided_by_endpoint_id_in_any_order(self) -> None:
        self.capture("a", "wh_1")
        self.capture("a", "wh_2")
        for regs in (self.regs(), list(reversed(self.regs()))):
            with (
                self.subTest(first=regs[0].endpoint_id),
                self.assertLogs("foundry_batch.vaultsync.service", "WARNING"),
            ):
                self.sync.reconcile(regs)
                self.assertEqual(self.record().endpoint_id, "wh_2")

    def test_a_conflicting_record_does_not_stop_the_others(self) -> None:
        self.capture("a", "wh_1")
        self.capture("b", "wh_2", SECRET_B)
        self.sync.reconcile(self.regs())
        real = self.sync._save

        def conflict_on_a(record: SyncRecord, etag: str | None) -> str | None:
            if record.slot == "a":
                raise ConcurrencyConflictError("moved")
            return real(record, etag)

        with mock.patch.object(self.sync, "_save", side_effect=conflict_on_a):
            stats = self.sync.run("eus2")
        self.assertEqual(stats.synced, 1)
        self.assertEqual((self.record("a").state, self.record("b").state), ("pending", "synced"))

    def test_status_is_scoped_to_the_exact_resource_key(self) -> None:
        self.capture("a", "wh_1", resource="eus")
        self.capture("a", "wh_2", resource="eus2")
        self.capture("a", "wh_3", resource="swc")
        self.sync.reconcile([*self.regs("eus"), *self.regs("eus2"), *self.regs("swc")])
        self.assertEqual([r.endpoint_id for r in self.sync.status("eus")], ["wh_1"])
        self.assertEqual([r.endpoint_id for r in self.sync.status("eus2")], ["wh_2"])
        self.assertEqual(len(self.sync.status()), 3)

    def test_attempts_count_every_try(self) -> None:
        self.capture()
        self.sync.run("eus2", self.regs())
        self.assertEqual(self.record().attempts, 1)
        self.capture("b", "wh_2", SECRET_B)
        self.vault.unavailable = True
        with self.assertLogs("foundry_batch.vaultsync.service", "WARNING") as logs:
            self.sync.run("eus2", self.regs())
            self.sync.run("eus2")
        failed = [r for r in logs.records if getattr(r, "batch_event", "") == "vaultsync.failed"]
        self.assertEqual([r.attempts for r in failed], [1, 2])  # type: ignore[attr-defined]
        self.vault.unavailable = False
        self.sync.run("eus2")
        self.assertEqual(self.record("b").attempts, 3)


class ReadinessTests(Harness):
    def test_ready_requires_all_endpoints_and_propagates_unavailable_store(self) -> None:
        self.capture()
        self.assertFalse(self.sync.ready("eus2", ["wh_1"]))
        self.sync.run("eus2", self.regs())
        self.assertTrue(self.sync.ready("eus2", ["wh_1"]))
        self.assertFalse(self.sync.ready("eus2", ["wh_1", "wh_2"]))
        self.assertFalse(self.sync.ready("eus2", []))
        with (
            mock.patch.object(self.store, "query", side_effect=StoreUnavailableError("down")),
            self.assertRaises(StoreUnavailableError),
        ):
            self.sync.ready("eus2", ["wh_1"])

    def test_converged_needs_a_final_state_no_error_and_nothing_owed(self) -> None:
        base = SyncRecord("eus2", "a", "wh", "src", "v1", 1, "dst", state="synced")
        self.assertTrue(base.converged)
        self.assertTrue(replace(base, state="retired").converged)
        for record in (
            replace(base, state="pending"),
            replace(base, state="blocked"),
            replace(base, state="superseded"),
            replace(base, state="retire_pending"),
            replace(base, last_error="destination_X"),
            replace(base, owed=(OwedRetirement(None, "t"),)),
        ):
            with self.subTest(record=record):
                self.assertFalse(record.converged)


if __name__ == "__main__":
    unittest.main()
