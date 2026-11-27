import io
import json
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import replace
from pathlib import Path
from unittest import mock

from foundry_batch import cli
from foundry_batch.composition import Runtime, build_runtime, memory_adapters
from foundry_batch.domain.models import JobRecord
from foundry_batch.emulator.client_vault import EmulatedClientVault
from foundry_batch.vaultsync.service import VaultSyncService
from tests.support import FakeEndpoints, FakeProvider, FakeProviderFactory, clock, make_env

BASE_URL = "https://f.example/api/webhooks"


class CliTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = clock()
        self.provider = FakeProvider(self.clock)
        self.endpoints = FakeEndpoints()
        self.runtime = None

        def factory(settings, args):  # type: ignore[no-untyped-def]
            if self.runtime is None:
                self.runtime = build_runtime(
                    settings,
                    memory_adapters(
                        FakeProviderFactory(
                            {"eus2": self.provider, "swc": FakeProvider(self.clock)}
                        ),
                        lambda _: self.endpoints,
                    ),
                    self.clock,
                )
            return self.runtime

        self.factory = factory
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.settings_file = Path(self.tmp.name, "settings.json")
        self.settings_file.write_text(json.dumps(make_env()), encoding="utf-8")

    def run_cli(self, *argv: str) -> tuple[int, object]:
        out = io.StringIO()
        with (
            redirect_stdout(out),
            redirect_stderr(io.StringIO()),
            mock.patch.dict("os.environ", {}, clear=False),
        ):
            code = cli.main(
                ["--settings", str(self.settings_file), *argv],
                self.factory,
                configure_logging=False,
            )
        text = out.getvalue()
        try:
            return code, json.loads(text)
        except json.JSONDecodeError:
            return code, text

    def test_register_lifecycle(self) -> None:
        base = ["register"]
        code, out = self.run_cli(
            *base,
            "ensure",
            "--resource",
            "eus2",
            "--receiver-base-url",
            "https://f.example/api/webhooks/",
        )
        self.assertEqual(code, 0)
        self.assertEqual(out[0]["action"], "created")  # type: ignore[index]
        self.assertTrue(out[0]["secret_name"].startswith("whsec-test-eus2"))  # type: ignore[index]
        self.assertEqual(
            next(iter(self.endpoints.endpoints.values())).url, "https://f.example/api/webhooks/eus2"
        )
        self.assertEqual(self.run_cli(*base, "health", "--resource", "eus2")[0], 0)
        self.assertEqual(
            self.run_cli(
                *base,
                "rotate",
                "--resource",
                "eus2",
                "--receiver-base-url",
                "https://f.example/api/webhooks",
            )[0],
            0,
        )
        self.assertEqual(self.run_cli(*base, "retire", "--resource", "eus2", "--slot", "a")[0], 0)
        self.assertEqual(
            self.run_cli(*base, "remove", "--resource", "eus2", "--confirm", "x")[0], 2
        )
        self.assertEqual(
            self.run_cli(*base, "remove", "--resource", "eus2", "--confirm", "eus2")[0], 0
        )
        self.assertEqual(self.run_cli(*base, "health", "--resource", "eus2")[0], 1)

    def test_relay_secret_is_created_once_and_never_printed(self) -> None:
        code, out = self.run_cli("relay-secret", "ensure")
        self.assertEqual((code, out["action"]), (0, "created"))  # type: ignore[index]
        assert self.runtime is not None
        value = self.runtime.relay_secret()
        assert value is not None
        self.assertNotIn(value, json.dumps(out))
        self.assertEqual(self.run_cli("relay-secret", "ensure")[1]["action"], "unchanged")  # type: ignore[index]

    def test_operational_reports(self) -> None:
        self.run_cli("quarantine", "list")
        assert self.runtime is not None
        self.runtime.registry.create_job(
            JobRecord("eus2", "b1", "test", deployment="gpt-batch", token_estimate=10)
        )
        self.assertEqual(self.run_cli("queued-tokens")[1][0]["estimated_tokens"], 10)  # type: ignore[index]
        self.assertEqual(self.run_cli("indicators", "--window", "7d")[0], 0)
        code, out = self.run_cli("reconcile", "--resource", "eus2")
        self.assertEqual((code, out[0]["resource_key"]), (0, "eus2"))  # type: ignore[index]
        self.assertEqual(self.run_cli("reconcile")[0], 0)
        self.assertEqual(self.run_cli("quarantine", "list")[1], [])
        self.provider.add("completed")
        code, out = self.run_cli("backfill", "--resource", "eus2")
        self.assertEqual((code, out["written"]), (0, 1))  # type: ignore[index]

    def test_replay_commands_default_to_dry_run(self) -> None:
        code, out = self.run_cli("replay", "outbox", "--operator", "t", "--reason", "r")
        self.assertEqual((code, out["dry_run"]), (0, True))  # type: ignore[index]
        self.run_cli("replay", "outbox", "--operator", "t", "--reason", "r")  # builds runtime
        assert self.runtime is not None
        poison = self.runtime.adapters.queue_admin.queues["batch-events-poison"]  # type: ignore[attr-defined]
        poison.send("stuck message")
        code, out = self.run_cli(
            "replay", "poison", "--queue", "incoming", "--operator", "t", "--reason", "r"
        )
        self.assertEqual((code, out), (0, {"selected": 1, "moved": 0, "dry_run": True}))
        code, out = self.run_cli(
            "replay",
            "poison",
            "--queue",
            "incoming",
            "--operator",
            "t",
            "--reason",
            "r",
            "--execute",
        )
        self.assertEqual((code, out), (0, {"selected": 1, "moved": 1, "dry_run": False}))
        self.assertEqual(self.runtime.adapters.incoming_queue.sent[-1], "stuck message")  # type: ignore[attr-defined]

    def test_replay_rejects_ambiguous_time_bounds(self) -> None:
        for bad in ("2026-12-01", "yesterday", "2026-12-01T10:00:00"):
            with self.subTest(bad=bad), self.assertRaises(SystemExit) as ctx:
                self.run_cli("replay", "outbox", "--operator", "t", "--reason", "r", "--since", bad)
            self.assertEqual(ctx.exception.code, 2)
        code, out = self.run_cli(
            "replay",
            "outbox",
            "--operator",
            "t",
            "--reason",
            "r",
            "--since",
            "2026-12-01T10:00:00Z",
            "--until",
            "2026-12-02T10:00:00-03:00",
        )
        self.assertEqual((code, out["selected"]), (0, 0))  # type: ignore[index]

    def test_submit_and_recover(self) -> None:
        jsonl = Path(self.tmp.name, "in.jsonl")
        jsonl.write_text(
            json.dumps(
                {
                    "custom_id": "1",
                    "method": "POST",
                    "url": "/v1/chat/completions",
                    "body": {"model": "gpt-batch", "messages": []},
                }
            ),
            encoding="utf-8",
        )
        code, out = self.run_cli(
            "submit", "--resource", "eus2", "--deployment", "gpt-batch", "--file", str(jsonl)
        )
        self.assertEqual((code, out["status"]), (0, "validating"))  # type: ignore[index]
        self.assertEqual(
            self.run_cli("recover-submissions", "--resource", "eus2")[1], {"resolved": []}
        )

    def test_simulate_and_preflight_need_no_settings(self) -> None:
        durations = Path(self.tmp.name, "d.txt")
        durations.write_text("3600\n7200\n", encoding="utf-8")
        out = io.StringIO()
        with redirect_stdout(out):
            self.assertEqual(
                cli.main(["simulate", "--durations", str(durations)], configure_logging=False), 0
            )
        self.assertGreater(json.loads(out.getvalue())["reduction"], 0.5)
        target = Path(self.tmp.name, "pf.json")
        target.write_text(
            json.dumps(
                {
                    "tenant_id": "t",
                    "subscription_id": "s",
                    "resource_group": "rg",
                    "location": "eastus2",
                    "model_name": "m",
                    "model_version": "v",
                }
            ),
            encoding="utf-8",
        )
        with (
            mock.patch.object(cli, "run_preflight", return_value=[]),
            redirect_stdout(io.StringIO()),
        ):
            self.assertEqual(
                cli.main(["preflight", "--input", str(target)], configure_logging=False), 0
            )
            self.assertEqual(
                cli.main(
                    ["preflight", "--input", str(target), "--format", "json"],
                    configure_logging=False,
                ),
                0,
            )


class RegisterAndVaultSyncExitCodeTests(CliTests):
    """Documented exit codes: 0 done, 1 unhealthy/not converged, 2 usage, 3 pending, 4 refused."""

    def runtime_now(self, **settings: str) -> Runtime:
        self.settings_file.write_text(json.dumps(make_env(**settings)), encoding="utf-8")
        self.run_cli("quarantine", "list")  # builds the runtime
        assert self.runtime is not None
        return self.runtime

    def with_client_vault(self) -> EmulatedClientVault:
        vault = EmulatedClientVault()
        self.runtime_now().adapters.client_vault = vault
        return vault

    def ensure(self, *extra: str) -> tuple[int, object]:
        return self.run_cli(
            "register", "ensure", "--resource", "eus2", "--receiver-base-url", BASE_URL, *extra
        )

    def break_slot_a(self) -> None:
        assert self.runtime is not None
        secrets = self.runtime.adapters.secrets
        value = secrets.get_secret("whsec-test-eus2-a")
        assert value is not None
        secrets.set_secret("whsec-test-eus2-a", value, tags={"endpoint_id": "stale"})

    def test_refusals_exit_4_with_a_reason_instead_of_a_traceback(self) -> None:
        self.runtime_now()
        self.assertEqual(self.ensure()[0], 0)
        code, out = self.run_cli("register", "retire", "--resource", "eus2", "--slot", "a")
        self.assertEqual(
            (code, out["status"], out["reason"]), (4, "refused", "only_healthy_endpoint")
        )  # type: ignore[index]
        self.assertIn("only healthy endpoint", out["error"])  # type: ignore[index]
        assert self.runtime is not None
        self.runtime.control.acquire("register|eus2", "someone-else", 300)
        code, out = self.ensure()
        self.assertEqual((code, out["reason"]), (4, "lease_held"))  # type: ignore[index]

    def test_unknown_resource_is_a_usage_error(self) -> None:
        self.with_client_vault()
        for argv in (
            ("register", "health", "--resource", "eus3"),
            ("register", "retire", "--resource", "eus3", "--slot", "a"),
            ("vault-sync", "run", "--resource", "eus3"),
            ("vault-sync", "status", "--resource", "eus3"),
        ):
            with self.subTest(argv=argv):
                code, out = self.run_cli(*argv)
                self.assertEqual((code, out["error"]), (2, "unknown resource 'eus3'"))  # type: ignore[index]
                self.assertEqual(out["known"], ["eus2", "swc"])  # type: ignore[index]

    def test_gated_repair_is_pending_then_completes(self) -> None:
        self.with_client_vault()
        self.ensure()
        self.assertEqual(self.run_cli("vault-sync", "run", "--resource", "eus2")[0], 0)
        old = next(iter(self.endpoints.endpoints))
        self.break_slot_a()
        code, out = self.ensure("--repair")
        self.assertEqual(code, 3)
        self.assertEqual([o["action"] for o in out], ["created", "repair_pending"])  # type: ignore[union-attr]
        self.assertIn(old, self.endpoints.endpoints)
        self.assertEqual(self.run_cli("vault-sync", "run", "--resource", "eus2")[0], 0)
        code, out = self.ensure("--repair")
        self.assertEqual((code, out[0]["action"], out[0]["slot"]), (0, "repaired", "b"))  # type: ignore[index]
        self.assertNotIn(old, self.endpoints.endpoints)
        self.assertEqual(self.run_cli("vault-sync", "run", "--resource", "eus2")[0], 0)

    def test_forced_repair_skips_the_gate(self) -> None:
        self.with_client_vault()
        self.ensure()
        old = next(iter(self.endpoints.endpoints))
        self.break_slot_a()
        code, out = self.ensure("--repair", "--force")
        self.assertEqual((code, [o["action"] for o in out]), (0, ["repaired"]))  # type: ignore[union-attr]
        self.assertNotIn(old, self.endpoints.endpoints)

    def test_vault_sync_required_gates_retire_without_a_client_vault(self) -> None:
        rt = self.runtime_now(BATCH_VAULT_SYNC_REQUIRED="true")
        self.assertIsNone(rt.adapters.client_vault)
        self.ensure()
        self.run_cli("register", "rotate", "--resource", "eus2", "--receiver-base-url", BASE_URL)
        code, out = self.run_cli("register", "retire", "--resource", "eus2", "--slot", "a")
        self.assertEqual((code, out["reason"]), (4, "vault_sync_gate"))  # type: ignore[index]
        worker = VaultSyncService(
            "test",
            rt.adapters.store,
            "batchcontrol",
            rt.adapters.secrets,
            EmulatedClientVault(),
            rt.clock,
        )
        self.assertEqual(worker.run("eus2").synced, 2)
        code, out = self.run_cli("register", "retire", "--resource", "eus2", "--slot", "a")
        self.assertEqual((code, out["reason"]), (4, "vault_sync_gate"))  # type: ignore[index]

    def test_retire_force_bypasses_the_vault_gate(self) -> None:
        self.runtime_now(BATCH_VAULT_SYNC_REQUIRED="true")
        self.ensure()
        self.run_cli("register", "rotate", "--resource", "eus2", "--receiver-base-url", BASE_URL)
        retire = ("register", "retire", "--resource", "eus2", "--slot", "a")
        self.assertEqual(self.run_cli(*retire)[0], 4)
        code, out = self.run_cli(*retire, "--force")
        self.assertEqual((code, out["action"]), (0, "retired"))  # type: ignore[index]

    def test_retire_rejects_destination_drift_and_unavailability(self) -> None:
        vault = self.with_client_vault()
        self.ensure()
        self.run_cli("register", "rotate", "--resource", "eus2", "--receiver-base-url", BASE_URL)
        self.assertEqual(self.run_cli("vault-sync", "run", "--resource", "eus2")[0], 0)
        retire = ("register", "retire", "--resource", "eus2", "--slot", "a")
        old_ids = set(self.endpoints.endpoints)
        vault.unavailable = True
        code, out = self.run_cli(*retire)
        self.assertEqual((code, out["reason"]), (4, "vault_sync_verification_unavailable"))  # type: ignore[index]
        vault.unavailable = False
        name = "batch-test-eus2-b"
        current = vault.current(name)
        assert current is not None
        vault.retire(name, current.version)
        code, out = self.run_cli(*retire)
        self.assertEqual((code, out["reason"]), (4, "vault_sync_gate"))  # type: ignore[index]
        self.assertEqual(set(self.endpoints.endpoints), old_ids)
        code, out = self.run_cli("vault-sync", "run", "--resource", "eus2")
        self.assertEqual((code, out["eus2"]["converged"]), (1, False))  # type: ignore[index]

    def test_without_the_setting_or_a_client_vault_retire_is_not_gated(self) -> None:
        self.runtime_now(BATCH_VAULT_SYNC_REQUIRED="false")
        self.ensure()
        self.run_cli("register", "rotate", "--resource", "eus2", "--receiver-base-url", BASE_URL)
        self.assertEqual(
            self.run_cli("register", "retire", "--resource", "eus2", "--slot", "a")[0], 0
        )

    def test_vault_sync_run_exit_code_follows_the_ledger(self) -> None:
        self.with_client_vault()
        self.ensure()
        code, out = self.run_cli("vault-sync", "run")
        self.assertEqual((code, out["eus2"]["converged"], out["eus2"]["unsettled"]), (0, True, []))  # type: ignore[index]
        assert self.runtime is not None
        control = self.runtime.control
        [record] = control.registrations("eus2")
        control.save_registration(replace(record, secret_version="unknown"))  # lost capture
        code, out = self.run_cli("vault-sync", "run", "--resource", "eus2")
        stats = out["eus2"]  # type: ignore[index]
        self.assertEqual(
            (code, stats["blocked"], stats["failed"]), (1, 0, 0)
        )  # counters say nothing
        self.assertEqual(stats["unsettled"][0]["state"], "blocked")
        self.assertEqual(self.run_cli("vault-sync", "run", "--resource", "eus2")[0], 1)  # stays
        self.assertEqual(self.run_cli("vault-sync", "run", "--resource", "swc")[0], 0)  # scoped
        self.assertEqual(self.run_cli("vault-sync", "status", "--resource", "eus2")[0], 0)

    def test_superseded_and_skipped_runs_are_not_success(self) -> None:
        vault = self.with_client_vault()
        self.run_cli("register", "ensure", "--resource", "swc", "--receiver-base-url", BASE_URL)
        vault.put("batch-test-swc-a", "x", request_token="other", metadata={"generation": "9"})
        code, out = self.run_cli("vault-sync", "run", "--resource", "swc")
        self.assertEqual((code, out["swc"]["unsettled"][0]["state"]), (1, "superseded"))  # type: ignore[index]
        assert self.runtime is not None
        self.ensure()
        self.runtime.control.acquire("vaultsync|eus2", "another-worker", 300)
        code, out = self.run_cli("vault-sync", "run", "--resource", "eus2")
        self.assertEqual(
            (code, out["eus2"]["skipped"], out["eus2"]["converged"]), (1, ["eus2"], False)
        )  # type: ignore[index]


if __name__ == "__main__":
    unittest.main()
