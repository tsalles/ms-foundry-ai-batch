"""Backend switch: local doubles only where allowed, and the local stack end to end."""

import io
import json
import unittest
from collections import Counter
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

from foundry_batch import cli
from foundry_batch.composition import adapters_from_settings, build_runtime
from foundry_batch.config import Settings
from foundry_batch.emulator.foundry import FoundryEmulator
from foundry_batch.emulator.local import (
    SCENARIOS,
    LocalStack,
    Scenario,
    local_settings,
    play,
    run_scenario,
    run_vault_rotation,
    sample_jsonl,
    timeline,
)
from foundry_batch.emulator.transport import EmulatorHttpServer
from foundry_batch.errors import ConfigurationError
from tests.support import make_env

ROOT = Path(__file__).resolve().parents[2]
LOOPBACK = [{"key": "eus2", "endpoint": "http://127.0.0.1:9/r/eus2/", "region": "eastus2"}]


class SettingsGuardTests(unittest.TestCase):
    def test_defaults_are_the_real_services(self) -> None:
        settings = Settings.from_env(make_env(BATCH_ENVIRONMENT="poc"))
        self.assertEqual(
            (settings.provider_backend, settings.storage_backend, settings.secrets_backend),
            ("foundry", "azure", "keyvault"),
        )
        self.assertFalse(settings.uses_local_doubles)
        self.assertFalse(settings.local_profile)

    def test_local_doubles_are_refused_in_cloud_environments_and_when_hosted(self) -> None:
        for env in (
            make_env(BATCH_ENVIRONMENT="poc", BATCH_PROVIDER_BACKEND="emulator"),
            make_env(
                BATCH_ENVIRONMENT="local", BATCH_STORAGE_BACKEND="memory", WEBSITE_INSTANCE_ID="abc"
            ),
            make_env(BATCH_ENVIRONMENT="prod", BATCH_CLIENT_VAULT_BACKEND="emulator"),
        ):
            with (
                self.subTest(env=env["BATCH_ENVIRONMENT"]),
                self.assertRaises(ConfigurationError) as ctx,
            ):
                Settings.from_env(env)
            self.assertIn("local double", str(ctx.exception))

    def test_unknown_backend_value(self) -> None:
        with self.assertRaises(ConfigurationError) as ctx:
            Settings.from_env(make_env(BATCH_SECRETS_BACKEND="vaultish"))
        self.assertIn("must be one of", str(ctx.exception))

    def test_loopback_endpoints_only_for_local_profiles(self) -> None:
        local = make_env(
            BATCH_RESOURCES_JSON=json.dumps(LOOPBACK),
            BATCH_DOWNSTREAM_URL="http://127.0.0.1:7071/api/demo/events",
        )
        self.assertTrue(Settings.from_env(local).local_profile)
        with self.assertRaises(ConfigurationError) as ctx:
            Settings.from_env({**local, "BATCH_ENVIRONMENT": "poc"})
        self.assertIn("endpoint must use https", str(ctx.exception))
        self.assertIn("BATCH_DOWNSTREAM_URL must use https", str(ctx.exception))

    def test_placeholder_token_only_for_loopback_emulators(self) -> None:
        with self.assertRaises(ConfigurationError) as ctx:
            Settings.from_env(make_env(BATCH_FOUNDRY_AUTH="emulator"))
        self.assertIn("never send a placeholder token", str(ctx.exception))
        ok = Settings.from_env(
            make_env(BATCH_FOUNDRY_AUTH="emulator", BATCH_RESOURCES_JSON=json.dumps(LOOPBACK))
        )
        self.assertTrue(ok.uses_local_doubles)

    def test_terraform_never_selects_a_local_double(self) -> None:
        tf = "".join(p.read_text(encoding="utf-8") for p in (ROOT / "infra").rglob("*.tf"))
        for name in (
            "BATCH_PROVIDER_BACKEND",
            "BATCH_STORAGE_BACKEND",
            "BATCH_SECRETS_BACKEND",
            "BATCH_FOUNDRY_AUTH",
            "BATCH_CLIENT_VAULT_BACKEND",
        ):
            self.assertNotIn(name, tf)


class CompositionTests(unittest.TestCase):
    def test_default_settings_build_the_azure_adapters(self) -> None:
        settings = Settings.from_env(make_env())
        with mock.patch("foundry_batch.composition.azure_adapters", return_value="azure") as real:
            self.assertEqual(adapters_from_settings(settings, allow_api_key=True), "azure")
        real.assert_called_once_with(settings, allow_api_key=True)

    def test_refuses_mixed_state_api_keys_and_non_local_profiles(self) -> None:
        mixed = local_settings(BATCH_STORAGE_BACKEND="azure")
        with self.assertRaises(ConfigurationError):
            adapters_from_settings(mixed)
        with self.assertRaises(ConfigurationError):
            adapters_from_settings(local_settings(), allow_api_key=True)
        hosted = mock.Mock(uses_local_doubles=True, local_profile=False)
        with self.assertRaises(ConfigurationError):
            adapters_from_settings(hosted)

    def test_emulator_backend_runs_the_real_client(self) -> None:
        adapters = adapters_from_settings(local_settings(BATCH_CLIENT_VAULT_BACKEND="emulator"))
        api = adapters.batch_apis.for_resource("eus2")
        file_id = api.upload_file(filename="a.jsonl", content=sample_jsonl())
        batch = api.create_batch(input_file_id=file_id, endpoint="/chat/completions", metadata={})
        self.assertEqual(batch.status, "validating")
        self.assertIsNotNone(adapters.client_vault)
        self.assertTrue(adapters.secrets.get_secret("relay-signing-local").startswith("whsec_"))

    def test_loopback_server_with_placeholder_auth(self) -> None:
        emulator = FoundryEmulator(
            "eus2", mock.Mock(now=lambda: __import__("tests.support").support.T0)
        )
        with EmulatorHttpServer({"eus2": emulator}) as server:
            env = make_env(
                BATCH_ENVIRONMENT="dev",
                BATCH_RESOURCES_JSON=json.dumps(
                    [{**LOOPBACK[0], "endpoint": server.endpoint("eus2")}]
                ),
                BATCH_FOUNDRY_AUTH="emulator",
                BATCH_STORAGE_BACKEND="memory",
                BATCH_SECRETS_BACKEND="memory",
            )
            settings = Settings.from_env(env)
            runtime = build_runtime(settings, adapters_from_settings(settings))
            page = runtime.adapters.batch_apis.for_resource("eus2").list_batches(
                after=None, limit=5
            )
            self.assertEqual(page.items, ())
            self.assertEqual(emulator.calls["batch.list"], 1)


class LocalStackTests(unittest.TestCase):
    def test_every_named_scenario_holds_the_invariants(self) -> None:
        for name, scenario in SCENARIOS.items():
            with self.subTest(name=name):
                report = run_scenario(scenario)
                self.assertTrue(report.passed, report.violations)
                self.assertEqual(report.jobs_terminal, report.jobs_submitted)
                self.assertIn("SYNTHETIC", report.label)
        lossy = run_scenario(SCENARIOS["lossy"])
        self.assertGreater(lossy.learned_from.get("polling", 0), 0)  # reconciliation did work
        self.assertGreater(lossy.webhook_deliveries.get("duplicates_sent", 0), 0)

    def test_report_flags_a_job_that_never_settles(self) -> None:
        short = Scenario("short", "jobs still running", jobs=1)
        report = run_scenario(short, settle_s=0)
        self.assertFalse(report.passed)
        self.assertIn("no final state", report.violations[0])

    def test_report_detects_each_invariant_violation(self) -> None:
        from foundry_batch.emulator.local import _report
        from foundry_batch.ports import Entity, TxOp

        stack = LocalStack()
        stack.register_webhooks()
        first, second = stack.submit(), stack.submit()
        stack.advance(3600)
        emulator = stack.emulators["eus2"]
        api = stack.adapters.batch_apis.for_resource("eus2")
        file_id = api.upload_file(filename="x.jsonl", content=sample_jsonl())
        api.create_batch(input_file_id=file_id, endpoint="/chat/completions", metadata={})
        emulator._batches[second].data["status"] = "expired"  # provider disagrees
        table = stack.settings.table_demo_receipts
        rows = stack.store.rows(table)
        doomed = next(r for r in rows if r.data["job_id"].endswith(first))
        stack.store.submit(table, [TxOp("delete", doomed, force=True)])
        twin = next(r for r in stack.store.rows(table))
        stack.store.submit(
            table, [TxOp("create", Entity(twin.partition_key, "dup-key", dict(twin.data)))]
        )
        stack.poisoned["batch-events"] = 1
        report = _report(stack, Scenario("x", "x"), emulator)
        text = " | ".join(report.violations)
        for expected in (
            "missing from the registry",
            "registry completed vs expired",
            "never reached the downstream",
            "duplicate downstream",
            "poison",
        ):
            self.assertIn(expected, text)

    def test_vault_rotation_reports_an_ungated_retirement(self) -> None:
        with mock.patch("foundry_batch.emulator.local.VaultSyncService.ready", return_value=True):
            report = run_vault_rotation()
        self.assertFalse(report["passed"])
        self.assertIn("retired before the new key", report["violations"][0])

    def test_vault_rotation_scenario(self) -> None:
        report = run_vault_rotation()
        self.assertTrue(report["passed"], report["violations"])

    def test_advance_needs_a_manual_clock_and_submit_gives_up(self) -> None:
        from foundry_batch.clock import SystemClock
        from foundry_batch.emulator.foundry import ChaosRule, Fault

        with self.assertRaises(TypeError):
            LocalStack(clock=SystemClock()).advance(1)
        stack = LocalStack()
        stack.emulators["eus2"].chaos("files.upload", ChaosRule(1.0, Fault(status=500)))
        self.assertIsNone(stack.submit())
        self.assertEqual(stack.trigger_failures["submit_retry"], 5)
        stack.emulators["eus2"].chaos("files.upload", None)
        stack.emulators["eus2"].inject(
            "batch.create", Fault(drop_connection=True, after_commit=True)
        )
        batch_id = stack.submit()  # ambiguous create resolved by discovery, not resubmitted
        self.assertEqual(stack.emulators["eus2"].batch_ids(), [batch_id])
        from foundry_batch.foundry.submission import SubmissionPending

        with mock.patch.object(stack.submissions, "submit", side_effect=SubmissionPending("c1")):
            self.assertIsNone(stack.submit())
        self.assertEqual(stack.trigger_failures["submit_pending"], 1)
        self.assertEqual(stack.recover_submissions(), [])
        self.assertEqual(stack.queue_depth("batch-events"), 0)

    def test_functions_host_retries_then_poisons_a_failing_message(self) -> None:
        stack = LocalStack()
        stack.adapters.incoming_queue.send("not a valid envelope")
        with mock.patch.object(
            stack.runtime.processor, "handle_queue_message", side_effect=RuntimeError
        ):
            for _ in range(6):
                stack.advance(31)
        self.assertEqual(stack.poisoned["batch-events"], 1)
        self.assertEqual(stack.queue_depth("batch-events-poison"), 1)

    def test_unknown_functions_route(self) -> None:
        stack = LocalStack()
        session = stack.adapters.downstream._session  # type: ignore[attr-defined]
        self.assertEqual(session.get("https://func.emulator.invalid/api/nothing").status_code, 404)


class CliTests(unittest.TestCase):
    def run_cli(self, *argv: str) -> tuple[int, object]:
        out = io.StringIO()
        with redirect_stdout(out):
            code = cli.main(list(argv), configure_logging=False)
        return code, json.loads(out.getvalue())

    def test_local_demo(self) -> None:
        code, reports = self.run_cli("local-demo", "--scenario", "happy")
        self.assertEqual((code, reports[0]["scenario"], reports[0]["passed"]), (0, "happy", True))
        code, data = self.run_cli("local-demo", "--scenario", "nope")
        self.assertEqual(code, 2)
        self.assertIn("vault-rotation", data["known"])
        with mock.patch("foundry_batch.emulator.local.run_scenario") as run:
            run.return_value.to_dict.return_value = {"passed": False}
            code, _ = self.run_cli("local-demo", "--scenario", "lossy")
        self.assertEqual(code, 1)
        code, reports = self.run_cli("local-demo", "--scenario", "vault-rotation")
        self.assertEqual(code, 0)

    def test_vault_sync_commands(self) -> None:
        stack = LocalStack(BATCH_CLIENT_VAULT_BACKEND="emulator")
        stack.register_webhooks()

        def factory(settings: Settings, args: object) -> object:
            return stack.runtime

        out = io.StringIO()
        env = dict(make_env(BATCH_ENVIRONMENT="local").items())
        with mock.patch.dict("os.environ", env, clear=True), redirect_stdout(out):
            code = cli.main(["vault-sync", "run"], runtime_factory=factory, configure_logging=False)
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(out.getvalue())["eus2"]["synced"], 1)
        out = io.StringIO()
        with mock.patch.dict("os.environ", env, clear=True), redirect_stdout(out):
            code = cli.main(
                ["vault-sync", "status", "--resource", "eus2"],
                runtime_factory=factory,
                configure_logging=False,
            )
        self.assertEqual(json.loads(out.getvalue())["eus2"][0]["state"], "synced")
        stack.adapters.client_vault.unavailable = True  # type: ignore[union-attr]
        stack.registrar.rotate("eus2", stack.receiver_url("eus2"))
        out = io.StringIO()
        with mock.patch.dict("os.environ", env, clear=True), redirect_stdout(out):
            code = cli.main(["vault-sync", "run"], runtime_factory=factory, configure_logging=False)
        self.assertEqual(code, 1)
        out = io.StringIO()
        with mock.patch.dict("os.environ", env, clear=True), redirect_stdout(out):
            code = cli.main(
                ["register", "retire", "--resource", "eus2", "--slot", "a"],
                runtime_factory=factory,
                configure_logging=False,
            )
        refusal = json.loads(out.getvalue())
        self.assertEqual(
            (code, refusal["status"], refusal["reason"]), (4, "refused", "vault_sync_gate")
        )
        self.assertIn("client vault", refusal["error"])
        stack.adapters.client_vault = None
        out = io.StringIO()
        with mock.patch.dict("os.environ", env, clear=True), redirect_stdout(out):
            code = cli.main(
                ["vault-sync", "status"], runtime_factory=factory, configure_logging=False
            )
        self.assertEqual((code, json.loads(out.getvalue())["status"]), (2, "not_configured"))


class ProviderFindingsLocalStackTests(unittest.TestCase):
    """Scenario expectations (E1), the file wait in scenarios (E2), replayable timelines (E3)
    and isolated timer runs (E8)."""

    def test_happy_needs_every_job_from_a_webhook_and_no_abandoned_delivery(self) -> None:
        from foundry_batch.webhook.http import HttpResponse
        from foundry_batch.webhook.receiver import WebhookReceiver

        with mock.patch.object(
            WebhookReceiver, "handle", lambda self, request: HttpResponse.json(401, {"error": "x"})
        ):
            report = run_scenario(SCENARIOS["happy"])
        text = " | ".join(report.violations)
        self.assertIn("expected every job from a webhook", text)
        self.assertIn("10 webhook deliveries abandoned", text)
        self.assertEqual(report.learned_from, {"polling": 10})  # the old invariants still held

    def test_happy_fails_when_accepted_webhooks_are_never_processed(self) -> None:
        from foundry_batch.processing.processor import EventProcessor

        with mock.patch.object(EventProcessor, "handle_queue_message", lambda self, text: None):
            report = run_scenario(SCENARIOS["happy"])
        self.assertEqual(len(report.violations), 1, report.violations)
        self.assertIn("expected every job from a webhook", report.violations[0])

    def test_every_downstream_effect_must_carry_the_provider_outcome(self) -> None:
        from foundry_batch.domain import contract

        original = contract.NormalizedEvent.from_job.__func__  # type: ignore[attr-defined]

        def always_completed(cls, job, *args, **kwargs):  # type: ignore[no-untyped-def]
            event = original(cls, job, *args, **kwargs)
            event.payload["outcome"] = event.payload["raw_status"] = "completed"
            return event

        with mock.patch.object(contract.NormalizedEvent, "from_job", classmethod(always_completed)):
            report = run_scenario(SCENARIOS["lossy"])
        wrong = [v for v in report.violations if "downstream told completed/completed" in v]
        expected = sum(n for status, n in report.outcomes.items() if status != "completed")
        self.assertEqual(len(wrong), expected)
        self.assertGreater(expected, 0)

    def test_lossy_learns_by_polling_exactly_the_jobs_whose_webhooks_were_all_dropped(self) -> None:
        from types import SimpleNamespace

        from foundry_batch.emulator.local import _expectations

        report = run_scenario(SCENARIOS["lossy"])
        self.assertTrue(report.passed, report.violations)
        self.assertEqual(report.learned_from["polling"], report.jobs_without_webhook)
        self.assertGreater(report.jobs_without_webhook, 0)
        jobs = [
            SimpleNamespace(batch_id=b, terminal_source=s)
            for b, s in (("a", "polling"), ("b", "webhook"))
        ]
        lossy = SCENARIOS["lossy"]
        self.assertEqual(_expectations(lossy, jobs, Counter(), {"a"}), [])  # type: ignore[arg-type]
        [problem] = _expectations(lossy, jobs, Counter(), {"a", "b"})  # type: ignore[arg-type]
        self.assertIn("learned by polling: 1, jobs whose webhooks were all dropped: 2", problem)
        self.assertEqual(_expectations(SCENARIOS["outages"], jobs, Counter(), {"b"}), [])  # type: ignore[arg-type]

    def test_scenarios_wait_for_each_input_file_before_creating(self) -> None:
        stack = play(SCENARIOS["happy"])
        emulator = stack.emulators["eus2"]
        self.assertEqual(emulator.file_processing_s, 20.0)
        creates = [entry[2] for entry in emulator.journal if entry[1] == "batch.create"]
        self.assertEqual(creates, ["200"] * 10)  # never refused for a pending file
        self.assertEqual(emulator.calls["files.retrieve"], 30)  # pending, pending, processed
        self.assertEqual(LocalStack().emulators["eus2"].file_processing_s, 0.0)

    def test_the_same_scenario_and_seed_replay_an_identical_timeline(self) -> None:
        from dataclasses import replace

        outages = SCENARIOS["outages"]
        first, second = timeline(play(outages)), timeline(play(outages))
        self.assertEqual(first, second)
        self.assertTrue(any("outbox:" in line for line in first))  # relay attempts included
        self.assertTrue(any('"local-11-00001"' in line for line in first))  # seeded ids
        reseeded = replace(outages, delivery=replace(outages.delivery, seed=12))
        self.assertNotEqual(first, timeline(play(reseeded)))
        happy = SCENARIOS["happy"]
        self.assertEqual(run_scenario(happy).timeline_sha256, run_scenario(happy).timeline_sha256)

    def test_a_failing_timer_run_is_counted_and_the_schedule_continues(self) -> None:
        from foundry_batch.emulator.foundry import Fault, LifecyclePlan
        from foundry_batch.emulator.local import _report

        stack = LocalStack()
        stack.register_webhooks()
        emulator = stack.emulators["eus2"]
        emulator.plan_next(LifecyclePlan(in_progress_s=5000))  # still open at the first sweep
        stack.submit()
        emulator.inject("batch.list", Fault(status=400, code="bad_request"))
        real_dispatch = stack.runtime.dispatcher.dispatch_due
        failures = iter([RuntimeError("store down")])

        def flaky_dispatch() -> object:
            error = next(failures, None)
            if error is not None:
                raise error
            return real_dispatch()

        with mock.patch.object(stack.runtime.dispatcher, "dispatch_due", flaky_dispatch):
            stack.advance(1000)
        self.assertEqual(dict(stack.timer_failures), {"sweep": 1, "outbox": 1})
        stack.advance(900)
        sweeps = [entry[3] for entry in stack.journal if entry[1:3] == ("timer", "sweep")]
        self.assertEqual(sweeps, ["failed", "ok"])  # rescheduled after the failure
        report = _report(stack, Scenario("x", "x"), emulator)
        self.assertEqual(report.timer_failures, {"sweep": 1, "outbox": 1})


if __name__ == "__main__":
    unittest.main()
