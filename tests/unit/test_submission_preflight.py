import json
import unittest
from datetime import timedelta

from foundry_batch.adapters.memory import MemoryTableStore
from foundry_batch.control import ControlStore, RegistrationRecord
from foundry_batch.errors import ProviderAmbiguousError
from foundry_batch.foundry.submission import (
    RECOVERY_MIN_AGE_SECONDS,
    SubmissionError,
    SubmissionPending,
    SubmissionService,
    validate_input,
)
from foundry_batch.metrics.queued_tokens import CharacterHeuristicEstimator
from foundry_batch.ops.preflight import (
    AzCommand,
    PreflightInput,
    UnsafeCommandError,
    exit_code,
    run_preflight,
    to_markdown,
)
from foundry_batch.ports import Query
from foundry_batch.processing.registry import JobRegistry
from tests.support import T0, FakeProvider, FakeProviderFactory, ambiguous, clock, make_settings


def line(custom_id: str, model: str = "gpt-batch") -> bytes:
    return json.dumps(
        {
            "custom_id": custom_id,
            "method": "POST",
            "url": "/v1/chat/completions",
            "body": {"model": model, "messages": [{"role": "user", "content": "synthetic"}]},
        }
    ).encode()


GOOD = b"\n".join([line("1"), line("2")])


class ValidateInputTests(unittest.TestCase):
    def test_tc_c03_documented_validation_errors_fail_before_upload(self) -> None:
        cases = {
            "empty": b"\n\n",
            "duplicate custom id": b"\n".join([line("1"), line("1")]),
            "invalid json": b"{oops",
            "mixed deployments": b"\n".join([line("1"), line("2", "other")]),
            "not object": b"[1]",
            "missing custom id": json.dumps(
                {"method": "POST", "body": {"model": "gpt-batch"}}
            ).encode(),
            "wrong method": json.dumps({"custom_id": "1", "method": "GET", "body": {}}).encode(),
        }
        for name, content in cases.items():
            with self.subTest(name), self.assertRaises(SubmissionError):
                validate_input(content, "gpt-batch")
        with self.assertRaises(SubmissionError):
            validate_input(GOOD, "gpt-batch", max_lines=1)
        self.assertEqual(validate_input(GOOD, "gpt-batch").lines, 2)


class SubmissionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = clock()
        self.settings = make_settings()
        self.store = MemoryTableStore()
        self.registry = JobRegistry(self.store, self.settings, self.clock)
        self.control = ControlStore(self.store, "batchcontrol", "test", self.clock)
        self.provider = FakeProvider(self.clock)
        self.service = SubmissionService(
            self.settings,
            self.store,
            self.registry,
            self.control,
            FakeProviderFactory({"eus2": self.provider}),
            CharacterHeuristicEstimator(),
            self.clock,
        )

    def intents(self) -> list[dict]:
        return [dict(e.data) for e in self.store.query("batchsubmissions", Query())]

    def test_submit_links_job_with_coverage_and_estimate(self) -> None:
        self.control.save_registration(
            RegistrationRecord("eus2", "a", "wh_1", "n", "u", "s", "v1", T0 - timedelta(hours=1))
        )
        job = self.service.submit("eus2", "gpt-batch", GOOD, correlation_id="c1")
        self.assertTrue(job.webhook_covered)
        self.assertEqual(job.registration_id, "wh_1")
        self.assertGreater(job.token_estimate or 0, 0)
        self.assertEqual(job.tokenizer, "chars-per-4-v1")
        batch = self.provider.batches[job.batch_id]
        self.assertEqual(batch["metadata"], {"batch_correlation_id": "c1"})
        self.assertEqual(self.intents()[0]["status"], "linked")
        self.assertEqual(len(self.registry.open_jobs()), 1)

    def test_rejects_unknown_resource_or_deployment(self) -> None:
        with self.assertRaises(SubmissionError):
            self.service.submit("nope", "gpt-batch", GOOD)
        with self.assertRaises(SubmissionError):
            self.service.submit("eus2", "other", GOOD)

    def test_tc_e08_ambiguous_create_is_discovered_not_resubmitted(self) -> None:
        self.provider.fail("create", ambiguous(ProviderAmbiguousError("timeout")))
        job = self.service.submit("eus2", "gpt-batch", GOOD, correlation_id="c2")
        self.assertEqual(len(self.provider.batches), 1)
        self.assertEqual(self.provider.calls["create"], 1)
        self.assertEqual(job.correlation_id, "c2")

    def test_tc_e08_lost_create_is_pending_then_needs_operator(self) -> None:
        self.provider.fail("create", ProviderAmbiguousError("timeout"))  # not committed
        with self.assertRaises(SubmissionPending):
            self.service.submit("eus2", "gpt-batch", GOOD, correlation_id="c3")
        self.assertEqual(self.intents()[0]["status"], "ambiguous")
        self.assertEqual(self.service.recover("eus2"), [])  # too recent
        self.clock.advance(RECOVERY_MIN_AGE_SECONDS + 1)
        with self.assertLogs("foundry_batch", "ERROR"):
            self.assertEqual(self.service.recover("eus2"), [])
        self.assertEqual(self.intents()[0]["status"], "needs_operator")

    def test_crash_after_provider_acceptance_is_recovered(self) -> None:
        file_id = self.provider.upload_file(filename="x", content=GOOD)
        self.provider.create_batch(
            input_file_id=file_id,
            endpoint="/chat/completions",
            metadata={"batch_correlation_id": "c4"},
        )
        self.service._put(
            "eus2",
            "c4",
            {
                "status": "creating",
                "deployment": "gpt-batch",
                "file_id": file_id,
                "created_at": "2026-11-20T11:00:00.000000Z",
                "token_estimate": 5,
                "tokenizer": "t",
            },
            create=True,
        )
        self.provider.add(input_file_id=file_id)  # a duplicate provider job is reported
        with self.assertLogs("foundry_batch", "ERROR") as logs:
            self.assertEqual(self.service.recover("eus2"), ["c4"])
        self.assertTrue(any("duplicate_detected" in line for line in logs.output))
        self.assertEqual(len(self.registry.open_jobs()), 1)
        self.assertEqual(self.service.recover("eus2"), [])  # idempotent


class PreflightTests(unittest.TestCase):
    target = PreflightInput(
        tenant_id="t",
        subscription_id="s",
        resource_group="rg-foundry-batch-poc",
        location="eastus2",
        model_name="gpt-4.1-mini",
        model_version="2025-04-14",
        planned_names=["kv-batch"],
        deployer_object_id="oid",
    )

    def responses(self, **overrides: object) -> dict[tuple[str, ...], object]:
        base: dict[tuple[str, ...], object] = {
            ("account", "show"): {"tenantId": "t", "id": "s"},
            ("group", "list"): [],
            ("provider", "show"): {"registrationState": "Registered"},
            ("cognitiveservices", "model", "list"): [
                {
                    "model": {
                        "name": "gpt-4.1-mini",
                        "version": "2025-04-14",
                        "skus": [{"name": "GlobalBatch"}],
                        "lifecycleStatus": "GenerallyAvailable",
                    }
                }
            ],
            ("cognitiveservices", "usage", "list"): [
                {
                    "name": {"value": "OpenAI.GlobalBatch.gpt-4.1-mini"},
                    "limit": 100,
                    "currentValue": 0,
                    "unit": "Count",
                }
            ],
            ("resource", "list"): [{"name": "other"}],
            ("role", "assignment", "list"): [{"roleDefinitionName": "Contributor"}],
            ("policy", "assignment", "list"): [],
        }
        base.update(overrides)  # type: ignore[arg-type]
        return base

    def runner(self, responses: dict[tuple[str, ...], object]):  # type: ignore[no-untyped-def]
        calls: list[list[str]] = []

        def run(command: AzCommand) -> object:
            calls.append(command.argv())
            value = responses[(*command.group, command.verb)]
            if isinstance(value, Exception):
                raise value
            return value

        run.calls = calls  # type: ignore[attr-defined]
        return run

    def test_tc_h07_all_checks_pass_with_read_only_commands(self) -> None:
        runner = self.runner(self.responses())
        findings = run_preflight(self.target, runner)
        self.assertEqual(exit_code(findings), 0, to_markdown(findings))
        mutating = {"create", "delete", "set", "register", "update", "assign"}
        for argv in runner.calls:
            self.assertEqual(argv[-3:], ["--only-show-errors", "-o", "json"])
            self.assertFalse(mutating & set(argv), argv)

    def test_tc_h07_wrong_tenant_stops_immediately(self) -> None:
        responses = self.responses()
        responses[("account", "show")] = {"tenantId": "other", "id": "s"}
        findings = run_preflight(self.target, self.runner(responses))
        self.assertEqual([f.check for f in findings], ["account"])
        self.assertEqual(exit_code(findings), 1)

    def test_findings_for_problems(self) -> None:
        responses = self.responses()
        responses[("group", "list")] = [{"location": "brazilsouth"}]
        responses[("provider", "show")] = {"registrationState": "NotRegistered"}
        responses[("cognitiveservices", "model", "list")] = [
            {
                "model": {
                    "name": "gpt-4.1-mini",
                    "version": "2025-04-14",
                    "skus": [{"name": "Standard"}],
                }
            }
        ]
        responses[("cognitiveservices", "usage", "list")] = []
        responses[("resource", "list")] = [{"name": "kv-batch"}]
        responses[("role", "assignment", "list")] = []
        responses[("policy", "assignment", "list")] = [{"displayName": "Allowed locations"}]
        findings = {f.check: f for f in run_preflight(self.target, self.runner(responses))}
        self.assertEqual(findings["resource_group"].status, "fail")
        self.assertEqual(findings["provider:Microsoft.Web"].status, "warn")
        self.assertEqual(findings["model"].status, "fail")
        self.assertEqual(findings["quota"].status, "warn")
        self.assertEqual(findings["collisions"].status, "fail")
        self.assertEqual(findings["deployer_roles"].status, "warn")
        self.assertEqual(findings["policy"].status, "warn")
        responses[("cognitiveservices", "model", "list")] = []
        responses[("account", "show")] = RuntimeError("az failed")
        findings = {f.check: f for f in run_preflight(self.target, self.runner(responses))}
        self.assertEqual(findings["model"].detail, "model/version not offered in this region")
        self.assertEqual(findings["account"].status, "fail")

    def test_mutating_verbs_are_refused(self) -> None:
        with self.assertRaises(UnsafeCommandError):
            AzCommand(("provider",), "register").argv()

        def unsafe(_: AzCommand) -> object:
            raise UnsafeCommandError("x")

        with self.assertRaises(UnsafeCommandError):
            run_preflight(self.target, unsafe)


if __name__ == "__main__":
    unittest.main()
