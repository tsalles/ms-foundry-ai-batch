"""Pipeline tools: entry points, exit codes and the protections the review added."""

import base64
import importlib.util
import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from types import ModuleType

from foundry_batch.composition import Runtime, build_runtime, memory_adapters
from foundry_batch.domain.contract import idempotency_key
from foundry_batch.domain.models import JobKey
from foundry_batch.ports import Entity, TxOp
from tests.support import FakeEndpoints, FakeProvider, FakeProviderFactory, clock, make_settings

ROOT = Path(__file__).resolve().parents[2]


def load(name: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(f"tool_{name}", ROOT / "tools" / f"{name}.py")
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


plan_guard = load("plan_guard")
secret_scan = load("secret_scan")
settings_from_outputs = load("settings_from_outputs")
smoke_test = load("smoke_test")


def azapi(address: str, actions: list[str], arm_type: str) -> dict:
    before = None if actions == ["create"] else {"type": arm_type}
    after = None if actions == ["delete"] else {"type": arm_type}
    return {
        "address": address,
        "type": "azapi_resource",
        "change": {"actions": actions, "before": before, "after": after},
    }


class PlanGuardHardeningTests(unittest.TestCase):
    def test_resource_group_prefix_collision_is_rejected(self) -> None:
        resource = azapi("outside", ["create"], "Microsoft.Storage/storageAccounts@2024-01-01")
        resource["change"]["after"]["parent_id"] = (
            "/subscriptions/approved/resourceGroups/rg-foundry-batch-poc-extra"
        )
        self.assertTrue(
            plan_guard.check(
                {"resource_changes": [resource]},
                resource_group="rg-foundry-batch-poc",
                allow_destroy=False,
            )
        )

    def test_wrong_subscription_is_rejected_even_with_the_same_resource_group(self) -> None:
        resource = azapi("outside", ["create"], "Microsoft.Storage/storageAccounts@2024-01-01")
        resource["change"]["after"]["parent_id"] = "/subscriptions/other/resourceGroups/rg"
        self.assertTrue(
            plan_guard.check(
                {"resource_changes": [resource]},
                resource_group="rg",
                subscription_id="approved",
                allow_destroy=False,
            )
        )

    def test_state_holding_azapi_resources_are_protected_even_when_destroy_is_allowed(self) -> None:
        plan = {
            "resource_changes": [
                azapi(
                    "module.storage.azapi_resource.account",
                    ["delete"],
                    "Microsoft.Storage/storageAccounts@2024-01-01",
                ),
                azapi(
                    'module.storage.azapi_resource.table["batchjobs"]',
                    ["delete", "create"],
                    "Microsoft.Storage/storageAccounts/tableServices/tables@2024-01-01",
                ),
                azapi(
                    'module.storage.azapi_resource.queue["x"]',
                    ["delete"],
                    "Microsoft.Storage/storageAccounts/queueServices/queues@2024-01-01",
                ),
                {
                    "address": "module.foundry.azurerm_cognitive_account.this",
                    "type": "azurerm_cognitive_account",
                    "change": {"actions": ["delete"], "after": None},
                },
            ]
        }
        problems = plan_guard.check(plan, resource_group="rg", allow_destroy=True)
        self.assertEqual(len(problems), 3)
        self.assertFalse(any("queue" in p for p in problems))  # stateless: allowed

    def test_cli_exit_codes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            good = Path(tmp, "good.json")
            good.write_text(json.dumps({"resource_changes": []}), encoding="utf-8")
            bad = Path(tmp, "bad.json")
            bad.write_text(
                json.dumps(
                    {
                        "resource_changes": [
                            {
                                "address": "a",
                                "type": "azurerm_key_vault",
                                "change": {"actions": ["delete"], "after": None},
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            with redirect_stdout(io.StringIO()) as out, redirect_stderr(io.StringIO()):
                self.assertEqual(
                    plan_guard.main(
                        [str(good), "--resource-group", "rg", "--subscription-id", "approved"]
                    ),
                    0,
                )
                self.assertEqual(
                    plan_guard.main(
                        [
                            str(bad),
                            "--resource-group",
                            "rg",
                            "--subscription-id",
                            "approved",
                            "--allow-destroy",
                        ]
                    ),
                    1,
                )
            self.assertIn("a: delete is not allowed", out.getvalue())


SUB = "22222222-2222-2222-2222-222222222222"
OTHER = "99999999-9999-9999-9999-999999999999"
RG = "rg-foundry-batch-poc"
RG_ID = f"/subscriptions/{SUB}/resourceGroups/{RG}"
ACCOUNT = f"{RG_ID}/providers/Microsoft.Storage/storageAccounts/stbatch"
NETWORK = f"{RG_ID}/providers/Microsoft.Network"
RUNNER_VNET = (
    f"/subscriptions/{OTHER}/resourceGroups/rg-agents/providers/Microsoft.Network/"
    "virtualNetworks/vnet-agents"
)


def resource(address: str, after: dict, kind: str = "azurerm_x", action: str = "create") -> dict:
    change = {"actions": [action], "after": after}
    return {"address": address, "mode": "managed", "type": kind, "change": change}


def role(scope: str) -> dict:
    return resource("azurerm_role_assignment.r", {"scope": scope}, "azurerm_role_assignment")


def guard(changes: list[dict], plan_extra: dict | None = None, **options: object) -> list[str]:
    settings: dict = {"subscription_id": SUB, "allowed_locations": ["eastus2"], **options}
    plan = {"resource_changes": changes, **(plan_extra or {})}
    return plan_guard.check(plan, resource_group=RG, allow_destroy=False, **settings)


class PlanGuardTargetTests(unittest.TestCase):
    """I3: every ARM id, role assignment scope and location in the plan is checked."""

    def legitimate_changes(self) -> list[dict]:
        table = f"{ACCOUNT}/tableServices/default/tables/batchcontrol"
        definition = f"/subscriptions/{SUB}/providers/Microsoft.Authorization/roleDefinitions/0a9"
        workspace = f"{RG_ID}/providers/microsoft.operationalinsights/workspaces/log-x".lower()
        endpoint = {
            "location": "eastus2",
            "resource_group_name": RG,
            "subnet_id": f"{NETWORK}/virtualNetworks/vnet/subnets/pe",
            "private_service_connection": [{"private_connection_resource_id": ACCOUNT}],
            "private_dns_zone_group": [
                {"private_dns_zone_ids": [f"{NETWORK}/privateDnsZones/privatelink.table"]}
            ],
        }
        other_rg = f"/subscriptions/{OTHER}/resourceGroups/x"
        read = {"actions": ["read"], "after": {"id": other_rg}}
        return [
            {"address": "data.azurerm_resource_group.x", "mode": "data", "change": read},
            resource("st", {"parent_id": RG_ID, "location": "eastus2"}, "azapi_resource"),
            resource("st.table", {"parent_id": f"{ACCOUNT}/tableServices/default", "location": ""}),
            resource("ra", {"scope": table, "role_definition_id": definition}, action="no-op"),
            resource("pe", endpoint, "azurerm_private_endpoint"),
            resource("link", {"resource_group_name": RG, "virtual_network_id": RUNNER_VNET}),
            resource("ag", {"location": "global", "resource_group_name": RG}),
            resource("wb", {"location": "East US 2", "data_json": json.dumps([workspace])}),
        ]

    def test_the_legitimate_plan_shape_passes(self) -> None:
        variables = {
            "allowed_locations": {"value": ["eastus2"]},
            "runner_vnet_ids": {"value": {"agents": RUNNER_VNET}},
        }
        plan = {"variables": variables, "resource_changes": self.legitimate_changes()}
        options = {"resource_group": RG, "allow_destroy": False, "subscription_id": SUB}
        self.assertEqual(plan_guard.check(plan, **options), [])

    def test_role_assignment_at_subscription_scope_is_rejected(self) -> None:
        problems = guard([role(f"/subscriptions/{SUB}")])
        self.assertTrue(any("not a single resource" in p for p in problems))
        self.assertTrue(any("subscription-level reference" in p for p in problems))

    def test_role_assignment_in_another_subscription_is_rejected(self) -> None:
        problems = guard([role(f"/subscriptions/{OTHER}/resourceGroups/rg-prod")])
        self.assertTrue(any("outside the target subscription" in p for p in problems))

    def test_role_assignment_at_resource_group_or_management_group_scope_is_rejected(self) -> None:
        self.assertEqual(
            guard([role(RG_ID)]),
            [f"azurerm_role_assignment.r: role assignment scope {RG_ID} is not a single resource"],
        )
        problems = guard([role("/providers/Microsoft.Management/managementGroups/root")])
        self.assertTrue(any("not a single resource" in p for p in problems))
        self.assertTrue(any("management group reference" in p for p in problems))
        policy = "/providers/Microsoft.Authorization/policyDefinitions/x"
        tenant = guard([resource("p", {"policy": policy})])
        self.assertEqual(tenant, ["p: policy tenant-level reference"])

    def test_diagnostic_setting_on_a_foreign_resource_is_rejected(self) -> None:
        vault = f"/subscriptions/{OTHER}/resourceGroups/rg-x/providers/Microsoft.KeyVault/vaults/kv"
        problems = guard([resource("diag", {"target_resource_id": vault})])
        self.assertEqual(problems, ["diag: target_resource_id outside the target subscription"])

    def test_private_endpoint_to_a_foreign_account_is_rejected(self) -> None:
        foreign = f"/subscriptions/{SUB}/resourceGroups/rg-prod/providers/A.B/accounts/prod"
        connection = [{"private_connection_resource_id": foreign}]
        problems = guard([resource("pe", {"private_service_connection": connection})])
        expected = "private_service_connection[0].private_connection_resource_id outside"
        self.assertEqual(problems, [f"pe: {expected} the target resource group"])

    def test_ids_embedded_in_json_strings_and_nested_resource_groups_are_checked(self) -> None:
        body = json.dumps({"scopes": [f"/subscriptions/{OTHER}/resourceGroups/x"]})
        problems = guard(
            [
                resource("a.json", {"data_json": body}),
                resource("a.nested", {"block": [{"resource_group_name": "rg-other"}]}),
            ]
        )
        self.assertEqual(len(problems), 2)
        nested = "a.nested: block[0].resource_group_name targets resource group rg-other"
        self.assertIn(nested, problems)

    def test_foundry_outside_the_approved_region_is_rejected(self) -> None:
        change = resource("oai", {"resource_group_name": RG, "location": "westeurope"})
        self.assertEqual(guard([change]), ["oai: location westeurope is not an allowed location"])
        # Allowed locations come from the CLI, else the plan's allowed_locations variable.
        self.assertEqual(guard([change], allowed_locations=["westeurope"]), [])
        variables = {"variables": {"allowed_locations": {"value": ["westeurope"]}}}
        self.assertEqual(guard([change], variables, allowed_locations=None), [])
        # Neither: fail closed.
        self.assertTrue(guard([change], allowed_locations=None))

    def test_runner_vnet_must_be_declared_to_be_linked(self) -> None:
        link = resource("link", {"virtual_network_id": RUNNER_VNET})
        self.assertTrue(guard([link]))
        self.assertEqual(guard([link], allowed_external_ids=[RUNNER_VNET.upper()]), [])

    def test_azapi_workspace_is_protected(self) -> None:
        workspace = azapi(
            "module.monitoring.azapi_resource.workspace",
            ["delete", "create"],
            "Microsoft.OperationalInsights/workspaces@2023-09-01",
        )
        plan = {"resource_changes": [workspace]}
        self.assertTrue(plan_guard.check(plan, resource_group=RG, allow_destroy=True))

    def test_cli_accepts_locations_and_external_ids(self) -> None:
        link = resource("link", {"virtual_network_id": RUNNER_VNET, "location": "swedencentral"})
        extra = ["--allowed-location", "swedencentral", "--allow-external-id", RUNNER_VNET]
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp, "plan.json")
            path.write_text(json.dumps({"resource_changes": [link]}), encoding="utf-8")
            base = [str(path), "--resource-group", RG, "--subscription-id", SUB]
            with redirect_stdout(io.StringIO()) as out, redirect_stderr(io.StringIO()):
                self.assertEqual(plan_guard.main(base), 1)
                self.assertEqual(plan_guard.main([*base, *extra]), 0)
        self.assertIn("location swedencentral is not an allowed location", out.getvalue())


class SecretScanCliTests(unittest.TestCase):
    def test_cli_reports_findings_and_skips_vendored_dirs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            Path(tmp, ".terraform").mkdir()
            Path(tmp, ".terraform", "x.txt").write_text("AccountKey=" + "A" * 60, encoding="utf-8")
            Path(tmp, "clean.txt").write_text("nothing here", encoding="utf-8")
            with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                self.assertEqual(secret_scan.main([tmp]), 0)
            Path(tmp, "leak.json").write_text('{"key1": "' + "k" * 40 + '"}', encoding="utf-8")
            with redirect_stdout(io.StringIO()) as out, redirect_stderr(io.StringIO()):
                self.assertEqual(secret_scan.main([str(Path(tmp, "leak.json"))]), 1)
            self.assertIn("cognitive_api_key", out.getvalue())
            self.assertNotIn("k" * 40, out.getvalue())


def b64(size: int) -> str:
    return base64.b64encode(bytes(range(size))).decode()


SECRET_SHAPES = {
    "shared_access_key": "Endpoint=sb://ns/;SharedAccessKeyName=R;SharedAccessKey=" + b64(32),
    "connection_string_password": "Server=tcp:s;User ID=u;Password=" + "p" * 12,
    "jwt": json.dumps({"access_token": "eyJ" + "a" * 20 + ".eyJ" + "b" * 30 + "." + "c" * 20}),
    "log_analytics_shared_key": json.dumps({"primary_shared_key": "s" * 40}),
    "client_secret_field": json.dumps({"clientSecret": "x" * 16}),
    "entra_client_secret": "AZURE_CLIENT_SECRET=abc" + "8Q~" + "d" * 34,
    "azure_list_keys_value": json.dumps({"keyName": "key1", "value": "k" * 44}),
    "base64_64_byte_key": json.dumps({"value": b64(64)}),
    "storage_account_key": "DefaultEndpointsProtocol=https;AccountKey=" + b64(64),
    "cognitive_api_key": json.dumps({"key2": "c" * 32}),
}
NOT_SECRETS = [
    "InstrumentationKey=00000000-0000-0000-0000-000000000000;IngestionEndpoint=https://x/",
    "AccountName=st;EndpointSuffix=core.windows.net",
    '"h1:' + b64(32) + '",',
    "--hash=sha256:" + "ab" * 32,
    json.dumps({"keyName": "key1", "permissions": "FULL"}),
    "password is required; see docs",
]


class SecretScanShapeTests(unittest.TestCase):
    """I4: each secret shape is found; strict mode ignores allowlist markers."""

    def scan_text(self, text: str, strict: bool = False) -> list[str]:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp, "sample.txt")
            path.write_text(text + "\n", encoding="utf-8")
            return [f.rsplit(": ", 1)[-1] for f in secret_scan.scan([path], strict=strict)]

    def test_every_shape_is_found(self) -> None:
        for rule, text in SECRET_SHAPES.items():
            with self.subTest(rule=rule):
                self.assertIn(rule, self.scan_text(text))

    def test_common_non_secrets_are_not_findings(self) -> None:
        for text in NOT_SECRETS:
            with self.subTest(text=text):
                self.assertEqual(self.scan_text(text), [])

    def test_marker_inside_json_never_hides_a_secret(self) -> None:
        whsec = "whsec_" + b64(32)
        after = {"tags": {"note": "secret-scan: allow"}, "value": whsec}
        minified = json.dumps({"resource_changes": [{"after": after}]}, separators=(",", ":"))
        self.assertEqual(self.scan_text(minified), ["webhook_signing_secret"])

    def test_strict_mode_ignores_trailing_allowlist_comments(self) -> None:
        line = "fixture = " + json.dumps("whsec_" + b64(32)) + "  # secret-scan: allow"
        self.assertEqual(self.scan_text(line), [])
        self.assertEqual(self.scan_text(line, strict=True), ["webhook_signing_secret"])
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp, "tfplan.json")
            path.write_text(line, encoding="utf-8")
            with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                self.assertEqual(secret_scan.main([str(path)]), 0)
                self.assertEqual(secret_scan.main(["--strict", str(path)]), 1)

    def test_the_repository_itself_is_clean_in_strict_mode(self) -> None:
        sources = [ROOT / name for name in ("tools", "infra", "pipelines", "functions", "docs")]
        self.assertEqual(secret_scan.scan(sources, strict=True), [])


class SettingsFromOutputsCliTests(unittest.TestCase):
    def test_cli_writes_the_settings_file_and_encodes_non_string_values(self) -> None:
        outputs = {
            name: {"value": f"https://{name}", "sensitive": False}
            for name in ("key_vault_uri", "queue_service_uri", "table_service_uri")
        }
        outputs["resources_json"] = {"value": [{"key": "eus2"}], "sensitive": False}
        outputs["environment"] = {"value": "dev", "sensitive": False}
        with tempfile.TemporaryDirectory() as tmp:
            src, out = Path(tmp, "o.json"), Path(tmp, "s.json")
            src.write_text(json.dumps(outputs), encoding="utf-8")
            settings_from_outputs.main([str(src), "--environment", "dev", "--out", str(out)])
            written = json.loads(out.read_text(encoding="utf-8"))
        self.assertEqual(written["BATCH_ENVIRONMENT"], "dev")
        self.assertEqual(json.loads(written["BATCH_RESOURCES_JSON"]), [{"key": "eus2"}])


def read(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


class DeliveryContractTests(unittest.TestCase):
    """Pipeline and bootstrap wiring that the review fixes rely on (I4, I7, I8, I12)."""

    def test_plan_and_outputs_are_scanned_strictly_and_guarded_by_region(self) -> None:
        plan = read("pipelines/templates/terraform-plan.yml")
        self.assertIn('secret_scan.py --strict "$(Agent.TempDirectory)/tfplan.json"', plan)
        self.assertIn("--allowed-location", plan)
        apply = read("pipelines/templates/terraform-apply.yml")
        self.assertIn("secret_scan.py --strict", apply)

    def test_tflint_token_is_never_the_raw_macro(self) -> None:
        validate = read("pipelines/templates/terraform-validate.yml")
        self.assertNotRegex(validate, r"(?m)^\s+GITHUB_TOKEN:")
        self.assertIn("TFLINT_GITHUB_TOKEN: $(GITHUB_TOKEN_FOR_TFLINT)", validate)

    def test_main_builds_can_require_the_dependency_lock(self) -> None:
        pipeline = read("azure-pipelines.yml")
        self.assertIn("--require-lock", pipeline)
        # Opt-in until the lock is committed, so main is not red before `make lock` has run.
        self.assertIn('[ "$(requireDependencyLock)" = "true" ]', pipeline)
        self.assertRegex(pipeline, r'name: requireDependencyLock\n\s+value: "false"')
        self.assertIn("pip-audit --strict --require-hashes --disable-pip", pipeline)
        self.assertIn("piptools compile --quiet --generate-hashes", read("Makefile"))

    def test_bootstrap_and_workload_state_have_separate_containers_and_roles(self) -> None:
        main = read("infra/bootstrap/main.tf")
        self.assertIn('name      = "tfstate-bootstrap"', main)
        roles = {
            "state": "azapi_resource.tfstate.id",
            "bootstrap_state": "azapi_resource.tfstate_bootstrap.id",
        }
        for name, scope in roles.items():
            block = main[main.index(f'resource "azurerm_role_assignment" "{name}"') :]
            self.assertIn(f"scope                = {scope}", block[: block.index("\n}")])
        backend = read("infra/bootstrap/bootstrap.backend.hcl.example")
        self.assertIn('container_name       = "tfstate-bootstrap"', backend)
        workload = read("infra/envs/poc/backend.hcl.example")
        self.assertIn('container_name       = "tfstate"', workload)


class SmokeTestTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = clock()
        self.provider = FakeProvider(self.clock)
        self.runtime: Runtime = build_runtime(
            make_settings(),
            memory_adapters(
                FakeProviderFactory({"eus2": self.provider, "swc": FakeProvider(self.clock)}),
                lambda _: FakeEndpoints(),
            ),
            self.clock,
        )
        self.ticks = 0.0

    def monotonic(self) -> float:
        return self.ticks

    def run_smoke(self, on_sleep: object, timeout_minutes: int = 3) -> tuple[int, str]:
        def sleep(seconds: float) -> None:
            self.ticks += seconds
            self.clock.advance(seconds)
            on_sleep()  # type: ignore[operator]

        with redirect_stdout(io.StringIO()) as out:
            code = smoke_test.main(
                [
                    "--resource",
                    "eus2",
                    "--deployment",
                    "gpt-batch",
                    "--timeout-minutes",
                    str(timeout_minutes),
                ],
                runtime_factory=lambda: self.runtime,
                sleep=sleep,
                monotonic=self.monotonic,
            )
        return code, out.getvalue()

    def finish(self, status: str) -> None:
        batch_id = self.provider.order[-1]
        self.provider.set_status(batch_id, status)
        self.runtime.reconciler.run_all()
        stored = self.runtime.registry.load(JobKey("eus2", batch_id))
        assert stored is not None and stored.record.outcome is not None
        key = idempotency_key("test", "eus2", batch_id, stored.record.outcome)
        self.runtime.adapters.store.submit(
            self.runtime.settings.table_demo_receipts,
            [TxOp("upsert", Entity("test", key, {"source": "polling"}))],
        )

    def test_delivered_completed_job_passes(self) -> None:
        code, out = self.run_smoke(lambda: self.finish("completed"))
        self.assertEqual(code, 0)
        self.assertIn("delivered outcome=completed source=polling", out)

    def test_delivered_failed_job_fails(self) -> None:
        self.assertEqual(self.run_smoke(lambda: self.finish("failed"))[0], 1)

    def test_timeout_is_a_warning_exit_code(self) -> None:
        code, out = self.run_smoke(lambda: None, timeout_minutes=2)
        self.assertEqual(code, 2)
        self.assertIn("timeout", out)

    def test_a_pending_submission_exits_3_and_points_to_recovery(self) -> None:
        from foundry_batch.errors import ProviderAmbiguousError

        error = ProviderAmbiguousError("batch.create: read timeout", status_code=None)
        error.after_commit = True  # type: ignore[attr-defined]  # the batch exists, response lost
        self.provider.fail("create", error)
        self.provider.fail("list", ProviderAmbiguousError("batch.list: 503", status_code=503))
        code, out = self.run_smoke(lambda: None)
        self.assertEqual(code, 3)
        self.assertIn("recover-submissions", out)
        self.assertEqual(len(self.provider.order), 1)  # no second batch was created


if __name__ == "__main__":
    unittest.main()
