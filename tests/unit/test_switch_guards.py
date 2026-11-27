"""Hosting guard for the real/local switch (S4) and its defence in depth in the composition root.

Flex and Linux Consumption workers may leave WEBSITE_INSTANCE_ID empty, so hosting is detected
from any platform variable; the Functions entry path also needs Core Tools on loopback before
it builds any local double. Values are never echoed, only variable names.
"""

import json
import sys
import types
import unittest
from dataclasses import replace
from pathlib import Path
from unittest import mock

from foundry_batch import composition
from foundry_batch.composition import adapters_from_settings, azure_credential, runtime_from_env
from foundry_batch.config import Settings, functions_host_is_local, hosting_signals
from foundry_batch.emulator.local import local_settings
from foundry_batch.errors import ConfigurationError

RESOURCES = json.dumps(
    [{"key": "eus2", "endpoint": "https://res.openai.azure.com/", "region": "eastus2"}]
)
DOUBLES = {
    "BATCH_ENVIRONMENT": "dev",
    "BATCH_RESOURCES_JSON": RESOURCES,
    "BATCH_PROVIDER_BACKEND": "emulator",
    "BATCH_STORAGE_BACKEND": "memory",
    "BATCH_SECRETS_BACKEND": "memory",
}
#: Platform variables of a Flex Consumption worker; WEBSITE_INSTANCE_ID is absent.
FLEX = {
    "WEBSITE_SITE_NAME": "func-batch-dev",
    "WEBSITE_SKU": "FlexConsumption",
    "WEBSITE_HOSTNAME": "func-batch-dev.azurewebsites.net",
    "CONTAINER_NAME": "c1",
}


class HostingSignalTests(unittest.TestCase):
    def test_each_platform_variable_means_hosted(self) -> None:
        for name, value in (
            ("WEBSITE_INSTANCE_ID", "abc"),
            ("WEBSITE_SITE_NAME", "func-batch-dev"),
            ("CONTAINER_NAME", "c1"),
            ("WEBSITE_HOSTNAME", "func-batch-dev.azurewebsites.net"),
            ("WEBSITE_HOSTNAME", "func-batch-dev.chinacloudsites.cn:443"),
            ("WEBSITE_HOSTNAME", "user@localhost"),  # not a plain host[:port]: fail closed
            ("WEBSITE_HOSTNAME", "localhost:7071/x"),
            ("WEBSITE_HOSTNAME", "localhost:abc"),
        ):
            with self.subTest(name=name, value=value):
                self.assertEqual(hosting_signals({name: value}), (name,))

    def test_loopback_special_use_and_blank_values_are_not_hosting(self) -> None:
        for env in (
            {},
            {"WEBSITE_INSTANCE_ID": "  ", "WEBSITE_SITE_NAME": "", "CONTAINER_NAME": " "},
            {"WEBSITE_HOSTNAME": "localhost:7071"},  # Azure Functions Core Tools
            {"WEBSITE_HOSTNAME": "127.0.0.1:7071"},
            {"WEBSITE_HOSTNAME": "[::1]:7071"},
            {"WEBSITE_HOSTNAME": "func.emulator.invalid"},  # LocalStack's in-process route
            {"WEBSITE_HOSTNAME": "func.example.net"},
            {"WEBSITE_HOSTNAME": "app.localhost"},
        ):
            with self.subTest(env=env):
                self.assertEqual(hosting_signals(env), ())

    def test_flex_like_process_refuses_local_doubles_and_names_the_signals(self) -> None:
        with self.assertRaises(ConfigurationError) as ctx:
            Settings.from_env({**DOUBLES, **FLEX})
        message = str(ctx.exception)
        self.assertIn("local double", message)
        self.assertIn("WEBSITE_SITE_NAME, CONTAINER_NAME, WEBSITE_HOSTNAME", message)
        self.assertNotIn("func-batch-dev", message)  # names only, never values
        hosted = Settings.from_env(
            {"BATCH_ENVIRONMENT": "dev", "BATCH_RESOURCES_JSON": RESOURCES, **FLEX}
        )
        self.assertEqual((hosted.hosted, hosted.local_profile), (True, False))

    def test_core_tools_on_loopback_is_the_only_local_functions_host(self) -> None:
        self.assertTrue(functions_host_is_local({"WEBSITE_HOSTNAME": "localhost:7071"}))
        self.assertTrue(functions_host_is_local({"WEBSITE_HOSTNAME": "[::1]:7071"}))
        for env in (
            {},
            {"WEBSITE_HOSTNAME": "func.emulator.invalid"},
            {"WEBSITE_HOSTNAME": "func-batch-dev.azurewebsites.net"},
            {"WEBSITE_HOSTNAME": "localhost:7071", "CONTAINER_NAME": "c1"},
            {"WEBSITE_HOSTNAME": "localhost:7071", "WEBSITE_SITE_NAME": "x"},
            {"WEBSITE_HOSTNAME": "localhost:7071", "WEBSITE_INSTANCE_ID": "x"},
        ):
            with self.subTest(env=env):
                self.assertFalse(functions_host_is_local(env))


class FunctionsEntryPathTests(unittest.TestCase):
    def test_flex_like_host_never_builds_doubles(self) -> None:
        with self.assertRaises(ConfigurationError):
            runtime_from_env({**DOUBLES, **FLEX})

    def test_doubles_need_core_tools_on_loopback(self) -> None:
        for extra in ({}, {"WEBSITE_HOSTNAME": "func.emulator.invalid"}):
            with self.subTest(extra=extra):
                self.assertTrue(Settings.from_env({**DOUBLES, **extra}).local_profile)
                with self.assertRaises(ConfigurationError) as ctx:
                    runtime_from_env({**DOUBLES, **extra})
                self.assertIn("Core Tools on loopback", str(ctx.exception))

    def test_func_start_on_loopback_builds_the_local_runtime(self) -> None:
        runtime = runtime_from_env({**DOUBLES, "WEBSITE_HOSTNAME": "localhost:7071"})
        self.assertEqual(type(runtime.adapters.store).__name__, "MemoryTableStore")

    def test_real_services_do_not_need_core_tools(self) -> None:
        env = {"BATCH_ENVIRONMENT": "poc", "BATCH_RESOURCES_JSON": RESOURCES, **FLEX}
        with (
            mock.patch.object(composition, "adapters_from_settings") as adapters,
            mock.patch.object(composition, "build_runtime", return_value="runtime") as build,
        ):
            self.assertEqual(runtime_from_env(env), "runtime")
        settings = adapters.call_args.args[0]
        self.assertEqual((settings.hosted, settings.uses_local_doubles), (True, False))
        build.assert_called_once_with(settings, adapters.return_value)

    def test_get_runtime_uses_the_guarded_entry_path(self) -> None:
        with (
            mock.patch.object(composition, "_RUNTIME", None),
            mock.patch.object(composition, "runtime_from_env", return_value="rt") as entry,
        ):
            self.assertEqual(composition.get_runtime(), "rt")
        entry.assert_called_once_with(composition.os.environ)


class CompositionDefenceInDepthTests(unittest.TestCase):
    """Settings already refuse these; the composition root must refuse them on its own too."""

    def test_settings_that_bypassed_validation_are_refused(self) -> None:
        valid = local_settings()
        for bypassed in (replace(valid, hosted=True), replace(valid, environment="poc")):
            with self.subTest(hosted=bypassed.hosted, environment=bypassed.environment):
                with self.assertRaises(ConfigurationError) as ctx:
                    adapters_from_settings(bypassed)
                self.assertIn("not allowed in this environment", str(ctx.exception))
        self.assertIsNotNone(adapters_from_settings(valid).store)


class AppOnlySettingTests(unittest.TestCase):
    def test_terraform_never_sets_vault_sync_required(self) -> None:
        infra = Path(__file__).resolve().parents[2] / "infra"
        tf = "".join(p.read_text(encoding="utf-8") for p in infra.rglob("*.tf"))
        self.assertGreater(len(tf), 0)
        self.assertNotIn("BATCH_VAULT_SYNC_REQUIRED", tf)


class AzureCredentialTests(unittest.TestCase):
    def credential(self, settings: Settings, client_id: str | None) -> object:
        identity = types.ModuleType("azure.identity")
        identity.ManagedIdentityCredential = lambda client_id: ("managed", client_id)  # type: ignore[attr-defined]
        identity.DefaultAzureCredential = lambda: ("default",)  # type: ignore[attr-defined]
        azure = sys.modules.get("azure") or types.ModuleType("azure")
        with mock.patch.dict(sys.modules, {"azure": azure, "azure.identity": identity}):
            return azure_credential(settings, client_id)

    def test_hosted_flex_uses_the_user_assigned_identity(self) -> None:
        hosted = Settings.from_env(
            {"BATCH_ENVIRONMENT": "poc", "BATCH_RESOURCES_JSON": RESOURCES, **FLEX}
        )
        self.assertEqual(self.credential(hosted, "client-1"), ("managed", "client-1"))
        self.assertEqual(self.credential(hosted, None), ("default",))
        workstation = Settings.from_env(
            {"BATCH_ENVIRONMENT": "poc", "BATCH_RESOURCES_JSON": RESOURCES}
        )
        self.assertEqual(self.credential(workstation, "client-1"), ("default",))


if __name__ == "__main__":
    unittest.main()
