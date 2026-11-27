"""Pipeline helper scripts: plan guard, settings from outputs, secret scan, package build."""

import base64
import importlib.util
import io
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
import zipfile
from contextlib import redirect_stdout
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
IGNORE = shutil.ignore_patterns("__pycache__", "*.pyc")


def load(name: str):  # type: ignore[no-untyped-def]
    spec = importlib.util.spec_from_file_location(name, ROOT / "tools" / f"{name}.py")
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


plan_guard = load("plan_guard")
settings_from_outputs = load("settings_from_outputs")
secret_scan = load("secret_scan")
build_function_package = load("build_function_package")


@unittest.skipUnless(sys.platform == "win32", "Windows desktop launcher")
class DesktopLauncherTests(unittest.TestCase):
    def test_multiple_tool_paths_still_reach_preflight(self) -> None:
        powershell = shutil.which("pwsh")
        if not powershell:
            self.skipTest("PowerShell 7 is not installed")
        tenant = "6a7f0bbb-81de-4d86-b1cc-af9648764868"
        subscription = "c65e7928-245a-4f69-be5d-13e09bf77e20"
        identity = "15364838-a763-4b93-b176-60961cab7c01"
        account = json.dumps(
            {
                "id": subscription,
                "tenantId": tenant,
                "state": "Enabled",
                "environmentName": "AzureCloud",
                "user": {"type": "user"},
            }
        )
        with tempfile.TemporaryDirectory() as workdir:
            primary = Path(workdir) / "first tools"
            secondary = Path(workdir) / "other tools"
            primary.mkdir()
            secondary.mkdir()
            shims = {
                primary / "terraform.cmd": 'echo {"terraform_version":"1.17.2"}\nexit /b 0',
                primary / "az.cmd": (
                    f'if "%~1"=="account" echo {account}\n'
                    f'if "%~1"=="ad" echo {{"id":"{identity}"}}\n'
                    'if "%~1"=="group" echo false\nexit /b 0'
                ),
                primary / "python.cmd": (
                    'if not "%~3"=="preflight" exit /b 83\n'
                    'echo [{"check":"fixture","status":"fail","detail":"FIRST_PYTHON"}]\n'
                    "exit /b 1"
                ),
                secondary / "python.cmd": "exit /b 98",
                secondary / "terraform.cmd": "exit /b 99",
            }
            for path, body in shims.items():
                path.write_text(f"@echo off\n{body}\n", encoding="ascii", newline="\r\n")
            environment = {
                **os.environ,
                "PATH": os.pathsep.join(map(str, (primary, secondary))),
                "LOCALAPPDATA": workdir,
            }
            result = subprocess.run(  # noqa: S603  # fixed argv, shell=False, test shims
                [
                    powershell,
                    "-NoProfile",
                    "-NonInteractive",
                    "-File",
                    str(ROOT / "tools" / "deploy-lab.ps1"),
                    "-TenantId",
                    tenant,
                    "-SubscriptionId",
                    subscription,
                    "-StateContributorObjectId",
                    identity,
                    "-DesktopPublicIp",
                    "8.8.8.8",
                    "-ModelName",
                    "gpt-5.4-mini",
                    "-ModelVersion",
                    "2026-03-17",
                    "-ModelCapacity",
                    "1",
                    "-DynamicQuota",
                    "disabled",
                    "-Owner",
                    "test-owner",
                    "-CostCenter",
                    "test-cost-center",
                ],
                env=environment,
                cwd=workdir,
                capture_output=True,
                text=True,
                timeout=30,
                check=False,
            )
            output = result.stdout + result.stderr
            self.assertNotEqual(result.returncode, 0, output)
            self.assertIn("FIRST_PYTHON", output)
            self.assertIn("Preflight did not pass", output)


def change(
    address: str, actions: list[str], after: dict | None = None, kind: str = "azurerm_x"
) -> dict:
    return {"address": address, "type": kind, "change": {"actions": actions, "after": after or {}}}


class PlanGuardTests(unittest.TestCase):
    def test_tc_h07_blocks_destroy_replace_and_foreign_targets(self) -> None:
        plan = {
            "resource_changes": [
                change("a.ok", ["create"], {"resource_group_name": "rg-foundry-batch-poc"}),
                change("a.del", ["delete"]),
                change("a.rep", ["delete", "create"]),
                change("a.kv", ["delete"], kind="azurerm_key_vault"),
                change("a.other", ["update"], {"resource_group_name": "rg-someone-else"}),
                change(
                    "a.parent",
                    ["create"],
                    {"parent_id": "/subscriptions/x/resourceGroups/rg-other"},
                ),
                change(
                    "a.parent_ok",
                    ["create"],
                    {
                        "parent_id": "/subscriptions/x/resourceGroups/RG-FOUNDRY-BATCH-POC/providers/y"
                    },
                ),
            ]
        }
        problems = plan_guard.check(
            plan, resource_group="rg-foundry-batch-poc", allow_destroy=False
        )
        self.assertEqual(len(problems), 5)
        allowed = plan_guard.check(plan, resource_group="rg-foundry-batch-poc", allow_destroy=True)
        self.assertEqual(sorted(p.split(":")[0] for p in allowed), ["a.kv", "a.other", "a.parent"])


class SettingsFromOutputsTests(unittest.TestCase):
    def outputs(self) -> dict:
        return {
            "environment": {"value": "pilot", "sensitive": False},
            "resources_json": {"value": "[]", "sensitive": False},
            "key_vault_uri": {"value": "https://kv/", "sensitive": False},
            "queue_service_uri": {"value": "https://q", "sensitive": False},
            "table_service_uri": {"value": "https://t", "sensitive": False},
        }

    def test_maps_outputs_and_refuses_sensitive_ones(self) -> None:
        outputs = self.outputs()
        settings = settings_from_outputs.build(outputs)
        self.assertEqual(settings["BATCH_KEY_VAULT_URI"], "https://kv/")
        outputs["key_vault_uri"]["sensitive"] = True
        with self.assertRaises(SystemExit):
            settings_from_outputs.build(outputs)

    def test_environment_comes_from_terraform_not_a_default(self) -> None:
        # I10: a non-default Terraform environment must reach the CLI (secret names use it).
        for expected in (None, "pilot"):
            settings = settings_from_outputs.build(self.outputs(), expected)
            self.assertEqual(settings["BATCH_ENVIRONMENT"], "pilot")
        with self.assertRaisesRegex(SystemExit, "does not match"):
            settings_from_outputs.build(self.outputs(), "poc")
        outputs = self.outputs()
        del outputs["environment"]
        with self.assertRaisesRegex(SystemExit, "output environment missing"):
            settings_from_outputs.build(outputs)

    def test_terraform_root_outputs_the_environment(self) -> None:
        outputs_tf = (ROOT / "infra" / "envs" / "poc" / "outputs.tf").read_text(encoding="utf-8")
        block = outputs_tf[outputs_tf.index('output "environment"') :]
        self.assertRegex(block[: block.index("\n}")], r"value\s*=\s*var\.environment")


class SecretScanTests(unittest.TestCase):
    def test_tc_h05_finds_secret_shapes_without_printing_values(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            value = "whsec_" + base64.b64encode(b"x" * 32).decode()
            Path(tmp, "plan.json").write_text(json.dumps({"v": value}), encoding="utf-8")
            Path(tmp, "ok.txt").write_text(f"{value}  # secret-scan: allow\n", encoding="utf-8")
            Path(tmp, "key.txt").write_text("AccountKey=" + "A" * 60, encoding="utf-8")
            findings = secret_scan.scan([Path(tmp)])
        self.assertEqual(
            sorted(f.split(": ")[-1] for f in findings),
            ["storage_account_key", "webhook_signing_secret"],
        )
        self.assertFalse(any(value in f for f in findings))


class PackageTests(unittest.TestCase):
    def test_package_contains_app_and_library_only_and_is_reproducible(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            first = build_function_package.build(Path(tmp, "a.zip"))
            second = build_function_package.build(Path(tmp, "b.zip"))
            names = zipfile.ZipFile(Path(tmp, "a.zip")).namelist()
        self.assertEqual(first, second)
        self.assertIn("function_app.py", names)
        self.assertIn("host.json", names)
        self.assertIn("foundry_batch/webhook/receiver.py", names)
        self.assertFalse(any(n.startswith("tests/") or "local.settings" in n for n in names))

    def copy_sources(self, root: Path) -> Path:
        shutil.copytree(ROOT / "functions", root / "functions", ignore=IGNORE)
        shutil.copytree(
            ROOT / "src" / "foundry_batch", root / "src" / "foundry_batch", ignore=IGNORE
        )
        return root

    def test_checkout_under_a_folder_named_tests_still_packages_everything(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = self.copy_sources(Path(tmp, "tests", ".venv", "repo"))
            build_function_package.build(Path(tmp, "out.zip"), root)
            names = zipfile.ZipFile(Path(tmp, "out.zip")).namelist()
        self.assertIn("function_app.py", names)
        self.assertIn("host.json", names)
        self.assertIn("foundry_batch/webhook/receiver.py", names)

    def test_incomplete_package_is_refused(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = self.copy_sources(Path(tmp, "repo"))
            (root / "functions" / "host.json").unlink()
            with self.assertRaisesRegex(build_function_package.PackageError, "host.json"):
                build_function_package.build(Path(tmp, "out.zip"), root)

    def test_dependency_lock_replaces_the_ranges_and_can_be_required(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = self.copy_sources(Path(tmp, "repo"))
            (root / "functions" / "requirements.lock").unlink(missing_ok=True)
            out = Path(tmp, "out.zip")
            with self.assertRaisesRegex(build_function_package.PackageError, "make lock"):
                build_function_package.build(out, root, require_lock=True)
            lock = "requests==2.32.3 \\\n    --hash=sha256:" + "0" * 64 + "\n"
            lock_bytes = lock.encode("utf-8")
            (root / "functions" / "requirements.lock").write_bytes(lock_bytes)
            build_function_package.build(out, root, require_lock=True)
            with zipfile.ZipFile(out) as archive:
                self.assertEqual(archive.read("requirements.txt"), lock_bytes)
                self.assertNotIn("requirements.lock", archive.namelist())

    def test_vendored_dependencies_ship_under_python_packages(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            vendor = Path(tmp, "site-packages")
            (vendor / "requests" / "__pycache__").mkdir(parents=True)
            (vendor / "requests" / "__init__.py").write_text("x = 1\n", encoding="utf-8")
            (vendor / "requests" / "__pycache__" / "a.pyc").write_bytes(b"\0")
            (vendor / "requests" / "stray.pyc").write_bytes(b"\0")
            (vendor / "requests-2.32.3.dist-info").mkdir()
            (vendor / "requests-2.32.3.dist-info" / "RECORD").write_text("r\n", encoding="utf-8")
            first = build_function_package.build(Path(tmp, "a.zip"), site_packages=vendor)
            second = build_function_package.build(Path(tmp, "b.zip"), site_packages=vendor)
            names = zipfile.ZipFile(Path(tmp, "a.zip")).namelist()
        self.assertEqual(first, second)
        prefix = ".python_packages/lib/site-packages/"
        self.assertIn(prefix + "requests/__init__.py", names)
        self.assertIn(prefix + "requests-2.32.3.dist-info/RECORD", names)
        self.assertFalse(any(n.endswith(".pyc") or "__pycache__" in n for n in names))
        self.assertIn("function_app.py", names)

    def test_missing_or_empty_vendor_directory_is_refused(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp, "out.zip")
            with self.assertRaisesRegex(build_function_package.PackageError, "not found"):
                build_function_package.build(out, site_packages=Path(tmp, "absent"))
            Path(tmp, "empty").mkdir()
            with self.assertRaisesRegex(build_function_package.PackageError, "empty"):
                build_function_package.build(out, site_packages=Path(tmp, "empty"))
            with redirect_stdout(io.StringIO()) as printed:
                code = build_function_package.main(
                    ["--output", str(out), "--site-packages", str(Path(tmp, "empty"))]
                )
        self.assertEqual(code, 1)
        self.assertIn("empty", printed.getvalue())

    def test_cli_exit_codes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, redirect_stdout(io.StringIO()) as out:
            code = build_function_package.main(["--output", str(Path(tmp, "a.zip"))])
            self.assertEqual(code, 0)
            locked = (ROOT / "functions" / "requirements.lock").is_file()
            strict = ["--output", str(Path(tmp, "b.zip")), "--require-lock"]
            code = build_function_package.main(strict)
            self.assertEqual(code, 0 if locked else 1)
        self.assertIn("sha256=", out.getvalue())


if __name__ == "__main__":
    unittest.main()
