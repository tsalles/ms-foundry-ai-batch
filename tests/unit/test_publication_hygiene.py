"""Guard the public source boundary; confidential context also needs human review."""

import re
import subprocess
import unittest
from pathlib import Path

from dotenv import dotenv_values

ROOT = Path(__file__).resolve().parents[2]


class PublicationHygieneTests(unittest.TestCase):
    def test_private_artifacts_are_ignored_but_examples_remain_publishable(self) -> None:
        private_paths = [
            ".env",
            ".env.production",
            "functions/local.settings.json",
            "infra/envs/poc/poc.tfvars",
            "infra/envs/poc/private.tfvars.json",
            "infra/bootstrap/bootstrap.backend.hcl",
            "infra/envs/poc/terraform.tfstate",
            "infra/envs/poc/workload.tfplan",
            "tf-outputs.json",
            "settings.json",
            "artifact.zip",
            "architecture.pdf",
            "signing.key",
            "identity.pem",
            "private.denylist",
        ]
        result = subprocess.run(  # noqa: S603  # fixed paths, no shell
            ["git", "check-ignore", "--no-index", *private_paths],  # noqa: S607
            capture_output=True,
            text=True,
            cwd=ROOT,
            check=False,
        )
        self.assertIn(result.returncode, (0, 1), result.stderr)
        self.assertEqual(set(result.stdout.splitlines()), set(private_paths))
        examples = subprocess.run(
            [  # noqa: S607
                "git",
                "check-ignore",
                "--no-index",
                ".env.example",
                "infra/envs/poc/terraform.tfvars.example",
            ],
            capture_output=True,
            text=True,
            cwd=ROOT,
            check=False,
        )
        self.assertEqual(examples.returncode, 1, examples.stderr)
        self.assertEqual(examples.stdout, "")

    def test_publication_candidates_exclude_private_artifacts(self) -> None:
        result = subprocess.run(
            ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"],  # noqa: S607
            capture_output=True,
            cwd=ROOT,
            check=True,
        )
        candidates = result.stdout.decode("utf-8").rstrip("\0").split("\0")
        self.assertIn(".env.example", candidates)
        forbidden = {
            ".tfvars",
            ".tfstate",
            ".tfplan",
            ".zip",
            ".pdf",
            ".log",
            ".key",
            ".pem",
            ".pfx",
            ".p12",
        }
        for name in candidates:
            path = Path(name)
            if name.endswith(".example"):
                continue
            self.assertFalse(forbidden.intersection(path.suffixes), name)
            self.assertFalse(path.name.startswith(".env") and path.name != ".env.example", name)
            self.assertNotIn(
                path.name, {"pip.ini", "pip.conf", "es-metadata.yml", "local.settings.json"}
            )

    def test_example_environment_is_local_and_cloud_ids_are_placeholders(self) -> None:
        from foundry_batch.config import Settings

        values = dotenv_values(ROOT / ".env.example", interpolate=False)
        self.assertTrue(all(value is not None for value in values.values()))
        env = {key: value for key, value in values.items() if value is not None}
        settings = Settings.from_env(env)
        self.assertTrue(settings.local_profile)
        self.assertTrue(settings.uses_local_doubles)
        self.assertEqual(env["TF_VAR_tenant_id"], "00000000-0000-0000-0000-000000000000")
        self.assertEqual(env["TF_VAR_subscription_id"], "00000000-0000-0000-0000-000000000000")

    def test_launcher_has_no_deployment_account_or_owner_defaults(self) -> None:
        script = (ROOT / "tools" / "deploy-lab.ps1").read_text(encoding="utf-8")
        self.assertNotRegex(
            script, re.compile(r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}", re.I)
        )
        self.assertIn("tags = @{ owner = $Owner; 'cost-center' = $CostCenter }", script)
        self.assertIn("$env:TF_VAR_tenant_id", script)
        self.assertIn("$env:TF_VAR_subscription_id", script)

    def test_pipeline_downloads_private_inputs_instead_of_committing_them(self) -> None:
        pipeline = (ROOT / "azure-pipelines.yml").read_text(encoding="utf-8")
        self.assertIn("DownloadSecureFile@1", pipeline)
        self.assertIn("varFile: $(workloadInputs.secureFilePath)", pipeline)
        self.assertNotIn("/poc.tfvars", pipeline)
        self.assertFalse((ROOT / "infra" / "envs" / "poc" / "poc.tfvars").exists())
