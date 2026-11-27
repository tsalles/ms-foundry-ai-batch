"""Settings contract between Terraform and the app, read from the .tf files themselves.

The earlier test compared Python defaults with hard-coded strings, so drift in Terraform
could not fail it. These tests parse infra/envs/poc and fail on any mismatch.
"""

import re
import unittest
from pathlib import Path

from foundry_batch.config import LOCAL_ENVIRONMENTS, MAX_ALLOWED_BODY_BYTES
from tests.support import make_settings

ROOT = Path(__file__).resolve().parents[2]
ENV_DIR = ROOT / "infra" / "envs" / "poc"
CONFIG_SOURCE = (ROOT / "src" / "foundry_batch" / "config.py").read_text(encoding="utf-8")
LOCALS = (ENV_DIR / "locals.tf").read_text(encoding="utf-8")
VARIABLES = (ENV_DIR / "variables.tf").read_text(encoding="utf-8")
RBAC = (ENV_DIR / "rbac.tf").read_text(encoding="utf-8")
# Used by the Functions bindings (function_app.py), not by Settings.
BINDING_ONLY = {"BATCH_SWEEP_SCHEDULE", "BATCH_OUTBOX_SCHEDULE"}


def app_settings() -> dict[str, str]:
    block = LOCALS[LOCALS.index("app_settings = {") :]
    block = block[: block.index("\n  }")]
    return dict(re.findall(r"^\s+(BATCH_[A-Za-z_]+)\s*=\s*(.+?)\s*$", block, flags=re.M))


def string_list(name: str, source: str = LOCALS) -> list[str]:
    match = re.search(rf"^\s*{name}\s*=\s*\[(.*?)\]", source, flags=re.M | re.S)
    assert match, name
    return re.findall(r'"([^"]+)"', match.group(1))


def variable_block(variable: str) -> str:
    block = VARIABLES[VARIABLES.index(f'variable "{variable}"') :]
    return block[: block.index("\n}\n")]


def validation_bounds(variable: str) -> tuple[int, int]:
    block = variable_block(variable)
    low = re.search(rf"var\.{variable} >= (\d+)", block)
    high = re.search(rf"var\.{variable} <= (\d+)", block)
    assert low and high, f"{variable} has no range validation"
    return int(low.group(1)), int(high.group(1))


def literal(value: str) -> str | None:
    return (
        value[1:-1] if value.startswith('"') and value.endswith('"') and "$" not in value else None
    )


class SettingsContractTests(unittest.TestCase):
    def test_every_terraform_app_setting_is_read_by_the_app(self) -> None:
        settings = app_settings()
        self.assertGreater(len(settings), 15)
        for name in settings:
            if name in BINDING_ONLY or name.startswith("BATCH_STORAGE__"):
                continue
            self.assertIn(f'"{name}"', CONFIG_SOURCE, f"{name} is set but never read")

    def test_literal_names_match_the_app_defaults(self) -> None:
        settings, defaults = app_settings(), make_settings()
        expected = {
            "BATCH_INCOMING_QUEUE": defaults.incoming_queue,
            "BATCH_RELAY_QUEUE": defaults.relay_queue,
            "BATCH_TABLE_JOBS": defaults.table_jobs,
            "BATCH_TABLE_CONTROL": defaults.table_control,
            "BATCH_TABLE_QUARANTINE": defaults.table_quarantine,
            "BATCH_TABLE_SUBMISSIONS": defaults.table_submissions,
            "BATCH_TABLE_DEMO_RECEIPTS": defaults.table_demo_receipts,
            "BATCH_API_TOKEN_SCOPE": defaults.batch_api_token_scope,
        }
        for name, default in expected.items():
            self.assertEqual(literal(settings[name]), default, name)
        self.assertEqual(
            settings["BATCH_RELAY_SIGNING_SECRET_NAME"], '"relay-signing-${var.environment}"'
        )

    def test_provisioned_queues_and_tables_cover_the_settings(self) -> None:
        settings = app_settings()
        queues, tables = set(string_list("queues")), set(string_list("tables"))
        for name in ("BATCH_INCOMING_QUEUE", "BATCH_RELAY_QUEUE"):
            queue = literal(settings[name])
            self.assertIn(queue, queues)
            self.assertIn(f"{queue}-poison", queues)  # monitored by the outbox timer
        for name in settings:
            if name.startswith("BATCH_TABLE_"):
                self.assertIn(literal(settings[name]), tables, name)

    def test_terraform_validations_match_the_app_bounds(self) -> None:
        self.assertEqual(validation_bounds("max_body_bytes"), (1024, MAX_ALLOWED_BODY_BYTES))
        low, high = validation_bounds("timestamp_tolerance_seconds")
        match = re.search(
            r'"BATCH_TIMESTAMP_TOLERANCE_SECONDS", (\d+), (\d+), (\d+)', CONFIG_SOURCE
        )
        assert match
        self.assertEqual((low, high), (int(match.group(2)), int(match.group(3))))

    def test_receiver_limit_variables_match_the_app_bounds_and_defaults(self) -> None:
        defaults = make_settings()
        for variable, env, default in (
            (
                "receiver_rate_per_second",
                "BATCH_RECEIVER_RATE_PER_SECOND",
                defaults.receiver_rate_per_second,
            ),
            ("receiver_burst", "BATCH_RECEIVER_BURST", defaults.receiver_burst),
        ):
            match = re.search(rf'"{env}", ([\d_.]+), ([\d_.]+), ([\d_.]+)', CONFIG_SOURCE)
            assert match, env
            low, high = (float(match.group(i).replace("_", "")) for i in (2, 3))
            self.assertEqual(validation_bounds(variable), (low, high), variable)
            tf_default = re.search(r"default\s*=\s*(\d+)", variable_block(variable))
            assert tf_default
            self.assertEqual(float(tf_default.group(1)), float(default), variable)
            self.assertIn(f"var.{variable}", app_settings()[env])

    def test_host_body_limit_is_set_above_the_app_cap(self) -> None:
        # N3: the receiver's cap runs after the host read the body; the host limit bounds that.
        block = LOCALS[LOCALS.index("app_settings = {") :]
        block = block[: block.index("\n  }")]
        match = re.search(r'^\s*FUNCTIONS_REQUEST_BODY_SIZE_LIMIT\s*=\s*"(\d+)"', block, flags=re.M)
        assert match, "FUNCTIONS_REQUEST_BODY_SIZE_LIMIT is not set"
        self.assertGreaterEqual(int(match.group(1)), MAX_ALLOWED_BODY_BYTES)
        self.assertLessEqual(int(match.group(1)), 1024 * 1024)

    def test_terraform_rejects_the_local_environment_names(self) -> None:
        # I9: these names select the app's local profile, so Terraform must never deploy them.
        block = variable_block("environment")
        match = re.search(r"!contains\(\[(.*?)\],\s*lower\(var\.environment\)\)", block)
        assert match, "variable environment has no reserved-name validation"
        self.assertEqual(set(re.findall(r'"([^"]+)"', match.group(1))), set(LOCAL_ENVIRONMENTS))

    def test_pipeline_identity_has_table_roles_on_the_tables_it_uses(self) -> None:
        # I2: register (control) and the smoke test (registry, submissions, receipts).
        settings, tables = app_settings(), set(string_list("tables"))
        granted = set(string_list("registration_tables", RBAC))
        used = {
            "BATCH_TABLE_CONTROL",
            "BATCH_TABLE_JOBS",
            "BATCH_TABLE_SUBMISSIONS",
            "BATCH_TABLE_DEMO_RECEIPTS",
        }
        self.assertEqual(granted, {literal(settings[name]) for name in used})
        self.assertLessEqual(granted, tables)
        self.assertIn("scope = module.storage.table_ids[table]", RBAC)


if __name__ == "__main__":
    unittest.main()
