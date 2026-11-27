"""Preflight quota row selection: exact "<Sku>.<model>" match (17 Nov full review, R10)."""

import unittest

from foundry_batch.ops.preflight import PreflightInput, _quota_finding

TARGET = PreflightInput(
    tenant_id="t",
    subscription_id="s",
    resource_group="rg-foundry-batch-poc",
    location="eastus2",
    model_name="gpt-4o",
    model_version="2024-08-06",
    planned_names=["kv-batch"],
    deployer_object_id="oid",
)


def row(name: object, current: int, limit: int) -> dict[str, object]:
    return {"name": name, "currentValue": current, "limit": limit, "unit": "Count"}


class QuotaRowTests(unittest.TestCase):
    def test_a_model_sharing_a_prefix_does_not_satisfy_the_check(self) -> None:
        usages = [
            row({"value": "OpenAI.GlobalBatch.gpt-4o-mini"}, 0, 1_000_000),
            row({"value": "OpenAI.GlobalBatch.gpt-4o"}, 5_000_000, 5_000_000),
        ]
        finding = _quota_finding(usages, TARGET)
        self.assertEqual(finding.status, "warn")
        self.assertIn("5000000 of 5000000", finding.detail)

    def test_only_the_requested_sku_counts(self) -> None:
        usages = [row({"value": "OpenAI.Standard.gpt-4o"}, 0, 10)]
        self.assertIn("no matching", _quota_finding(usages, TARGET).detail)

    def test_exact_row_with_headroom_passes_and_bare_names_match(self) -> None:
        for name in ("OpenAI.GlobalBatch.gpt-4o", "GlobalBatch.GPT-4o"):
            with self.subTest(name=name):
                finding = _quota_finding([row({"value": name}, 1, 10)], TARGET)
                self.assertEqual(finding.status, "pass")

    def test_malformed_rows_are_ignored(self) -> None:
        usages = ["text", row(None, 0, 1), row("OpenAI.GlobalBatch.gpt-4o", 0, 1)]
        self.assertIn("no matching", _quota_finding(usages, TARGET).detail)


if __name__ == "__main__":
    unittest.main()
