"""Private networking guard (REQ-I13, ADR 0010): tools/plan_guard.py public-access check.

Also holds static contract checks on the Terraform sources, so the variables the guard reads and
the private settings stay in step with the code even where no Terraform binary is available.
"""

from __future__ import annotations

import io
import json
import re
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from typing import Any

from tests.unit.test_tools import load

plan_guard = load("plan_guard")

REPO = Path(__file__).resolve().parents[2]
INFRA = REPO / "infra"
SUB = "22222222-2222-2222-2222-222222222222"
RG = "rg-foundry-batch-poc"
RG_ID = f"/subscriptions/{SUB}/resourceGroups/{RG}"
HUB_ZONE = (
    "/subscriptions/99999999-9999-9999-9999-999999999999/resourceGroups/rg-hub-dns/providers/"
    "Microsoft.Network/privateDnsZones/privatelink.blob.core.windows.net"
)

STORAGE = "Microsoft.Storage/storageAccounts@2024-01-01"
SITE = "Microsoft.Web/sites@2025-03-01"
WORKSPACE = "Microsoft.OperationalInsights/workspaces@2023-09-01"


def azapi(
    address: str,
    arm_type: str,
    properties: dict[str, Any] | None,
    *,
    as_json: bool = False,
    actions: list[str] | None = None,
) -> dict[str, Any]:
    after: dict[str, Any] = {"type": arm_type, "parent_id": RG_ID, "location": "eastus2"}
    if properties is not None:
        body = {"properties": properties}
        after["body"] = json.dumps(body) if as_json else body
    return {
        "address": address,
        "mode": "managed",
        "type": "azapi_resource",
        "change": {"actions": actions or ["create"], "after": after},
    }


def azurerm(address: str, kind: str, after: dict[str, Any]) -> dict[str, Any]:
    full = {"resource_group_name": RG, "location": "eastus2", **after}
    return {
        "address": address,
        "mode": "managed",
        "type": kind,
        "change": {"actions": ["create"], "after": full},
    }


def variables(**values: Any) -> dict[str, Any]:
    return {name: {"value": value} for name, value in values.items()}


def guard(
    changes: list[dict[str, Any]], plan_variables: dict[str, Any] | None = None, **options: Any
) -> list[str]:
    plan = {
        "variables": {"allowed_locations": {"value": ["eastus2"]}, **(plan_variables or {})},
        "resource_changes": changes,
    }
    return plan_guard.check(
        plan, resource_group=RG, allow_destroy=False, subscription_id=SUB, **options
    )


def private_storage() -> dict[str, Any]:
    return azapi(
        "module.storage.azapi_resource.account",
        STORAGE,
        {
            "publicNetworkAccess": "Disabled",
            "networkAcls": {"defaultAction": "Deny", "bypass": "None"},
        },
    )


PRIVATE_SCM = {
    "scmIpSecurityRestrictionsDefaultAction": "Deny",
    "scmIpSecurityRestrictionsUseMain": False,
}


def site(public: str, scm: dict[str, Any] | None = None) -> dict[str, Any]:
    return azapi(
        "module.function_app.azapi_resource.site",
        SITE,
        {"publicNetworkAccess": public, "siteConfig": PRIVATE_SCM if scm is None else scm},
    )


def workspace(ingestion: str, query: str) -> dict[str, Any]:
    return azapi(
        "module.monitoring.azapi_resource.workspace",
        WORKSPACE,
        {
            "publicNetworkAccessForIngestion": ingestion,
            "publicNetworkAccessForQuery": query,
        },
    )


def app_insights(ingestion: bool, query: bool) -> dict[str, Any]:
    return azurerm(
        "module.monitoring.azurerm_application_insights.this",
        "azurerm_application_insights",
        {"internet_ingestion_enabled": ingestion, "internet_query_enabled": query},
    )


class PublicAccessGuardTests(unittest.TestCase):
    def test_private_resources_pass(self) -> None:
        vault = azurerm(
            "module.key_vault.azurerm_key_vault.this",
            "azurerm_key_vault",
            {
                "public_network_access_enabled": False,
                "network_acls": [{"default_action": "Deny", "bypass": "None"}],
            },
        )
        changes = [
            private_storage(),
            site("Disabled"),
            workspace("Disabled", "Disabled"),
            app_insights(False, False),
            vault,
        ]
        self.assertEqual(guard(changes), [])

    def test_public_storage_and_vault_are_rejected(self) -> None:
        storage = azapi(
            "st",
            STORAGE,
            {"publicNetworkAccess": "Enabled", "networkAcls": {"defaultAction": "Deny"}},
        )
        vault = azurerm("kv", "azurerm_key_vault", {"public_network_access_enabled": True})
        problems = guard([storage, vault])
        self.assertEqual(
            problems,
            [
                "st: publicNetworkAccess allows public network access (not a declared exception, ADR 0010)",
                "kv: public_network_access_enabled allows public network access (not a declared exception, ADR 0010)",
            ],
        )

    def test_arm_default_counts_as_public(self) -> None:
        # ARM defaults these services to public: an omitted property is a finding.
        problems = guard(
            [azapi("st", STORAGE, {"minimumTlsVersion": "TLS1_2"}), azapi("log", WORKSPACE, {})]
        )
        self.assertIn(
            "st: publicNetworkAccess (not set) allows public network access "
            "(not a declared exception, ADR 0010)",
            problems,
        )
        self.assertEqual(sum("log:" in p for p in problems), 2)

    def test_unknown_body_fails_closed_and_deletes_are_not_judged(self) -> None:
        unknown = azapi("st", STORAGE, None)
        unknown["change"]["after_unknown"] = {"body": True}
        self.assertEqual(
            guard([unknown]),
            [
                "st: publicNetworkAccess (unknown at plan) allows public network access "
                "(not a declared exception, ADR 0010)",
            ],
        )
        # A reviewed address passes; a change without any body has nothing to judge.
        self.assertEqual(guard([unknown], allowed_public_addresses=["st"]), [])
        self.assertEqual(guard([azapi("st", STORAGE, None)]), [])
        deleted = azapi("old", STORAGE, {"publicNetworkAccess": "Enabled"}, actions=["delete"])
        self.assertEqual(guard([deleted]), ["old: delete is not allowed"])

    def test_nx1_never_covers_a_public_deployment_endpoint(self) -> None:
        nx1 = variables(function_public_ingress=True)
        open_scm = site("Enabled", {"scmIpSecurityRestrictionsDefaultAction": "Allow"})
        shared_rules = site("Enabled", {**PRIVATE_SCM, "scmIpSecurityRestrictionsUseMain": True})
        no_config = site("Enabled", {})
        for change in (open_scm, shared_rules, no_config):
            problems = guard([change], nx1)
            self.assertEqual(len(problems), 1, problems)
            self.assertIn("deployment endpoint public", problems[0])
        # A private app needs no SCM rule of its own: the endpoint has no public side.
        self.assertEqual(guard([site("Disabled", {})]), [])

    def test_public_storage_needs_a_closed_firewall_default(self) -> None:
        nx3 = variables(state_public_network_access_enabled=True)
        open_firewall = azapi(
            "state",
            STORAGE,
            {"publicNetworkAccess": "Enabled", "networkAcls": {"defaultAction": "Allow"}},
        )
        problems = guard([open_firewall], nx3)
        self.assertEqual(
            problems,
            [
                "state: networkAcls.defaultAction (firewall open) allows public "
                "network access (not a declared exception, ADR 0010)"
            ],
        )

    def test_update_resources_are_checked_without_defaults(self) -> None:
        update = azapi("upd", SITE, {"publicNetworkAccess": "Enabled", "siteConfig": PRIVATE_SCM})
        update["type"] = "azapi_update_resource"
        self.assertEqual(len(guard([update])), 1)
        partial = azapi("upd2", SITE, {"siteConfig": {"http20Enabled": True}})
        partial["type"] = "azapi_update_resource"
        self.assertEqual(guard([partial]), [])
        unknown = azapi("upd3", SITE, None)
        unknown["type"] = "azapi_update_resource"
        unknown["change"]["after_unknown"] = {"body": True}
        self.assertEqual(guard([unknown]), [])

    def test_json_string_bodies_are_parsed(self) -> None:
        legacy = azapi(
            "st",
            STORAGE,
            {"publicNetworkAccess": "Enabled", "networkAcls": {"defaultAction": "Deny"}},
            as_json=True,
        )
        self.assertEqual(len(guard([legacy])), 1)
        broken = azapi("st2", STORAGE, None)
        broken["change"]["after"]["body"] = "{not json"
        self.assertEqual(guard([broken]), [])

    def test_trusted_service_bypass_is_rejected_everywhere(self) -> None:
        storage = azapi(
            "st",
            STORAGE,
            {"publicNetworkAccess": "Disabled", "networkAcls": {"bypass": "AzureServices"}},
        )
        vault = azurerm(
            "kv",
            "azurerm_key_vault",
            {
                "public_network_access_enabled": False,
                "network_acls": [{"bypass": "AzureServices"}],
            },
        )
        problems = guard([storage, vault])
        self.assertEqual(len(problems), 2)
        self.assertTrue(all("trusted-service bypass" in p for p in problems))
        # Logging/Metrics bypass alone (storage) is not a trusted-service exception.
        logging_only = azapi(
            "st3",
            STORAGE,
            {"publicNetworkAccess": "Disabled", "networkAcls": {"bypass": "Logging, Metrics"}},
        )
        self.assertEqual(guard([logging_only]), [])

    def test_webhook_ingress_needs_the_declared_exception(self) -> None:
        public_site = site("Enabled")
        self.assertEqual(len(guard([public_site])), 1)
        self.assertEqual(guard([public_site], variables(function_public_ingress=True)), [])
        # The NX-1 exception covers the site only, never storage.
        storage = azapi(
            "st",
            STORAGE,
            {"publicNetworkAccess": "Enabled", "networkAcls": {"defaultAction": "Deny"}},
        )
        self.assertEqual(len(guard([storage], variables(function_public_ingress=True))), 1)
        # Declared off: a public site is a finding again.
        self.assertEqual(len(guard([public_site], variables(function_public_ingress=False))), 1)

    def test_portal_queries_exception_covers_queries_only(self) -> None:
        nx2 = variables(monitor_public_query_enabled=True, monitor_private_link_enabled=True)
        self.assertEqual(
            guard([workspace("Disabled", "Enabled"), app_insights(False, True)], nx2), []
        )
        problems = guard([workspace("Enabled", "Enabled"), app_insights(True, True)], nx2)
        self.assertEqual(len(problems), 2)
        self.assertTrue(all("ingestion" in p.lower() for p in problems))

    def test_without_the_private_link_public_ingestion_is_declared(self) -> None:
        no_ampls = variables(monitor_public_query_enabled=True, monitor_private_link_enabled=False)
        self.assertEqual(
            guard([workspace("Enabled", "Enabled"), app_insights(True, True)], no_ampls), []
        )

    def test_bootstrap_state_exception(self) -> None:
        state = azapi(
            "azapi_resource.state",
            STORAGE,
            {
                "publicNetworkAccess": "Enabled",
                "networkAcls": {"bypass": "None", "defaultAction": "Deny"},
            },
        )
        self.assertEqual(len(guard([state])), 1)
        self.assertEqual(guard([state], variables(state_public_network_access_enabled=True)), [])
        self.assertEqual(
            len(guard([state], variables(state_public_network_access_enabled=False))), 1
        )

    def test_reviewed_address_exception(self) -> None:
        storage = azapi(
            "module.x.azapi_resource.account", STORAGE, {"publicNetworkAccess": "Enabled"}
        )
        self.assertEqual(
            guard([storage], allowed_public_addresses=["module.x.azapi_resource.account"]), []
        )

    def test_landing_zone_dns_zones_are_allowed_external_ids(self) -> None:
        endpoint = azurerm(
            "pe",
            "azurerm_private_endpoint",
            {
                "private_dns_zone_group": [{"private_dns_zone_ids": [HUB_ZONE]}],
            },
        )
        self.assertTrue(guard([endpoint]))
        self.assertEqual(
            guard([endpoint], variables(existing_private_dns_zone_ids={"blob": HUB_ZONE})), []
        )

    def test_cli_allow_public(self) -> None:
        storage = azapi("st", STORAGE, {"publicNetworkAccess": "Enabled"})
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp, "plan.json")
            path.write_text(
                json.dumps(
                    {
                        "resource_changes": [storage],
                        "variables": {"allowed_locations": {"value": ["eastus2"]}},
                    }
                ),
                encoding="utf-8",
            )
            base = [str(path), "--resource-group", RG, "--subscription-id", SUB]
            with redirect_stdout(io.StringIO()) as out, redirect_stderr(io.StringIO()):
                self.assertEqual(plan_guard.main(base), 1)
                self.assertEqual(plan_guard.main([*base, "--allow-public", "st"]), 0)
        self.assertIn("st: publicNetworkAccess allows public network access", out.getvalue())


class GuardEdgeCaseTests(unittest.TestCase):
    """Branches the earlier suites left unexercised (body shapes, role assignment scopes)."""

    def test_body_without_properties_counts_as_public_default(self) -> None:
        change = azapi("st", STORAGE, None)
        change["change"]["after"]["body"] = {"kind": "StorageV2"}
        self.assertEqual(
            guard([change]),
            [
                "st: publicNetworkAccess (not set) allows public network access "
                "(not a declared exception, ADR 0010)",
            ],
        )

    def test_non_string_bypass_values_are_ignored(self) -> None:
        storage = azapi(
            "st", STORAGE, {"publicNetworkAccess": "Disabled", "networkAcls": {"bypass": None}}
        )
        vault = azurerm(
            "kv",
            "azurerm_key_vault",
            {"public_network_access_enabled": False, "network_acls": [{"bypass": None}]},
        )
        self.assertEqual(guard([storage, vault]), [])

    def test_role_assignment_scopes(self) -> None:
        table = (
            f"{RG_ID}/providers/Microsoft.Storage/storageAccounts/st/tableServices/default/tables/t"
        )
        ok = azurerm("ra", "azurerm_role_assignment", {"scope": table})
        unknown = azurerm("ra2", "azurerm_role_assignment", {"scope": None})
        self.assertEqual(guard([ok, unknown]), [])
        arm = "Microsoft.Authorization/roleAssignments@2022-04-01"
        on_resource = azapi("ra3", arm, None)
        on_resource["change"]["after"]["parent_id"] = table
        on_group = azapi("ra4", arm, None)
        self.assertEqual(guard([on_resource]), [])
        self.assertEqual(
            guard([on_group]), [f"ra4: role assignment scope {RG_ID} is not a single resource"]
        )


def tf_sources() -> dict[str, str]:
    return {
        path.relative_to(INFRA).as_posix(): path.read_text(encoding="utf-8")
        for path in INFRA.rglob("*.tf")
        if ".terraform" not in path.parts
    }


class TerraformPrivateContractTests(unittest.TestCase):
    """Static checks: the Terraform keeps the private settings the guard and docs rely on."""

    def test_guard_variables_exist_in_the_roots(self) -> None:
        workload = (INFRA / "envs/poc/variables.tf").read_text(encoding="utf-8")
        for name in (
            "function_public_ingress",
            "monitor_public_query_enabled",
            "monitor_private_link_enabled",
            "existing_private_dns_zone_ids",
            "runner_vnet_ids",
            "allowed_locations",
        ):
            self.assertIn(f'variable "{name}"', workload)
        bootstrap = (INFRA / "bootstrap/variables.tf").read_text(encoding="utf-8")
        self.assertIn('variable "state_public_network_access_enabled"', bootstrap)

    def test_no_trusted_service_bypass_and_no_literal_public_access(self) -> None:
        for name, text in tf_sources().items():
            self.assertNotRegex(text, r'(?i)bypass\s*=\s*"AzureServices"', name)
            self.assertNotRegex(text, r'publicNetworkAccess\w*\s*=\s*"Enabled"', name)
            self.assertNotRegex(text, r"public_network_access_enabled\s*=\s*true", name)

    def test_every_private_endpoint_service_has_its_zone(self) -> None:
        sources = tf_sources()
        network = sources["modules/network/main.tf"]
        for zone in (
            "privatelink.blob.core.windows.net",
            "privatelink.queue.core.windows.net",
            "privatelink.table.core.windows.net",
            "privatelink.vaultcore.azure.net",
            "privatelink.openai.azure.com",
            "privatelink.azurewebsites.net",
            "privatelink.monitor.azure.com",
            "privatelink.oms.opinsights.azure.com",
            "privatelink.ods.opinsights.azure.com",
            "privatelink.agentsvc.azure-automation.net",
        ):
            self.assertIn(zone, network)
        subresources = set()
        for text in sources.values():
            subresources |= set(re.findall(r"subresource_names\s*=\s*\[([^\]]*)\]", text))
        joined = " ".join(subresources)
        for sub in ("blob", "sites", "azuremonitor", "vault", "account", "each.value"):
            self.assertIn(sub, joined)

    def test_monitor_zones_are_not_linked_to_runner_vnets(self) -> None:
        network = tf_sources()["modules/network/main.tf"]
        self.assertIn(
            'if pair[1] == "workload" || !contains(keys(local.monitor_zones), pair[0])', network
        )

    def test_function_app_has_a_private_endpoint_and_private_scm(self) -> None:
        app = tf_sources()["modules/function_app/main.tf"]
        self.assertIn('resource "azurerm_private_endpoint" "site"', app)
        self.assertIn('scmIpSecurityRestrictionsDefaultAction = "Deny"', app)
        self.assertIn("scmIpSecurityRestrictionsUseMain       = false", app)

    def test_checkov_skip_still_documents_the_ingress_exception(self) -> None:
        checkov = (INFRA / ".checkov.yaml").read_text(encoding="utf-8")
        self.assertIn("CKV_AZURE_221", checkov)
        self.assertIn("NX-1", checkov)


if __name__ == "__main__":
    unittest.main()
