"""Fail a Terraform plan that destroys/replaces resources or reaches outside the target RG.

Input: `terraform show -json tfplan`. US-7.1 AC3: reject unintended destruction/replacement and
target mismatch. An approved teardown passes `--allow-destroy` explicitly.

Every ARM id found anywhere in a planned resource (scope, target_resource_id, nested
private_connection_resource_id, ids embedded in JSON strings, ...) must stay inside the target
subscription and resource group. The only exceptions are role definition ids and ids allowed
explicitly: `--allow-external-id` or the plan's `runner_vnet_ids` variable (peered runner VNets
linked to the private DNS zones). Role assignments must be scoped to a resource, never to the
resource group, subscription or a management group. Locations must be in `--allowed-location`,
else the plan's `allowed_locations` variable; with neither, any location fails (fail closed).

Private networking (REQ-I13, ADR 0010): no planned resource may accept public network traffic
or a trusted-service bypass, except the exceptions the plan itself declares through its
variables (``function_public_ingress`` -> NX-1, ``monitor_public_query_enabled`` -> NX-2,
``monitor_private_link_enabled = false`` -> public ingestion,
``state_public_network_access_enabled`` -> NX-3) or a reviewed ``--allow-public ADDRESS``.
ARM bodies of storage accounts, workspaces and sites must state their public access
explicitly, because ARM defaults to public; a body unknown at plan time fails (fail closed).
NX-1 never covers the site's deployment (SCM) endpoint, and a public storage account (NX-3)
must keep its firewall default at Deny.
Zones in the plan's ``existing_private_dns_zone_ids`` (landing zone DNS) are allowed external ids.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any

# Resources that hold state worth protecting even during an approved change: secrets, the job
# registry (storage account and its tables), the Foundry account and deployments (deleting
# them fails running batch jobs) and the telemetry workspace.
PROTECTED_TYPES = {
    "azurerm_resource_group",
    "azurerm_key_vault",
    "azurerm_cognitive_account",
    "azurerm_cognitive_deployment",
    "azurerm_log_analytics_workspace",
}
# azapi resources all share one Terraform type; their ARM type (``type`` attribute) decides.
PROTECTED_ARM_TYPES = (
    "Microsoft.Storage/storageAccounts@",
    "Microsoft.Storage/storageAccounts/tableServices/tables@",
    "Microsoft.KeyVault/vaults@",
    "Microsoft.OperationalInsights/workspaces@",
)
ROLE_ASSIGNMENT_TYPES = {"azurerm_role_assignment"}
ROLE_ASSIGNMENT_ARM_TYPE = "microsoft.authorization/roleassignments@"
# Locations that are not regions (action groups, DNS zones and other global resources).
GLOBAL_LOCATIONS = {"global"}

_SEGMENT = r"[^/\s\"'\\,;()\[\]{}]+"
ARM_ID = re.compile(
    rf"(?i)(?<![A-Za-z0-9])(?P<id>/(?:subscriptions|providers)/{_SEGMENT}(?:/{_SEGMENT})*)"
)
ROLE_DEFINITION = re.compile(
    r"(?i)^(?:/subscriptions/[^/]+)?/providers/Microsoft\.Authorization/roleDefinitions/[^/]+$"
)


def _protected(change: dict[str, Any]) -> bool:
    if change.get("type") in PROTECTED_TYPES:
        return True
    if change.get("type") != "azapi_resource":
        return False
    details = change.get("change", {})
    for state in (details.get("before"), details.get("after")):
        arm_type = state.get("type") if isinstance(state, dict) else None
        if isinstance(arm_type, str) and arm_type.startswith(PROTECTED_ARM_TYPES):
            return True
    return False


def _walk(value: Any, path: str = "") -> Iterator[tuple[str, str, Any]]:
    """Yield (path, key, value) for every leaf, recursing through objects and lists."""
    if isinstance(value, dict):
        for key, item in value.items():
            child = f"{path}.{key}" if path else str(key)
            if isinstance(item, dict | list):
                yield from _walk(item, child)
            else:
                yield child, str(key), item
    elif isinstance(value, list):
        for index, item in enumerate(value):
            child = f"{path}[{index}]"
            if isinstance(item, dict | list):
                yield from _walk(item, child)
            else:
                yield child, path.rsplit(".", 1)[-1], item


# Public-network settings by kind of resource. azurerm attributes are checked by value (the
# provider fills in its defaults, so the plan always carries them); azapi ARM bodies must also
# state the setting, since an omitted property means the service default (public).
PUBLIC_ATTRIBUTES = (
    "public_network_access_enabled",
    "internet_ingestion_enabled",
    "internet_query_enabled",
)
PUBLIC_BODY_KEYS = (
    "publicNetworkAccess",
    "publicNetworkAccessForIngestion",
    "publicNetworkAccessForQuery",
)
REQUIRED_BODY_KEYS = {
    "microsoft.storage/storageaccounts@": ("publicNetworkAccess",),
    "microsoft.web/sites@": ("publicNetworkAccess",),
    "microsoft.operationalinsights/workspaces@": (
        "publicNetworkAccessForIngestion",
        "publicNetworkAccessForQuery",
    ),
}


def _arm_type(after: dict[str, Any]) -> str:
    value = after.get("type")
    return value.lower() if isinstance(value, str) else ""


def _body(after: dict[str, Any]) -> dict[str, Any] | None:
    body = after.get("body")
    if isinstance(body, str):  # azapi v1 plans carry the body as a JSON string
        try:
            body = json.loads(body)
        except ValueError:
            return None
    return body if isinstance(body, dict) else None


def _bypass_findings(value: Any) -> list[str]:
    return [
        f"{path} (trusted-service bypass)"
        for path, key, item in _walk(value)
        if key == "bypass" and isinstance(item, str) and "azureservices" in item.lower()
    ]


def _is_public(value: Any) -> bool:
    return isinstance(value, str) and value.lower() != "disabled"


def _body_findings(arm_type: str, body: dict[str, Any], *, partial: bool) -> list[str]:
    """Findings in an ARM body; ``partial`` bodies (azapi_update_resource) skip the defaults."""
    properties = body.get("properties")
    properties = properties if isinstance(properties, dict) else {}
    found: list[str] = []
    if not partial:
        for prefix, keys in REQUIRED_BODY_KEYS.items():
            if arm_type.startswith(prefix):
                found += [f"{key} (not set)" for key in keys if key not in properties]
    found += [key for key in PUBLIC_BODY_KEYS if _is_public(properties.get(key))]
    public = _is_public(properties.get("publicNetworkAccess"))
    if public and arm_type.startswith("microsoft.web/sites@"):
        # NX-1 covers the app's routes, never the deployment (SCM) endpoint.
        site_config = properties.get("siteConfig")
        site_config = site_config if isinstance(site_config, dict) else {}
        scm_default = site_config.get("scmIpSecurityRestrictionsDefaultAction")
        if site_config.get("scmIpSecurityRestrictionsUseMain") is True or scm_default != "Deny":
            found.append(
                "siteConfig.scmIpSecurityRestrictionsDefaultAction (deployment endpoint public)"
            )
    if public and arm_type.startswith("microsoft.storage/storageaccounts@"):
        # NX-3 is an IP-restricted endpoint: the firewall default must stay Deny.
        acls = properties.get("networkAcls")
        acls = acls if isinstance(acls, dict) else {}
        if str(acls.get("defaultAction", "")).lower() != "deny":
            found.append("networkAcls.defaultAction (firewall open)")
    return found + _bypass_findings(body)


def _public_settings(change: dict[str, Any], after: dict[str, Any]) -> list[str]:
    """Public-network settings this resource enables or leaves to a public default."""
    kind = change.get("type")
    if kind in ("azapi_resource", "azapi_update_resource"):
        arm_type = _arm_type(after)
        body = _body(after)
        if body is None:
            unknown = change.get("change", {}).get("after_unknown")
            body_unknown = isinstance(unknown, dict) and unknown.get("body") is True
            if kind == "azapi_resource" and body_unknown:
                # Fail closed: a body that cannot be read cannot be shown to be private.
                return [
                    f"{key} (unknown at plan)"
                    for prefix, keys in REQUIRED_BODY_KEYS.items()
                    if arm_type.startswith(prefix)
                    for key in keys
                ]
            return []  # no body in this change (or an update with nothing to judge)
        return _body_findings(arm_type, body, partial=kind == "azapi_update_resource")
    found = [key for key in PUBLIC_ATTRIBUTES if after.get(key) is True]
    return found + _bypass_findings(after)


def _declared_public(plan: dict[str, Any]) -> set[tuple[str, str]]:
    """(resource kind, setting) pairs the plan's own variables declare as exceptions."""
    allowed: set[tuple[str, str]] = set()
    if _variable(plan, "function_public_ingress") is True:  # NX-1 webhook ingress
        allowed.add(("microsoft.web/sites@", "publicNetworkAccess"))
    if _variable(plan, "monitor_public_query_enabled") is True:  # NX-2 portal queries
        allowed.add(("microsoft.operationalinsights/workspaces@", "publicNetworkAccessForQuery"))
        allowed.add(("azurerm_application_insights", "internet_query_enabled"))
    if _variable(plan, "monitor_private_link_enabled") is False:  # no AMPLS: public ingestion
        workspace = "microsoft.operationalinsights/workspaces@"
        allowed.add((workspace, "publicNetworkAccessForIngestion"))
        allowed.add(("azurerm_application_insights", "internet_ingestion_enabled"))
    if _variable(plan, "state_public_network_access_enabled") is True:  # NX-3 bootstrap state
        allowed.add(("microsoft.storage/storageaccounts@", "publicNetworkAccess"))
    return allowed


def _public_problems(
    change: dict[str, Any],
    after: dict[str, Any],
    declared: set[tuple[str, str]],
    allowed_addresses: set[str],
) -> list[str]:
    address = change.get("address", "?")
    if address in allowed_addresses:
        return []
    kind = change.get("type", "")
    arm_type = _arm_type(after)
    problems: list[str] = []
    for setting in _public_settings(change, after):
        name = setting.split(" ", 1)[0]
        if (kind, name) in declared:
            continue
        if any(arm_type.startswith(prefix) and name == key for prefix, key in declared):
            continue
        problems.append(
            f"{address}: {setting} allows public network access "
            "(not a declared exception, ADR 0010)"
        )
    return problems


def _location(value: str) -> str:
    return value.replace(" ", "").lower()


def _variable(plan: dict[str, Any], name: str) -> Any:
    entry = plan.get("variables", {}).get(name)
    return entry.get("value") if isinstance(entry, dict) else None


def _id_problem(
    arm_id: str, *, resource_group: str, subscription_id: str | None, allowed: set[str]
) -> str | None:
    if arm_id.lower() in allowed or ROLE_DEFINITION.match(arm_id):
        return None
    segments = arm_id.strip("/").split("/")
    if segments[0].lower() == "providers":
        if len(segments) > 1 and segments[1].lower() == "microsoft.management":
            return "management group reference"
        return "tenant-level reference"
    if subscription_id and segments[1].lower() != subscription_id.lower():
        return "outside the target subscription"
    if len(segments) < 4 or segments[2].lower() != "resourcegroups":
        return "subscription-level reference"
    if segments[3].lower() != resource_group.lower():
        return "outside the target resource group"
    return None


def _role_assignment_problem(change: dict[str, Any], after: dict[str, Any]) -> str | None:
    if change.get("type") in ROLE_ASSIGNMENT_TYPES:
        scope = after.get("scope")
    elif str(after.get("type", "")).lower().startswith(ROLE_ASSIGNMENT_ARM_TYPE):
        scope = after.get("parent_id")
    else:
        return None
    if not isinstance(scope, str):
        return None  # unknown until apply: derived from resources this plan also checks
    segments = [s.lower() for s in scope.strip("/").split("/")]
    if (
        len(segments) >= 8
        and segments[0] == "subscriptions"
        and segments[2] == "resourcegroups"
        and segments[4] == "providers"
    ):
        return None
    return f"role assignment scope {scope} is not a single resource"


def check(
    plan: dict[str, Any],
    *,
    resource_group: str,
    allow_destroy: bool,
    subscription_id: str | None = None,
    allowed_locations: Iterable[str] | None = None,
    allowed_external_ids: Iterable[str] = (),
    allowed_public_addresses: Iterable[str] = (),
) -> list[str]:
    problems: list[str] = []
    locations = list(allowed_locations or [])
    if not locations:
        planned = _variable(plan, "allowed_locations")
        locations = [str(v) for v in planned] if isinstance(planned, list) else []
    allowed_regions = {_location(v) for v in locations} | GLOBAL_LOCATIONS
    external = {v.lower() for v in allowed_external_ids}
    for name in ("runner_vnet_ids", "existing_private_dns_zone_ids"):
        declared_ids = _variable(plan, name)
        if isinstance(declared_ids, dict):
            external |= {str(v).lower() for v in declared_ids.values()}
    declared_public = _declared_public(plan)
    public_addresses = set(allowed_public_addresses)

    def add(problem: str) -> None:
        if problem not in problems:
            problems.append(problem)

    for change in plan.get("resource_changes", []):
        if change.get("mode") == "data":
            continue
        actions = change.get("change", {}).get("actions", [])
        address = change.get("address", "?")
        if "delete" in actions:
            kind = "replace" if "create" in actions else "delete"
            if not allow_destroy or _protected(change):
                add(f"{address}: {kind} is not allowed")
        after = change.get("change", {}).get("after")
        if not isinstance(after, dict):
            continue
        # Child resources (azapi containers, tables) carry an empty location.
        location = after.get("location") or ""
        if isinstance(location, str) and location and _location(location) not in allowed_regions:
            add(f"{address}: location {location} is not an allowed location")
        role_problem = _role_assignment_problem(change, after)
        if role_problem:
            add(f"{address}: {role_problem}")
        if actions != ["delete"]:
            for problem in _public_problems(change, after, declared_public, public_addresses):
                add(problem)
        for path, key, value in _walk(after):
            if not isinstance(value, str):
                continue
            if key == "resource_group_name" and value and value.lower() != resource_group.lower():
                add(f"{address}: {path} targets resource group {value}")
            for match in ARM_ID.finditer(value):
                problem = _id_problem(
                    match.group("id"),
                    resource_group=resource_group,
                    subscription_id=subscription_id,
                    allowed=external,
                )
                if problem:
                    add(f"{address}: {path} {problem}")
    return problems


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("plan_json")
    parser.add_argument("--resource-group", required=True)
    parser.add_argument("--subscription-id", required=True)
    parser.add_argument("--allow-destroy", action="store_true")
    parser.add_argument(
        "--allowed-location",
        action="append",
        default=[],
        help="approved region (repeatable); default: the plan's allowed_locations variable",
    )
    parser.add_argument(
        "--allow-public",
        action="append",
        default=[],
        help="resource address allowed to keep public network access (reviewed exception)",
    )
    parser.add_argument(
        "--allow-external-id",
        action="append",
        default=[],
        help="ARM id outside the resource group that the plan may reference (repeatable)",
    )
    args = parser.parse_args(argv)
    plan = json.loads(Path(args.plan_json).read_text(encoding="utf-8"))
    problems = check(
        plan,
        resource_group=args.resource_group,
        allow_destroy=args.allow_destroy,
        subscription_id=args.subscription_id,
        allowed_locations=args.allowed_location,
        allowed_external_ids=args.allow_external_id,
        allowed_public_addresses=args.allow_public,
    )
    for problem in problems:
        print(problem)
    print(f"{len(problems)} problem(s)", file=sys.stderr)
    return 1 if problems else 0


if __name__ == "__main__":
    raise SystemExit(main())
