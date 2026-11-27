"""Read-only preflight (US-7.5): confirm target, permissions, providers, model and quota.

Uses the Azure CLI with ``show``/``list`` verbs only; the runner refuses anything else, never
changes CLI defaults and never registers providers or creates resources. Findings are reported,
not fixed.
"""

from __future__ import annotations

import json
import shutil
import subprocess  # nosec B404: read-only Azure CLI runner with an explicit argument vector
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

READ_ONLY_VERBS = frozenset({"show", "list"})
REQUIRED_PROVIDERS = (
    "Microsoft.CognitiveServices",
    "Microsoft.Web",
    "Microsoft.App",
    "Microsoft.Storage",
    "Microsoft.KeyVault",
    "Microsoft.OperationalInsights",
    "Microsoft.Insights",
    "Microsoft.Network",
    "Microsoft.ManagedIdentity",
)


class UnsafeCommandError(ValueError):
    pass


@dataclass(frozen=True)
class AzCommand:
    group: tuple[str, ...]
    verb: str
    #: ``{"--name": "value"}``; an empty value means a bare switch such as ``--all``.
    options: Mapping[str, str] = field(default_factory=dict)

    def argv(self) -> list[str]:
        if self.verb not in READ_ONLY_VERBS:
            raise UnsafeCommandError(f"verb {self.verb!r} is not read-only")
        args = [*self.group, self.verb]
        for key, value in self.options.items():
            args += [key, value] if value else [key]
        return [*args, "--only-show-errors", "-o", "json"]


Runner = Callable[[AzCommand], Any]


def az_runner(command: AzCommand) -> Any:
    executable = shutil.which("az")
    if executable is None:
        raise RuntimeError("Azure CLI not found")
    completed = subprocess.run(  # noqa: S603  # nosec B603: fixed CLI, read-only argv, shell=False
        [executable, *command.argv()], capture_output=True, text=True, check=False, timeout=120
    )
    if completed.returncode != 0:
        raise RuntimeError(f"az {' '.join(command.group)} {command.verb} failed")
    return json.loads(completed.stdout or "null")


@dataclass(frozen=True)
class Finding:
    check: str
    status: str  # pass | warn | fail
    detail: str


@dataclass(frozen=True)
class PreflightInput:
    tenant_id: str
    subscription_id: str
    resource_group: str
    location: str
    model_name: str
    model_version: str
    sku_name: str = "GlobalBatch"
    planned_names: Sequence[str] = ()
    deployer_object_id: str | None = None


def run_preflight(target: PreflightInput, runner: Runner = az_runner) -> list[Finding]:
    findings: list[Finding] = []

    def safe(name: str, command: AzCommand) -> Any:
        try:
            return runner(command)
        except UnsafeCommandError:
            raise
        except Exception as exc:  # noqa: BLE001 - every failure becomes a finding
            findings.append(Finding(name, "fail", f"could not run check: {type(exc).__name__}"))
            return None

    account = safe("account", AzCommand(("account",), "show"))
    if isinstance(account, Mapping):
        ok = (
            account.get("tenantId") == target.tenant_id
            and account.get("id") == target.subscription_id
        )
        findings.append(
            Finding(
                "account",
                "pass" if ok else "fail",
                "signed-in tenant/subscription match"
                if ok
                else "signed-in tenant/subscription differ from the target; stop",
            )
        )
        if not ok:
            return findings

    sub = {"--subscription": target.subscription_id}
    group = safe(
        "resource_group",
        AzCommand(("group",), "list", {**sub, "--query": f"[?name=='{target.resource_group}']"}),
    )
    if isinstance(group, list):
        if group and group[0].get("location") != target.location:
            findings.append(
                Finding("resource_group", "fail", "resource group exists in a different region")
            )
        else:
            findings.append(
                Finding(
                    "resource_group",
                    "pass",
                    "exists" if group else "absent; bootstrap will create it",
                )
            )

    for provider in REQUIRED_PROVIDERS:
        info = safe(
            f"provider:{provider}",
            AzCommand(("provider",), "show", {**sub, "--namespace": provider}),
        )
        if isinstance(info, Mapping):
            state = info.get("registrationState")
            findings.append(
                Finding(
                    f"provider:{provider}",
                    "pass" if state == "Registered" else "warn",
                    f"registration state {state}; registration needs approval"
                    if state != "Registered"
                    else "registered",
                )
            )

    models = safe(
        "model",
        AzCommand(("cognitiveservices", "model"), "list", {**sub, "--location": target.location}),
    )
    if isinstance(models, list):
        findings.append(_model_finding(models, target))

    usages = safe(
        "quota",
        AzCommand(("cognitiveservices", "usage"), "list", {**sub, "--location": target.location}),
    )
    if isinstance(usages, list):
        findings.append(_quota_finding(usages, target))

    if target.planned_names:
        resources = safe(
            "collisions",
            AzCommand(
                ("resource",),
                "list",
                {**sub, "--query": "[].{name:name, rg:resourceGroup, tags:tags}"},
            ),
        )
        if isinstance(resources, list):
            findings.append(_collision_finding(resources, target))

    if target.deployer_object_id:
        roles = safe(
            "deployer_roles",
            AzCommand(
                ("role", "assignment"),
                "list",
                {**sub, "--assignee": target.deployer_object_id, "--all": ""},
            ),
        )
        if isinstance(roles, list):
            names = sorted({str(r.get("roleDefinitionName")) for r in roles})
            findings.append(
                Finding(
                    "deployer_roles",
                    "pass" if names else "warn",
                    ", ".join(names) or "no role assignments found",
                )
            )

    policies = safe("policy", AzCommand(("policy", "assignment"), "list", sub))
    if isinstance(policies, list):
        names = sorted({str(p.get("displayName") or p.get("name")) for p in policies})
        findings.append(
            Finding(
                "policy",
                "warn" if names else "pass",
                f"{len(names)} policy assignments to review"
                if names
                else "no policy assignments visible",
            )
        )
    return findings


def _model_finding(models: list[Any], target: PreflightInput) -> Finding:
    for item in models:
        model = item.get("model", {}) if isinstance(item, Mapping) else {}
        if model.get("name") != target.model_name or model.get("version") != target.model_version:
            continue
        skus = {str(s.get("name")) for s in model.get("skus", []) if isinstance(s, Mapping)}
        if target.sku_name in skus:
            lifecycle = model.get("lifecycleStatus", "unknown")
            return Finding(
                "model",
                "pass",
                f"{target.model_name} {target.model_version} offers {target.sku_name} "
                f"(lifecycle {lifecycle})",
            )
        return Finding("model", "fail", f"model found but {target.sku_name} is not offered")
    return Finding("model", "fail", "model/version not offered in this region")


def _quota_finding(usages: list[Any], target: PreflightInput) -> Finding:
    # Usage rows are named "<Provider>.<Sku>.<model>" (e.g. "OpenAI.GlobalBatch.gpt-4o"). Match
    # the whole "<Sku>.<model>" suffix: a substring test lets "gpt-4o" match "gpt-4o-mini".
    suffix = f"{target.sku_name}.{target.model_name}".lower()
    rows = [u for u in usages if isinstance(u, Mapping) and _usage_name(u).endswith(f".{suffix}")]
    if not rows:
        return Finding("quota", "warn", "no matching batch quota row found; check the portal")
    row = rows[0]
    limit, current = row.get("limit"), row.get("currentValue")
    return Finding(
        "quota",
        "pass" if limit and (current or 0) < limit else "warn",
        f"usage {current} of {limit} {row.get('unit', '')}".strip(),
    )


def _usage_name(row: Mapping[str, Any]) -> str:
    name = row.get("name")
    value = name.get("value", "") if isinstance(name, Mapping) else ""
    return f".{value}".lower()  # leading dot: a bare "<Sku>.<model>" name also matches


def _collision_finding(resources: list[Any], target: PreflightInput) -> Finding:
    existing = {str(r.get("name")) for r in resources if isinstance(r, Mapping)}
    clashes = sorted(set(target.planned_names) & existing)
    if clashes:
        return Finding("collisions", "fail", "names already used: " + ", ".join(clashes))
    return Finding("collisions", "pass", "no planned name is in use")


def exit_code(findings: Sequence[Finding]) -> int:
    return 1 if any(f.status == "fail" for f in findings) else 0


def to_markdown(findings: Sequence[Finding]) -> str:
    lines = ["| Check | Status | Detail |", "|---|---|---|"]
    lines += [f"| {f.check} | {f.status} | {f.detail} |" for f in findings]
    return "\n".join(lines) + "\n"
