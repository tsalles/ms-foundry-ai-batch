# Infrastructure

Terraform provisions the resource group, state storage, Foundry accounts/deployments, VNet,
private endpoints, queues/tables, Key Vault, Flex Function App, monitoring and scoped roles.
Webhook registrations and secret values are created by the operator CLI, never Terraform.

## Private inputs

1. Review the [deployment architecture](../docs/architecture-deployment.md) and its explicit
   network exceptions. Confirm model/SKU availability and data residency.
2. Copy the root [.env.example](../.env.example) to an ignored `.env`. Replace deployment IDs
   and tags. Use an authorized CLI/workload identity; no application password is required.
3. Copy each root's `terraform.tfvars.example` to its ignored `terraform.tfvars`.
   When using `TF_VAR_*`, remove the duplicate tenant/subscription/tag placeholder assignments
   from the local variable file: Terraform variable files override environment variables.
   Pin approved model name, version and capacity in the workload file.
4. Create private backend files from the examples after bootstrap. Never publish actual
   backend coordinates, state, saved plans or output JSON.

The default region is an example, not a residency approval. Global Batch may process
inference outside the resource region; use synthetic data only. Change allowed regions and
model inputs only after checking provider documentation and your own requirements.

## Order of operations

Run read-only preflight with a private copy of [preflight.example.json](../samples/preflight.example.json).
Resolve failures before any plan. The tooling does not register resource providers implicitly.

Initialize, validate and test both Terraform roots before planning. The bootstrap root starts
with local state and provides separate bootstrap/workload state containers. Migrate bootstrap
state only after verifying remote access. The workload root references, rather than creates,
the resource group. Use private state access once the workload network and runner exist.

Example local validation (no cloud apply):

```powershell
terraform -chdir=infra\bootstrap init -backend=false -lockfile=readonly
terraform -chdir=infra\bootstrap validate
terraform -chdir=infra\bootstrap test
terraform -chdir=infra\envs\poc init -backend=false -lockfile=readonly
terraform -chdir=infra\envs\poc validate
terraform -chdir=infra\envs\poc test
```

For a real plan, load the private environment explicitly:

```powershell
dotenv -f .env run --no-override -- terraform -chdir=infra\envs\poc plan -var-file=terraform.tfvars -out=workload.tfplan
```

Run `tools/plan_guard.py` and strict `tools/secret_scan.py` on a private JSON rendering of each
saved plan. Review the exact saved plan independently, then apply only that plan through an
approved workflow. The guard blocks deletes/replacements, foreign scope and undeclared public
access; passing it is not deployment approval.

## Windows desktop launcher

[deploy-lab.ps1](../tools/deploy-lab.ps1) reads deployment IDs and tags from parameters or the
environment. It requires a public desktop egress IP and explicit model, version, capacity and
dynamic-quota choice. There are no account defaults. Applying requires an approval reference
and per-plan typed confirmation. Without `-Apply`, no Azure resources or state are written.

State and saved plans are stored outside the checkout under the user's local application-data
directory, separated by subscription/resource group. Treat that directory as restricted.
The script provisions infrastructure only; app packaging, registration and smoke testing are
separate steps from a private runner.

## Packaging

The Functions runtime targets Linux Python 3.11. Generate the hashed lock and build dependency
packages on that platform. [build-linux.ps1](../tools/build-linux.ps1) defaults to public PyPI
and accepts an explicit credential-free HTTPS index override. It requires a running container
engine. `Package` audits the hashed lock and embeds dependencies so deployment can disable
remote builds. A source-only zip is not a verified deployable Linux artifact.

## Optional Azure DevOps deployment pipeline

[azure-pipelines.yml](../azure-pipelines.yml) is a reusable optional workflow, not a configured
deployment. Supply the documented generic variable group, federated service connections,
private agent pool, approval environment, state account and synthetic smoke deployment.
Store the workload variable file as an approved Secure File and set
`WORKLOAD_TFVARS_SECURE_FILE` to its name; it is downloaded only by the plan job.
Keep resource-group/state-key settings consistent across plan, apply and deployment.
Saved plan/output artifacts require private access controls and short retention.

The [GitHub workflow](../.github/workflows/ci.yml) performs local-only CI and does not deploy.
Do not add Azure credentials merely to run its tests.
