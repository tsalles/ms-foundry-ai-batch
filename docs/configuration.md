# Configuration

`foundry_batch.config.Settings.from_env` validates application settings at startup and
reports field names without echoing values. The CLI uses the process environment or
`--settings <json>` for non-secret settings; JSON overrides the process environment.
Azure Functions uses app settings, not a local environment file.

## Environment files

Copy [.env.example](../.env.example) to `.env`, edit it privately and load it explicitly:

```powershell
dotenv -f .env run --no-override -- foundry-batch indicators
```

`python-dotenv[cli]` is a development dependency, not a deployed Functions dependency.
The child process inherits these values without changing the caller's environment.
Keep deployment variables and application settings in separate ignored files when switching
between local doubles and cloud services. Never deploy the example's local backend settings.

## Application settings

| Setting | Default / purpose |
|---|---|
| `BATCH_ENVIRONMENT` | Required; 2-10 lowercase letters/digits starting with a letter |
| `BATCH_RESOURCES_JSON` | Required list of `{key, endpoint, region, deployments}` resources |
| `BATCH_KEY_VAULT_URI` | Vault URI holding signing slots, not a secret value |
| `BATCH_STORAGE__queueServiceUri`, `BATCH_STORAGE__tableServiceUri` | Identity-based storage endpoints; Functions queue bindings use connection prefix `BATCH_STORAGE` |
| `BATCH_INCOMING_QUEUE`, `BATCH_RELAY_QUEUE` | `batch-events`, `relay-dispatch` |
| `BATCH_TABLE_JOBS`, `BATCH_TABLE_CONTROL`, `BATCH_TABLE_QUARANTINE`, `BATCH_TABLE_SUBMISSIONS`, `BATCH_TABLE_DEMO_RECEIPTS` | `batchjobs`, `batchcontrol`, `batchquarantine`, `batchsubmissions`, `demoreceipts` |
| `BATCH_DOWNSTREAM_MODE`, `BATCH_DOWNSTREAM_URL` | `demo`; external `http` mode requires HTTPS outside local loopback |
| `BATCH_RELAY_SIGNING_SECRET_NAME` | Name of the separate relay key in Key Vault |
| `BATCH_MAX_BODY_BYTES` | 32768, maximum 49152; host cap is independently set to 65536 |
| `BATCH_TIMESTAMP_TOLERANCE_SECONDS` | 300 |
| `BATCH_RECEIVER_RATE_PER_SECOND`, `BATCH_RECEIVER_BURST` | 50, 200; per process/resource, not a global edge rate limit |
| `BATCH_UNKNOWN_JOB_POLICY`, `BATCH_UNKNOWN_JOB_GRACE_SECONDS` | `quarantine`, 1800 |
| `BATCH_STATUS_CALL_FLOOR_SECONDS` | 60; cannot be lowered below the minimum |
| `BATCH_SWEEP_SCHEDULE`, `BATCH_OUTBOX_SCHEDULE` | `0 */15 * * * *`, `0 */2 * * * *` |
| `BATCH_SWEEP_PAGE_SIZE`, `BATCH_SWEEP_MAX_CALLS`, `BATCH_SWEEP_LEASE_SECONDS`, `BATCH_SWEEP_OVERLAP_SECONDS` | 100, 200, 600, 3600 |
| `BATCH_MISSED_EVENT_GRACE_SECONDS`, `BATCH_LONG_RUNNING_THRESHOLD_SECONDS` | 1800, 86400 |
| `BATCH_OUTBOX_MAX_ATTEMPTS`, `BATCH_OUTBOX_BASE_DELAY_SECONDS`, `BATCH_OUTBOX_MAX_DELAY_SECONDS`, `BATCH_OUTBOX_RETRY_HORIZON_SECONDS` | 12, 5, 900, 21600 |
| `BATCH_SECRET_CACHE_TTL_SECONDS` | 300 |
| `BATCH_API_TOKEN_SCOPE` | `https://cognitiveservices.azure.com/.default` |
| `BATCH_PROVIDER_BACKEND`, `BATCH_STORAGE_BACKEND`, `BATCH_SECRETS_BACKEND`, `BATCH_FOUNDRY_AUTH`, `BATCH_CLIENT_VAULT_BACKEND` | `foundry`, `azure`, `keyvault`, `entra`, `none`; local doubles are explicit opt-ins |
| `BATCH_VAULT_SYNC_REQUIRED` | Retirement/repair fails closed without fresh destination verification outside local profiles and the standalone `poc` environment |

Local doubles are permitted only for `local`, `dev`, `test` or `ci` outside Azure hosting.
Hosting signals prevent a hosted process from bypassing this rule by claiming a local name.
See the settings module for full ranges and cross-field validation.

## Deployment inputs

Terraform accepts private `TF_VAR_*` variables, including tenant/subscription IDs and tags.
The model, version and capacity come from an ignored `terraform.tfvars` created from its
example. Backend coordinates go in an ignored backend file. These inputs are environment
metadata, not necessarily credentials, but must not be published.

The desktop launcher reads `TF_VAR_tenant_id`, `TF_VAR_subscription_id`, `BATCH_OWNER` and
`BATCH_COST_CENTER`, or the explicit `TenantId`, `SubscriptionId`, `Owner`, `CostCenter`
parameters. Missing deployment inputs fail rather than selecting a default account.

The default cloud authentication uses managed/workload identity or your signed-in CLI
identity. Never put signing keys, storage account keys or access tokens in command arguments,
environment examples, Terraform variables, logs or source control. Use Key Vault for runtime
signing material. Derive non-secret CLI settings with `tools/settings_from_outputs.py` and
keep that generated file private.

## Telemetry and contracts

Structured traces use `batch_event`; queries and the workbook use that same property.
The wire schema is `foundry.batch.event.v1` in
[the JSON schema](schemas/batch-event-v1.schema.json).
Renaming the package, settings prefix, telemetry property and schema intentionally changes
external integration names; there are no compatibility aliases to private deployments.
