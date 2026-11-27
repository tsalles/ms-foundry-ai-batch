# Architecture

This sample tracks batch jobs without requiring a caller to poll each job continuously.
It separates authenticated observations from durable state and downstream business effects.

## Components

- **Receiver:** checks a Standard Webhooks signature over the unchanged body, timestamp,
  resource slot, body size and payload shape, then queues supported events.
- **Processor:** normalizes lifecycle observations and conditionally updates the job,
  deduplication receipt and outbox within one job partition.
- **Dispatcher:** claims due outbox items, signs delivery and uses a stable idempotency key.
- **Reconciler:** pages provider job lists and performs paced authoritative retrieval for
  unresolved jobs, missing details or conflicting terminal evidence.
- **Registrar:** creates and rotates provider endpoints with one-time secret capture into
  Key Vault. No secret values are managed by Terraform.
- **Metrics:** reports lifecycle windows, queued-token estimates, API pressure and sweep lag.
- **Local doubles:** exercise the same application paths without contacting live services.

See [runtime and state](architecture-runtime.md) and
[deployment and trust boundaries](architecture-deployment.md).

## Design constraints

The public sample uses Azure Functions Flex Consumption, Storage Queues and Tables,
Key Vault, Foundry resources, Application Insights and Log Analytics.
Azure data-plane paths are private by default. Webhook ingress, operator log queries and
initial state bootstrap have explicit, configurable exceptions; review them before use.

The downstream API is deliberately generic. The built-in receiver demonstrates signing and
deduplication, not an external application's authorization or business rules.
An external secret-store port has an emulator, but no production destination adapter.

## Data handling

Use synthetic input files. Normalized events contain metadata and file identifiers, not
prompts, model outputs, provider error bodies or signing-secret values. Quarantined intake
material and submission metadata remain restricted operational data and need access controls
and retention policies. Do not treat metadata or Terraform plans as public just because they
are not credentials.

## Known boundaries

At-least-once delivery is intentional: a receiver can commit an effect and lose its response.
The downstream API must durably deduplicate by `idempotency_key`. Local tests prove application
invariants, not private DNS, cloud RBAC, regional capacity or cloud availability.
