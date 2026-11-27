# Deployment and operations architecture

## Topology

The bootstrap root creates a resource group and identity-based state storage with separate
bootstrap/workload containers. The workload root references that resource group and creates
the application, managed identity, Foundry resources, storage, vault, networking and monitoring.
An optional private runner supports release and operational work inside the network boundary.

Storage, Key Vault and Foundry use private endpoints. The Function App has VNet integration
and a private inbound endpoint; SCM deployment access is private. Dependencies are packaged on
a Linux build host so the application does not need public package-index access.

## Explicit network exceptions

| ID | Purpose | Control |
|---|---|---|
| NX-1 | Public Function endpoint for provider webhooks | Signature and timestamp checks, body cap, optional source tags; SCM stays private. Disable `function_public_ingress` for polling-only mode and unregister old webhooks first. |
| NX-2 | Operator log queries from outside the VNet | Entra authorization; disable `monitor_public_query_enabled` once private operator access works. |
| NX-3 | State bootstrap before a private runner exists | Restricted source IPs and identity-based access; migrate to a private endpoint after verification. |
| NX-2b | Public telemetry ingestion if private monitoring is explicitly disabled | Entra-only ingestion; prefer the default `monitor_private_link_enabled = true`. |

These are sample options, not approvals for any organization. The plan guard rejects
undeclared public access and trusted-service bypass. External downstream routes require
separately designed connectivity; the default egress lockdown blocks public destinations.

## Identity and signing material

The application uses a managed identity for storage, Key Vault and Foundry. Terraform state
uses Entra authentication, not storage keys. Registration uses a separately privileged
identity to capture provider-issued signing secrets directly into Key Vault.

Two slots (`whsec-<env>-<resource>-a` and `-b`) allow overlap during rotation.
The relay uses its own `relay-signing-<env>` secret. Only names and version references belong
in settings, state and logs. A production external-vault integration requires an approved
adapter, placement, identity, network path and exact-version readback verification.

## Acceptance before cloud use

Verify provider API support, model capacity and residency; private DNS and routing; managed
identity grants; packaged dependencies; signature conformance; missing-webhook recovery;
duplicate delivery; slot rotation; alerts; and private state access.
Use only synthetic jobs. See [infrastructure](../infra/README.md) and
[runbooks](runbooks/README.md). Local tests do not establish cloud acceptance.
