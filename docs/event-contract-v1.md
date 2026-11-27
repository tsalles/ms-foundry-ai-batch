# Batch event contract v1

The producer is `foundry_batch.domain.contract.NormalizedEvent`.
The machine-readable contract is [batch-event-v1.schema.json](schemas/batch-event-v1.schema.json).
The generic schema identifier is `foundry.batch.event.v1`.

The relay posts metadata only: environment, resource-scoped job ID, provider batch ID, region,
deployment, model, correlation, raw status, normalized outcome, provider timestamps,
observation timestamps, request counts, output/error file IDs, source and correction metadata.
Missing provider data remains null/unavailable rather than a synthetic zero.
Prompts, outputs, provider error bodies and signing secrets are excluded.

`completed` maps to completed, `cancelled` to cancelled, and both `failed` and `expired` to
failed while retaining the raw provider status. The event carries a stable `idempotency_key`,
also sent in `Idempotency-Key` and `webhook-id`. Standard Webhooks headers sign the unchanged
body with the separate relay secret.

## Consumer obligations

- Verify signature and timestamp before parsing or applying effects.
- Durably deduplicate on `idempotency_key`, including retries and operator replay.
- Return 2xx only after durable acceptance.
- Treat authoritative corrections as linked events, not duplicate original outcomes.
- Do not infer request contents from file IDs or expose them in public telemetry.

The relay retries 408, 409, 425, 429, 5xx and network errors with backoff and `Retry-After`.
Transient failures are quarantined only after both attempt and age thresholds are met.
Other 4xx and redirects are quarantined; redirects are not followed. Confirm authorization,
deduplication retention and field compatibility for each external downstream system.
