# Runtime and data

## Intake and processing

1. The receiver verifies headers against the raw bytes and enabled signing slots.
2. A supported event is acknowledged only after the queue send is acknowledged. A timeout
   can mean a committed send, so retries must be safe.
3. The processor loads job state and decides whether to apply progress, terminal evidence,
   a correction, a deferral or quarantine.
4. A conditional transaction updates the job and creates applicable receipt/outbox rows.
   Stale ETags or duplicate creates cause a reload rather than duplicate effects.
5. A queue nudge and timer independently wake the dispatcher.

Unknown jobs are deferred for a bounded submission-linking grace, then quarantined by
default. Adoption is an explicit setting intended only for resources owned by the application.
Direct status calls share a per-job reservation with a minimum 60-second gap.

## Storage boundaries

| Table | Contents |
|---|---|
| `batchjobs` | Job state, per-job receipts, outbox records and status-call reservations |
| `batchcontrol` | Leases, reconciliation checkpoints, registration and vault-sync metadata |
| `batchsubmissions` | Upload/create intent and recovery correlation |
| `batchquarantine` | Restricted material retained for operator triage |
| `demoreceipts` | Demonstration downstream idempotency receipts |

Job keys include environment, resource and batch ID. Transactional records share the job
partition. Queues include `batch-events`, `relay-dispatch` and host-managed poison queues.
Queue messages expire after 14 days. Receipt/lifecycle rows have no automatic cleanup job.

## Delivery and recovery

The dispatcher sends the [normalized contract](event-contract-v1.md) using the same stable
key for retries and replay. Network failures, selected transient HTTP statuses and lost
responses use bounded backoff. Non-retryable failures are quarantined. A downstream 2xx is
delivery confirmation, not evidence that a different downstream system applied business rules.

The reconciler uses resource-scoped leases and resumable list/fallback checkpoints.
It can recover a terminal state when the webhook is missing and can fill missing details
without generating a second business event. Authoritative corrections have a distinct key
linked to the prior event.

Ambiguous batch creation must be resolved through submission recovery; do not blindly
resubmit. Replay defaults to dry run and records the operator and reason.
