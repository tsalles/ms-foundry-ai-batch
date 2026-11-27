# Operator runbooks

Use an authorized identity from a host that can reach the private endpoints.
Load non-secret settings with `--settings` or an explicit private environment file.
Never paste secret values, actual deployment evidence or state into public issues.

## Register and rotate

- `foundry-batch relay-secret ensure` creates the separate relay key if absent.
- `foundry-batch register ensure --resource <key> --receiver-base-url <url>` captures a
  one-time provider signing secret into Key Vault.
- Check `register health --resource <key>` before submitting synthetic jobs.
- `register rotate --resource <key> --receiver-base-url <url>` uses the free signing slot.
  Verify replacement delivery and, where required, fresh external-vault readback before
  `register retire --resource <key> --slot <a-or-b>`.
- A failed capture can leave an endpoint requiring recovery if cleanup also fails.
  Follow the explicit CLI result; never overwrite a live slot blindly.
- `--force` bypasses safety gates and requires independently approved emergency handling.

## Reconcile

Use `foundry-batch reconcile --resource <key>` after checking provider reachability, throttles,
queue health and the sweep lease. Preserve the shared status-call floor. A stale checkpoint
or interrupted sweep is recoverable; do not delete control rows to force progress.

## Replay and poison messages

`foundry-batch replay outbox --operator <alias> --reason <reason>` is a dry run.
Use bounded time filters, inspect the selection and add `--execute` only after resolving
the failure. Poison replay similarly uses `replay poison --queue <incoming-or-relay>`.
Keep downstream deduplication intact; replay can repeat an already committed HTTP effect.

## Submission recovery

A pending or ambiguous create result must use
`foundry-batch recover-submissions --resource <key>`. Do not resubmit the same job blindly.
Keep correlation IDs stable and investigate `submission.needs_operator` results.
Exit 3 means pending/recovery work, not permission to create another batch.

## Teardown and retention

Stop new submissions, resolve active jobs, remove provider webhooks with
`register remove --resource <key> --confirm <key>`, drain/reconcile/replay as appropriate,
and archive restricted evidence privately under your retention policy.
Review a separate destroy plan; routine plan guards intentionally refuse deletion.
Key Vault purge protection remains enabled.

Queues expire after 14 days. Receipt/lifecycle cleanup is not implemented. Establish a
retention/cleanup procedure before production use. Define your own incident contacts and
escalation chain; none is embedded in this sample.
