# Public publication checklist

## Source boundary

This public-oriented tree is an independent source export. It does not import an existing
repository's Git objects, branches, tags, reflogs, remotes or agent checkpoint references.
Only selected tracked text implementation files were exported.

Engagement planning, meeting-derived requirements, dated review evidence, private integration
research, original prose architecture documents and their binary export were not copied.
The public documentation describes generic implementation behavior rather than renamed
engagement records. Deployment-specific variable files, SSH material, enterprise feed settings
and organizational ownership metadata were excluded.

Names and interfaces are generic: distribution `ms-foundry-ai-batch`, package `foundry_batch`,
CLI `foundry-batch`, environment prefix `BATCH_`, telemetry `batch_event`.
No private compatibility aliases are included.

## Before every publication

1. Inspect `git status --short --untracked-files=all` and the complete proposed staged diff.
2. Run `python tools/secret_scan.py .`, the publication-hygiene tests and a full-history
   gitleaks scan with redaction. Investigate every unexpected finding.
3. Review names and file contents for customer names, employee contacts, meeting excerpts,
   private endpoints, actual deployment IDs and test evidence. Automated secret patterns do
   not reliably detect confidential business context.
4. Check that only synthetic inputs and placeholder examples are included.
5. Confirm no populated `.env`, local Functions settings, Terraform state/plan/variable file,
   backend file, package archive, binary report, key, log or deployment output is staged.
   Ignore rules are not an access boundary and do not untrack already committed files.
6. Confirm the Git origin is the intended public repository and there is no inherited
   private history. Never use a mirror push from a private working repository.
7. Confirm the necessary code ownership/publication approvals and preserve the
   [MIT License](../LICENSE) and copyright notice in source distributions.

Do not use a whole-directory copy or archive including hidden/ignored files. Build a release
from the reviewed Git snapshot. Development environments, local caches and generated metadata
may contain usernames, absolute paths and credentials even when source control excludes them.

## Authentication and deployment

The cloud default requires no committed password or access key. Use managed/workload
identity or a signed-in CLI identity. Capture runtime signing secrets directly into Key Vault.
Environment examples contain only placeholders or local emulator settings.

Tenant/subscription/object IDs and secret names are not passwords, but actual values still
belong in private configuration. Saved Terraform plans and state embed operational data and
must remain private even when no secret-looking string is detected.

## Scope and limits

Successful scans show no matches for their rules in the inspected snapshot; they cannot prove
the absence of every confidential fact or future credential. Dependency advisories, cloud
configuration, licenses and publication authorization require separate review.
If a real credential is ever committed, revoke/rotate it and remove it from all publication
history before pushing; merely deleting the current file is insufficient.
