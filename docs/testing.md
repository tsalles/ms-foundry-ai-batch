# Testing

Install the `dev`, `azure` and `functions` extras, then:

```powershell
python -m pytest -m "not integration" --cov --cov-report=term-missing
python -m ruff format --check .
python -m ruff check .
python -m mypy
python -m bandit -c pyproject.toml -r src -q
python tools/secret_scan.py .
```

The non-integration suite includes unit tests, in-memory end-to-end scenarios, local provider
contract tests, signing conformance, Functions wiring and Terraform/application setting
contracts. Branch coverage must reach 90%. Tests use synthetic identifiers and payloads;
built-in Azure role GUIDs are public constants.

Azurite integration is opt-in with `AZURITE=1` and a locally running emulator on its standard
ports. Live provider tests require `BATCH_LIVE_TESTS=1` and `BATCH_LIVE_FOUNDRY_ENDPOINT`.
Creating synthetic batches also requires `BATCH_LIVE_BATCH=1` and `BATCH_LIVE_DEPLOYMENT`.
Those tests need authorization, credentials and explicit cleanup and are never enabled in
public CI by default.

Terraform roots have mocked-provider tests. From a configured machine, run `fmt -check`,
initialize each root with `-backend=false -lockfile=readonly`, then `validate` and `test`.
No cloud plan or apply is needed for those checks. TFLint and Checkov remain additional
infrastructure gates; the sample's documented policy skips are not production approvals.

The [GitHub workflow](../.github/workflows/ci.yml) runs local-only lint, type, tests,
publication-hygiene checks and secret scans. A separate job scans full Git history with
gitleaks. Deployment and integration acceptance require their own private environment.

Follow the [publication checklist](publication.md) before pushing any new commits.
