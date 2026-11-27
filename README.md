# Microsoft Foundry AI Batch sample

A Python sample for Azure OpenAI Batch lifecycle webhooks, durable reconciliation,
idempotent downstream delivery and operational metrics. It includes local emulators,
Azure Functions entry points, Terraform and automated tests.

This is a reference implementation, not a production support commitment. Use synthetic
inputs only. Verify API availability, quotas, model versions, residency, networking and
security policy in your own environment before deployment.

## Quick start

Requires Python 3.11 or newer. From PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e ".[dev,azure,functions]"
foundry-batch local-demo
foundry-batch simulate --durations samples/durations-example.txt
python -m pytest -m "not integration"
```

The local demo needs no Azure account, external vault or credentials. All scenarios use
synthetic data. See [local development](docs/local-development.md) and
[testing](docs/testing.md).

## Configuration and deployment

Copy [.env.example](.env.example) to `.env` for private local environment variables:

```powershell
Copy-Item .env.example .env
dotenv -f .env run --no-override -- foundry-batch indicators
```

`dotenv` is installed by the `dev` extra. It loads variables for the child process only;
existing process variables take precedence. The application does not implicitly load files.
Never commit a populated environment file.

Azure configuration uses managed/workload identity and Key Vault rather than credentials
in source code. Terraform inputs, private backend coordinates and approved model settings
are supplied by each operator. See [configuration](docs/configuration.md),
[infrastructure](infra/README.md) and [deployment architecture](docs/architecture-deployment.md).
Deployment is not part of the quick start.

## Design and layout

Signed webhook -> bounded verification -> queue -> conditional job/receipt/outbox transaction
-> signed, idempotent downstream delivery. A periodic sweep recovers missing observations.

| Path | Purpose |
|---|---|
| [src/foundry_batch](src/foundry_batch) | Domain logic, Azure adapters, operator CLI and local emulators |
| [functions](functions) | Azure Functions HTTP, queue and timer bindings |
| [infra](infra) | Bootstrap root, workload root and Terraform modules |
| [tests](tests) | Unit, in-memory end-to-end, provider contract and optional integration tests |
| [tools](tools) | Package building, plan checks, secret scans and smoke tests |
| [samples](samples) | Synthetic inputs and placeholder preflight configuration |
| [docs](docs) | Architecture, configuration, event contract and operational guidance |

Read the [architecture overview](docs/architecture.md),
[runtime details](docs/architecture-runtime.md) and
[event contract](docs/event-contract-v1.md). Operator commands and recovery procedures are
in the [runbooks](docs/runbooks/README.md).

## Public-release safeguards

This source tree contains no deployment-specific variable file, corporate package feed,
private project metadata, meeting records or binary architecture export. Names, imports,
settings (`BATCH_*`), telemetry fields and event schemas use generic identifiers.
Test identifiers and sample payloads are synthetic. Azure role definition GUIDs are public
built-in role identifiers, not credentials.

The [publication checklist](docs/publication.md) explains the export boundary, checks,
ignored files and limitations. Neither a clean scan nor an ignore rule is a guarantee:
review the exact Git diff before every publication. Do not copy local state or configuration
into an archive.

## Quality and limitations

Ruff, strict mypy, Bandit, pytest with branch coverage >= 90%, secret scanning and
Terraform checks are provided. [GitHub CI](.github/workflows/ci.yml) runs local-only checks;
the optional [Azure DevOps pipeline](azure-pipelines.yml) requires operator-provided
service connections, approved secure inputs and a private agent pool.

Live cloud behavior, cross-cloud private connectivity and an external production vault
adapter require environment-specific validation. Receipt/lifecycle cleanup, deadline
cancellation and production observability export are not implemented. Per-instance rate
limits are not a substitute for an edge gateway.

## License

This project is licensed under the [MIT License](LICENSE).
