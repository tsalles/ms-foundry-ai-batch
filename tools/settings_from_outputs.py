"""Turn `terraform output -json` into the non-secret CLI settings file (env-variable names).

The environment comes from the root's `environment` output, so the CLI derives the same Key
Vault secret names as the Function App. `--environment` is only a cross-check.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

MAPPING = {
    "BATCH_ENVIRONMENT": "environment",
    "BATCH_RESOURCES_JSON": "resources_json",
    "BATCH_KEY_VAULT_URI": "key_vault_uri",
    "BATCH_STORAGE__queueServiceUri": "queue_service_uri",
    "BATCH_STORAGE__tableServiceUri": "table_service_uri",
}


def build(outputs: dict[str, Any], environment: str | None = None) -> dict[str, str]:
    settings: dict[str, str] = {}
    for env_name, output_name in MAPPING.items():
        entry = outputs.get(output_name)
        if not isinstance(entry, dict) or entry.get("sensitive"):
            raise SystemExit(f"output {output_name} missing or sensitive")
        value = entry.get("value")
        settings[env_name] = value if isinstance(value, str) else json.dumps(value)
    if environment is not None and environment != settings["BATCH_ENVIRONMENT"]:
        raise SystemExit("--environment does not match the Terraform environment output")
    return settings


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("outputs_json")
    parser.add_argument(
        "--environment", help="expected environment; fails when the outputs say otherwise"
    )
    parser.add_argument("--out", required=True)
    args = parser.parse_args(argv)
    outputs = json.loads(Path(args.outputs_json).read_text(encoding="utf-8"))
    settings = build(outputs, args.environment)
    Path(args.out).write_text(json.dumps(settings, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
