"""Scan files (repository, plan JSON, state, pipeline logs) for secret-like values (US-2.2 AC2).

Exits 1 when a finding exists. Prints the location and the rule only, never the value.
Complements gitleaks: it also knows the shapes this PoC handles (Standard Webhooks secrets,
Azure keys and connection strings, Entra client secrets, tokens).

A line ending with a ``# secret-scan: allow`` (or ``//``) comment is skipped, for deliberate test
fixtures. ``--strict`` ignores that marker: the pipeline uses it for plan, state and output JSON,
where any text (a tag value, for example) could carry the marker onto the single JSON line.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

_B64 = r"[A-Za-z0-9+/]"
_JWT_CHARS = r"[A-Za-z0-9_+/-]"
_ENTRA = r"[A-Za-z0-9_~.-]"

RULES = {
    "webhook_signing_secret": re.compile(r"whsec_[A-Za-z0-9+/=]{20,}"),
    "storage_account_key": re.compile(r"AccountKey=[A-Za-z0-9+/=]{40,}"),
    "shared_access_key": re.compile(r"SharedAccessKey=[A-Za-z0-9+/=]{20,}"),
    "sas_signature": re.compile(r"[?&]sig=[A-Za-z0-9%+/=]{20,}"),
    "connection_string_password": re.compile(
        r"(?i)(?:^|[;\"'\s])(?:password|pwd)\s*=\s*[^;\"'\s]{8,}"
    ),
    "bearer_token": re.compile(r"Bearer\s+eyJ[A-Za-z0-9._-]{20,}"),
    "jwt": re.compile(
        rf"(?<!{_JWT_CHARS})eyJ{_JWT_CHARS}{{10,}}\.eyJ{_JWT_CHARS}{{10,}}\.{_JWT_CHARS}{{10,}}"
    ),
    "private_key": re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----"),
    "cognitive_api_key": re.compile(
        r'"(?:key1|key2|primary_access_key|secondary_access_key)"\s*:\s*"[^"]{16,}"'
    ),
    "log_analytics_shared_key": re.compile(r'"(?:primary|secondary)_shared_key"\s*:\s*"[^"]{16,}"'),
    "client_secret_field": re.compile(r'(?i)"client_?secret"\s*:\s*"[^"\s]{8,}"'),
    # Entra ID application secrets: three characters, a digit, "Q~", then 31-34 characters.
    "entra_client_secret": re.compile(
        rf"(?<!{_ENTRA}){_ENTRA}{{3}}\dQ~{_ENTRA}{{31,34}}(?!{_ENTRA})"
    ),
    # listKeys responses ({"keyName": "key1", "value": "..."}) and bare 64-byte base64 keys
    # (storage, Log Analytics, Cosmos DB) wherever they appear.
    "azure_list_keys_value": re.compile(
        rf'"keyName"\s*:\s*"[^"]*"\s*,\s*"value"\s*:\s*"{_B64}{{40,}}={{0,2}}"'
    ),
    "base64_64_byte_key": re.compile(rf"(?<!{_B64}){_B64}{{86}}==(?![A-Za-z0-9+/=])"),
}
SKIP_DIRS = {".git", ".venv", "node_modules", "__pycache__", ".terraform", "dist"}
ALLOWLIST_MARKER = re.compile(r"(?:#|//)\s*secret-scan: allow\s*$")


def scan(paths: list[Path], *, strict: bool = False) -> list[str]:
    findings: list[str] = []
    for root in paths:
        candidates = [root] if root.is_file() else [p for p in root.rglob("*") if p.is_file()]
        for path in candidates:
            if SKIP_DIRS & set(path.parts) or path.stat().st_size > 50 * 1024 * 1024:
                continue
            try:
                text = path.read_text(encoding="utf-8", errors="ignore")
            except OSError:
                continue
            for number, line in enumerate(text.splitlines(), start=1):
                if not strict and ALLOWLIST_MARKER.search(line):
                    continue
                for rule, pattern in RULES.items():
                    if pattern.search(line):
                        findings.append(f"{path}:{number}: {rule}")
    return findings


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("paths", nargs="+")
    parser.add_argument(
        "--strict",
        action="store_true",
        help="ignore allowlist markers (use for plan, state and output JSON)",
    )
    args = parser.parse_args(argv)
    findings = scan([Path(p) for p in args.paths], strict=args.strict)
    for finding in findings:
        print(finding)
    print(f"{len(findings)} finding(s)", file=sys.stderr)
    return 1 if findings else 0


if __name__ == "__main__":
    raise SystemExit(main())
