"""Standard Webhooks signature verification as used by Azure OpenAI (US-3.1).

Algorithm (identical to ``openai.resources.webhooks.Webhooks.verify_signature``):

* headers ``webhook-id``, ``webhook-timestamp`` and ``webhook-signature`` are required;
* the timestamp must be within ``tolerance`` seconds in the past **or** the future;
* the key is the base64-decoded part after ``whsec_`` (or the raw UTF-8 secret otherwise);
* the signed content is ``f"{id}.{timestamp}.{body}"`` over the unchanged request bytes;
* the signature header is a space-separated list of ``v1,<base64 HMAC-SHA256>`` values and
  any match is accepted (constant-time comparison).

The verifier is dependency-free so the same code can run on Azure Functions or, if decision
D-01 lands on option B, on AWS. ``tests/unit/test_signature_conformance.py`` checks it against
the official SDK whenever ``openai`` is installed.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime

HEADER_ID = "webhook-id"
HEADER_TIMESTAMP = "webhook-timestamp"
HEADER_SIGNATURE = "webhook-signature"
DEFAULT_TOLERANCE_SECONDS = 300


class SignatureError(Exception):
    """Delivery rejected. ``reason`` is a stable, non-sensitive code used in logs and alerts."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


@dataclass(frozen=True)
class VerifiedDelivery:
    webhook_id: str
    timestamp: int
    secret_index: int


def lower_headers(headers: Mapping[str, str] | Iterable[tuple[str, str]]) -> dict[str, str]:
    items = headers.items() if isinstance(headers, Mapping) else headers
    return {str(k).lower(): str(v) for k, v in items}


def decode_secret(secret: str) -> bytes:
    if secret.startswith("whsec_"):
        try:
            return base64.b64decode(secret[len("whsec_") :], validate=True)
        except (binascii.Error, ValueError) as exc:
            raise SignatureError("secret_malformed") from exc
    return secret.encode("utf-8")


def secret_is_usable(secret: str) -> bool:
    """True if ``secret`` decodes to a non-empty signing key (for readiness; no value leaves)."""
    try:
        return bool(decode_secret(secret))
    except SignatureError:
        return False


def compute_signature(secret: str, webhook_id: str, timestamp: int | str, body: bytes) -> str:
    signed = f"{webhook_id}.{timestamp}.".encode() + body
    digest = hmac.new(decode_secret(secret), signed, hashlib.sha256).digest()
    return base64.b64encode(digest).decode("ascii")


def sign_headers(secret: str, webhook_id: str, timestamp: int, body: bytes) -> dict[str, str]:
    """Headers for an outgoing signed request (relay to downstream, tests, local tools)."""
    return {
        "webhook-id": webhook_id,
        "webhook-timestamp": str(timestamp),
        "webhook-signature": "v1," + compute_signature(secret, webhook_id, timestamp, body),
    }


def verify(
    body: bytes,
    headers: Mapping[str, str],
    secrets: Sequence[str],
    *,
    now: datetime,
    tolerance_seconds: int = DEFAULT_TOLERANCE_SECONDS,
) -> VerifiedDelivery:
    """Verify one delivery against any of ``secrets`` (current and previous during rotation)."""
    h = lower_headers(headers)
    webhook_id = h.get(HEADER_ID, "").strip()
    timestamp_raw = h.get(HEADER_TIMESTAMP, "").strip()
    signature_header = h.get(HEADER_SIGNATURE, "").strip()
    if not webhook_id or not timestamp_raw or not signature_header:
        raise SignatureError("missing_headers")
    try:
        timestamp = int(timestamp_raw)
    except ValueError as exc:
        raise SignatureError("timestamp_malformed") from exc
    current = int(now.timestamp())
    if current - timestamp > tolerance_seconds or timestamp - current > tolerance_seconds:
        raise SignatureError("timestamp_out_of_tolerance")
    if not secrets:
        raise SignatureError("no_secret_configured")

    # Like the SDK, "v1,<base64>" values are unwrapped and unversioned values are compared as
    # they are. Compare bytes: hmac.compare_digest rejects non-ASCII str input with TypeError,
    # so an unauthenticated caller could otherwise turn a 401 into a 500.
    candidates = [
        (part[3:] if part.startswith("v1,") else part).encode("utf-8", errors="replace")
        for part in signature_header.split()
    ]
    usable = 0
    for index, secret in enumerate(secrets):
        try:
            expected = compute_signature(secret, webhook_id, timestamp_raw, body).encode("ascii")
        except SignatureError:
            continue  # a malformed slot must not block the other slot during rotation
        usable += 1
        # Evaluate every candidate so timing does not depend on which one matches.
        if any([hmac.compare_digest(expected, c) for c in candidates]):  # noqa: C419
            return VerifiedDelivery(webhook_id=webhook_id, timestamp=timestamp, secret_index=index)
    if usable == 0:
        raise SignatureError("secret_malformed")
    raise SignatureError("signature_mismatch")
