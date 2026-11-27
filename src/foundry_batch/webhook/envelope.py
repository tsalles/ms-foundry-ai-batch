"""Queue envelope written by the receiver and read by the processor.

The envelope keeps the verified provider event (bounded by the receiver's body cap) plus
receipt metadata. It never contains secrets or signature headers.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from foundry_batch.clock import from_iso, to_iso

ENVELOPE_SCHEMA = "batch.envelope.v1"


class EnvelopeError(ValueError):
    """The queue message cannot be trusted or parsed. It is quarantined, never processed."""


@dataclass(frozen=True)
class Envelope:
    environment: str
    resource_key: str
    webhook_id: str
    delivery_timestamp: int
    received_at: datetime
    event_id: str | None
    event_type: str
    event_created_at: int | None
    batch_id: str | None
    event: Mapping[str, Any]
    source: str = "webhook"
    attempt: int = 0  # deferrals so far (any reason)
    enrichment_failures: int = 0  # failed detail retrieves so far (bounded separately)

    def to_json(self) -> str:
        return json.dumps(
            {
                "schema": ENVELOPE_SCHEMA,
                "environment": self.environment,
                "resource_key": self.resource_key,
                "webhook_id": self.webhook_id,
                "delivery_timestamp": self.delivery_timestamp,
                "received_at": to_iso(self.received_at),
                "event_id": self.event_id,
                "event_type": self.event_type,
                "event_created_at": self.event_created_at,
                "batch_id": self.batch_id,
                "source": self.source,
                "attempt": self.attempt,
                "enrichment_failures": self.enrichment_failures,
                "event": self.event,
            },
            separators=(",", ":"),
        )

    @classmethod
    def from_json(cls, text: str) -> Envelope:
        try:
            data = json.loads(text)
        except json.JSONDecodeError as exc:
            raise EnvelopeError("not_json") from exc
        if not isinstance(data, dict) or data.get("schema") != ENVELOPE_SCHEMA:
            raise EnvelopeError("unknown_schema")
        received_at = from_iso(data.get("received_at"))
        required_text = ("environment", "resource_key", "webhook_id", "event_type")
        if received_at is None or any(
            not isinstance(data.get(k), str) or not data.get(k) for k in required_text
        ):
            raise EnvelopeError("missing_fields")
        event = data.get("event")
        if not isinstance(event, dict):
            raise EnvelopeError("missing_event")
        batch_id = data.get("batch_id")
        event_id = data.get("event_id")
        created = data.get("event_created_at")
        delivery = data.get("delivery_timestamp")
        return cls(
            environment=data["environment"],
            resource_key=data["resource_key"],
            webhook_id=data["webhook_id"],
            delivery_timestamp=delivery if isinstance(delivery, int) else 0,
            received_at=received_at,
            event_id=event_id if isinstance(event_id, str) else None,
            event_type=data["event_type"],
            event_created_at=created if isinstance(created, int) else None,
            batch_id=batch_id if isinstance(batch_id, str) and batch_id else None,
            event=event,
            source=str(data.get("source", "webhook")),
            attempt=_non_negative_int(data.get("attempt")),
            enrichment_failures=_non_negative_int(data.get("enrichment_failures")),
        )


def _non_negative_int(value: object) -> int:
    return value if isinstance(value, int) and not isinstance(value, bool) and value > 0 else 0


def extract_batch_id(event: Mapping[str, Any]) -> str | None:
    """Batch payload shape is not documented (SP-4): accept ``data.id`` or ``data.batch_id``."""
    data = event.get("data")
    if not isinstance(data, Mapping):
        return None
    for name in ("id", "batch_id"):
        value = data.get(name)
        if isinstance(value, str) and value:
            return value
    return None
