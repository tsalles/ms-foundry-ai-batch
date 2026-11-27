"""Structured, metadata-only event logging.

Every operational signal is a log record whose ``batch_event`` attribute names the event. With
Azure Monitor OpenTelemetry these attributes land in ``AppTraces.Properties``, which is what
the Terraform alert rules and the workbook query. Keys that could carry secrets or payloads
are redacted here so a careless call site cannot leak them (REQ-I10, US-3.1 AC5).
"""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from typing import Any

# Substrings that mark a key as sensitive wherever they appear in it.
_SENSITIVE_FRAGMENTS = (
    "secret",
    "password",
    "authorization",
    "api_key",
    "apikey",
    "signature",
    "bearer",
    "access_token",
    "credential",
    "connection_string",
)
# Whole keys that would carry payload or prompt/output contents.
_SENSITIVE_KEYS = frozenset({"body", "payload", "content", "prompt", "token", "headers", "raw"})
_RESERVED = frozenset(
    logging.LogRecord("x", logging.INFO, "x", 0, "", None, None).__dict__.keys()
) | {"message", "asctime"}
_MAX_TEXT = 512

REDACTED = "[redacted]"


def _sanitise(key: str, value: Any) -> Any:
    lowered = key.lower()
    if lowered in _SENSITIVE_KEYS or any(f in lowered for f in _SENSITIVE_FRAGMENTS):
        return REDACTED
    if value is None or isinstance(value, bool | int | float):
        return value
    text = str(value)
    return text if len(text) <= _MAX_TEXT else text[:_MAX_TEXT] + "..."


def event_properties(event: str, props: Mapping[str, Any]) -> dict[str, Any]:
    """Build the ``extra`` mapping for a log call (exposed for tests)."""
    extra: dict[str, Any] = {"batch_event": event}
    for key, value in props.items():
        safe_key = f"{key}_" if key in _RESERVED else key
        extra[safe_key] = _sanitise(key, value)
    return extra


def log_event(
    logger: logging.Logger, event: str, *, level: int = logging.INFO, **props: Any
) -> None:
    """Log one operational event with flat, redacted properties."""
    extra = event_properties(event, props)
    summary = " ".join(f"{k}={v}" for k, v in extra.items() if k != "batch_event")
    logger.log(level, "%s %s", event, summary, extra=extra)


class JsonFormatter(logging.Formatter):
    """Line-delimited JSON for local runs and CLI tools."""

    def format(self, record: logging.LogRecord) -> str:
        data: dict[str, Any] = {
            "time": self.formatTime(record),
            "level": record.levelname,
            "logger": record.name,
        }
        for key, value in record.__dict__.items():
            if key not in _RESERVED:
                data[key] = value
        if "batch_event" not in data:
            data["message"] = record.getMessage()
        if record.exc_info:
            data["exception"] = self.formatException(record.exc_info)
        return json.dumps(data, default=str, sort_keys=True)
