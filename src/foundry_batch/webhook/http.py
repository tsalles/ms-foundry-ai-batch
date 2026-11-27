"""Framework-neutral HTTP request/response types (Azure Functions, tests, or AWS)."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class HttpRequest:
    method: str
    body: bytes
    headers: Mapping[str, str] = field(default_factory=dict)
    route_params: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class HttpResponse:
    status: int
    body: bytes = b""
    headers: Mapping[str, str] = field(default_factory=dict)

    @classmethod
    def json(
        cls, status: int, data: Mapping[str, Any], headers: Mapping[str, str] | None = None
    ) -> HttpResponse:
        merged = {"content-type": "application/json"}
        merged.update(headers or {})
        return cls(status=status, body=json.dumps(data).encode("utf-8"), headers=merged)
