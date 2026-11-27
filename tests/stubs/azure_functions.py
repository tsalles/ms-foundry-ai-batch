"""Minimal stand-in for ``azure.functions`` (Python v2 programming model) used to test the
entry points' wiring without the Functions host. It records decorator arguments the way the
real ``FunctionApp`` does, so tests can assert routes, auth levels, queues and schedules.
"""

from __future__ import annotations

import enum
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any


class AuthLevel(enum.Enum):
    ANONYMOUS = "anonymous"
    FUNCTION = "function"
    ADMIN = "admin"


@dataclass
class Registered:
    name: str
    trigger: str
    options: dict[str, Any]
    handler: Callable[..., Any]

    def get_function_name(self) -> str:
        return self.name


@dataclass
class FunctionApp:
    functions: list[Registered] = field(default_factory=list)

    def _register(self, trigger: str, options: dict[str, Any]) -> Callable[..., Any]:
        def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
            self.functions.append(Registered(fn.__name__, trigger, options, fn))
            return fn

        return decorator

    def route(self, **options: Any) -> Callable[..., Any]:
        return self._register("http", options)

    def queue_trigger(self, **options: Any) -> Callable[..., Any]:
        return self._register("queue", options)

    def timer_trigger(self, **options: Any) -> Callable[..., Any]:
        return self._register("timer", options)

    def get_functions(self) -> list[Registered]:
        return list(self.functions)


class HttpRequest:
    def __init__(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        body: bytes = b"",
        route_params: Mapping[str, str] | None = None,
    ) -> None:
        self.method = method
        self.url = url
        self.headers = dict(headers or {})
        self.route_params = dict(route_params or {})
        self._body = body

    def get_body(self) -> bytes:
        return self._body


class HttpResponse:
    def __init__(
        self,
        body: str | bytes | None = None,
        *,
        status_code: int = 200,
        headers: Mapping[str, str] | None = None,
        mimetype: str | None = None,
    ) -> None:
        self.body = body
        self.status_code = status_code
        self.headers = dict(headers or {})
        self.mimetype = mimetype

    def get_body(self) -> bytes:
        if self.body is None:
            return b""
        return self.body if isinstance(self.body, bytes) else self.body.encode()


class QueueMessage:
    def __init__(self, *, body: bytes) -> None:
        self._body = body

    def get_body(self) -> bytes:
        return self._body


class TimerRequest:
    past_due = False
