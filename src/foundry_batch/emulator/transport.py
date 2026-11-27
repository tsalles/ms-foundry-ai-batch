"""Transports that route the production HTTP clients to emulators or in-process handlers.

``RoutingAdapter`` is a ``requests`` transport adapter: ``FoundryRestClient`` and
``HttpDownstream`` run unchanged (URL building, multipart encoding, signing, redirects, error
mapping) while the bytes go to a local handler instead of the network. Hosts use the reserved
``.invalid`` top-level domain, so a leaked emulator URL can never resolve to a real service.

``EmulatorHttpServer`` exposes emulators on a loopback socket for tools that need a real port
(Functions Core Tools, curl, another process).
"""

from __future__ import annotations

import threading
from collections import deque
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qsl, urlsplit

import requests
from requests.adapters import BaseAdapter
from requests.structures import CaseInsensitiveDict

from foundry_batch.emulator.foundry import (
    FoundryEmulator,
    WebhookCall,
    WireRequest,
    WireResponse,
)
from foundry_batch.webhook.http import HttpRequest, HttpResponse

API_PREFIX = "/openai/v1/"
#: Reserved, never-resolvable domain for in-process routes (RFC 2606).
EMULATOR_DOMAIN = "emulator.invalid"

WireHandler = Callable[[WireRequest], WireResponse]


def emulator_endpoint(resource_key: str) -> str:
    return f"https://{resource_key}.{EMULATOR_DOMAIN}/"


def foundry_handler(emulator: FoundryEmulator) -> WireHandler:
    """Wire handler for ``https://<key>.emulator.invalid/openai/v1/...`` requests."""

    def handle(request: WireRequest) -> WireResponse:
        if not request.path.startswith(API_PREFIX):
            return WireResponse.error(404, "route_not_found")
        return emulator.handle(
            WireRequest(
                request.method,
                request.path[len(API_PREFIX) :],
                request.query,
                request.headers,
                request.body,
            )
        )

    return handle


def http_handler(handler: Callable[[HttpRequest], HttpResponse], route: str) -> WireHandler:
    """Adapt a framework-neutral handler (receiver, demo API).

    ``route`` is a path template such as ``/api/webhooks/{resource_key}`` whose single
    placeholder becomes a route parameter, as the Functions host would supply it.
    """
    prefix, _, rest = route.partition("{")
    param = rest.rstrip("}") if rest else ""

    def handle(request: WireRequest) -> WireResponse:
        if not request.path.startswith(prefix):
            return WireResponse.error(404, "route_not_found")
        params: dict[str, str] = {}
        if param:
            value = request.path[len(prefix) :].strip("/")
            if not value or "/" in value:
                return WireResponse.error(404, "route_not_found")
            params[param] = value
        elif request.path.rstrip("/") != prefix.rstrip("/"):
            return WireResponse.error(404, "route_not_found")
        response = handler(HttpRequest(request.method, request.body, request.headers, params))
        return WireResponse(response.status, response.body, response.headers)

    return handle


@dataclass(frozen=True)
class HostFault:
    """Failure of an in-process host: an HTTP status, or the response lost after the call."""

    status: int | None = 503
    after_call: bool = False  # the handler ran (committed), then the connection dropped
    retry_after: float | None = None


class FaultQueue:
    """FIFO of faults per host, for downstream outages and committed-then-lost responses."""

    def __init__(self) -> None:
        self._faults: dict[str, deque[HostFault]] = {}
        self._lock = threading.Lock()

    def add(self, host: str, *faults: HostFault) -> None:
        with self._lock:
            self._faults.setdefault(host, deque()).extend(faults)

    def take(self, host: str) -> HostFault | None:
        with self._lock:
            queued = self._faults.get(host)
            return queued.popleft() if queued else None


class RoutingAdapter(BaseAdapter):
    """``requests`` adapter that dispatches by host to in-process wire handlers."""

    def __init__(self, routes: Mapping[str, WireHandler], faults: FaultQueue | None = None) -> None:
        super().__init__()
        self._routes = dict(routes)
        self.faults = faults or FaultQueue()
        self.requests: list[tuple[str, str]] = []  # (method, url): non-sensitive trace

    def send(
        self,
        request: requests.PreparedRequest,
        stream: bool = False,
        timeout: Any = None,
        verify: Any = True,
        cert: Any = None,
        proxies: Any = None,
    ) -> requests.Response:
        url = request.url or ""
        parts = urlsplit(url)
        host = parts.hostname or ""
        handler = self._routes.get(host)
        if handler is None:
            raise requests.ConnectionError(f"no emulated route for host {host!r}")
        self.requests.append((request.method or "GET", url))
        body = request.body or b""
        wire = WireRequest(
            method=request.method or "GET",
            path=parts.path,
            query=dict(parse_qsl(parts.query)),
            headers={k.lower(): v for k, v in request.headers.items()},
            body=body.encode("utf-8") if isinstance(body, str) else bytes(body),
        )
        fault = self.faults.take(host)
        if fault is not None and not fault.after_call:
            headers = {"retry-after": str(int(fault.retry_after))} if fault.retry_after else {}
            return _response(request, WireResponse(fault.status or 503, b"", headers))
        result = handler(wire)
        if fault is not None or result.drop_connection:
            raise requests.ConnectionError("emulated: response lost after the call")
        if result.connect_timeout:
            raise requests.ConnectTimeout("emulated: connection never established")
        return _response(request, result)

    def close(self) -> None:
        return None


def _response(request: requests.PreparedRequest, wire: WireResponse) -> requests.Response:
    response = requests.Response()
    response.status_code = wire.status
    response.headers = CaseInsensitiveDict(dict(wire.headers))
    response._content = wire.body
    response.url = request.url or ""
    response.request = request
    response.encoding = "utf-8"
    response.reason = "emulated"
    return response


def routed_session(
    routes: Mapping[str, WireHandler], faults: FaultQueue | None = None
) -> requests.Session:
    """A session whose HTTPS traffic to the given hosts never leaves the process."""
    session = requests.Session()
    adapter = RoutingAdapter(routes, faults)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.trust_env = False  # ignore proxy variables: nothing here may reach a network
    return session


def session_webhook_transport(
    session: requests.Session, timeout_s: float = 10.0
) -> Callable[[WebhookCall], int | None]:
    """Webhook sender for the emulator (in-process route or a real URL)."""

    def send(call: WebhookCall) -> int | None:
        try:
            response = session.post(
                call.url,
                data=call.body,
                headers=dict(call.headers),
                timeout=timeout_s,
                allow_redirects=False,
            )
        except requests.RequestException:
            return None
        return response.status_code

    return send


class EmulatorHttpServer:
    """Loopback HTTP server: ``http://127.0.0.1:<port>/r/<resource_key>/openai/v1/...``."""

    def __init__(self, emulators: Mapping[str, FoundryEmulator], port: int = 0) -> None:
        handlers = {key: foundry_handler(emulator) for key, emulator in emulators.items()}
        self._server = ThreadingHTTPServer(("127.0.0.1", port), _make_handler(handlers))
        self._thread: threading.Thread | None = None

    @property
    def port(self) -> int:
        return int(self._server.server_address[1])

    def endpoint(self, resource_key: str) -> str:
        return f"http://127.0.0.1:{self.port}/r/{resource_key}/"

    def start(self) -> EmulatorHttpServer:
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        return self

    def stop(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=5)

    def __enter__(self) -> EmulatorHttpServer:
        return self.start()

    def __exit__(self, *exc: object) -> None:
        self.stop()


def _make_handler(handlers: Mapping[str, WireHandler]) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def _dispatch(self) -> None:
            parts = urlsplit(self.path)
            segments = parts.path.split("/", 3)  # ['', 'r', key, rest]
            routed = len(segments) == 4 and segments[1] == "r"
            handler = handlers.get(segments[2]) if routed else None
            length = int(self.headers.get("content-length") or 0)
            body = self.rfile.read(length) if length else b""
            if handler is None:
                result = WireResponse.error(404, "route_not_found")
            else:
                result = handler(
                    WireRequest(
                        self.command,
                        "/" + segments[3],
                        dict(parse_qsl(parts.query)),
                        {k.lower(): v for k, v in self.headers.items()},
                        body,
                    )
                )
            if result.drop_connection or result.connect_timeout:
                self.close_connection = True
                self.connection.close()
                return
            self.send_response(result.status)
            for name, value in result.headers.items():
                self.send_header(name, value)
            self.send_header("content-length", str(len(result.body)))
            self.end_headers()
            self.wfile.write(result.body)

        do_GET = do_POST = do_DELETE = _dispatch

        def log_message(self, format: str, *args: Any) -> None:
            return None  # keep request lines (which carry ids) out of stderr

    return Handler
