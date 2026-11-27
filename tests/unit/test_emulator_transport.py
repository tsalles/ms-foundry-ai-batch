"""Routing adapter, in-process handlers, loopback server and webhook transports."""

import unittest

import requests

from foundry_batch.emulator.foundry import (
    Fault,
    FoundryEmulator,
    WebhookCall,
    WireRequest,
    WireResponse,
)
from foundry_batch.emulator.local import sample_jsonl
from foundry_batch.emulator.transport import (
    EmulatorHttpServer,
    FaultQueue,
    HostFault,
    emulator_endpoint,
    foundry_handler,
    http_handler,
    routed_session,
    session_webhook_transport,
)
from foundry_batch.errors import ProviderAmbiguousError, ProviderError, ProviderRetryableError
from foundry_batch.foundry.auth import StaticTokenAuth
from foundry_batch.foundry.rest_client import FoundryRestClient
from foundry_batch.webhook.http import HttpRequest, HttpResponse
from tests.support import clock


def echo(request: HttpRequest) -> HttpResponse:
    return HttpResponse.json(201, {"params": dict(request.route_params), "len": len(request.body)})


class RoutingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.faults = FaultQueue()
        routes = {
            "func.emulator.invalid": http_handler(echo, "/api/webhooks/{resource_key}"),
            "demo.emulator.invalid": http_handler(echo, "/api/demo/events"),
        }
        self.session = routed_session(routes, self.faults)

    def test_route_parameters_and_404s(self) -> None:
        ok = self.session.post("https://func.emulator.invalid/api/webhooks/eus2", data=b"abc")
        self.assertEqual(
            (ok.status_code, ok.json()), (201, {"params": {"resource_key": "eus2"}, "len": 3})
        )
        for url in (
            "https://func.emulator.invalid/api/other",
            "https://func.emulator.invalid/api/webhooks/",
            "https://func.emulator.invalid/api/webhooks/a/b",
            "https://demo.emulator.invalid/api/demo/events/x",
        ):
            with self.subTest(url=url):
                self.assertEqual(self.session.post(url, data=b"").status_code, 404)
        self.assertEqual(
            self.session.post("https://demo.emulator.invalid/api/demo/events").status_code, 201
        )
        self.assertEqual(
            self.session.get("https://demo.emulator.invalid/api/demo/events?x=1").status_code, 201
        )

    def test_unknown_host_never_reaches_a_network(self) -> None:
        with self.assertRaises(requests.ConnectionError):
            self.session.get("https://example.com/")

    def test_host_faults_before_and_after_the_call(self) -> None:
        url = "https://func.emulator.invalid/api/webhooks/eus2"
        self.faults.add(
            "func.emulator.invalid", HostFault(503, retry_after=9), HostFault(after_call=True)
        )
        first = self.session.post(url, data=b"")
        self.assertEqual((first.status_code, first.headers["retry-after"]), (503, "9"))
        with self.assertRaises(requests.ConnectionError):
            self.session.post(url, data=b"")
        self.assertEqual(self.session.post(url, data="text").status_code, 201)  # str bodies too

    def test_wire_level_drop_and_connect_timeout(self) -> None:
        def dropping(request: WireRequest) -> WireResponse:
            return WireResponse(
                0, drop_connection=request.method == "POST", connect_timeout=request.method == "GET"
            )

        session = routed_session({"x.emulator.invalid": dropping})
        with self.assertRaises(requests.ConnectionError):
            session.post("https://x.emulator.invalid/a")
        with self.assertRaises(requests.ConnectTimeout):
            session.get("https://x.emulator.invalid/a")

    def test_foundry_handler_needs_the_api_prefix(self) -> None:
        handler = foundry_handler(FoundryEmulator("eus2", clock()))
        self.assertEqual(handler(WireRequest("GET", "/other")).status, 404)

    def test_webhook_transport_reports_status_or_none(self) -> None:
        send = session_webhook_transport(self.session)
        call = WebhookCall(
            "https://func.emulator.invalid/api/webhooks/eus2", b"{}", {}, "wh", "eus2"
        )
        self.assertEqual(send(call), 201)
        self.assertIsNone(send(WebhookCall("https://nowhere.example/", b"", {}, "wh", "eus2")))


class ConnectTimeoutParityTests(unittest.TestCase):
    """E5: ``Fault(connect_timeout=True)`` is faithful in process only. A loopback server has
    already accepted the connection and read the request when it decides, so it can only drop
    the connection: a write then looks ambiguous. Pinned here and documented in
    docs/local-development.md; use the in-process transport for connect-timeout cases."""

    def attempt(self, client: FoundryRestClient, emulator: FoundryEmulator) -> ProviderError:
        file_id = client.upload_file(filename="a.jsonl", content=sample_jsonl())
        emulator.inject("batch.create", Fault(connect_timeout=True))
        with self.assertRaises(ProviderError) as ctx:
            client.create_batch(input_file_id=file_id, endpoint="/chat/completions", metadata={})
        self.assertEqual(emulator.batch_ids(), [])  # never reached the service either way
        return ctx.exception

    def test_in_process_is_a_clean_connect_timeout_loopback_looks_ambiguous(self) -> None:
        emulator = FoundryEmulator("eus2", clock())
        session = routed_session({"eus2.emulator.invalid": foundry_handler(emulator)})
        in_process = self.attempt(
            FoundryRestClient(
                "eus2", emulator_endpoint("eus2"), StaticTokenAuth(), session=session
            ),
            emulator,
        )
        self.assertIsInstance(in_process, ProviderRetryableError)
        self.assertNotIsInstance(in_process, ProviderAmbiguousError)
        self.assertIsInstance(in_process.__cause__, requests.ConnectTimeout)
        emulator = FoundryEmulator("eus2", clock())
        with EmulatorHttpServer({"eus2": emulator}) as server:
            client = FoundryRestClient("eus2", server.endpoint("eus2"), StaticTokenAuth())
            loopback = self.attempt(client, emulator)
        self.assertIsInstance(loopback, ProviderAmbiguousError)
        self.assertIsInstance(loopback.__cause__, requests.ConnectionError)
        self.assertNotIsInstance(loopback.__cause__, requests.ConnectTimeout)


class LoopbackServerTests(unittest.TestCase):
    def test_routes_by_resource_and_drops_connections(self) -> None:
        emulator = FoundryEmulator("eus2", clock())
        with EmulatorHttpServer({"eus2": emulator}) as server:
            base = f"http://127.0.0.1:{server.port}"
            headers = {"authorization": "Bearer t"}
            self.assertEqual(
                requests.get(
                    f"{base}/r/eus2/openai/v1/batches", headers=headers, timeout=5
                ).status_code,
                200,
            )
            self.assertEqual(
                requests.get(
                    f"{base}/r/swc/openai/v1/batches", headers=headers, timeout=5
                ).status_code,
                404,
            )
            self.assertEqual(requests.get(f"{base}/nothing", timeout=5).status_code, 404)
            emulator.inject("batch.list", Fault(drop_connection=True))
            with self.assertRaises(requests.ConnectionError):
                requests.get(f"{base}/r/eus2/openai/v1/batches", headers=headers, timeout=5)
            self.assertTrue(server.endpoint("eus2").endswith("/r/eus2/"))


if __name__ == "__main__":
    unittest.main()
