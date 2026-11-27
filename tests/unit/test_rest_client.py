"""FoundryRestClient against a local HTTP stub of the documented v1 endpoints."""

import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, ClassVar
from unittest import mock

import requests

from foundry_batch.errors import (
    ProviderAmbiguousError,
    ProviderNotFoundError,
    ProviderRequestError,
    ProviderRetryableError,
)
from foundry_batch.foundry.auth import ApiKeyAuth, EntraTokenAuth
from foundry_batch.foundry.files import ProviderFile
from foundry_batch.foundry.rest_client import (
    INPUT_FILE_EXPIRY_SECONDS,
    FoundryClientFactory,
    FoundryRestClient,
)


class _Stub(BaseHTTPRequestHandler):
    routes: ClassVar[dict[tuple[str, str], list[tuple[int, Any, dict[str, str]]]]] = {}
    seen: ClassVar[list[tuple[str, str, dict[str, str], bytes]]] = []

    def _handle(self) -> None:
        length = int(self.headers.get("content-length", 0) or 0)
        body = self.rfile.read(length) if length else b""
        path = self.path.split("?")[0]
        type(self).seen.append(
            (self.command, self.path, {k.lower(): v for k, v in self.headers.items()}, body)
        )
        queue = type(self).routes.get((self.command, path), [])
        status, payload, headers = queue.pop(0) if queue else (404, {"error": {"code": "nf"}}, {})
        data = (
            b""
            if payload is None
            else (payload if isinstance(payload, bytes) else json.dumps(payload).encode())
        )
        self.send_response(status)
        for k, v in headers.items():
            self.send_header(k, v)
        self.send_header("content-length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    do_GET = do_POST = do_DELETE = _handle

    def log_message(self, *args: object) -> None:
        return


class _Token:
    def __init__(self, token: str, expires_on: int) -> None:
        self.token, self.expires_on = token, expires_on


class _Credential:
    def __init__(self) -> None:
        self.calls = 0

    def get_token(self, *scopes: str) -> _Token:
        self.calls += 1
        return _Token(f"tok{self.calls}", 1000)


BATCH = {
    "id": "batch_1",
    "status": "completed",
    "created_at": 1_700_000_000,
    "completed_at": 1_700_000_600,
    "input_file_id": "file-1",
    "request_counts": {"total": 2, "completed": 2, "failed": 0},
}


class RestClientTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), _Stub)
        threading.Thread(target=cls.server.serve_forever, daemon=True).start()
        cls.endpoint = f"http://127.0.0.1:{cls.server.server_address[1]}/"

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.shutdown()
        cls.server.server_close()

    def setUp(self) -> None:
        _Stub.routes, _Stub.seen = {}, []
        self.credential = _Credential()
        self.client = FoundryRestClient(
            "eus2", self.endpoint, EntraTokenAuth(self.credential, "scope", now=lambda: 0)
        )

    def route(self, method: str, path: str, *responses: tuple[int, Any, dict[str, str]]) -> None:
        _Stub.routes[(method, "/openai/v1/" + path)] = list(responses)

    def test_list_retrieve_cancel_with_bearer_token(self) -> None:
        self.route(
            "GET",
            "batches",
            (200, {"data": [BATCH, {"bad": 1}], "has_more": True, "last_id": "batch_1"}, {}),
        )
        self.route("GET", "batches/batch_1", (200, BATCH, {}))
        self.route("POST", "batches/batch_1/cancel", (200, {**BATCH, "status": "cancelling"}, {}))
        page = self.client.list_batches(after="batch_0", limit=500)
        self.assertEqual([b.batch_id for b in page.items], ["batch_1"])
        self.assertTrue(page.has_more)
        self.assertEqual(page.last_id, "batch_1")
        self.assertIn("limit=100", _Stub.seen[0][1])
        self.assertIn("after=batch_0", _Stub.seen[0][1])
        self.assertEqual(_Stub.seen[0][2]["authorization"], "Bearer tok1")
        self.assertEqual(self.client.retrieve_batch("batch_1").request_counts.total, 2)
        self.assertEqual(self.client.cancel_batch("batch_1").status, "cancelling")
        self.assertEqual(self.credential.calls, 1)  # token cached
        self.assertEqual(self.client.counter.total(operation_prefix="batch."), 3)

    def test_create_and_upload(self) -> None:
        self.route("POST", "files", (200, {"id": "file-9"}, {}))
        self.route("POST", "batches", (200, {**BATCH, "status": "validating"}, {}))
        self.assertEqual(self.client.upload_file(filename="x.jsonl", content=b"{}\n"), "file-9")
        self.assertIn(b'name="purpose"', _Stub.seen[0][3])
        snap = self.client.create_batch(
            input_file_id="file-9",
            endpoint="/chat/completions",
            metadata={"batch_correlation_id": "c"},
        )
        body = json.loads(_Stub.seen[1][3])
        self.assertEqual(body["completion_window"], "24h")
        self.assertEqual(body["metadata"], {"batch_correlation_id": "c"})
        self.assertEqual(snap.status, "validating")
        self.route("POST", "files", (200, {"object": "file"}, {}))
        with self.assertRaises(ProviderRequestError):
            self.client.upload_file(filename="x", content=b"")

    def test_upload_sets_the_documented_input_file_expiration(self) -> None:
        self.route("POST", "files", (200, {"id": "file-9"}, {}), (200, {"id": "file-10"}, {}))
        self.client.upload_file(filename="x.jsonl", content=b"{}\n")
        body = _Stub.seen[0][3]
        self.assertIn(b'name="expires_after.anchor"\r\n\r\ncreated_at', body)
        self.assertIn(b'name="expires_after.seconds"\r\n\r\n1209600', body)
        self.assertEqual(INPUT_FILE_EXPIRY_SECONDS, 14 * 24 * 3600)
        without = FoundryRestClient(
            "eus2",
            self.endpoint,
            ApiKeyAuth(lambda: "k"),
            input_file_expiry_seconds=None,
        )
        self.assertEqual(without.upload_file(filename="x.jsonl", content=b"{}\n"), "file-10")
        self.assertNotIn(b"expires_after", _Stub.seen[1][3])
        for seconds in (14 * 24 * 3600 - 1, 30 * 24 * 3600 + 1):
            with self.subTest(seconds=seconds), self.assertRaises(ValueError):
                FoundryRestClient(
                    "eus2",
                    self.endpoint,
                    ApiKeyAuth(lambda: "k"),
                    input_file_expiry_seconds=seconds,
                )
        FoundryRestClient(
            "eus2", self.endpoint, ApiKeyAuth(lambda: "k"), input_file_expiry_seconds=30 * 86400
        )

    def test_file_status_and_idempotent_delete(self) -> None:
        self.route(
            "GET",
            "files/file-9",
            (200, {"id": "file-9", "status": "pending", "expires_at": 1_800_000_000}, {}),
            (200, {"id": "file-9", "expires_at": "soon"}, {}),
        )
        self.route(
            "DELETE",
            "files/file-9",
            (200, {"id": "file-9", "deleted": True}, {}),
            (404, {"error": {"code": "not_found"}}, {}),
        )
        self.assertEqual(
            self.client.retrieve_file("file-9"), ProviderFile("file-9", "pending", 1_800_000_000)
        )
        self.assertEqual(self.client.retrieve_file("file-9"), ProviderFile("file-9", "unknown"))
        self.client.delete_file("file-9")
        self.client.delete_file("file-9")  # already gone counts as deleted
        self.assertEqual(
            [(m, p) for m, p, _, _ in _Stub.seen][-1], ("DELETE", "/openai/v1/files/file-9")
        )
        self.assertEqual(self.client.counter.total(operation_prefix="files."), 4)
        with self.assertRaises(ProviderNotFoundError):
            self.client.retrieve_file("file-9")  # no route left: 404

    def test_webhook_endpoint_management(self) -> None:
        created = {
            "id": "wh_1",
            "object": "webhook.endpoint",
            "name": "n",
            "url": "https://u",
            "event_types": ["batch.completed"],
            "signing_secret": "whsec_abc",
        }
        self.route(
            "POST",
            "dashboard/webhook_endpoints",
            (200, created, {}),
            (200, {**created, "signing_secret": None}, {}),
        )
        self.route(
            "GET",
            "dashboard/webhook_endpoints",
            (200, {"data": [created]}, {}),
            (200, [created], {}),
        )
        self.route("POST", "dashboard/webhook_endpoints/wh_1", (200, created, {}))
        self.route("DELETE", "dashboard/webhook_endpoints/wh_1", (204, None, {}))
        result = self.client.create_endpoint(
            name="n", url="https://u", event_types=["batch.completed"]
        )
        self.assertEqual(result.signing_secret, "whsec_abc")
        self.assertNotIn("whsec_abc", repr(result))
        with self.assertRaises(ProviderRequestError):
            self.client.create_endpoint(name="n", url="https://u", event_types=[])
        self.assertEqual(self.client.list_endpoints()[0].endpoint_id, "wh_1")
        self.assertEqual(self.client.list_endpoints()[0].event_types, ("batch.completed",))
        self.assertEqual(
            self.client.update_endpoint("wh_1", name="n", url="https://u", event_types=[]).name, "n"
        )
        self.client.delete_endpoint("wh_1")

    def test_error_mapping(self) -> None:
        self.route(
            "GET",
            "batches/b",
            (404, {"error": {"code": "not_found"}}, {}),
            (429, {"error": {"code": "rate"}}, {"retry-after": "20"}),
            (503, b"not json", {}),
            (400, {"error": "flat"}, {}),
            (200, b"", {}),
            (200, b"<html>", {}),
        )
        with self.assertRaises(ProviderNotFoundError):
            self.client.retrieve_batch("b")
        with self.assertRaises(ProviderRetryableError) as ctx:
            self.client.retrieve_batch("b")
        self.assertEqual((ctx.exception.status_code, ctx.exception.retry_after_seconds), (429, 20))
        with self.assertRaises(ProviderRetryableError):
            self.client.retrieve_batch("b")
        with self.assertRaises(ProviderRequestError) as ctx2:
            self.client.retrieve_batch("b")
        self.assertIn("unknown", str(ctx2.exception))
        with self.assertRaises(ProviderRetryableError):
            self.client.retrieve_batch("b")  # empty body
        with self.assertRaises(ProviderRetryableError):
            self.client.retrieve_batch("b")  # invalid JSON
        self.assertEqual(self.client.counter.total(status="429"), 1)

    def test_tc_c03_batch_input_rejection_keeps_the_provider_error_code(self) -> None:
        rejection = {"errors": {"data": [{"code": "duplicate_custom_id", "message": "line 2"}]}}
        self.route("GET", "batches/b", (400, rejection, {}), (400, {"errors": {"data": []}}, {}))
        with self.assertRaises(ProviderRequestError) as ctx:
            self.client.retrieve_batch("b")
        self.assertIn("duplicate_custom_id", str(ctx.exception))
        self.assertNotIn("line 2", str(ctx.exception))  # messages can echo input content
        with self.assertRaises(ProviderRequestError) as empty:
            self.client.retrieve_batch("b")
        self.assertIn("unknown", str(empty.exception))

    def test_network_failures_get_vs_post(self) -> None:
        closed = FoundryRestClient(
            "eus2", "http://127.0.0.1:9/", ApiKeyAuth(lambda: "k"), timeout_seconds=(0.2, 0.2)
        )
        with mock.patch.object(
            closed._session, "request", side_effect=requests.ConnectionError("connection reset")
        ):
            with self.assertRaises(ProviderRetryableError) as ctx:
                closed.retrieve_batch("b")
            self.assertNotIsInstance(ctx.exception, ProviderAmbiguousError)
            with self.assertRaises(ProviderAmbiguousError):
                closed.create_batch(input_file_id="f", endpoint="/x", metadata={})

        class Broken(requests.Session):
            def request(self, *a, **k):  # type: ignore[no-untyped-def,override]
                raise requests.exceptions.InvalidURL("bad")

        broken = FoundryRestClient("eus2", self.endpoint, ApiKeyAuth(lambda: "k"), session=Broken())
        with self.assertRaises(ProviderRetryableError):
            broken.retrieve_batch("b")
        self.assertEqual(closed.counter.total(status="network"), 2)

    def test_api_key_header_and_factory(self) -> None:
        self.route("GET", "batches/batch_1", (200, BATCH, {}))
        client = FoundryRestClient("eus2", self.endpoint, ApiKeyAuth(lambda: "key-1"))
        client.retrieve_batch("batch_1")
        self.assertEqual(_Stub.seen[0][2]["api-key"], "key-1")
        factory = FoundryClientFactory({"eus2": client})
        self.assertIs(factory.for_resource("eus2"), client)
        with self.assertRaises(KeyError):
            factory.for_resource("x")
        with self.assertRaises(ValueError):
            FoundryRestClient("eus2", "http://insecure.example/", ApiKeyAuth(lambda: "k"))

    def test_token_refreshes_before_expiry(self) -> None:
        clock = {"now": 0.0}
        auth = EntraTokenAuth(self.credential, "scope", now=lambda: clock["now"])
        auth.headers()
        clock["now"] = 701  # within five minutes of expiry (1000)
        self.assertEqual(auth.headers()["authorization"], "Bearer tok2")


if __name__ == "__main__":
    unittest.main()
