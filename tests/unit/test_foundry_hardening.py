"""Foundry client, submission and registration failure paths (code review findings)."""

import json
import unittest
from collections.abc import Sequence
from datetime import timedelta
from email.utils import format_datetime
from typing import Any

import requests

from foundry_batch.adapters.memory import MemorySecrets, MemoryTableStore
from foundry_batch.clock import ManualClock
from foundry_batch.control import ControlStore
from foundry_batch.errors import (
    CompensationFailedError,
    EndpointSecretMissingError,
    ProviderAmbiguousError,
    ProviderError,
    ProviderNotFoundError,
    ProviderRequestError,
    ProviderRetryableError,
    SecretUnavailableError,
    StoreUnavailableError,
)
from foundry_batch.foundry.rest_client import FoundryRestClient
from foundry_batch.foundry.submission import SubmissionService
from foundry_batch.metrics.queued_tokens import CharacterHeuristicEstimator
from foundry_batch.ops.preflight import AzCommand
from foundry_batch.ports import CreatedWebhookEndpoint, Query
from foundry_batch.processing.registry import JobRegistry
from foundry_batch.registration.registrar import RegistrationError, WebhookRegistrar
from tests.support import (
    T0,
    FakeEndpoints,
    FakeProvider,
    FakeProviderFactory,
    clock,
    make_settings,
)

URL = "https://func.example.net/api/webhooks/eus2"


class _Response:
    def __init__(
        self,
        status: int,
        body: object = None,
        headers: dict[str, str] | None = None,
        raw: bytes | None = None,
    ) -> None:
        self.status_code = status
        self.content = (
            raw if raw is not None else (b"" if body is None else json.dumps(body).encode())
        )
        self.headers = headers or {}

    def json(self) -> Any:
        return json.loads(self.content)


class _Session:
    def __init__(self, *outcomes: object) -> None:
        self.outcomes = list(outcomes)
        self.calls: list[tuple[str, str]] = []

    def request(self, method: str, url: str, **_: object) -> _Response:
        self.calls.append((method, url))
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        assert isinstance(outcome, _Response)
        return outcome


class _Auth:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error

    def headers(self) -> dict[str, str]:
        if self.error:
            raise self.error
        return {"authorization": "Bearer t"}


def client(*outcomes: object, auth: _Auth | None = None) -> FoundryRestClient:
    return FoundryRestClient(
        "eus2", "https://res.openai.azure.com/", auth or _Auth(), session=_Session(*outcomes)
    )  # type: ignore[arg-type]


BATCH = {"id": "batch_1", "status": "validating", "created_at": 1_700_000_000}


class RestClientOutcomeTests(unittest.TestCase):
    def test_gateway_errors_on_writes_are_ambiguous(self) -> None:
        for status in (408, 500, 502, 504):
            with self.subTest(status=status), self.assertRaises(ProviderAmbiguousError) as ctx:
                client(_Response(status, headers={"retry-after": "7"})).create_batch(
                    input_file_id="f", endpoint="/chat/completions", metadata={}
                )
            self.assertEqual(
                (ctx.exception.status_code, ctx.exception.retry_after_seconds), (status, 7.0)
            )

    def test_throttling_on_writes_is_retryable_not_ambiguous(self) -> None:
        for status in (429, 503):
            with self.subTest(status=status), self.assertRaises(ProviderRetryableError) as ctx:
                client(_Response(status)).create_batch(
                    input_file_id="f", endpoint="/chat/completions", metadata={}
                )
            self.assertNotIsInstance(ctx.exception, ProviderAmbiguousError)

    def test_every_5xx_is_retryable_and_ambiguous_on_writes_except_503(self) -> None:
        for status in (500, 501, 502, 505, 507, 511, 520, 599):
            with self.subTest(status=status):
                with self.assertRaises(ProviderRetryableError) as read:
                    client(_Response(status)).retrieve_batch("batch_1")
                self.assertNotIsInstance(read.exception, ProviderAmbiguousError)
                with self.assertRaises(ProviderAmbiguousError) as write:
                    client(_Response(status)).create_batch(
                        input_file_id="f", endpoint="/chat/completions", metadata={}
                    )
                self.assertEqual(write.exception.status_code, status)
        for status in (499, 600):  # outside 5xx: a definite client error
            with self.subTest(status=status), self.assertRaises(ProviderRequestError) as ctx:
                client(_Response(status)).retrieve_batch("batch_1")
            self.assertNotIsInstance(ctx.exception, ProviderRetryableError)

    def test_http_date_retry_after_is_measured_on_the_injected_clock(self) -> None:
        manual = ManualClock(T0)
        header = format_datetime(T0 + timedelta(seconds=120), usegmt=True)
        api = FoundryRestClient(
            "eus2",
            "https://res.openai.azure.com/",
            _Auth(),
            session=_Session(_Response(429, headers={"retry-after": header})),  # type: ignore[arg-type]
            clock=manual,
        )
        with self.assertRaises(ProviderRetryableError) as ctx:
            api.retrieve_batch("batch_1")
        self.assertEqual(ctx.exception.retry_after_seconds, 120.0)

    def test_gateway_errors_on_reads_are_plain_retryable(self) -> None:
        with self.assertRaises(ProviderRetryableError) as ctx:
            client(_Response(504)).retrieve_batch("batch_1")
        self.assertNotIsInstance(ctx.exception, ProviderAmbiguousError)

    def test_unreadable_2xx_body(self) -> None:
        with self.assertRaises(ProviderAmbiguousError):  # the write happened
            client(_Response(200, raw=b"<html>")).create_batch(
                input_file_id="f", endpoint="/chat/completions", metadata={}
            )
        with self.assertRaises(ProviderAmbiguousError):
            client(_Response(200, raw=b"")).upload_file(filename="x.jsonl", content=b"{}")
        with self.assertRaises(ProviderRetryableError) as ctx:
            client(_Response(200, raw=b"<html>")).retrieve_batch("batch_1")
        self.assertNotIsInstance(ctx.exception, ProviderAmbiguousError)

    def test_transport_errors(self) -> None:
        cases: list[tuple[str, Exception, type[ProviderError], bool]] = [
            ("POST", requests.ConnectTimeout("no connection"), ProviderRetryableError, False),
            ("POST", requests.ReadTimeout("sent, no answer"), ProviderAmbiguousError, True),
            ("POST", requests.exceptions.ChunkedEncodingError("cut"), ProviderAmbiguousError, True),
            ("POST", requests.ConnectionError("reset"), ProviderAmbiguousError, True),
            ("GET", requests.ReadTimeout("slow"), ProviderRetryableError, False),
        ]
        for method, error, expected, is_ambiguous in cases:
            with self.subTest(method=method, error=type(error).__name__):
                api = client(error)
                with self.assertRaises(expected) as ctx:
                    if method == "GET":
                        api.retrieve_batch("batch_1")
                    else:
                        api.create_batch(
                            input_file_id="f", endpoint="/chat/completions", metadata={}
                        )
                self.assertEqual(isinstance(ctx.exception, ProviderAmbiguousError), is_ambiguous)
                self.assertEqual(api.counter.total(status="network"), 1)  # still counted

    def test_credential_failure_is_retryable_and_makes_no_call(self) -> None:
        api = client(auth=_Auth(RuntimeError("ManagedIdentityCredential unavailable")))
        with self.assertRaises(ProviderRetryableError) as ctx:
            api.retrieve_batch("batch_1")
        self.assertIn("auth RuntimeError", str(ctx.exception))
        self.assertEqual(api._session.calls, [])  # type: ignore[attr-defined]
        passthrough = client(auth=_Auth(ProviderNotFoundError("x", status_code=404)))
        with self.assertRaises(ProviderNotFoundError):
            passthrough.retrieve_batch("batch_1")

    def test_create_endpoint_without_secret_names_the_orphan(self) -> None:
        api = client(_Response(200, {"id": "wh_9", "name": "n", "url": URL, "event_types": []}))
        with self.assertRaises(EndpointSecretMissingError) as ctx:
            api.create_endpoint(name="n", url=URL, event_types=["batch.completed"])
        self.assertEqual(ctx.exception.endpoint_id, "wh_9")

    def test_delete_endpoint_is_idempotent(self) -> None:
        client(_Response(404, {"error": {"code": "not_found"}})).delete_endpoint("wh_gone")
        client(_Response(204)).delete_endpoint("wh_1")


class SubmissionRejectionTests(unittest.TestCase):
    def test_definite_rejection_is_recorded_and_not_chased_by_recovery(self) -> None:
        settings = make_settings()
        clk = clock()
        store = MemoryTableStore()
        provider = FakeProvider(clk)
        service = SubmissionService(
            settings,
            store,
            JobRegistry(store, settings, clk),
            ControlStore(store, "batchcontrol", "test", clk),
            FakeProviderFactory({"eus2": provider}),
            CharacterHeuristicEstimator(),
            clk,
        )
        provider.fail("create", ProviderRetryableError("quota", status_code=429))
        good = json.dumps(
            {
                "custom_id": "1",
                "method": "POST",
                "url": "/v1/chat/completions",
                "body": {"model": "gpt-batch", "messages": []},
            }
        ).encode()
        with self.assertLogs("foundry_batch", "ERROR"), self.assertRaises(ProviderRetryableError):
            service.submit("eus2", "gpt-batch", good, correlation_id="c9")
        [row] = [e.data for e in store.query(settings.table_submissions, Query())]
        self.assertEqual((row["status"], row["error_status"]), ("rejected", 429))
        clk.advance(3600)
        self.assertEqual(service.recover("eus2"), [])
        [row] = [e.data for e in store.query(settings.table_submissions, Query())]
        self.assertEqual(row["status"], "rejected")


class NoSecretEndpoints(FakeEndpoints):
    """Creates the endpoint but the response carries no signing secret."""

    def create_endpoint(
        self, *, name: str, url: str, event_types: Sequence[str]
    ) -> CreatedWebhookEndpoint:
        created = super().create_endpoint(name=name, url=url, event_types=event_types)
        raise EndpointSecretMissingError("no secret", endpoint_id=created.endpoint.endpoint_id)


class FlakyControl(ControlStore):
    fail_next_save = False

    def save_registration(self, record: Any) -> None:
        if self.fail_next_save:
            self.fail_next_save = False
            raise StoreUnavailableError("table timeout")
        super().save_registration(record)


class RegistrarHardeningTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = clock()
        self.settings = make_settings()
        self.control = FlakyControl(MemoryTableStore(), "batchcontrol", "test", self.clock)
        self.secrets = MemorySecrets()

    def registrar(self, api: FakeEndpoints) -> WebhookRegistrar:
        return WebhookRegistrar(
            self.settings, lambda _: api, self.secrets, self.control, self.clock, holder="r1"
        )

    def test_missing_secret_removes_the_new_endpoint(self) -> None:
        api = NoSecretEndpoints()
        with self.assertLogs("foundry_batch", "WARNING"), self.assertRaises(RegistrationError):
            self.registrar(api).ensure("eus2", URL)
        self.assertEqual(api.endpoints, {})
        self.assertEqual(self.secrets.names(), [])

    def test_missing_secret_and_failed_delete_is_a_hard_error(self) -> None:
        api = NoSecretEndpoints()
        api.fail("delete", ProviderRetryableError("busy", status_code=503))
        with self.assertLogs("foundry_batch", "ERROR"), self.assertRaises(CompensationFailedError):
            self.registrar(api).ensure("eus2", URL)
        self.assertEqual(len(api.endpoints), 1)

    def test_lost_registration_record_is_repaired_on_the_next_run(self) -> None:
        api = FakeEndpoints()
        self.control.fail_next_save = True
        with self.assertRaises(StoreUnavailableError):
            self.registrar(api).ensure("eus2", URL)
        self.assertEqual(self.control.registrations("eus2"), [])
        with self.assertLogs("foundry_batch", "WARNING") as logs:
            [outcome] = self.registrar(api).ensure("eus2", URL)
        self.assertEqual((outcome.action, outcome.healthy), ("unchanged", True))
        [record] = self.control.registrations("eus2")
        self.assertEqual(record.endpoint_id, outcome.endpoint_id)
        self.assertTrue(any("registration.record_repaired" in line for line in logs.output))
        self.registrar(api).ensure("eus2", URL)
        self.assertEqual(len(self.control.registrations("eus2")), 1)  # no duplicates

    def test_key_vault_outage_is_not_mistaken_for_an_unhealthy_endpoint(self) -> None:
        api = FakeEndpoints()
        self.registrar(api).ensure("eus2", URL)
        self.secrets.fail_reads = True
        with self.assertRaises(SecretUnavailableError):
            self.registrar(api).ensure("eus2", URL, repair=True)
        self.assertEqual(len(api.endpoints), 1)  # nothing was replaced


class PreflightCommandTests(unittest.TestCase):
    def test_empty_option_value_is_a_bare_switch(self) -> None:
        argv = AzCommand(("role", "assignment"), "list", {"--assignee": "oid", "--all": ""}).argv()
        self.assertEqual(argv[:6], ["role", "assignment", "list", "--assignee", "oid", "--all"])
        self.assertNotIn("true", argv)


if __name__ == "__main__":
    unittest.main()
