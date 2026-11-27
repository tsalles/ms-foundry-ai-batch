"""Azure adapters against SDK stand-ins: request shapes and error mapping.

The adapters are loaded under private module names with stubbed ``azure.*`` packages, so these
tests run without the SDKs and never disturb the real ones in CI. Behaviour against the real
services (ETags, transactions, visibility) is covered by tests/integration (Azurite).
"""

import importlib.util
import json
import sys
import types
import unittest
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from unittest import mock

from foundry_batch.errors import (
    ConcurrencyConflictError,
    QueueUnavailableError,
    SecretUnavailableError,
    StoreUnavailableError,
)
from foundry_batch.ports import Entity, Query, TxOp
from tests.stubs import azure_sdk as sdk

SRC = Path(__file__).resolve().parents[2] / "src" / "foundry_batch" / "adapters"


@contextmanager
def stubbed() -> Iterator[None]:
    wanted = sdk.modules()
    saved = {name: sys.modules.get(name) for name in wanted}
    sys.modules.update(wanted)
    try:
        yield
    finally:
        for name, module in saved.items():
            if module is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = module


def load(name: str) -> types.ModuleType:
    with stubbed():
        spec = importlib.util.spec_from_file_location(f"_stubbed_{name}", SRC / f"{name}.py")
        assert spec and spec.loader
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    return module


tables = load("azure_tables")
queues = load("azure_queues")
keyvault = load("azure_keyvault")


class FakeTableClient:
    def __init__(self) -> None:
        self.transactions: list[list[Any]] = []
        self.queries: list[tuple[str, dict[str, Any]]] = []
        self.entities: list[sdk.TableEntity] = []
        self.error: Exception | None = None

    def get_entity(self, pk: str, rk: str) -> sdk.TableEntity:
        if self.error:
            raise self.error
        return sdk.table_entity(
            {
                "PartitionKey": pk,
                "RowKey": rk,
                "n": 1,
                "big": sdk.EntityProperty(3_000_000_000, sdk.EdmType.INT64),
            },
            'W/"1"',
        )

    def query_entities(self, flt: str, *, parameters: dict[str, Any]) -> Iterator[Any]:
        if self.error:
            raise self.error
        self.queries.append((flt, parameters))
        yield from self.entities

    def list_entities(self) -> Iterator[Any]:
        self.queries.append(("", {}))
        yield from self.entities

    def submit_transaction(self, operations: list[Any]) -> list[dict[str, str]]:
        if self.error:
            raise self.error
        self.transactions.append(operations)
        return [{"etag": f'W/"{i + 10}"'} for i in range(len(operations))]


class FakeService:
    def __init__(self, client: Any) -> None:
        self.client = client
        self.names: list[str] = []

    def get_table_client(self, name: str) -> Any:
        self.names.append(name)
        return self.client

    def get_queue_client(self, name: str) -> Any:
        self.names.append(name)
        return self.client


class TableAdapterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = FakeTableClient()
        self.store = tables.AzureTableStore(FakeService(self.client))

    def test_get_unwraps_int64_and_returns_the_etag(self) -> None:
        entity = self.store.get("t", "pk", "job")
        assert entity is not None
        self.assertEqual(entity.data, {"n": 1, "big": 3_000_000_000})
        self.assertEqual(entity.etag, 'W/"1"')
        self.client.error = sdk.ResourceNotFoundError("nope", status_code=404)
        self.assertIsNone(self.store.get("t", "pk", "job"))
        self.client.error = sdk.ServiceRequestError("dns")
        with self.assertRaises(StoreUnavailableError):
            self.store.get("t", "pk", "job")

    def test_transaction_shapes_and_etag_conditions(self) -> None:
        ops = [
            TxOp("create", Entity("pk", "a", {"big": 2**40, "none": None, "ok": True})),
            TxOp("update", Entity("pk", "b", {"x": 1}, 'W/"5"')),
            TxOp("update", Entity("pk", "c", {"x": 2}, None), force=True),
            TxOp("upsert", Entity("pk", "d", {})),
            TxOp("delete", Entity("pk", "e", {}, 'W/"6"')),
        ]
        etags = self.store.submit("t", ops)
        [sent] = self.client.transactions
        kinds = [op[0] for op in sent]
        self.assertEqual(kinds, ["create", "update", "update", "upsert", "delete"])
        create_body = sent[0][1]
        self.assertIsInstance(create_body["big"], sdk.EntityProperty)  # beyond Int32
        self.assertNotIn("none", create_body)
        self.assertEqual(
            sent[1][2],
            {
                "mode": sdk.UpdateMode.REPLACE,
                "etag": 'W/"5"',
                "match_condition": sdk.MatchConditions.IfNotModified,
            },
        )
        self.assertEqual(sent[2][2], {"mode": sdk.UpdateMode.REPLACE})  # forced
        self.assertEqual(
            sent[4][2], {"etag": 'W/"6"', "match_condition": sdk.MatchConditions.IfNotModified}
        )
        self.assertEqual(etags, {"a": 'W/"10"', "b": 'W/"11"', "c": 'W/"12"', "d": 'W/"13"'})
        self.assertEqual(self.store.submit("t", []), {})

    def test_transaction_errors_map_to_conflict_or_unavailable(self) -> None:
        op = [TxOp("create", Entity("pk", "a", {}))]
        for error, expected in (
            (
                sdk.TableTransactionError("x", error_code="EntityAlreadyExists"),
                ConcurrencyConflictError,
            ),
            (
                sdk.TableTransactionError("x", error_code="UpdateConditionNotSatisfied"),
                ConcurrencyConflictError,
            ),
            (sdk.TableTransactionError("x", error_code="InternalError"), StoreUnavailableError),
            (sdk.ResourceExistsError("x"), ConcurrencyConflictError),
            (sdk.ResourceModifiedError("x"), ConcurrencyConflictError),
            (sdk.ServiceRequestError("timeout"), StoreUnavailableError),
        ):
            with self.subTest(error=type(error).__name__):
                self.client.error = error
                with self.assertRaises(expected):
                    self.store.submit("t", op)

    def test_query_filter_parameters_and_limit(self) -> None:
        self.client.entities = [
            sdk.table_entity({"PartitionKey": "p", "RowKey": f"outbox:{i}", "v": i}, f"e{i}")
            for i in range(5)
        ]
        rows = self.store.query(
            "t",
            Query(
                partition_key="p",
                row_key_prefix="outbox:",
                equals={"status": "pending", "is_open": True},
                less_or_equal={"next": "2026"},
                limit=3,
            ),
        )
        self.assertEqual([r.row_key for r in rows], ["outbox:0", "outbox:1", "outbox:2"])
        flt, params = self.client.queries[0]
        self.assertEqual(
            flt,
            "PartitionKey eq @pk and RowKey ge @rklow and RowKey lt @rkhigh "
            "and is_open eq @eq0 and status eq @eq1 and next le @le0",
        )
        self.assertEqual(
            params,
            {
                "pk": "p",
                "rklow": "outbox:",
                "rkhigh": "outbox;",
                "eq0": True,
                "eq1": "pending",
                "le0": "2026",
            },
        )
        self.assertEqual(len(self.store.query("t", Query())), 5)  # no filter: list_entities
        self.client.error = sdk.HttpResponseError("throttled", status_code=503)
        with self.assertRaises(StoreUnavailableError):
            self.store.query("t", Query(partition_key="p"))


class FakeQueueClient:
    def __init__(self) -> None:
        self.sent: list[tuple[str, Any]] = []
        self.error: Exception | None = None
        self.messages = [
            types.SimpleNamespace(id=f"m{i}", pop_receipt=f"r{i}", content=f"c{i}", dequeue_count=i)
            for i in range(40)
        ]
        self.deleted: list[tuple[str, str]] = []
        self.receive_args: dict[str, Any] = {}
        self.time_to_live: int | None = None

    def send_message(
        self,
        content: str,
        *,
        visibility_timeout: int | None = None,
        time_to_live: int | None = None,
    ) -> None:
        if self.error:
            raise self.error
        self.sent.append((content, visibility_timeout))
        self.time_to_live = time_to_live

    def get_queue_properties(self) -> Any:
        return types.SimpleNamespace(approximate_message_count=7)

    def receive_messages(self, **kwargs: Any) -> Iterator[Any]:
        self.receive_args = kwargs
        yield from self.messages

    def peek_messages(self, *, max_messages: int) -> list[Any]:
        return self.messages[:max_messages]

    def delete_message(self, message_id: str, pop_receipt: str) -> None:
        self.deleted.append((message_id, pop_receipt))


class QueueAdapterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = FakeQueueClient()
        self.admin = queues.AzureQueueAdmin(FakeService(self.client))

    def test_send_passes_visibility_and_maps_failures(self) -> None:
        queue = queues.AzureMessageQueue(self.client)
        queue.send("hello", visibility_timeout=30)
        self.assertEqual(self.client.sent, [("hello", 30)])
        self.assertEqual(self.client.time_to_live, 14 * 24 * 3600)  # not the 7-day default
        self.client.error = sdk.ServiceRequestError("timeout")
        with self.assertRaises(QueueUnavailableError):
            queue.send("again")

    def test_admin_operations(self) -> None:
        self.assertEqual(self.admin.approximate_depth("q"), 7)
        received = self.admin.receive("q", max_messages=35, visibility_timeout=300)
        self.assertEqual(len(received), 35)
        self.assertEqual(
            self.client.receive_args, {"messages_per_page": 32, "visibility_timeout": 300}
        )
        peeked = self.admin.peek("q", max_messages=50)
        self.assertEqual(len(peeked), 32)  # the service peeks at most 32
        self.assertEqual(peeked[0].pop_receipt, "")
        self.admin.delete("q", received[3])
        self.assertEqual(self.client.deleted, [("m3", "r3")])
        self.admin.send_to("q2", "body")
        self.assertEqual(self.client.sent[-1], ("body", None))


class FakeSecretClient:
    def __init__(self) -> None:
        self.error: Exception | None = None
        self.enabled = True
        self.set_calls: list[tuple[str, str, dict[str, Any]]] = []
        self.disabled: list[str] = []
        self.versions_read: list[str | None] = []

    def get_secret(self, name: str, version: str | None = None) -> Any:
        if self.error:
            raise self.error
        self.versions_read.append(version)
        props = types.SimpleNamespace(enabled=self.enabled, tags={"endpoint_id": "wh_1"})
        return types.SimpleNamespace(value="whsec_x", properties=props)

    def set_secret(self, name: str, value: str, **kwargs: Any) -> Any:
        self.set_calls.append((name, value, kwargs))
        return types.SimpleNamespace(properties=types.SimpleNamespace(version="v7"))

    def update_secret_properties(self, name: str, *, enabled: bool) -> None:
        if self.error:
            raise self.error
        self.disabled.append(name)


DISABLED_MESSAGE = "Operation get is not allowed on a disabled secret."
FIREWALL_MESSAGE = (
    "Public access is disabled and the request was not made from a trusted service or via an "
    "approved private link."
)


class FakeODataV4Format:
    """Shape of ``azure.core.exceptions.ODataV4Format`` (``HttpResponseError.error``)."""

    def __init__(self, error: dict[str, Any]) -> None:
        self.code = error.get("code")
        self.message = error.get("message")
        self.innererror = error.get("innererror", {})


def key_vault_error(
    status: int, code: str, message: str, *, inner: str | None = None, via: str = "odata"
) -> sdk.HttpResponseError:
    """An ``HttpResponseError`` as azure-core builds it from a Key Vault error response: the
    body, ``error`` parsed from it, or the generated ``KeyVaultError`` in ``model``."""
    body: dict[str, Any] = {"code": code, "message": message}
    if inner:
        body["innererror"] = {"code": inner}
    exc = sdk.HttpResponseError(message, status_code=status)
    exc.response = types.SimpleNamespace(  # type: ignore[attr-defined]
        text=lambda: json.dumps({"error": body}) if via == "body" else ""
    )
    exc.error = FakeODataV4Format(body) if via == "odata" else None  # type: ignore[attr-defined]
    inner_model = types.SimpleNamespace(code=inner, inner_error=None) if inner else None
    model_error = types.SimpleNamespace(code=code, message=message, inner_error=inner_model)
    exc.model = types.SimpleNamespace(error=model_error) if via == "model" else None  # type: ignore[attr-defined]
    return exc


class KeyVaultAdapterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = FakeSecretClient()
        self.vault = keyvault.KeyVaultSecrets(self.client)

    def test_reads(self) -> None:
        self.assertEqual(self.vault.get_secret("s"), "whsec_x")
        self.assertEqual(self.vault.get_secret_tags("s"), {"endpoint_id": "wh_1"})
        self.client.enabled = False
        self.assertIsNone(self.vault.get_secret("s"))
        self.assertIsNone(self.vault.get_secret_tags("s"))

    def test_exact_version_reads(self) -> None:
        self.assertEqual(self.vault.get_secret_version("s", "abc123"), "whsec_x")
        self.assertEqual(self.client.versions_read[-1], "abc123")
        self.client.enabled = False
        self.assertIsNone(self.vault.get_secret_version("s", "abc123"))
        self.client.error = sdk.ResourceNotFoundError("gone", status_code=404)
        self.assertIsNone(self.vault.get_secret_version("s", "old"))

    def test_missing_and_disabled_read_as_none(self) -> None:
        for error in (
            sdk.ResourceNotFoundError("gone", status_code=404),
            key_vault_error(403, "Forbidden", DISABLED_MESSAGE, inner="SecretDisabled"),
            key_vault_error(
                403, "Forbidden", DISABLED_MESSAGE, inner="SecretDisabled", via="model"
            ),
            key_vault_error(403, "Forbidden", DISABLED_MESSAGE, inner="SecretDisabled", via="body"),
            key_vault_error(403, "SecretDisabled", DISABLED_MESSAGE),
        ):
            with self.subTest(error=repr(error)):
                self.client.error = error
                self.assertIsNone(self.vault.get_secret("s"))
                self.assertIsNone(self.vault.get_secret_tags("s"))
                self.assertIsNone(self.vault.get_secret_version("s", "v1"))

    def test_network_and_rbac_403s_are_outages_even_when_they_say_disabled(self) -> None:
        for error in (
            key_vault_error(403, "Forbidden", FIREWALL_MESSAGE, inner="ForbiddenByConnection"),
            key_vault_error(
                403, "Forbidden", FIREWALL_MESSAGE, inner="ForbiddenByFirewall", via="body"
            ),
            key_vault_error(403, "Forbidden", "Caller is not authorized", inner="ForbiddenByRbac"),
            key_vault_error(403, "Forbidden", DISABLED_MESSAGE, via="none"),  # no code to trust
            key_vault_error(409, "Conflict", DISABLED_MESSAGE, inner="SecretDisabled"),  # not a 403
        ):
            with self.subTest(error=repr(error)):
                self.client.error = error
                for read in (
                    lambda: self.vault.get_secret("s"),
                    lambda: self.vault.get_secret_tags("s"),
                    lambda: self.vault.get_secret_version("s", "v1"),
                ):
                    with self.assertRaises(SecretUnavailableError):
                        read()

    def test_unreadable_error_body_is_not_disabled(self) -> None:
        error = key_vault_error(403, "Forbidden", DISABLED_MESSAGE, via="none")
        error.response = types.SimpleNamespace(text=mock.Mock(side_effect=ValueError("closed")))
        self.client.error = error
        with self.assertRaises(SecretUnavailableError):
            self.vault.get_secret("s")

    def test_firewall_403_is_retried_by_vault_sync_not_blocked(self) -> None:
        from foundry_batch.adapters.memory import MemoryTableStore
        from foundry_batch.control import ControlStore, RegistrationRecord
        from foundry_batch.emulator.client_vault import EmulatedClientVault
        from foundry_batch.vaultsync.service import VaultSyncService
        from tests.support import T0, clock

        clk, store = clock(), MemoryTableStore()
        control = ControlStore(store, "batchcontrol", "poc", clk)
        sync = VaultSyncService(
            "poc", store, "batchcontrol", self.vault, EmulatedClientVault(), clk
        )
        control.save_registration(
            RegistrationRecord("eus2", "a", "wh_1", "n", "https://x", "s", "v1", T0)
        )
        self.client.error = key_vault_error(
            403, "Forbidden", FIREWALL_MESSAGE, inner="ForbiddenByConnection"
        )
        stats = sync.run("eus2", control.registrations("eus2"))
        self.assertEqual((stats.failed, stats.blocked), (1, 0))
        self.assertEqual(sync.status("eus2")[0].last_error, "source_unavailable")
        self.client.error = None
        self.assertEqual(sync.run("eus2", control.registrations("eus2")).synced, 1)

    def test_outages_are_secret_unavailable_not_missing(self) -> None:
        for error in (
            sdk.HttpResponseError("throttled", status_code=429),
            sdk.HttpResponseError("forbidden by RBAC", status_code=403),
            sdk.ServiceRequestError("network"),
        ):
            with self.subTest(error=str(error)):
                self.client.error = error
                with self.assertRaises(SecretUnavailableError):
                    self.vault.get_secret("s")
                with self.assertRaises(SecretUnavailableError):
                    self.vault.get_secret_tags("s")

    def test_writes(self) -> None:
        self.assertEqual(self.vault.set_secret("s", "v", tags={"a": "b"}), "v7")
        self.assertEqual(
            self.client.set_calls,
            [
                (
                    "s",
                    "v",
                    {"tags": {"a": "b"}, "content_type": "application/vnd.batch.webhook-secret"},
                )
            ],
        )
        self.vault.disable_secret("s")
        self.assertEqual(self.client.disabled, ["s"])
        self.client.error = sdk.ResourceNotFoundError("gone", status_code=404)
        self.vault.disable_secret("missing")  # idempotent


if __name__ == "__main__":
    unittest.main()
