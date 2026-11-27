"""Azure adapters against Azurite (integration stage). Skipped unless AZURITE=1 and the SDKs exist.

Run locally:  docker run -p 10000-10002:10000-10002 mcr.microsoft.com/azure-storage/azurite
              AZURITE=1 pytest tests/integration -m integration
"""

import os
import unittest
import uuid

try:
    import pytest

    pytestmark = pytest.mark.integration
except ImportError:  # pragma: no cover - unittest runner without pytest
    pass

try:
    from azure.data.tables import TableServiceClient
    from azure.storage.queue import QueueServiceClient
except ImportError:  # pragma: no cover
    TableServiceClient = None  # type: ignore[assignment,misc]
    QueueServiceClient = None  # type: ignore[assignment,misc]

AZURITE = "UseDevelopmentStorage=true"
AZURITE_QUEUE_API_VERSION = "2023-11-03"
ENABLED = os.environ.get("AZURITE") == "1" and TableServiceClient is not None


@unittest.skipUnless(ENABLED, "Azurite integration disabled")
class AzureTableStoreTests(unittest.TestCase):  # pragma: no cover - runs in the integration stage
    def setUp(self) -> None:
        from foundry_batch.adapters.azure_tables import AzureTableStore

        self.table = "t" + uuid.uuid4().hex[:10]
        service = TableServiceClient.from_connection_string(AZURITE)
        service.create_table(self.table)
        self.addCleanup(service.delete_table, self.table)
        self.store = AzureTableStore(service)

    def test_status_call_reservation_is_shared_by_independent_registry_instances(self) -> None:
        from dataclasses import replace

        from foundry_batch.domain.models import JobKey, JobRecord
        from foundry_batch.errors import RetryLater
        from foundry_batch.processing.registry import JobRegistry
        from tests.support import clock, make_settings

        now = clock()
        settings = replace(make_settings(), table_jobs=self.table)
        first = JobRegistry(self.store, settings, now)
        second = JobRegistry(self.store, settings, now)
        key = JobKey("eus2", "shared-reservation")
        first.create_job(JobRecord(key.resource_key, key.batch_id, settings.environment))
        first.reserve_status_call(key)
        with self.assertRaises(RetryLater):
            second.reserve_status_call(key)
        now.advance(60)
        second.reserve_status_call(key)
        with self.assertRaises(RetryLater):
            first.reserve_status_call(key)

    def test_same_semantics_as_memory_store(self) -> None:
        from foundry_batch.errors import ConcurrencyConflictError
        from foundry_batch.ports import Entity, Query, TxOp

        etags = self.store.submit(
            self.table,
            [
                TxOp("create", Entity("p", "r1", {"n": 3_000_000_000})),
                TxOp("create", Entity("p", "r2", {"s": "x"})),
            ],
        )
        with self.assertRaises(ConcurrencyConflictError):
            self.store.submit(self.table, [TxOp("create", Entity("p", "r1", {}))])
        with self.assertRaises(ConcurrencyConflictError):
            self.store.submit(self.table, [TxOp("update", Entity("p", "r1", {}, 'W/"x"'))])
        self.store.submit(self.table, [TxOp("update", Entity("p", "r1", {"n": 1}, etags["r1"]))])
        self.assertEqual(self.store.get(self.table, "p", "r1").data["n"], 1)  # type: ignore[union-attr]
        self.assertEqual(len(self.store.query(self.table, Query(row_key_prefix="r"))), 2)
        self.assertEqual(len(self.store.query(self.table, Query(equals={"s": "x"}))), 1)
        self.assertIsNone(self.store.get(self.table, "p", "missing"))


@unittest.skipUnless(ENABLED, "Azurite integration disabled")
class AzureQueueTests(unittest.TestCase):  # pragma: no cover
    def test_send_receive_delete(self) -> None:
        from foundry_batch.adapters.azure_queues import AzureMessageQueue, AzureQueueAdmin

        name, service = self.queue()
        AzureMessageQueue(service.get_queue_client(name)).send('{"a":1}')
        admin = AzureQueueAdmin(service)
        self.assertEqual(admin.approximate_depth(name), 1)
        [message] = admin.receive(name, max_messages=5, visibility_timeout=30)
        self.assertEqual(message.content, '{"a":1}')
        admin.delete(name, message)

    def queue(self) -> tuple[str, "QueueServiceClient"]:
        name = "q" + uuid.uuid4().hex[:10]
        service = QueueServiceClient.from_connection_string(
            AZURITE, api_version=AZURITE_QUEUE_API_VERSION
        )
        service.create_queue(name)
        self.addCleanup(service.delete_queue, name)
        return name, service

    def test_messages_are_kept_for_14_days_not_the_7_day_default(self) -> None:
        from foundry_batch.adapters.azure_queues import MESSAGE_TTL_SECONDS, AzureMessageQueue

        name, service = self.queue()
        client = service.get_queue_client(name)
        AzureMessageQueue(client).send("ttl")
        [message] = client.peek_messages()
        lifetime = (message.expires_on - message.inserted_on).total_seconds()
        self.assertEqual(lifetime, MESSAGE_TTL_SECONDS)

    def test_receiver_client_options_reach_the_sdk_pipeline(self) -> None:
        from azure.storage.queue import QueueClient

        from foundry_batch.adapters.azure_queues import AzureMessageQueue, receiver_queue_client

        name, _ = self.queue()

        def from_azurite(_url: str, queue: str, **options: object) -> QueueClient:
            options.pop("credential", None)  # Azurite uses its connection string
            return QueueClient.from_connection_string(
                AZURITE, queue, api_version=AZURITE_QUEUE_API_VERSION, **options
            )

        client = receiver_queue_client("", name, None, factory=from_azurite)
        retry = client._config.retry_policy
        self.assertEqual((retry.total_retries, retry.initial_backoff), (1, 1))
        connection = client._pipeline._transport.connection_config
        self.assertEqual((connection.timeout, connection.read_timeout), (2, 3))
        AzureMessageQueue(client).send("fast")
        self.assertEqual(len(client.peek_messages()), 1)
