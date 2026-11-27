"""The receiver's queue client and message retention, against SDK stand-ins (no SDK needed).

The adapters are loaded with stubbed ``azure.*`` packages and private module names, as in
``test_azure_adapters_unit``; sys.modules is restored afterwards so nothing leaks into other tests.
"""

import importlib.util
import os
import sys
import types
import unittest
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from unittest import mock

from foundry_batch.adapters.memory import MESSAGE_TTL_SECONDS as MEMORY_TTL_SECONDS
from tests.stubs import azure_sdk as sdk
from tests.support import make_settings

SRC = Path(__file__).resolve().parents[2] / "src" / "foundry_batch" / "adapters"
ADAPTER_MODULES = ("azure_queues", "azure_tables", "azure_keyvault")


@contextmanager
def patched_modules(wanted: dict[str, types.ModuleType]) -> Iterator[None]:
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


def load_queues() -> types.ModuleType:
    with patched_modules(sdk.modules()):
        spec = importlib.util.spec_from_file_location(
            "_stubbed_receiver_queues", SRC / "azure_queues.py"
        )
        assert spec and spec.loader
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    return module


queues = load_queues()


def stub_module(name: str, **attributes: Any) -> types.ModuleType:
    module = types.ModuleType(name)
    module.__dict__.update(attributes)
    return module


class RecordingQueueClient:
    """Fake ``QueueClient``: records constructor options and sends (real keyword names)."""

    def __init__(self, account_url: str, queue_name: str, credential: Any = None, **kwargs: Any):
        self.account_url, self.queue_name, self.credential = account_url, queue_name, credential
        self.options = kwargs
        self.sent: list[tuple[str, int | None, int | None]] = []

    def send_message(
        self,
        content: str,
        *,
        visibility_timeout: int | None = None,
        time_to_live: int | None = None,
        timeout: int | None = None,
    ) -> None:
        self.sent.append((content, visibility_timeout, time_to_live))


class RecordingQueueService:
    def __init__(self, account_url: str = "", credential: Any = None, **kwargs: Any) -> None:
        self.clients: dict[str, RecordingQueueClient] = {}

    def get_queue_client(self, name: str) -> RecordingQueueClient:
        return self.clients.setdefault(name, RecordingQueueClient("shared", name))


class ReceiverQueueClientTests(unittest.TestCase):
    def test_receiver_client_has_one_quick_retry_and_short_timeouts(self) -> None:
        client = queues.receiver_queue_client(
            "https://acct.queue.core.windows.net",
            "batch-events",
            "cred",
            factory=RecordingQueueClient,
        )
        self.assertEqual(
            (client.account_url, client.queue_name, client.credential),
            ("https://acct.queue.core.windows.net", "batch-events", "cred"),
        )
        options = client.options
        self.assertEqual(options["retry_total"], 1)
        # Worst case: every attempt times out connecting and reading, plus the largest back-off.
        attempts = options["retry_total"] + 1
        backoff = options["initial_backoff"] + options["random_jitter_range"]
        worst_case = attempts * (options["connection_timeout"] + options["read_timeout"]) + backoff
        self.assertLessEqual(worst_case, 15)  # the SDK default spends about 57 s on back-off alone

    def test_default_factory_is_the_sdk_queue_client(self) -> None:
        signature = queues.receiver_queue_client.__kwdefaults__
        self.assertIs(signature["factory"], queues.QueueClient)


class MessageRetentionTests(unittest.TestCase):
    def test_every_send_keeps_the_message_for_14_days(self) -> None:
        client = RecordingQueueClient("u", "q")
        queue = queues.AzureMessageQueue(client)
        queue.send("now")
        queue.send("later", visibility_timeout=60)
        self.assertEqual(client.sent, [("now", None, 1_209_600), ("later", 60, 1_209_600)])

    def test_replay_to_a_source_queue_also_keeps_14_days(self) -> None:
        service = RecordingQueueService()
        queues.AzureQueueAdmin(service).send_to("batch-events", "replayed")
        self.assertEqual(service.clients["batch-events"].sent, [("replayed", None, 1_209_600)])

    def test_memory_double_uses_the_same_retention(self) -> None:
        self.assertEqual(queues.MESSAGE_TTL_SECONDS, MEMORY_TTL_SECONDS)


class CompositionWiringTests(unittest.TestCase):
    """``azure_adapters`` builds the receiver's queue with its own tight client."""

    def stub_sdk(self) -> dict[str, types.ModuleType]:
        wanted = sdk.modules()
        wanted["azure.storage.queue"] = stub_module(
            "azure.storage.queue",
            QueueClient=RecordingQueueClient,
            QueueServiceClient=RecordingQueueService,
        )
        wanted["azure.identity"] = stub_module(
            "azure.identity",
            DefaultAzureCredential=lambda: "default-credential",
            ManagedIdentityCredential=lambda **_: "managed-credential",
        )
        return wanted

    @contextmanager
    def fresh_adapter_modules(self) -> Iterator[None]:
        import foundry_batch.adapters as package

        names = [f"foundry_batch.adapters.{name}" for name in ADAPTER_MODULES]
        saved = {name: sys.modules.pop(name, None) for name in names}
        attributes = {name: getattr(package, name, None) for name in ADAPTER_MODULES}
        try:
            yield
        finally:
            for name, module in saved.items():
                sys.modules.pop(name, None)
                if module is not None:
                    sys.modules[name] = module
            for name, value in attributes.items():
                if value is None and hasattr(package, name):
                    delattr(package, name)
                elif value is not None:
                    setattr(package, name, value)

    def test_incoming_queue_uses_the_receiver_client_and_relay_the_shared_one(self) -> None:
        from foundry_batch.composition import azure_adapters

        settings = make_settings(
            **{
                "BATCH_STORAGE__queueServiceUri": "https://acct.queue.core.windows.net",
                "BATCH_STORAGE__tableServiceUri": "https://acct.table.core.windows.net",
            }
        )
        hosting = ("AZURE_CLIENT_ID", "WEBSITE_INSTANCE_ID")
        env = {k: v for k, v in os.environ.items() if k not in hosting}
        with (
            self.fresh_adapter_modules(),
            patched_modules(self.stub_sdk()),
            mock.patch.dict(os.environ, env, clear=True),
        ):
            adapters = azure_adapters(settings)
            wired = sys.modules["foundry_batch.adapters.azure_queues"]
            options = dict(wired.RECEIVER_QUEUE_CLIENT_OPTIONS)
        incoming = adapters.incoming_queue._client  # type: ignore[attr-defined]
        relay = adapters.relay_queue._client  # type: ignore[attr-defined]
        self.assertEqual(
            (incoming.account_url, incoming.queue_name, incoming.credential),
            ("https://acct.queue.core.windows.net", "batch-events", "default-credential"),
        )
        self.assertEqual(incoming.options, options)
        self.assertEqual(incoming.options["retry_total"], 1)
        self.assertEqual((relay.account_url, relay.queue_name), ("shared", "relay-dispatch"))
        self.assertEqual(relay.options, {})


if __name__ == "__main__":
    unittest.main()
