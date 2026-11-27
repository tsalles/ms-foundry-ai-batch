"""One contract, three targets: the switch to the real service is proven, not assumed.

The same assertions run against the in-process emulator route, the loopback HTTP emulator and
(when explicitly enabled) a real Foundry resource, always through ``FoundryRestClient``. When a
live run disagrees with the emulator, the emulator is wrong: fix it and keep this suite green.

Live run (synthetic data, tiny cost; creates and deletes one webhook endpoint and pages
through the batches that already exist, read-only)::

    BATCH_LIVE_TESTS=1 BATCH_LIVE_FOUNDRY_ENDPOINT=https://<resource>.openai.azure.com/ \\
        pytest tests/contract -m integration

Uploading input files and creating real batch jobs is a separate opt-in (``BATCH_LIVE_BATCH=1``
*and* a deployment in ``BATCH_LIVE_DEPLOYMENT``) because it consumes quota: tiny jobs are
cancelled and their input files deleted afterwards (they also expire after 14 days).
"""

from __future__ import annotations

import contextlib
import json
import os
import time
import unittest
import uuid
from collections.abc import Callable, Mapping

from foundry_batch.clock import Clock, ManualClock, SystemClock
from foundry_batch.emulator.foundry import FoundryEmulator
from foundry_batch.emulator.transport import (
    EmulatorHttpServer,
    emulator_endpoint,
    foundry_handler,
    routed_session,
)
from foundry_batch.errors import ProviderError, ProviderNotFoundError
from foundry_batch.foundry.auth import StaticTokenAuth
from foundry_batch.foundry.files import wait_until_processed
from foundry_batch.foundry.rest_client import FoundryRestClient
from foundry_batch.ports import BatchPage
from tests.support import T0


def live_options(env: Mapping[str, str]) -> tuple[str, bool, bool, str]:
    """(endpoint, live, live_batch, deployment). Uploading files and creating real jobs needs
    the opt-in *and* a deployment: never run against an empty model name."""
    endpoint = env.get("BATCH_LIVE_FOUNDRY_ENDPOINT", "")
    live = env.get("BATCH_LIVE_TESTS") == "1" and endpoint.startswith("https://")
    deployment = env.get("BATCH_LIVE_DEPLOYMENT", "")
    live_batch = live and env.get("BATCH_LIVE_BATCH") == "1" and bool(deployment)
    return endpoint, live, live_batch, deployment


LIVE_ENDPOINT, LIVE, LIVE_BATCH, LIVE_DEPLOYMENT = live_options(os.environ)
EVENTS = ["batch.completed", "batch.failed", "batch.cancelled", "batch.expired"]
#: Statuses a just-uploaded file may report before it is ``processed``.
FILE_STATUSES = {"uploaded", "pending", "running", "processed"}


def jsonl(deployment: str) -> bytes:
    row = {
        "custom_id": "contract-1",
        "method": "POST",
        "url": "/chat/completions",
        "body": {"model": deployment, "messages": [{"role": "user", "content": "Say OK."}]},
    }
    return (json.dumps(row) + "\n").encode()


class FoundryContract:
    """Mixin: ``self.client`` is a ``FoundryRestClient``; ``self.deployment`` a model name.

    ``self.clock`` and ``self.sleep`` drive file-status polling (emulated or real time).
    """

    client: FoundryRestClient
    clock: Clock
    sleep: Callable[[float], None]
    deployment: str = "gpt-batch"
    can_create_batches = True
    #: Emulators order equal ``created_at`` by creation; the service only promises newest-first.
    exact_tie_order = True
    max_pages = 50

    def upload(self) -> str:
        """A tiny synthetic input file, deleted at cleanup (it would also expire)."""
        file_id = self.client.upload_file(filename="contract.jsonl", content=jsonl(self.deployment))
        self.addCleanup(self.quietly, self.client.delete_file, file_id)  # type: ignore[attr-defined]
        return file_id

    def submit_tiny(self) -> str:
        tc: unittest.TestCase = self  # type: ignore[assignment]
        file_id = self.upload()
        wait = wait_until_processed(
            self.client, file_id, self.clock, self.sleep, timeout_s=300, poll_s=5
        )
        tc.assertEqual(wait.outcome, "processed", wait)
        batch = self.client.create_batch(
            input_file_id=file_id, endpoint="/chat/completions", metadata={"batch_contract": "1"}
        )
        tc.addCleanup(self.quietly, self.client.cancel_batch, batch.batch_id)
        return batch.batch_id

    @staticmethod
    def quietly(action: Callable[[str], object], object_id: str) -> None:
        with contextlib.suppress(ProviderError):
            action(object_id)

    def walk(self, limit: int) -> list[BatchPage]:
        pages = [self.client.list_batches(after=None, limit=limit)]
        while pages[-1].has_more and pages[-1].last_id and len(pages) < self.max_pages:
            pages.append(self.client.list_batches(after=pages[-1].last_id, limit=limit))
        return pages

    def test_webhook_endpoint_lifecycle(self) -> None:
        tc: unittest.TestCase = self  # type: ignore[assignment]
        name = f"batch-contract-{uuid.uuid4().hex[:8]}"
        created = self.client.create_endpoint(
            name=name, url="https://contract.example.invalid/hook", event_types=EVENTS
        )
        try:
            tc.assertTrue(created.signing_secret.startswith("whsec_"))
            tc.assertEqual(set(created.endpoint.event_types), set(EVENTS))
            listed = {e.endpoint_id: e for e in self.client.list_endpoints()}
            tc.assertIn(created.endpoint.endpoint_id, listed)
            updated = self.client.update_endpoint(
                created.endpoint.endpoint_id,
                name=name,
                url="https://contract.example.invalid/other",
                event_types=["batch.completed"],
            )
            tc.assertEqual(updated.url, "https://contract.example.invalid/other")
        finally:
            self.client.delete_endpoint(created.endpoint.endpoint_id)
        self.client.delete_endpoint(created.endpoint.endpoint_id)  # idempotent
        tc.assertNotIn(
            created.endpoint.endpoint_id, {e.endpoint_id for e in self.client.list_endpoints()}
        )

    def test_paging_with_after_is_disjoint_newest_first_and_ends(self) -> None:
        tc: unittest.TestCase = self  # type: ignore[assignment]
        created = [self.submit_tiny() for _ in range(3)] if self.can_create_batches else []
        pages = self.walk(limit=1)
        ids = [b.batch_id for page in pages for b in page.items]
        if len(ids) < 2 and not created:
            tc.skipTest("fewer than two batches to page through (set BATCH_LIVE_BATCH=1)")
        for page in pages:
            tc.assertLessEqual(len(page.items), 1)  # limit honoured
            if page.items:
                tc.assertEqual(page.last_id, page.items[-1].batch_id)
        tc.assertEqual(len(ids), len(set(ids)), "pages overlap")
        stamps = [b.created_at for page in pages for b in page.items if b.created_at]
        tc.assertEqual(stamps, sorted(stamps, reverse=True), "not newest-first across pages")
        if len(pages) < self.max_pages:
            tc.assertFalse(pages[-1].has_more)
        for batch_id in created:
            tc.assertIn(batch_id, ids)
        if self.exact_tie_order and created:
            tc.assertEqual([i for i in ids if i in created], list(reversed(created)))

    def test_uploaded_file_is_tracked_until_processed_and_deleted(self) -> None:
        tc: unittest.TestCase = self  # type: ignore[assignment]
        if not self.can_create_batches:
            tc.skipTest("set BATCH_LIVE_BATCH=1 and BATCH_LIVE_DEPLOYMENT to upload a file")
        file_id = self.upload()
        first = self.client.retrieve_file(file_id)
        tc.assertEqual(first.file_id, file_id)
        tc.assertIn(first.status, FILE_STATUSES)
        wait = wait_until_processed(
            self.client, file_id, self.clock, self.sleep, timeout_s=300, poll_s=5
        )
        tc.assertEqual((wait.outcome, wait.status), ("processed", "processed"))
        processed = self.client.retrieve_file(file_id)
        tc.assertIsNotNone(processed.expires_at)  # uploaded with the 14-day expiration
        self.client.delete_file(file_id)
        with tc.assertRaises(ProviderNotFoundError):
            self.client.retrieve_file(file_id)
        self.client.delete_file(file_id)  # idempotent

    def test_unknown_batch_is_not_found(self) -> None:
        with self.assertRaises(ProviderNotFoundError):  # type: ignore[attr-defined]
            self.client.retrieve_batch("batch_does_not_exist_00000000")

    def test_create_retrieve_and_cancel_a_tiny_batch(self) -> None:
        tc: unittest.TestCase = self  # type: ignore[assignment]
        if not self.can_create_batches:
            tc.skipTest("set BATCH_LIVE_BATCH=1 and BATCH_LIVE_DEPLOYMENT to create a real job")
        batch_id = self.submit_tiny()
        batch = self.client.retrieve_batch(batch_id)
        tc.assertIn(batch.status, ("validating", "in_progress"))
        tc.assertIsNotNone(batch.created_at)
        tc.assertEqual(batch.metadata.get("batch_contract"), "1")
        cancelled = self.client.cancel_batch(batch_id)
        tc.assertIn(cancelled.status, ("cancelling", "cancelled", "failed", "completed"))


class LiveOptionTests(unittest.TestCase):
    def test_live_batch_cases_need_the_opt_in_and_a_deployment(self) -> None:
        base = {
            "BATCH_LIVE_TESTS": "1",
            "BATCH_LIVE_FOUNDRY_ENDPOINT": "https://r.openai.azure.com/",
        }
        self.assertEqual(live_options({}), ("", False, False, ""))
        self.assertEqual(live_options(base)[1:3], (True, False))
        self.assertEqual(live_options({**base, "BATCH_LIVE_BATCH": "1"})[1:3], (True, False))
        both = {**base, "BATCH_LIVE_BATCH": "1", "BATCH_LIVE_DEPLOYMENT": "gpt-batch"}
        self.assertEqual(live_options(both)[1:], (True, True, "gpt-batch"))
        insecure = {**both, "BATCH_LIVE_FOUNDRY_ENDPOINT": "http://r.openai.azure.com/"}
        self.assertEqual(live_options(insecure)[1:3], (False, False))


class InProcessEmulatorContract(FoundryContract, unittest.TestCase):
    """Emulated time; files stay ``pending`` for 20 s, so the status wait is exercised."""

    def setUp(self) -> None:
        manual = ManualClock(T0)
        self.clock = manual
        self.sleep = manual.advance
        self.emulator = FoundryEmulator("eus2", manual, file_processing_s=20.0)
        session = routed_session({"eus2.emulator.invalid": foundry_handler(self.emulator)})
        self.client = FoundryRestClient(
            "eus2", emulator_endpoint("eus2"), StaticTokenAuth(), session=session, clock=manual
        )


class LoopbackHttpEmulatorContract(FoundryContract, unittest.TestCase):
    """Real sockets and wall-clock time: files are processed at once, so nothing sleeps."""

    def setUp(self) -> None:
        self.clock = SystemClock()
        self.sleep = self.no_sleep
        self.emulator = FoundryEmulator("eus2", self.clock)
        self.server = EmulatorHttpServer({"eus2": self.emulator}).start()
        self.addCleanup(self.server.stop)
        self.client = FoundryRestClient("eus2", self.server.endpoint("eus2"), StaticTokenAuth())

    def no_sleep(self, seconds: float) -> None:
        self.fail(f"loopback emulator files are processed at once (asked to sleep {seconds}s)")


try:  # pragma: no cover - pytest marker only
    import pytest

    _live_marker = pytest.mark.integration
except ImportError:  # pragma: no cover

    def _live_marker(cls):  # type: ignore[no-untyped-def]
        return cls


@_live_marker
@unittest.skipUnless(LIVE, "live Foundry contract disabled (BATCH_LIVE_TESTS=1 and endpoint)")
class LiveFoundryContract(FoundryContract, unittest.TestCase):  # pragma: no cover - live only
    can_create_batches = LIVE_BATCH
    deployment = LIVE_DEPLOYMENT
    exact_tie_order = False
    max_pages = 20  # read-only sampling of an existing resource

    def setUp(self) -> None:
        self.clock = SystemClock()
        self.sleep = time.sleep
        from azure.identity import DefaultAzureCredential

        from foundry_batch.foundry.auth import EntraTokenAuth

        auth = EntraTokenAuth(
            DefaultAzureCredential(), "https://cognitiveservices.azure.com/.default"
        )
        self.client = FoundryRestClient("live", LIVE_ENDPOINT, auth)


if __name__ == "__main__":
    unittest.main()
