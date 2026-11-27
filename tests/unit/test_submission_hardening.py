"""Batch submission: input rules, intent lifecycle, discovery, recovery and linking (US-5.1).

Every test states the guarantee it protects. The theme: a submission is never silently lost,
never created twice by us, and a job that is already final when we link it still produces its
downstream event.
"""

import io
import json
import unittest
from contextlib import redirect_stderr, redirect_stdout
from datetime import timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from unittest import mock

from foundry_batch import cli
from foundry_batch.adapters.memory import MemoryQueue, MemoryTableStore
from foundry_batch.composition import build_runtime, memory_adapters
from foundry_batch.control import ControlStore
from foundry_batch.domain.models import JobKey, JobRecord
from foundry_batch.errors import (
    ProviderAmbiguousError,
    ProviderError,
    ProviderNotFoundError,
    ProviderRequestError,
    ProviderRetryableError,
    StoreUnavailableError,
)
from foundry_batch.foundry.files import ProviderFile, wait_until_processed
from foundry_batch.foundry.submission import (
    FILE_PENDING_MAX_AGE_SECONDS,
    MAX_DISCOVERY_PAGES,
    RECOVERY_MIN_AGE_SECONDS,
    SubmissionError,
    SubmissionPending,
    SubmissionService,
    validate_input,
)
from foundry_batch.metrics.queued_tokens import CharacterHeuristicEstimator
from foundry_batch.ports import BatchPage, Query
from foundry_batch.processing.processor import EventProcessor
from foundry_batch.processing.registry import JobRegistry
from foundry_batch.reconcile.sweeper import Reconciler
from tests.support import (
    RESOURCES,
    T0,
    FakeEndpoints,
    FakeProvider,
    FakeProviderFactory,
    ambiguous,
    clock,
    make_env,
    make_settings,
)


def line(
    custom_id: object,
    *,
    model: str = "gpt-batch",
    url: str = "/v1/chat/completions",
    method: str = "POST",
    body: object = None,
) -> bytes:
    payload = {"model": model, "messages": [{"role": "user", "content": "synthetic"}]}
    return json.dumps(
        {
            "custom_id": custom_id,
            "method": method,
            "url": url,
            "body": payload if body is None else body,
        }
    ).encode()


GOOD = b"\n".join([line("1"), line("2")])


class InputRuleTests(unittest.TestCase):
    def rejects(self, content: bytes, message: str, **kwargs: Any) -> None:
        with self.assertRaises(SubmissionError) as ctx:
            validate_input(content, "gpt-batch", **kwargs)
        self.assertIn(message, str(ctx.exception))

    def test_each_rule_is_enforced_on_its_own(self) -> None:
        self.rejects(line(1), "line 1: custom_id is required")  # not a string
        self.rejects(line(""), "line 1: custom_id is required")
        self.rejects(line("1", method="GET"), "line 1: method must be POST")
        self.rejects(line("1", body=[1]), "line 1: method must be POST")
        self.rejects(line("1", url="/v1/embeddings"), "line 1: url must be /chat/completions")
        self.rejects(
            json.dumps(
                {"custom_id": "1", "method": "POST", "body": {"model": "gpt-batch"}}
            ).encode(),
            "url must be",
        )
        self.rejects(line("1", model="other"), "model must be the deployment 'gpt-batch'")

    def test_both_documented_url_forms_match_the_endpoint(self) -> None:
        both = b"\n".join([line("1", url="/chat/completions"), line("2")])
        self.assertEqual(validate_input(both, "gpt-batch").lines, 2)
        embeddings = line("1", url="/v1/embeddings", body={"model": "gpt-batch", "input": "x"})
        self.assertEqual(validate_input(embeddings, "gpt-batch", endpoint="/embeddings").lines, 1)

    def test_blank_lines_are_skipped_but_numbering_is_physical(self) -> None:
        content = b"\n".join([line("1"), b"   ", line("1")])
        self.rejects(content, "line 3: duplicate custom_id")
        self.assertEqual(
            validate_input(b"\n".join([line("1"), b"", line("2")]), "gpt-batch").lines, 2
        )

    def test_size_limits_are_inclusive(self) -> None:
        self.assertEqual(validate_input(GOOD, "gpt-batch", max_lines=2).lines, 2)
        self.rejects(GOOD, "more than 1 requests", max_lines=1)
        self.assertEqual(validate_input(GOOD, "gpt-batch", max_bytes=len(GOOD)).lines, 2)
        self.rejects(GOOD, "larger than", max_bytes=len(GOOD) - 1)


class LifecycleProvider(FakeProvider):
    """Records the stored intent at the moment the provider is called."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.seen_at_create: list[dict[str, Any]] = []
        self.peek: Any = None

    def create_batch(self, *, input_file_id: str, endpoint: str, metadata: Any) -> Any:
        if self.peek:
            self.seen_at_create.append(self.peek())
        return super().create_batch(
            input_file_id=input_file_id, endpoint=endpoint, metadata=metadata
        )


class Harness(unittest.TestCase):
    def setUp(self, settings: Any = None) -> None:
        self.clock = clock()
        self.settings = settings or make_settings()
        self.store = MemoryTableStore()
        self.registry = JobRegistry(self.store, self.settings, self.clock)
        self.control = ControlStore(self.store, "batchcontrol", "test", self.clock)
        self.provider = LifecycleProvider(self.clock)
        self.factory = FakeProviderFactory({"eus2": self.provider, "swc": FakeProvider(self.clock)})
        self.relay = MemoryQueue("relay")
        self.processor = EventProcessor(
            self.settings, self.registry, self.store, self.factory, self.relay, self.clock
        )
        self.estimator = CharacterHeuristicEstimator()
        self.service = self.make_service(with_processor=True)

    def make_service(self, *, with_processor: bool) -> SubmissionService:
        return SubmissionService(
            self.settings,
            self.store,
            self.registry,
            self.control,
            self.factory,
            self.estimator,
            self.clock,
            processor=self.processor if with_processor else None,
        )

    def intents(self) -> dict[str, dict[str, Any]]:
        return {e.row_key: dict(e.data) for e in self.store.query("batchsubmissions", Query())}

    def job(self, batch_id: str) -> JobRecord:
        stored = self.registry.load(JobKey("eus2", batch_id))
        assert stored is not None
        return stored.record

    def outbox(self) -> list[dict[str, Any]]:
        return [
            json.loads(str(e.data["event_json"]))
            for e in self.store.query("batchjobs", Query(row_key_prefix="outbox:"))
        ]

    def stale_intent(self, correlation_id: str, **fields: Any) -> None:
        data = {
            "status": "creating",
            "deployment": "gpt-batch",
            "endpoint": "/chat/completions",
            "token_estimate": 7,
            "tokenizer": "chars-per-4-v1",
            "created_at": "2026-11-20T11:00:00.000000Z",
            **fields,
        }
        self.service._put("eus2", correlation_id, data, create=True)


class SubmitTests(Harness):
    def test_invalid_input_stops_before_any_record_or_upload(self) -> None:
        with self.assertRaises(SubmissionError):
            self.service.submit("eus2", "gpt-batch", line("1", url="/v1/embeddings"))
        with self.assertRaises(SubmissionError) as ctx:  # valid file, unconfigured deployment
            self.service.submit("eus2", "gpt-4o", line("1", model="gpt-4o"))
        self.assertIn("not configured for eus2", str(ctx.exception))
        self.assertEqual(self.intents(), {})
        self.assertEqual((self.provider.calls["upload"], self.provider.calls["create"]), (0, 0))

    def test_resource_without_a_deployment_list_accepts_any_deployment(self) -> None:
        open_resource = [{k: v for k, v in RESOURCES[0].items() if k != "deployments"}]
        self.setUp(make_settings(BATCH_RESOURCES_JSON=json.dumps(open_resource)))
        content = b"\n".join([line("1", model="any-name")])
        self.assertEqual(self.service.submit("eus2", "any-name", content).deployment, "any-name")

    def test_correlation_id_must_be_a_safe_key(self) -> None:
        for bad in ("a/b", "#1", "", "-lead", "x" * 129, "ação"):
            with self.subTest(bad=bad), self.assertRaises(SubmissionError):
                self.service.submit("eus2", "gpt-batch", GOOD, correlation_id=bad or "?")
        self.assertEqual(self.provider.calls["upload"], 0)
        self.assertEqual(
            self.service.submit(
                "eus2", "gpt-batch", GOOD, correlation_id="Run_2026-11.20"
            ).correlation_id,
            "Run_2026-11.20",
        )

    def test_reused_correlation_id_never_creates_a_second_batch(self) -> None:
        self.service.submit("eus2", "gpt-batch", GOOD, correlation_id="c1")
        with self.assertRaises(SubmissionError) as ctx:
            self.service.submit("eus2", "gpt-batch", GOOD, correlation_id="c1")
        self.assertIn("already used (status linked)", str(ctx.exception))
        self.assertEqual((self.provider.calls["upload"], self.provider.calls["create"]), (1, 1))
        self.assertEqual(len(self.provider.batches), 1)

    def test_intent_is_creating_with_its_file_before_the_create_call(self) -> None:
        def current_intent() -> dict[str, Any]:
            return self.intents()["c1"]

        self.provider.peek = current_intent
        job = self.service.submit("eus2", "gpt-batch", GOOD, correlation_id="c1")
        [at_create] = self.provider.seen_at_create
        self.assertEqual(at_create["status"], "creating")
        self.assertEqual(at_create["file_id"], job.input_file_id)
        final = self.intents()["c1"]
        self.assertEqual(
            (final["status"], final["batch_id"], final["file_id"]),
            ("linked", job.batch_id, job.input_file_id),
        )
        self.assertEqual(
            (final["deployment"], final["endpoint"], final["tokenizer"]),
            ("gpt-batch", "/chat/completions", "chars-per-4-v1"),
        )
        self.assertEqual(final["created_at"], "2026-11-20T12:00:00.000000Z")

    def test_linked_record_carries_every_submission_fact(self) -> None:
        job = self.service.submit("eus2", "gpt-batch", GOOD, correlation_id="c1")
        stored = self.job(job.batch_id)
        self.assertEqual(stored, job)
        self.assertEqual(
            (job.region, job.deployment, job.correlation_id, job.raw_status, job.outcome),
            ("eastus2", "gpt-batch", "c1", "validating", None),
        )
        self.assertEqual(job.token_estimate, self.estimator.estimate(GOOD))
        self.assertGreater(job.token_estimate or 0, 0)
        self.assertEqual((job.submitted_at, job.last_status_call_at), (T0, T0))
        self.assertTrue(job.input_file_id and job.input_file_id.startswith("file-"))
        self.assertEqual((job.webhook_covered, job.registration_id), (False, None))
        self.assertTrue(job.trusted)

    def test_definite_rejections_record_the_status_code(self) -> None:
        for error, code in (
            (ProviderRequestError("bad file", status_code=400), 400),
            (ProviderRetryableError("refused before sending"), 0),
        ):
            with self.subTest(code=code):
                self.setUp()
                self.provider.fail("create", error)
                with self.assertLogs("foundry_batch", "ERROR"), self.assertRaises(type(error)):
                    self.service.submit("eus2", "gpt-batch", GOOD, correlation_id="c1")
                self.assertEqual(
                    (self.intents()["c1"]["status"], self.intents()["c1"]["error_status"]),
                    ("rejected", code),
                )

    def test_pending_carries_the_correlation_id(self) -> None:
        self.provider.fail("create", ProviderAmbiguousError("timeout"))
        with self.assertRaises(SubmissionPending) as ctx:
            self.service.submit("eus2", "gpt-batch", GOOD, correlation_id="c9")
        self.assertEqual(ctx.exception.correlation_id, "c9")
        self.assertEqual(ctx.exception.reason, "create_outcome_unknown")
        self.assertIn("c9", str(ctx.exception))

    def test_an_injected_correlation_id_factory_names_new_submissions(self) -> None:
        names = iter(["seq-1", "seq-2"])
        service = SubmissionService(
            self.settings,
            self.store,
            self.registry,
            self.control,
            self.factory,
            self.estimator,
            self.clock,
            correlation_ids=lambda: next(names),
        )
        self.assertEqual(service.submit("eus2", "gpt-batch", GOOD).correlation_id, "seq-1")
        self.assertEqual(service.submit("eus2", "gpt-batch", GOOD).correlation_id, "seq-2")
        self.assertEqual(sorted(self.intents()), ["seq-1", "seq-2"])


class RecoveryTests(Harness):
    def test_crash_before_the_create_call_is_abandoned_not_chased(self) -> None:
        self.provider.fail("upload", ProviderRetryableError("upload failed", status_code=503))
        with self.assertRaises(ProviderRetryableError):
            self.service.submit("eus2", "gpt-batch", GOOD, correlation_id="c1")
        self.assertEqual(self.intents()["c1"]["status"], "intent")
        self.clock.advance(RECOVERY_MIN_AGE_SECONDS - 1)
        self.assertEqual(self.service.recover("eus2"), [])
        self.assertEqual(self.intents()["c1"]["status"], "intent")  # still too young
        self.clock.advance(1)  # the age limit is inclusive
        with self.assertLogs("foundry_batch", "WARNING") as logs:
            self.assertEqual(self.service.recover("eus2"), [])
        self.assertEqual(self.intents()["c1"]["status"], "abandoned")
        self.assertTrue(any("submission.abandoned" in line for line in logs.output))
        self.assertEqual(self.provider.calls["list"], 0)  # no discovery needed

    def test_finished_rows_are_never_touched(self) -> None:
        for status in ("linked", "rejected", "abandoned", "needs_operator"):
            self.stale_intent(f"row-{status}", status=status)
        self.assertEqual(self.service.recover("eus2"), [])
        self.assertEqual(
            {k: v["status"] for k, v in self.intents().items()},
            {f"row-{s}": s for s in ("linked", "rejected", "abandoned", "needs_operator")},
        )
        self.assertEqual(self.provider.calls["list"], 0)

    def test_correlation_metadata_finds_a_batch_when_the_file_id_was_not_recorded(self) -> None:
        file_id = self.provider.upload_file(filename="x", content=GOOD)
        self.provider.create_batch(
            input_file_id=file_id,
            endpoint="/chat/completions",
            metadata={"batch_correlation_id": "c5"},
        )
        self.stale_intent("c5")  # no file_id stored
        self.assertEqual(self.service.recover("eus2"), ["c5"])
        self.assertEqual(self.intents()["c5"]["status"], "linked")

    def test_a_missing_estimate_stays_unknown(self) -> None:
        file_id = self.provider.upload_file(filename="x", content=GOOD)
        self.provider.create_batch(input_file_id=file_id, endpoint="/chat/completions", metadata={})
        self.stale_intent("c10", file_id=file_id, token_estimate=None)
        self.service.recover("eus2")
        [job] = [j.record for j in self.registry.open_jobs()]
        self.assertIsNone(job.token_estimate)  # counted under jobs_without_estimate

    def test_recover_is_idempotent(self) -> None:
        file_id = self.provider.upload_file(filename="x", content=GOOD)
        self.provider.create_batch(input_file_id=file_id, endpoint="/chat/completions", metadata={})
        self.stale_intent("c6", file_id=file_id)
        self.assertEqual(self.service.recover("eus2"), ["c6"])
        self.assertEqual(self.service.recover("eus2"), [])
        self.assertEqual(len(self.registry.open_jobs()), 1)


class DiscoveryTests(Harness):
    def lost_create(self, correlation_id: str = "c1") -> str:
        """An ambiguous create the provider committed; returns our batch id."""
        self.provider.fail("create", ambiguous(ProviderAmbiguousError("timeout")))
        return self.service.submit(
            "eus2", "gpt-batch", GOOD, correlation_id=correlation_id
        ).batch_id

    def test_our_batch_is_found_on_a_later_page(self) -> None:
        self.provider.fail("create", ProviderAmbiguousError("timeout"))  # not committed
        with self.assertRaises(SubmissionPending):
            self.service.submit("eus2", "gpt-batch", GOOD, correlation_id="c1")
        file_id = self.intents()["c1"]["file_id"]
        ours = self.provider.add(input_file_id=file_id)  # committed late, then 250 newer
        for _ in range(250):
            self.provider.add()
        self.clock.advance(RECOVERY_MIN_AGE_SECONDS)
        self.provider.calls["list"] = 0
        self.assertEqual(self.service.recover("eus2"), ["c1"])
        self.assertEqual(self.provider.calls["list"], 3)  # 100 + 100 + 51 items
        self.assertEqual(self.intents()["c1"]["batch_id"], ours)

    def test_discovery_is_bounded_to_the_newest_pages(self) -> None:
        self.provider.fail("create", ProviderAmbiguousError("timeout"))
        with self.assertRaises(SubmissionPending):
            self.service.submit("eus2", "gpt-batch", GOOD, correlation_id="c1")
        self.provider.add(input_file_id=self.intents()["c1"]["file_id"])
        for _ in range(MAX_DISCOVERY_PAGES * 100):
            self.provider.add()
        self.clock.advance(RECOVERY_MIN_AGE_SECONDS)
        self.provider.calls["list"] = 0
        with self.assertLogs("foundry_batch", "ERROR"):
            self.assertEqual(self.service.recover("eus2"), [])
        self.assertEqual(self.provider.calls["list"], MAX_DISCOVERY_PAGES)
        self.assertEqual(self.intents()["c1"]["status"], "needs_operator")

    def test_a_page_without_a_cursor_ends_discovery(self) -> None:
        calls = []

        def broken(*, after: str | None, limit: int) -> BatchPage:
            calls.append(after)
            return BatchPage(items=(), has_more=True, last_id=None)

        self.provider.list_batches = broken  # type: ignore[method-assign]
        self.provider.fail("create", ProviderAmbiguousError("timeout"))
        with self.assertRaises(SubmissionPending):
            self.service.submit("eus2", "gpt-batch", GOOD, correlation_id="c1")
        self.assertEqual(calls, [None])

    def test_duplicates_are_reported_and_the_earliest_wins(self) -> None:
        self.provider.fail("create", ProviderAmbiguousError("timeout"))
        with self.assertRaises(SubmissionPending):
            self.service.submit("eus2", "gpt-batch", GOOD, correlation_id="c1")
        file_id = self.intents()["c1"]["file_id"]
        first = self.provider.add(input_file_id=file_id)
        self.clock.advance(5)
        second = self.provider.add(input_file_id=file_id)
        self.clock.advance(RECOVERY_MIN_AGE_SECONDS)
        with self.assertLogs("foundry_batch", "ERROR") as logs:
            self.service.recover("eus2")
        self.assertEqual(self.intents()["c1"]["batch_id"], first)
        report = next(line for line in logs.output if "duplicate_detected" in line)
        self.assertIn(f"batch_ids={','.join(sorted([first, second]))}", report)

    def test_the_same_batch_on_two_pages_is_not_a_duplicate(self) -> None:
        real = self.provider.list_batches

        def overlapping(*, after: str | None, limit: int) -> BatchPage:
            page = real(after=after, limit=limit)
            if after is None:
                return BatchPage(page.items, True, page.last_id)
            first = real(after=None, limit=limit)
            return BatchPage(first.items, False, None)  # the provider repeats page one

        self.provider.list_batches = overlapping  # type: ignore[method-assign]
        with self.assertNoLogs("foundry_batch.foundry.submission", "ERROR"):
            self.lost_create()


class DiscoveryFailureTests(Harness):
    """R2: after an ambiguous create, a failed list proves nothing: pending, never a refusal."""

    def test_a_failed_discovery_after_an_ambiguous_create_is_pending(self) -> None:
        for error in (
            ProviderRetryableError("batch.list: HTTP 429", status_code=429),
            ProviderRetryableError("batch.list: ConnectionError"),
            ProviderRequestError("batch.list: HTTP 403", status_code=403),
        ):
            with self.subTest(error=str(error)):
                self.setUp()
                self.provider.fail("create", ambiguous(ProviderAmbiguousError("timeout")))
                self.provider.fail("list", error)
                with (
                    self.assertLogs("foundry_batch", "WARNING") as logs,
                    self.assertRaises(SubmissionPending) as ctx,
                ):
                    self.service.submit("eus2", "gpt-batch", GOOD, correlation_id="c1")
                self.assertEqual(ctx.exception.reason, "discovery_failed")
                self.assertIsInstance(ctx.exception.__cause__, ProviderError)
                self.assertEqual(self.intents()["c1"]["status"], "ambiguous")
                self.assertTrue(any("submission.discovery_failed" in x for x in logs.output))
                [batch_id] = self.provider.batches  # the create had committed
                self.clock.advance(RECOVERY_MIN_AGE_SECONDS)
                self.assertEqual(self.service.recover("eus2"), ["c1"])
                self.assertEqual(self.intents()["c1"]["batch_id"], batch_id)
                self.assertEqual(self.provider.calls["create"], 1)

    def test_recovery_defers_a_row_whose_discovery_fails_and_continues(self) -> None:
        for correlation_id in ("c1", "c2"):
            file_id = self.provider.upload_file(filename="x", content=GOOD)
            self.provider.create_batch(input_file_id=file_id, endpoint="/x", metadata={})
            self.stale_intent(correlation_id, file_id=file_id)
        self.provider.fail("list", ProviderRetryableError("throttled", status_code=429))
        with self.assertLogs("foundry_batch", "WARNING") as logs:
            self.assertEqual(self.service.recover("eus2"), ["c2"])
        self.assertEqual(self.intents()["c1"]["status"], "creating")  # left for the next run
        self.assertTrue(any("submission.recovery_deferred" in x for x in logs.output))
        self.assertEqual(self.service.recover("eus2"), ["c1"])

    def test_a_link_failure_after_the_create_is_pending_not_a_refusal(self) -> None:
        with (
            mock.patch.object(
                self.registry, "create_job", side_effect=StoreUnavailableError("tables down")
            ),
            self.assertLogs("foundry_batch", "WARNING") as logs,
            self.assertRaises(SubmissionPending) as ctx,
        ):
            self.service.submit("eus2", "gpt-batch", GOOD, correlation_id="c1")
        self.assertEqual(ctx.exception.reason, "link_failed")
        self.assertTrue(any("submission.link_failed" in x for x in logs.output))
        self.assertEqual(self.intents()["c1"]["status"], "creating")
        self.clock.advance(RECOVERY_MIN_AGE_SECONDS)
        self.assertEqual(self.service.recover("eus2"), ["c1"])
        self.assertEqual(len(self.registry.open_jobs()), 1)
        self.assertEqual(self.provider.calls["create"], 1)


class FileProvider(LifecycleProvider):
    """Adds the Files status API: one scripted status (or error) per read, the last repeating."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.file_statuses: list[str | Exception] = ["processed"]
        self.deleted: list[str] = []
        self.delete_error: Exception | None = None
        self.on_retrieve: Any = None

    def retrieve_file(self, file_id: str) -> ProviderFile:
        self.calls["retrieve_file"] += 1
        if self.on_retrieve:
            self.on_retrieve()
        if file_id not in self.files:
            raise ProviderNotFoundError("files.retrieve: HTTP 404", status_code=404)
        item = self.file_statuses.pop(0) if len(self.file_statuses) > 1 else self.file_statuses[0]
        if isinstance(item, Exception):
            raise item
        return ProviderFile(file_id, item)

    def delete_file(self, file_id: str) -> None:
        self.calls["delete_file"] += 1
        if self.delete_error is not None:
            raise self.delete_error
        self.deleted.append(file_id)
        self.files.pop(file_id, None)


class FileHarness(Harness):
    def setUp(self, settings: Any = None) -> None:
        self.sleeps: list[float] = []
        super().setUp(settings)
        self.provider = FileProvider(self.clock)
        self.factory.providers["eus2"] = self.provider
        self.service = self.make_service(with_processor=True)

    def make_service(self, *, with_processor: bool) -> SubmissionService:
        return SubmissionService(
            self.settings,
            self.store,
            self.registry,
            self.control,
            self.factory,
            self.estimator,
            self.clock,
            processor=self.processor if with_processor else None,
            sleep=self.sleep,
            file_wait_timeout_s=60,
            file_poll_s=15,
        )

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.clock.advance(seconds)

    def uploaded_intent(self, correlation_id: str, *statuses: str | Exception) -> str:
        file_id = self.provider.upload_file(filename="x", content=GOOD)
        self.stale_intent(correlation_id, status="uploaded", file_id=file_id)
        self.provider.file_statuses = list(statuses)
        return file_id


class FileReadinessTests(FileHarness):
    """E2: the file is tracked until ``processed`` before create; anything else never creates."""

    def test_create_waits_until_the_file_is_processed(self) -> None:
        self.provider.file_statuses = ["pending", "pending", "processed"]
        while_waiting: list[str] = []
        self.provider.on_retrieve = lambda: while_waiting.append(self.intents()["c1"]["status"])
        self.provider.peek = lambda: self.intents()["c1"]["status"]
        job = self.service.submit("eus2", "gpt-batch", GOOD, correlation_id="c1")
        self.assertEqual(self.sleeps, [15, 15])
        self.assertEqual(while_waiting, ["uploaded"] * 3)
        self.assertEqual(self.provider.seen_at_create, ["creating"])
        self.assertEqual(self.intents()["c1"]["status"], "linked")
        self.assertEqual(job.submitted_at, T0 + timedelta(seconds=30))

    def test_transient_status_errors_keep_polling(self) -> None:
        self.provider.file_statuses = [ProviderRetryableError("429", status_code=429), "processed"]
        self.service.submit("eus2", "gpt-batch", GOOD, correlation_id="c1")
        self.assertEqual(
            (self.provider.calls["retrieve_file"], self.provider.calls["create"]), (2, 1)
        )

    def test_an_unusable_file_is_rejected_with_its_reason_deleted_and_never_created(self) -> None:
        cases: list[tuple[str | Exception, str]] = [
            ("error", "file_error"),
            (
                ProviderRequestError("files.retrieve: HTTP 403", status_code=403),
                "file_unreadable_403",
            ),
        ]
        for status, reason in cases:
            with self.subTest(reason=reason):
                self.setUp()
                self.provider.file_statuses = ["pending", status]
                with self.assertLogs("foundry_batch", "ERROR"), self.assertRaises(SubmissionError):
                    self.service.submit("eus2", "gpt-batch", GOOD, correlation_id="c1")
                intent = self.intents()["c1"]
                self.assertEqual(
                    (intent["status"], intent["reason"], intent["error_status"]),
                    ("rejected", reason, 0),
                )
                self.assertEqual(self.provider.deleted, [intent["file_id"]])
                self.assertTrue(intent["file_deleted"])
                self.assertEqual(self.provider.calls["create"], 0)

    def test_a_vanished_file_is_rejected(self) -> None:
        self.provider.on_retrieve = lambda: self.provider.files.clear()
        with self.assertLogs("foundry_batch", "ERROR"), self.assertRaises(SubmissionError) as ctx:
            self.service.submit("eus2", "gpt-batch", GOOD, correlation_id="c1")
        self.assertIn("'not_found'", str(ctx.exception))
        self.assertEqual(self.intents()["c1"]["reason"], "file_not_found")

    def test_a_slow_file_is_pending_without_a_create_and_recovery_resumes_it(self) -> None:
        self.provider.file_statuses = ["pending"]
        with (
            self.assertLogs("foundry_batch", "WARNING") as logs,
            self.assertRaises(SubmissionPending) as ctx,
        ):
            self.service.submit("eus2", "gpt-batch", GOOD, correlation_id="c1")
        self.assertEqual(ctx.exception.reason, "file_not_processed")
        self.assertTrue(any("submission.file_pending" in x for x in logs.output))
        self.assertEqual(self.sleeps, [15, 15, 15, 15])  # bounded by the 60 s timeout
        self.assertEqual(self.provider.calls["retrieve_file"], 5)
        self.assertEqual(self.intents()["c1"]["status"], "uploaded")
        self.assertEqual(self.provider.calls["create"], 0)
        self.assertEqual(self.service.recover("eus2"), [])  # still too young
        self.clock.advance(RECOVERY_MIN_AGE_SECONDS)
        self.assertEqual(self.service.recover("eus2"), [])  # still pending: left alone
        self.assertEqual(self.intents()["c1"]["status"], "uploaded")
        self.provider.file_statuses = ["processed"]
        self.assertEqual(self.service.recover("eus2"), ["c1"])
        self.assertEqual(self.intents()["c1"]["status"], "linked")
        self.assertEqual(self.provider.calls["create"], 1)
        self.assertEqual(self.service.recover("eus2"), [])

    def test_recovery_rejects_an_uploaded_intent_whose_file_failed(self) -> None:
        file_id = self.uploaded_intent("c1", "error")
        with self.assertLogs("foundry_batch", "ERROR"):
            self.assertEqual(self.service.recover("eus2"), [])
        self.assertEqual(
            (self.intents()["c1"]["status"], self.intents()["c1"]["reason"]),
            ("rejected", "file_error"),
        )
        self.assertEqual(self.provider.deleted, [file_id])

    def test_recovery_abandons_a_file_still_pending_after_a_day_and_deletes_it(self) -> None:
        file_id = self.uploaded_intent("c1", "pending")  # created one hour before T0
        self.clock.advance(FILE_PENDING_MAX_AGE_SECONDS - 3600 - 1)
        self.service.recover("eus2")
        self.assertEqual(self.intents()["c1"]["status"], "uploaded")
        self.clock.advance(1)
        with self.assertLogs("foundry_batch", "WARNING") as logs:
            self.service.recover("eus2")
        intent = self.intents()["c1"]
        self.assertEqual((intent["status"], intent["file_deleted"]), ("abandoned", True))
        self.assertEqual(self.provider.deleted, [file_id])
        self.assertTrue(any("submission.abandoned" in x for x in logs.output))
        self.assertEqual(self.provider.calls["create"], 0)

    def test_a_resumed_create_that_is_refused_or_ambiguous_is_recorded(self) -> None:
        self.uploaded_intent("c1", "processed")
        self.provider.fail("create", ProviderRequestError("bad", status_code=400))
        with self.assertLogs("foundry_batch", "ERROR"):
            self.assertEqual(self.service.recover("eus2"), [])
        self.assertEqual(self.intents()["c1"]["status"], "rejected")
        self.uploaded_intent("c2", "processed")
        self.provider.fail("create", ProviderAmbiguousError("timeout"))
        self.assertEqual(self.service.recover("eus2"), [])
        self.assertEqual(self.intents()["c2"]["status"], "ambiguous")

    def test_the_create_claim_is_conditional_so_two_processes_never_both_create(self) -> None:
        def another_process_takes_over() -> None:
            intent = self.intents()["c1"]
            self.service._put("eus2", "c1", {**intent, "status": "creating"})

        self.provider.on_retrieve = another_process_takes_over
        with (
            self.assertLogs("foundry_batch", "WARNING"),
            self.assertRaises(SubmissionPending) as ctx,
        ):
            self.service.submit("eus2", "gpt-batch", GOOD, correlation_id="c1")
        self.assertEqual(ctx.exception.reason, "claimed_elsewhere")
        self.assertEqual(self.provider.calls["create"], 0)


class FileWaitTests(unittest.TestCase):
    def test_polls_are_bounded_even_if_sleep_does_not_move_the_clock(self) -> None:
        provider = FileProvider(clock())
        file_id = provider.upload_file(filename="x", content=GOOD)
        provider.file_statuses = ["pending"]
        wait = wait_until_processed(
            provider, file_id, clock(), lambda _: None, timeout_s=60, poll_s=15
        )
        self.assertEqual((wait.outcome, wait.status, wait.polls), ("timeout", "pending", 5))
        self.assertEqual(provider.calls["retrieve_file"], 5)

    def test_unknown_statuses_keep_waiting_and_bad_intervals_are_refused(self) -> None:
        manual = clock()
        provider = FileProvider(manual)
        file_id = provider.upload_file(filename="x", content=GOOD)
        provider.file_statuses = ["uploaded", "running", "processed"]
        wait = wait_until_processed(provider, file_id, manual, manual.advance, poll_s=1)
        self.assertEqual((wait.outcome, wait.polls), ("processed", 3))
        for timeout_s, poll_s in ((60, 0), (-1, 15)):
            with self.subTest(timeout_s=timeout_s, poll_s=poll_s), self.assertRaises(ValueError):
                wait_until_processed(
                    provider, file_id, manual, manual.advance, timeout_s=timeout_s, poll_s=poll_s
                )


class InputFileCleanupTests(FileHarness):
    """R7: a submission that ends ``rejected`` or ``abandoned`` deletes its input file."""

    def test_a_refused_create_deletes_the_input_file(self) -> None:
        self.provider.fail("create", ProviderRequestError("bad", status_code=400))
        with self.assertLogs("foundry_batch", "ERROR"), self.assertRaises(ProviderRequestError):
            self.service.submit("eus2", "gpt-batch", GOOD, correlation_id="c1")
        intent = self.intents()["c1"]
        self.assertEqual((intent["status"], intent["file_deleted"]), ("rejected", True))
        self.assertEqual(self.provider.deleted, [intent["file_id"]])

    def test_a_failed_delete_is_logged_and_does_not_change_the_outcome(self) -> None:
        self.provider.delete_error = ProviderRetryableError(
            "files.delete: HTTP 503", status_code=503
        )
        self.provider.fail("create", ProviderRequestError("bad", status_code=400))
        with (
            self.assertLogs("foundry_batch", "WARNING") as logs,
            self.assertRaises(ProviderRequestError),
        ):
            self.service.submit("eus2", "gpt-batch", GOOD, correlation_id="c1")
        intent = self.intents()["c1"]
        self.assertEqual(intent["status"], "rejected")
        self.assertNotIn("file_deleted", intent)
        self.assertTrue(any("submission.file_delete_failed" in x for x in logs.output))

    def test_linked_and_pending_submissions_keep_their_file(self) -> None:
        self.service.submit("eus2", "gpt-batch", GOOD, correlation_id="c1")
        self.provider.fail("create", ProviderAmbiguousError("timeout"))
        with self.assertRaises(SubmissionPending):
            self.service.submit("eus2", "gpt-batch", GOOD, correlation_id="c2")
        self.assertEqual(self.provider.deleted, [])


class FinalOnLinkTests(Harness):
    def batch_failed_fast(self, *, with_processor: bool) -> str:
        """The create response was lost and the batch failed validation meanwhile."""
        self.service = self.make_service(with_processor=with_processor)
        self.provider.fail("create", ambiguous(ProviderAmbiguousError("timeout")))
        real_list = self.provider.list_batches

        def list_after_failure(*, after: str | None, limit: int) -> BatchPage:
            for batch_id in self.provider.order:
                if self.provider.batches[batch_id]["status"] == "validating":
                    self.provider.set_status(batch_id, "failed")
            return real_list(after=after, limit=limit)

        self.provider.list_batches = list_after_failure  # type: ignore[method-assign]
        return self.service.submit("eus2", "gpt-batch", GOOD, correlation_id="c1").batch_id

    def test_a_job_final_at_link_time_emits_its_event_immediately(self) -> None:
        batch_id = self.batch_failed_fast(with_processor=True)
        job = self.job(batch_id)
        self.assertEqual(
            (job.raw_status, job.outcome, job.terminal_source), ("failed", "failed", "submission")
        )
        [event] = self.outbox()
        self.assertEqual(
            (event["outcome"], event["source"], event["correlation_id"]),
            ("failed", "submission", "c1"),
        )
        self.assertEqual(len(self.relay.sent), 1)  # relay nudged
        receipts = [e.row_key for e in self.registry.receipts(JobKey("eus2", batch_id))]
        self.assertEqual(receipts, ["rcpt:submission:failed"])

    def test_without_a_processor_the_reconciler_emits_it(self) -> None:
        batch_id = self.batch_failed_fast(with_processor=False)
        job = self.job(batch_id)
        self.assertIsNone(job.outcome)  # registered open, never silently final
        self.assertEqual(self.outbox(), [])
        reconciler = Reconciler(
            self.settings,
            self.registry,
            self.processor,
            self.control,
            self.factory,
            self.clock,
            holder="t",
        )
        self.clock.advance(120)
        self.assertEqual(reconciler.run("eus2").terminal_found, 1)
        self.assertEqual([e["outcome"] for e in self.outbox()], ["failed"])


class ExistingJobTests(Harness):
    def test_an_adopted_job_gains_the_submission_facts_without_losing_its_own(self) -> None:
        file_id = self.provider.upload_file(filename="x", content=GOOD)
        snap = self.provider.create_batch(
            input_file_id=file_id,
            endpoint="/chat/completions",
            metadata={"batch_correlation_id": "c7"},
        )
        self.registry.create_job(
            JobRecord(
                "eus2",
                snap.batch_id,
                "test",
                raw_status="validating",
                deployment="seen-by-webhook",
                trusted=False,
            )
        )
        self.stale_intent("c7", file_id=file_id)
        with self.assertLogs("foundry_batch", "WARNING") as logs:
            self.assertEqual(self.service.recover("eus2"), ["c7"])
        job = self.job(snap.batch_id)
        self.assertEqual(
            (job.correlation_id, job.token_estimate, job.input_file_id), ("c7", 7, file_id)
        )
        self.assertEqual(job.deployment, "seen-by-webhook")  # never overwritten
        self.assertTrue(job.trusted)
        self.assertTrue(any("submission.linked_existing" in line for line in logs.output))

    def test_a_complete_existing_job_is_left_unchanged(self) -> None:
        batch_id = self.service.submit("eus2", "gpt-batch", GOOD, correlation_id="c8").batch_id
        before = self.registry.load(JobKey("eus2", batch_id))
        self.service._put(
            "eus2",
            "c8",
            {
                **self.intents()["c8"],
                "status": "creating",
                "created_at": "2026-11-20T11:00:00.000000Z",
            },
        )
        self.assertEqual(self.service.recover("eus2"), ["c8"])
        after = self.registry.load(JobKey("eus2", batch_id))
        assert before is not None and after is not None
        self.assertEqual(before.etag, after.etag)


class SubmitCliTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = clock()
        self.provider = FakeProvider(self.clock)
        self.runtime = build_runtime(
            make_settings(),
            memory_adapters(
                FakeProviderFactory({"eus2": self.provider, "swc": FakeProvider(self.clock)}),
                lambda _: FakeEndpoints(),
            ),
            self.clock,
        )
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.settings_file = Path(self.tmp.name, "s.json")
        self.settings_file.write_text(json.dumps(make_env()), encoding="utf-8")
        self.input = Path(self.tmp.name, "in.jsonl")
        self.input.write_bytes(GOOD)

    def run_cli(self, *argv: str) -> tuple[int, Any]:
        out = io.StringIO()
        with redirect_stdout(out), redirect_stderr(io.StringIO()):
            code = cli.main(
                ["--settings", str(self.settings_file), *argv],
                lambda s, a: self.runtime,
                configure_logging=False,
            )
        return code, json.loads(out.getvalue())

    def submit(self, *extra: str) -> tuple[int, Any]:
        return self.run_cli(
            "submit",
            "--resource",
            "eus2",
            "--deployment",
            "gpt-batch",
            "--file",
            str(self.input),
            *extra,
        )

    def test_exit_codes_and_outputs(self) -> None:
        code, out = self.submit("--correlation-id", "cli-1")
        self.assertEqual((code, out["correlation_id"], out["status"]), (0, "cli-1", "validating"))
        code, out = self.submit("--correlation-id", "cli-1")
        self.assertEqual((code, out["status"]), (1, "error"))
        self.assertIn("already used", out["error"])
        self.provider.fail("create", ProviderAmbiguousError("timeout"))
        code, out = self.submit("--correlation-id", "cli-2")
        self.assertEqual(
            (code, out),
            (
                3,
                {
                    "status": "pending",
                    "correlation_id": "cli-2",
                    "reason": "create_outcome_unknown",
                    "next_step": "recover-submissions",
                },
            ),
        )
        self.clock.advance(120)
        code, out = self.run_cli(
            "recover-submissions", "--resource", "eus2", "--min-age-seconds", "60"
        )
        self.assertEqual((code, out), (0, {"resolved": []}))  # nothing committed: operator
        self.provider.fail("create", ProviderRequestError("bad", status_code=400))
        code, out = self.submit("--correlation-id", "cli-3")
        self.assertEqual((code, out["status"]), (1, "error"))

    def test_a_failed_discovery_after_an_ambiguous_create_exits_3_not_1(self) -> None:
        self.provider.fail("create", ambiguous(ProviderAmbiguousError("batch.create: ReadTimeout")))
        self.provider.fail("list", ProviderRetryableError("batch.list: HTTP 429", status_code=429))
        code, out = self.submit("--correlation-id", "cli-x")
        self.assertEqual((code, out["status"], out["reason"]), (3, "pending", "discovery_failed"))
        intent = self.runtime.adapters.store.get(
            self.runtime.settings.table_submissions, "test|eus2", "cli-x"
        )
        assert intent is not None
        self.assertEqual(intent.data["status"], "ambiguous")
        self.clock.advance(120)
        code, out = self.run_cli(
            "recover-submissions", "--resource", "eus2", "--min-age-seconds", "60"
        )
        self.assertEqual((code, out), (0, {"resolved": ["cli-x"]}))
        self.assertEqual(len(self.provider.batches), 1)


if __name__ == "__main__":
    unittest.main()
