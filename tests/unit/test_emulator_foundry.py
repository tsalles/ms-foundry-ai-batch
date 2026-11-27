"""The Foundry emulator itself: documented behaviour, assumptions and fault injection."""

import json
import unittest
from datetime import timedelta

from foundry_batch.emulator.foundry import (
    ChaosRule,
    DeliveryPolicy,
    Fault,
    FoundryEmulator,
    LifecyclePlan,
    WebhookCall,
    WireRequest,
)
from foundry_batch.webhook.signature import verify
from tests.support import clock

AUTH = {"authorization": "Bearer t"}


def line(
    n: int = 1, model: str = "gpt-batch", url: str = "/v1/chat/completions", cid: str | None = None
) -> str:
    return json.dumps(
        {
            "custom_id": cid or f"r{n}",
            "method": "POST",
            "url": url,
            "body": {"model": model, "messages": [{"role": "user", "content": "x" * 40}]},
        }
    )


class Harness(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = clock()
        self.sent: list[WebhookCall] = []
        self.reply: int | None = 200
        self.emu = FoundryEmulator("eus2", self.clock, transport=self.transport)

    def transport(self, call: WebhookCall) -> int | None:
        self.sent.append(call)
        return self.reply

    def call(
        self, method: str, path: str, body: object = None, headers: dict | None = None, **query: str
    ):  # type: ignore[no-untyped-def]
        raw = json.dumps(body).encode() if body is not None else b""
        return self.emu.handle(
            WireRequest(method, path, query, AUTH if headers is None else headers, raw)
        )

    def upload(self, content: bytes) -> str:
        boundary = "BOUNDARY"
        body = (
            (
                f'--{boundary}\r\nContent-Disposition: form-data; name="purpose"\r\n\r\nbatch\r\n'
                f'--{boundary}\r\nContent-Disposition: form-data; name="file"; filename="in.jsonl"\r\n'
                "Content-Type: application/jsonl\r\n\r\n"
            ).encode()
            + content
            + f"\r\n--{boundary}--\r\n".encode()
        )
        headers = {**AUTH, "content-type": f"multipart/form-data; boundary={boundary}"}
        response = self.emu.handle(WireRequest("POST", "files", {}, headers, body))
        self.assertEqual(response.status, 200, response.body)
        return json.loads(response.body)["id"]

    def create(self, content: bytes | None = None, **metadata: str) -> dict:
        file_id = self.upload(content if content is not None else (line() + "\n").encode())
        response = self.call(
            "POST",
            "batches",
            {
                "input_file_id": file_id,
                "endpoint": "/v1/chat/completions",
                "completion_window": "24h",
                "metadata": metadata,
            },
        )
        self.assertEqual(response.status, 200, response.body)
        return json.loads(response.body)

    def status(self, batch_id: str) -> dict:
        return json.loads(self.call("GET", f"batches/{batch_id}").body)

    def endpoint(self, url: str = "https://h.example/w", events: list[str] | None = None) -> dict:
        body = {
            "name": "n",
            "url": url,
            "event_types": events
            or ["batch.completed", "batch.failed", "batch.cancelled", "batch.expired"],
        }
        response = self.call("POST", "dashboard/webhook_endpoints", body)
        self.assertEqual(response.status, 200, response.body)
        return json.loads(response.body)


class LifecycleTests(Harness):
    def test_completed_lifecycle_has_documented_timestamps_counts_and_usage(self) -> None:
        batch = self.create(k="v")
        self.assertEqual((batch["status"], batch["request_counts"]["total"]), ("validating", 1))
        self.assertEqual(batch["metadata"], {"k": "v"})
        self.clock.advance(31)
        self.assertEqual(self.status(batch["id"])["status"], "in_progress")
        self.clock.advance(121)
        self.assertEqual(self.status(batch["id"])["status"], "finalizing")
        self.clock.advance(31)
        done = self.status(batch["id"])
        self.assertEqual(done["status"], "completed")
        for stamp in ("in_progress_at", "finalizing_at", "completed_at"):
            self.assertIsInstance(done[stamp], int)
        self.assertEqual(done["request_counts"], {"total": 1, "completed": 1, "failed": 0})
        self.assertGreater(done["usage"]["input_tokens"], 0)
        self.assertTrue(done["output_file_id"])
        self.assertIsNone(done["error_file_id"])

    def test_plans_for_failed_expired_cancelled_and_partial_jobs(self) -> None:
        self.emu.plan_next(
            LifecyclePlan(outcome="failed"),
            LifecyclePlan(outcome="expired"),
            LifecyclePlan(outcome="cancelled"),
            LifecyclePlan(failed_requests=1, omit_in_progress_at=True),
        )
        ids = [self.create((line(1) + "\n" + line(2) + "\n").encode())["id"] for _ in range(4)]
        self.clock.advance(3600)
        got = [self.status(i) for i in ids]
        self.assertEqual(
            [g["status"] for g in got], ["failed", "expired", "cancelled", "completed"]
        )
        self.assertIsNotNone(got[2]["cancelling_at"])
        self.assertEqual(got[3]["request_counts"], {"total": 2, "completed": 1, "failed": 1})
        self.assertTrue(got[3]["error_file_id"])
        self.assertIsNone(got[3]["in_progress_at"])  # CON-B05 sample
        self.assertEqual(got[1]["request_counts"]["completed"], 1)  # partial results

    def test_cancel_moves_through_cancelling_and_refuses_terminal_jobs(self) -> None:
        batch_id = self.create()["id"]
        response = self.call("POST", f"batches/{batch_id}/cancel")
        self.assertEqual(json.loads(response.body)["status"], "cancelling")
        self.assertEqual(self.call("POST", f"batches/{batch_id}/cancel").status, 200)  # idempotent
        self.clock.advance(61)
        self.assertEqual(self.status(batch_id)["status"], "cancelled")
        self.assertEqual(self.call("POST", f"batches/{batch_id}/cancel").status, 409)
        self.assertEqual(self.call("POST", "batches/missing/cancel").status, 404)

    def test_validation_codes_mark_the_job_failed(self) -> None:
        cases = {
            b"": "empty_file",
            b"{not json\n": "invalid_json_line",
            b"[1]\n": "invalid_request",
            (line(1, cid="a") + "\n" + line(2, cid="a")).encode(): "duplicate_custom_id",
            line(url="/v1/embeddings").encode(): "url_mismatch",
            line(model="other").encode(): "model_not_found",
        }
        for content, code in cases.items():
            with self.subTest(code=code):
                batch_id = self.create(content)["id"]
                self.clock.advance(31)
                got = self.status(batch_id)
                self.assertEqual(got["status"], "failed")
                self.assertIn(code, [e["code"] for e in got["errors"]["data"]])

    def test_mixed_models_and_too_many_tasks(self) -> None:
        emu = FoundryEmulator("eus2", self.clock, deployments=("a", "b"))
        self.emu = emu
        mixed = self.create((line(1, model="a") + "\n" + line(2, model="b")).encode())
        self.clock.advance(31)
        self.assertIn(
            "model_mismatch", [e["code"] for e in self.status(mixed["id"])["errors"]["data"]]
        )
        from foundry_batch.emulator import foundry

        original = foundry.MAX_REQUESTS_PER_FILE
        foundry.MAX_REQUESTS_PER_FILE = 1
        try:
            many = self.create((line(1, model="a") + "\n" + line(2, model="a")).encode())
        finally:
            foundry.MAX_REQUESTS_PER_FILE = original
        self.clock.advance(31)
        self.assertEqual(self.status(many["id"])["errors"]["data"][0]["code"], "too_many_tasks")

    def test_create_rejects_bad_requests(self) -> None:
        file_id = self.upload((line() + "\n").encode())
        good = {
            "input_file_id": file_id,
            "endpoint": "/v1/chat/completions",
            "completion_window": "24h",
        }
        for change, code in (
            ({"input_file_id": "nope"}, "invalid_input_file"),
            ({"completion_window": "48h"}, "invalid_completion_window"),
            ({"endpoint": "chat"}, "invalid_endpoint"),
            ({"metadata": {str(i): "v" for i in range(17)}}, "invalid_metadata"),
        ):
            with self.subTest(code=code):
                response = self.call("POST", "batches", {**good, **change})
                self.assertEqual(
                    (response.status, json.loads(response.body)["error"]["code"]), (400, code)
                )
        bad_json = self.emu.handle(WireRequest("POST", "batches", {}, AUTH, b"{"))
        self.assertEqual(bad_json.status, 400)
        not_object = self.emu.handle(WireRequest("POST", "batches", {}, AUTH, b"[]"))
        self.assertEqual(not_object.status, 400)

    def test_list_is_newest_first_with_cursor_and_can_hide_jobs(self) -> None:
        ids = []
        for _ in range(3):
            ids.append(self.create()["id"])
            self.clock.advance(1)
        page = json.loads(self.call("GET", "batches", limit="2").body)
        self.assertEqual([b["id"] for b in page["data"]], [ids[2], ids[1]])
        self.assertTrue(page["has_more"])
        rest = json.loads(self.call("GET", "batches", limit="2", after=page["last_id"]).body)
        self.assertEqual(([b["id"] for b in rest["data"]], rest["has_more"]), ([ids[0]], False))
        self.emu.hide_from_list(ids[0])
        self.assertNotIn(
            ids[0], [b["id"] for b in json.loads(self.call("GET", "batches").body)["data"]]
        )
        self.assertEqual(self.call("GET", "batches", limit="x").status, 400)
        self.assertEqual(self.call("GET", "batches/nope").status, 404)
        self.assertEqual(sorted(self.emu.batch_ids()), sorted(ids))
        self.assertEqual(self.emu.batch(ids[0])["id"], ids[0])

    def test_without_auto_advance_time_moves_only_on_tick(self) -> None:
        self.emu.auto_advance = False
        batch_id = self.create()["id"]
        self.clock.advance(3600)
        self.assertEqual(self.status(batch_id)["status"], "validating")
        self.assertEqual(self.emu.advance(), [batch_id])


class FileTests(Harness):
    def test_upload_errors(self) -> None:
        plain = self.emu.handle(WireRequest("POST", "files", {}, AUTH, b"x"))
        self.assertEqual(json.loads(plain.body)["error"]["code"], "expected_multipart")
        boundary = "B"
        headers = {**AUTH, "content-type": f"multipart/form-data; boundary={boundary}"}
        no_file = (
            f'--{boundary}\r\nContent-Disposition: form-data; name="purpose"\r\n\r\nbatch\r\n--{boundary}--\r\n'
        ).encode()
        response = self.emu.handle(WireRequest("POST", "files", {}, headers, no_file))
        self.assertEqual(json.loads(response.body)["error"]["code"], "missing_file")
        wrong = no_file.replace(b"batch", b"fine-tune")
        response = self.emu.handle(WireRequest("POST", "files", {}, headers, wrong))
        self.assertEqual(json.loads(response.body)["error"]["code"], "invalid_purpose")
        nameless = (
            f"--{boundary}\r\nContent-Disposition: form-data\r\n\r\nx\r\n--{boundary}--\r\n"
        ).encode()
        response = self.emu.handle(WireRequest("POST", "files", {}, headers, nameless))
        self.assertEqual(json.loads(response.body)["error"]["code"], "invalid_purpose")

    def test_file_limit(self) -> None:
        from foundry_batch.emulator import foundry

        original = foundry.MAX_INPUT_FILES
        foundry.MAX_INPUT_FILES = 1
        try:
            self.upload(b"x")
            boundary = "B"
            headers = {**AUTH, "content-type": f"multipart/form-data; boundary={boundary}"}
            response = self.emu.handle(WireRequest("POST", "files", {}, headers, b""))
            self.assertEqual(json.loads(response.body)["error"]["code"], "too_many_files")
        finally:
            foundry.MAX_INPUT_FILES = original


class FileStatusTests(Harness):
    """Uploaded files are tracked until ``processed`` (Learn); create refuses anything else."""

    def multipart(self, content: bytes, **extra: str) -> WireRequest:
        boundary = "BOUNDARY"
        fields = {"purpose": "batch", **extra}
        parts = "".join(
            f'--{boundary}\r\nContent-Disposition: form-data; name="{k}"\r\n\r\n{v}\r\n'
            for k, v in fields.items()
        )
        body = (
            (
                parts + f'--{boundary}\r\nContent-Disposition: form-data; name="file"; '
                'filename="in.jsonl"\r\nContent-Type: application/jsonl\r\n\r\n'
            ).encode()
            + content
            + f"\r\n--{boundary}--\r\n".encode()
        )
        headers = {**AUTH, "content-type": f"multipart/form-data; boundary={boundary}"}
        return WireRequest("POST", "files", {}, headers, body)

    def create_from(self, file_id: str):  # type: ignore[no-untyped-def]
        return self.call(
            "POST",
            "batches",
            {
                "input_file_id": file_id,
                "endpoint": "/v1/chat/completions",
                "completion_window": "24h",
            },
        )

    def test_file_is_pending_until_the_processing_delay_and_create_waits_for_it(self) -> None:
        self.emu.file_processing_s = 20
        file_id = self.upload((line() + "\n").encode())
        self.assertEqual(json.loads(self.call("GET", f"files/{file_id}").body)["status"], "pending")
        refused = self.create_from(file_id)
        error = json.loads(refused.body)["error"]
        self.assertEqual((refused.status, error["code"]), (400, "invalid_request"))
        self.assertIn("Only files with status 'Processed' are accepted", error["message"])
        self.assertEqual(self.emu.batch_ids(), [])
        self.clock.advance(19)
        self.assertEqual(self.emu.file(file_id)["status"], "pending")
        self.clock.advance(1)
        got = json.loads(self.call("GET", f"files/{file_id}").body)
        self.assertEqual(
            (got["id"], got["status"], got["purpose"]), (file_id, "processed", "batch")
        )
        self.assertEqual(self.create_from(file_id).status, 200)

    def test_a_file_planned_to_fail_ends_in_error_and_cannot_be_used(self) -> None:
        self.emu.plan_files("error")
        failed = self.upload(b"x")
        self.assertEqual(self.emu.file(failed)["status"], "error")
        self.assertEqual(self.create_from(failed).status, 400)
        self.emu.file_processing_s = 5
        self.emu.plan_files("error")
        later = self.upload(b"x")
        self.assertEqual(self.emu.file(later)["status"], "pending")
        self.clock.advance(5)
        self.assertEqual(json.loads(self.call("GET", f"files/{later}").body)["status"], "error")
        self.emu.file_processing_s = 0
        self.assertEqual(self.emu.file(self.upload(b"x"))["status"], "processed")  # plan used up

    def test_without_auto_advance_files_settle_only_on_tick(self) -> None:
        self.emu.auto_advance, self.emu.file_processing_s = False, 5
        file_id = self.upload(b"x")
        self.clock.advance(60)
        self.assertEqual(json.loads(self.call("GET", f"files/{file_id}").body)["status"], "pending")
        self.emu.tick()
        self.assertEqual(self.emu.file(file_id)["status"], "processed")

    def test_retrieve_and_delete_files(self) -> None:
        file_id = self.upload(b"x")
        self.assertEqual(self.call("GET", "files/file-missing").status, 404)
        deleted = self.call("DELETE", f"files/{file_id}")
        self.assertEqual(
            json.loads(deleted.body), {"id": file_id, "object": "file", "deleted": True}
        )
        self.assertEqual(self.call("GET", f"files/{file_id}").status, 404)
        self.assertEqual(self.call("DELETE", f"files/{file_id}").status, 404)
        self.assertEqual(self.emu.file_ids(), [])
        self.assertEqual((self.emu.calls["files.retrieve"], self.emu.calls["files.delete"]), (2, 2))

    def test_deleting_a_file_frees_its_slot(self) -> None:
        from foundry_batch.emulator import foundry

        original = foundry.MAX_INPUT_FILES
        foundry.MAX_INPUT_FILES = 1
        try:
            first = self.upload(b"x")
            self.assertEqual(self.emu.handle(self.multipart(b"y")).status, 400)
            self.call("DELETE", f"files/{first}")
            self.assertEqual(self.emu.handle(self.multipart(b"y")).status, 200)
        finally:
            foundry.MAX_INPUT_FILES = original

    def test_expires_after_is_validated_recorded_and_enforced(self) -> None:
        dotted = self.emu.handle(
            self.multipart(
                b"x", **{"expires_after.anchor": "created_at", "expires_after.seconds": "1209600"}
            )
        )
        data = json.loads(dotted.body)
        self.assertEqual(data["expires_at"], data["created_at"] + 1_209_600)
        bracketed = self.emu.handle(
            self.multipart(
                b"x", **{"expires_after[anchor]": "created_at", "expires_after[seconds]": "2592000"}
            )
        )
        self.assertEqual(json.loads(bracketed.body)["expires_at"] - data["created_at"], 2_592_000)
        self.assertIsNone(json.loads(self.emu.handle(self.multipart(b"x")).body)["expires_at"])
        for anchor, seconds in (
            ("created_at", "1209599"),
            ("created_at", "2592001"),
            ("uploaded_at", "1209600"),
            ("created_at", "14d"),
            (None, "1209600"),
        ):
            fields = {"expires_after.seconds": seconds}
            if anchor:
                fields["expires_after.anchor"] = anchor
            with self.subTest(anchor=anchor, seconds=seconds):
                refused = self.emu.handle(self.multipart(b"x", **fields))
                self.assertEqual(json.loads(refused.body)["error"]["code"], "invalid_expires_after")
        self.clock.advance(1_209_600)
        self.assertEqual(self.call("GET", f"files/{data['id']}").status, 404)  # expired, gone
        self.assertEqual(len(self.emu.file_ids()), 2)

    def test_expiring_files_have_their_own_larger_limit(self) -> None:
        from foundry_batch.emulator import foundry

        expiring = {"expires_after.anchor": "created_at", "expires_after.seconds": "1209600"}
        original = (foundry.MAX_INPUT_FILES, foundry.MAX_INPUT_FILES_WITH_EXPIRY)
        foundry.MAX_INPUT_FILES, foundry.MAX_INPUT_FILES_WITH_EXPIRY = 1, 2
        try:
            self.assertEqual(self.emu.handle(self.multipart(b"x")).status, 200)
            self.assertEqual(self.emu.handle(self.multipart(b"x")).status, 400)  # 500-file pool
            for _ in range(2):
                self.assertEqual(self.emu.handle(self.multipart(b"x", **expiring)).status, 200)
            refused = self.emu.handle(self.multipart(b"x", **expiring))
            self.assertEqual(json.loads(refused.body)["error"]["code"], "too_many_files")
        finally:
            foundry.MAX_INPUT_FILES, foundry.MAX_INPUT_FILES_WITH_EXPIRY = original


class CancellationAccountingTests(Harness):
    """Cancelled work is prorated: nothing is invented for a job that never started (E6)."""

    def cancel_at(self, seconds: float, *, lines: int = 4, failed: int = 0) -> dict:
        self.emu.plan_next(LifecyclePlan(in_progress_s=120, failed_requests=failed))
        batch_id = self.create("".join(line(n) + "\n" for n in range(1, lines + 1)).encode())["id"]
        self.clock.advance(seconds)
        self.call("POST", f"batches/{batch_id}/cancel")
        self.clock.advance(61)
        got = self.status(batch_id)
        self.assertEqual(got["status"], "cancelled")
        return got

    def test_cancel_while_validating_reports_no_work_and_no_usage(self) -> None:
        got = self.cancel_at(10, failed=1)
        self.assertEqual(got["request_counts"], {"total": 4, "completed": 0, "failed": 0})
        self.assertEqual(got["usage"], {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0})
        self.assertIsNone(got["output_file_id"])
        self.assertIsNone(got["error_file_id"])
        self.assertIsNone(got["in_progress_at"])

    def test_cancel_during_progress_is_prorated_by_elapsed_work(self) -> None:
        got = self.cancel_at(30 + 90)  # 90 s of a 120 s run: three quarters done
        self.assertEqual(got["request_counts"], {"total": 4, "completed": 3, "failed": 0})
        self.assertGreater(got["usage"]["input_tokens"], 0)
        self.assertEqual(got["usage"]["output_tokens"], 60)

    def test_cancel_while_finalizing_keeps_all_finished_work(self) -> None:
        got = self.cancel_at(30 + 120 + 10, failed=1)
        self.assertEqual(got["request_counts"], {"total": 4, "completed": 3, "failed": 1})

    def test_expired_and_planned_cancellations_keep_partial_results(self) -> None:
        self.emu.plan_next(
            LifecyclePlan(outcome="expired"),
            LifecyclePlan(outcome="cancelled"),
            LifecyclePlan(outcome="cancelled", in_progress_s=0),  # no run time: all done
        )
        four = "".join(line(n) + "\n" for n in range(1, 5)).encode()
        ids = [self.create(four)["id"] for _ in range(3)]
        self.clock.advance(3600)
        self.assertEqual([self.status(i)["request_counts"]["completed"] for i in ids], [2, 2, 4])


class EmulatorTimelineTests(Harness):
    def test_event_and_delivery_use_the_transition_time_not_the_apply_time(self) -> None:
        self.emu.auto_advance = False
        self.emu.delivery = DeliveryPolicy(delay_s=60)
        self.endpoint()
        batch_id = self.create()["id"]
        self.clock.advance(4000)  # nobody looked for a while
        self.emu.advance()
        done = self.emu.batch(batch_id)
        [delivery] = self.emu.deliveries
        self.assertEqual(json.loads(delivery.body)["created_at"], done["completed_at"])
        self.assertEqual(delivery.due_at.timestamp(), done["completed_at"] + 60)

    def test_seeded_jitter_reorders_deliveries_reproducibly(self) -> None:
        def due_times(seed: int) -> list[float]:
            self.setUp()
            self.emu = FoundryEmulator(
                "eus2",
                self.clock,
                transport=self.transport,
                delivery=DeliveryPolicy(jitter_s=300, seed=seed),
            )
            self.endpoint()
            for _ in range(4):
                self.create()
            self.clock.advance(3600)
            self.emu.advance()
            return [
                d.due_at.timestamp() - json.loads(d.body)["created_at"] for d in self.emu.deliveries
            ]

        first = due_times(3)
        self.assertEqual(first, due_times(3))
        self.assertNotEqual(first, due_times(4))
        self.assertTrue(all(0 <= delay <= 300 for delay in first))
        self.assertNotEqual(first, sorted(first))  # a later event can be delivered first

    def test_journal_records_each_call_and_webhook_attempt_with_its_time(self) -> None:
        self.endpoint()
        self.emu.inject("batch.list", Fault(status=429), Fault(drop_connection=True))
        self.call("GET", "batches")
        self.call("GET", "batches")
        self.emu.inject("batch.list", Fault(connect_timeout=True))
        self.call("GET", "batches")
        self.reply = 503
        self.create()
        self.clock.advance(3600)
        self.emu.tick()
        self.assertEqual(
            [entry[1:] for entry in self.emu.journal],
            [
                ("webhooks.create", "200"),
                ("batch.list", "429"),
                ("batch.list", "dropped"),
                ("batch.list", "connect_timeout"),
                ("files.upload", "200"),
                ("batch.create", "200"),
                ("webhook", self.emu.deliveries[0].delivery_id, "503"),
            ],
        )
        self.assertEqual(self.emu.journal[-1][0], "2026-11-20T13:00:00.000000Z")


class EndpointTests(Harness):
    def test_one_time_secret_update_keeps_it_and_delete(self) -> None:
        created = self.endpoint()
        secret = created["signing_secret"]
        self.assertTrue(secret.startswith("whsec_"))
        listed = json.loads(self.call("GET", "dashboard/webhook_endpoints").body)["data"]
        self.assertNotIn("signing_secret", listed[0])
        updated = self.call(
            "POST",
            f"dashboard/webhook_endpoints/{created['id']}",
            {"name": "m", "url": "https://h.example/v", "event_types": ["batch.failed"]},
        )
        self.assertNotIn("signing_secret", json.loads(updated.body))
        self.assertEqual(self.emu.endpoint_secret(created["id"]), secret)
        self.assertEqual(
            self.call("DELETE", f"dashboard/webhook_endpoints/{created['id']}").status, 200
        )
        self.assertEqual(
            self.call("DELETE", f"dashboard/webhook_endpoints/{created['id']}").status, 404
        )
        self.assertEqual(self.call("POST", "dashboard/webhook_endpoints/nope", {}).status, 404)

    def test_endpoint_validation_and_limit(self) -> None:
        good = {"name": "n", "url": "https://h.example/w", "event_types": ["batch.completed"]}
        for change, code in (
            ({"name": ""}, "invalid_name"),
            ({"url": "http://h.example/w"}, "invalid_url"),
            ({"url": "http://127.0.0.1:7071/w"}, "invalid_url"),
            ({"event_types": ["response.completed"]}, "invalid_event_types"),
            ({"event_types": []}, "invalid_event_types"),
        ):
            with self.subTest(code=code):
                response = self.call("POST", "dashboard/webhook_endpoints", {**good, **change})
                self.assertEqual(json.loads(response.body)["error"]["code"], code)
        self.emu.allow_http_webhook_urls = True
        self.endpoint(url="http://127.0.0.1:7071/api/webhooks/eus2")
        self.emu.max_endpoints = 1
        response = self.call("POST", "dashboard/webhook_endpoints", good)
        self.assertEqual(json.loads(response.body)["error"]["code"], "endpoint_limit_reached")


class AuthAndFaultTests(Harness):
    def test_auth_modes(self) -> None:
        self.assertEqual(self.call("GET", "batches", headers={}).status, 401)
        self.assertEqual(
            self.call("GET", "batches", headers={"authorization": "Bearer "}).status, 401
        )
        denied = self.call("GET", "batches", headers={"api-key": "k"})
        self.assertEqual(
            (denied.status, json.loads(denied.body)["error"]["code"]),
            (403, "AuthenticationTypeDisabled"),
        )
        self.emu.key_auth_disabled = False
        self.assertEqual(self.call("GET", "batches", headers={"api-key": "k"}).status, 200)
        self.assertEqual(self.call("GET", "unknown/route").status, 404)
        self.assertEqual(self.call("PATCH", "batches").status, 404)

    def test_queued_faults_before_and_after_commit(self) -> None:
        self.emu.inject("batch.list", Fault(status=429, retry_after=7))
        throttled = self.call("GET", "batches")
        self.assertEqual((throttled.status, throttled.headers["retry-after"]), (429, "7"))
        self.assertEqual(self.call("GET", "batches").status, 200)  # one-shot
        file_id = self.upload((line() + "\n").encode())
        body = {
            "input_file_id": file_id,
            "endpoint": "/v1/chat/completions",
            "completion_window": "24h",
        }
        self.emu.inject("batch.create", Fault(status=504, after_commit=True))
        self.assertEqual(self.call("POST", "batches", body).status, 504)
        self.assertEqual(len(self.emu.batch_ids()), 1)  # committed anyway
        self.emu.inject("batch.create", Fault(drop_connection=True, after_commit=True))
        self.assertTrue(self.call("POST", "batches", body).drop_connection)
        self.assertEqual(len(self.emu.batch_ids()), 2)
        self.emu.inject("batch.create", Fault(connect_timeout=True))
        self.assertTrue(self.call("POST", "batches", body).connect_timeout)
        self.assertEqual(len(self.emu.batch_ids()), 2)  # never reached the service
        self.emu.inject("batch.create", Fault(drop_connection=True))
        self.assertTrue(self.call("POST", "batches", body).drop_connection)
        self.assertEqual(len(self.emu.batch_ids()), 2)

    def test_chaos_rule_is_seeded_and_removable(self) -> None:
        self.emu.chaos("batch.list", ChaosRule(1.0, Fault(status=500)))
        self.assertEqual(self.call("GET", "batches").status, 500)
        self.emu.chaos("batch.list", None)
        self.assertEqual(self.call("GET", "batches").status, 200)
        self.assertEqual(self.emu.calls["batch.list"], 2)


class WebhookTests(Harness):
    def finish(self, plan: LifecyclePlan | None = None) -> str:
        if plan:
            self.emu.plan_next(plan)
        batch_id = self.create()["id"]
        self.clock.advance(3600)
        self.emu.tick()
        return batch_id

    def test_signed_delivery_verifies_with_the_endpoint_secret(self) -> None:
        endpoint = self.endpoint()
        batch_id = self.finish()
        [call] = self.sent
        event = json.loads(call.body)
        self.assertEqual((event["type"], event["data"]["id"]), ("batch.completed", batch_id))
        verify(call.body, call.headers, [endpoint["signing_secret"]], now=self.clock.now())
        self.assertEqual((call.endpoint_id, call.resource_key), (endpoint["id"], "eus2"))
        self.assertEqual(self.emu.pending_deliveries(), 0)

    def test_retries_follow_the_schedule_then_abandon_with_a_stable_id(self) -> None:
        self.emu = FoundryEmulator(
            "eus2",
            self.clock,
            transport=self.transport,
            delivery=DeliveryPolicy(retry_schedule_s=(10.0, 20.0)),
        )
        self.endpoint()
        self.reply = 503
        self.finish()
        self.clock.advance(10)
        self.emu.tick()
        self.clock.advance(20)
        self.emu.tick()
        self.clock.advance(3600)
        self.assertEqual(self.emu.tick(), 0)
        [delivery] = self.emu.deliveries
        self.assertEqual(
            (delivery.state, delivery.attempts, delivery.statuses), ("abandoned", 3, [503] * 3)
        )
        self.assertEqual(len({c.headers["webhook-id"] for c in self.sent}), 1)

    def test_unstable_ids_transport_errors_and_recovery(self) -> None:
        self.emu = FoundryEmulator(
            "eus2",
            self.clock,
            transport=self.transport,
            delivery=DeliveryPolicy(stable_webhook_id=False),
        )
        self.endpoint()
        self.reply = None
        self.finish()
        self.reply = 200
        self.clock.advance(5)
        self.emu.tick()
        self.assertEqual(len({c.headers["webhook-id"] for c in self.sent}), 2)
        self.assertEqual(self.emu.deliveries[0].state, "delivered")

        def boom(call: WebhookCall) -> int | None:
            raise OSError("socket")

        self.emu.transport = boom
        self.finish()
        self.assertEqual(self.emu.deliveries[-1].statuses, [None])

    def test_duplicate_endpoint_url_is_rejected_like_the_live_provider(self) -> None:
        first = self.endpoint()
        duplicate = self.call(
            "POST",
            "dashboard/webhook_endpoints",
            {"name": "n2", "url": "https://h.example/w", "event_types": ["batch.completed"]},
        )
        self.assertEqual(duplicate.status, 400)
        other = self.endpoint(url="https://h.example/w?slot=b")
        clash = self.call(
            "POST",
            f"dashboard/webhook_endpoints/{other['id']}",
            {"name": "n", "url": "https://h.example/w", "event_types": ["batch.completed"]},
        )
        self.assertEqual(clash.status, 400)
        self.assertNotEqual(first["id"], other["id"])

    def test_dropped_types_probability_duplicates_and_filters(self) -> None:
        self.emu = FoundryEmulator(
            "eus2",
            self.clock,
            transport=self.transport,
            delivery=DeliveryPolicy(
                drop_event_types=frozenset({"batch.failed"}), duplicate_probability=1.0
            ),
        )
        self.endpoint(events=["batch.completed", "batch.failed"])
        self.endpoint(events=["batch.cancelled"], url="https://h.example/w2")
        self.finish(LifecyclePlan(outcome="failed"))
        self.assertEqual(self.emu.deliveries, [])
        self.finish()
        self.assertEqual(len(self.emu.deliveries), 2)
        self.assertEqual(self.emu.deliveries[1].duplicate_of, self.emu.deliveries[0].delivery_id)
        self.clock.advance(2)
        self.emu.tick()
        self.assertEqual({d.state for d in self.emu.deliveries}, {"delivered"})
        lossy = FoundryEmulator(
            "eus2",
            self.clock,
            transport=self.transport,
            delivery=DeliveryPolicy(drop_probability=1.0),
        )
        self.emu = lossy
        self.endpoint()
        self.finish()
        self.assertEqual(lossy.deliveries, [])

    def test_deleted_endpoint_and_no_transport(self) -> None:
        endpoint = self.endpoint()
        self.emu.inject("webhooks.list", Fault(status=500))
        self.emu.delivery = DeliveryPolicy(delay_s=60)
        self.create()
        self.clock.advance(3600)
        self.emu.advance()
        self.call("DELETE", f"dashboard/webhook_endpoints/{endpoint['id']}")
        self.clock.advance(timedelta(minutes=2).total_seconds())
        self.assertEqual(self.emu.deliver_due(), 0)
        self.assertEqual(self.emu.deliveries[0].state, "abandoned")
        silent = FoundryEmulator("eus2", self.clock)
        self.assertEqual(silent.deliver_due(), 0)


if __name__ == "__main__":
    unittest.main()
