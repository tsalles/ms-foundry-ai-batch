"""A complete PoC stack in one process: emulated Foundry, Functions-like triggers, demo API.

``LocalStack`` wires the production runtime (receiver, processor, relay, reconciler, registrar,
submission) through the real HTTP clients to in-process emulators, with Functions semantics:
queue retries and poison queues, a 15-minute reconciliation timer and a one-minute outbox timer.
Time is a ``ManualClock`` by default, so hours of behaviour run in seconds, deterministically.

``run_scenario`` drives named scenarios and checks the PoC invariants (SC-1, SC-8) plus each
scenario's own expectations (how results must have arrived). Every source of randomness is
seeded (webhook faults, relay jitter, correlation ids), so the same scenario and seed give the
same ``timeline``. Results are synthetic evidence of the code's behaviour against assumed
service behaviour, never evidence about Azure (see ``DeliveryPolicy``).
"""

from __future__ import annotations

import hashlib
import itertools
import json
import logging
import random
import time
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import urlsplit

from foundry_batch.adapters.memory import MemoryQueue, MemoryQueueAdmin, MemoryTableStore
from foundry_batch.clock import Clock, ManualClock, to_iso
from foundry_batch.composition import Runtime, adapters_from_settings, build_runtime
from foundry_batch.config import Settings
from foundry_batch.domain.models import JobKey, JobRecord
from foundry_batch.domain.status import outcome_for
from foundry_batch.emulator.foundry import (
    ChaosRule,
    Delivery,
    DeliveryPolicy,
    Fault,
    FoundryEmulator,
    LifecyclePlan,
    WireRequest,
    WireResponse,
)
from foundry_batch.emulator.transport import (
    EMULATOR_DOMAIN,
    FaultQueue,
    emulator_endpoint,
    http_handler,
    routed_session,
    session_webhook_transport,
)
from foundry_batch.errors import ProviderError
from foundry_batch.foundry.submission import SubmissionPending, SubmissionService
from foundry_batch.metrics.queued_tokens import CharacterHeuristicEstimator
from foundry_batch.registration.registrar import RegistrationError, WebhookRegistrar
from foundry_batch.vaultsync.service import VaultSyncService

logger = logging.getLogger(__name__)

FUNCTIONS_HOST = f"func.{EMULATOR_DOMAIN}"
START = datetime(2026, 11, 20, 12, 0, tzinfo=UTC)
#: Functions defaults mirrored from functions/host.json.
MAX_DEQUEUE_COUNT = 5
VISIBILITY_TIMEOUT_S = 30
#: Settings attributes naming every table a local stack writes (dumped by ``timeline``).
TIMELINE_TABLES = (
    "table_jobs",
    "table_control",
    "table_quarantine",
    "table_submissions",
    "table_demo_receipts",
)


def local_settings(resources: Sequence[str] = ("eus2",), **overrides: str) -> Settings:
    """Settings for a fully local stack. Validated by the same rules as the cloud."""
    env = {
        "BATCH_ENVIRONMENT": "local",
        "BATCH_RESOURCES_JSON": json.dumps(
            [
                {
                    "key": key,
                    "endpoint": emulator_endpoint(key),
                    "region": "eastus2",
                    "deployments": ["gpt-batch"],
                }
                for key in resources
            ]
        ),
        "BATCH_PROVIDER_BACKEND": "emulator",
        "BATCH_STORAGE_BACKEND": "memory",
        "BATCH_SECRETS_BACKEND": "memory",
        "BATCH_DOWNSTREAM_MODE": "demo",
        "WEBSITE_HOSTNAME": FUNCTIONS_HOST,
    }
    env.update(overrides)
    return Settings.from_env(env)


def sample_jsonl(lines: int = 3, deployment: str = "gpt-batch") -> bytes:
    """Synthetic, non-sensitive batch input."""
    rows = [
        {
            "custom_id": f"req-{n}",
            "method": "POST",
            "url": "/chat/completions",
            "body": {
                "model": deployment,
                "messages": [{"role": "user", "content": f"synthetic request {n}"}],
            },
        }
        for n in range(1, lines + 1)
    ]
    return ("\n".join(json.dumps(r) for r in rows) + "\n").encode("utf-8")


class LocalStack:
    def __init__(
        self,
        *,
        resources: Sequence[str] = ("eus2",),
        clock: Clock | None = None,
        delivery: DeliveryPolicy | None = None,
        sweep_every_s: float = 900.0,
        outbox_every_s: float = 120.0,  # Terraform default outbox_schedule: every 2 minutes
        seed: int = 0,
        file_processing_s: float = 0.0,
        **setting_overrides: str,
    ) -> None:
        self.clock = clock or ManualClock(START)
        self.settings = local_settings(resources, **setting_overrides)
        self.faults = FaultQueue()
        self.receiver_down_until: datetime | None = None
        self.downstream_down_until: datetime | None = None
        self.sweep_every = timedelta(seconds=sweep_every_s)
        self.outbox_every = timedelta(seconds=outbox_every_s)
        self.poisoned: Counter[str] = Counter()
        self.trigger_failures: Counter[str] = Counter()
        self.timer_failures: Counter[str] = Counter()
        self.demo_responses: Counter[int] = Counter()
        #: (time, kind, detail...) for host-side events: HTTP routes and timer runs.
        self.journal: list[tuple[str, ...]] = []
        self._runtime: Runtime | None = None
        functions = self._functions_handler()
        webhook_session = routed_session({FUNCTIONS_HOST: functions})
        self.emulators = {
            key: FoundryEmulator(
                key,
                self.clock,
                deployments=("gpt-batch",),
                delivery=delivery,
                transport=session_webhook_transport(webhook_session),
                file_processing_s=file_processing_s,
            )
            for key in resources
        }
        adapters = adapters_from_settings(
            self.settings,
            clock=self.clock,
            emulators=self.emulators,
            routes={FUNCTIONS_HOST: functions},
            faults=self.faults,
        )
        self.adapters = adapters
        # Seeded simulation randomness (relay jitter), not cryptography.
        relay_rng = random.Random(seed)  # noqa: S311  # nosec B311
        self._runtime = build_runtime(self.settings, adapters, self.clock, rng=relay_rng)
        self.store: MemoryTableStore = adapters.store  # type: ignore[assignment]
        self.queue_admin: MemoryQueueAdmin = adapters.queue_admin  # type: ignore[assignment]
        numbers = itertools.count(1)
        self.submissions = SubmissionService(
            self.settings,
            adapters.store,
            self.runtime.registry,
            self.runtime.control,
            adapters.batch_apis,
            CharacterHeuristicEstimator(),
            self.clock,
            processor=self.runtime.processor,
            sleep=self._sleep,
            correlation_ids=lambda: f"local-{seed}-{next(numbers):05d}",
        )
        self.registrar = WebhookRegistrar(
            self.settings,
            adapters.endpoints_for,
            adapters.secrets,
            self.runtime.control,
            self.clock,
        )
        self._next_sweep = self.clock.now() + self.sweep_every
        self._next_outbox = self.clock.now() + self.outbox_every

    @property
    def runtime(self) -> Runtime:
        if self._runtime is None:
            raise RuntimeError("Local stack runtime is not initialized")
        return self._runtime

    def receiver_url(self, resource_key: str) -> str:
        return f"https://{FUNCTIONS_HOST}/api/webhooks/{resource_key}"

    # -- Functions host emulation -----------------------------------------------------------

    def _log(self, *entry: str) -> None:
        self.journal.append((to_iso(self.clock.now()) or "", *entry))

    def _functions_handler(self) -> Callable[[WireRequest], WireResponse]:
        def webhook(request: WireRequest) -> WireResponse:
            down = self.receiver_down_until
            if down is not None and self.clock.now() < down:
                return WireResponse.error(503, "receiver_unavailable")
            route = http_handler(self.runtime.receiver.handle, "/api/webhooks/{resource_key}")
            return route(request)

        def demo(request: WireRequest) -> WireResponse:
            down = self.downstream_down_until
            if down is not None and self.clock.now() < down:
                return WireResponse.error(503, "downstream_unavailable")
            response = http_handler(self.runtime.demo_api.handle, "/api/demo/events")(request)
            self.demo_responses[response.status] += 1
            return response

        def route(request: WireRequest) -> WireResponse:
            path = urlsplit(request.path).path
            if path.startswith("/api/webhooks/"):
                response = webhook(request)
            elif path == "/api/demo/events":
                response = demo(request)
            else:
                response = WireResponse.error(404, "route_not_found")
            self._log("http", path, str(response.status))
            return response

        return route

    def _sleep(self, seconds: float) -> None:
        """The submission's file wait: emulated time passes and every trigger keeps running."""
        if isinstance(self.clock, ManualClock):
            self.advance(seconds)
        else:
            time.sleep(seconds)

    def register_webhooks(self) -> None:
        for resource in self.settings.resources:
            self.registrar.ensure(resource.key, self.receiver_url(resource.key))

    def submit(self, resource_key: str = "eus2", lines: int = 3) -> str | None:
        """Submit like Batch: retry definite failures with a new id, recover pending ones."""
        for _ in range(5):
            try:
                job = self.submissions.submit(resource_key, "gpt-batch", sample_jsonl(lines))
                return job.batch_id
            except SubmissionPending:
                self.trigger_failures["submit_pending"] += 1
                return None  # recover_submissions links it later
            except ProviderError:
                self.trigger_failures["submit_retry"] += 1
                self.advance(10)
        return None

    def recover_submissions(self) -> list[str]:
        linked: list[str] = []
        for resource in self.settings.resources:
            linked += self.submissions.recover(resource.key, min_age_seconds=0)
        return linked

    def _drain(self, queue: str, handler: Callable[[str], object]) -> int:
        processed = 0
        while True:
            messages = self.queue_admin.receive(
                queue, max_messages=32, visibility_timeout=VISIBILITY_TIMEOUT_S
            )
            if not messages:
                return processed
            for message in messages:
                processed += 1
                try:
                    handler(message.content)
                except Exception:  # noqa: BLE001 - the host retries, then poisons the message
                    self.trigger_failures[queue] += 1
                    if message.dequeue_count >= MAX_DEQUEUE_COUNT:
                        self.queue_admin.delete(queue, message)
                        self.queue_admin.send_to(f"{queue}-poison", message.content)
                        self.poisoned[queue] += 1
                    continue
                self.queue_admin.delete(queue, message)

    def pump(self) -> None:
        """Run triggers until queues are idle at the current time."""
        now = self.clock.now()
        for queue in self.queue_admin.queues.values():
            queue.now = now.timestamp()
        for emulator in self.emulators.values():
            emulator.tick()
        settings = self.settings
        while self._drain(
            settings.incoming_queue, self.runtime.processor.handle_queue_message
        ) + self._drain(settings.relay_queue, self.runtime.dispatcher.dispatch_nudge):
            pass
        if now >= self._next_outbox:
            try:
                self._timer("outbox", self.runtime.dispatcher.dispatch_due)
            finally:
                self._next_outbox = now + self.outbox_every
        if now >= self._next_sweep:
            try:
                self._timer("sweep", self.runtime.reconciler.run_all)
            finally:
                self._next_sweep = now + self.sweep_every
            self._drain(settings.relay_queue, self.runtime.dispatcher.dispatch_nudge)

    def _timer(self, name: str, run: Callable[[], object]) -> None:
        """One timer run. Like the Functions host, a failed run is recorded (counted, not
        raised) and the timer fires again on schedule."""
        try:
            run()
        except Exception:  # noqa: BLE001 - the host logs a failed run; the schedule continues
            self.timer_failures[name] += 1
            self._log("timer", name, "failed")
            return
        self._log("timer", name, "ok")

    def advance(self, seconds: float, step_s: float = 30.0) -> None:
        """Move emulated time forward in steps, running every trigger that falls due."""
        if not isinstance(self.clock, ManualClock):
            raise TypeError("advance() needs a ManualClock")
        remaining = seconds
        while remaining > 0:
            step = min(step_s, remaining)
            self.clock.advance(step)
            remaining -= step
            self.pump()

    def queue_depth(self, name: str) -> int:
        queue: MemoryQueue = self.queue_admin.queues[name]
        return len(queue)


# ---------------------------------------------------------------------------------------------
# Scenarios
# ---------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Scenario:
    name: str
    description: str
    jobs: int = 10
    delivery: DeliveryPolicy = field(default_factory=DeliveryPolicy)
    plans: tuple[LifecyclePlan, ...] = (LifecyclePlan(),)
    receiver_outage_s: float = 0.0
    downstream_outage_s: float = 0.0
    files_failure_rate: float = 0.0
    duration_s: float = 3 * 3600.0
    #: Seconds an uploaded file stays ``pending``: every submission waits for ``processed``.
    file_processing_s: float = 20.0
    #: Every job is learned from a webhook and no delivery is abandoned (the M1 event path).
    webhook_only: bool = False
    #: Jobs learned by polling are exactly those whose terminal webhooks were all dropped.
    polling_matches_drops: bool = False


_MIXED = (
    LifecyclePlan(),
    LifecyclePlan(outcome="failed", validating_s=60),
    LifecyclePlan(outcome="cancelled", in_progress_s=600),
    LifecyclePlan(failed_requests=1, in_progress_s=900),
    LifecyclePlan(omit_in_progress_at=True, in_progress_s=1800),
)

SCENARIOS: dict[str, Scenario] = {
    "happy": Scenario("happy", "Every webhook arrives once; the M1 event path.", webhook_only=True),
    "lossy": Scenario(
        "lossy",
        "30% of webhooks lost, 20% duplicated, batch.failed never sent, mixed outcomes.",
        jobs=20,
        delivery=DeliveryPolicy(
            drop_probability=0.3,
            duplicate_probability=0.2,
            drop_event_types=frozenset({"batch.failed"}),
            seed=7,
        ),
        plans=_MIXED,
        polling_matches_drops=True,
    ),
    "outages": Scenario(
        "outages",
        "Receiver down 20 min, downstream down 45 min, Files API failing 30% of uploads.",
        jobs=15,
        delivery=DeliveryPolicy(seed=11),
        plans=_MIXED,
        receiver_outage_s=1200,
        downstream_outage_s=2700,
        files_failure_rate=0.3,
    ),
}


@dataclass
class ScenarioReport:
    scenario: str
    label: str
    jobs_submitted: int
    jobs_terminal: int
    outcomes: dict[str, int]
    learned_from: dict[str, int]
    downstream_effects: int
    downstream_duplicates_suppressed: int
    webhook_deliveries: dict[str, int]
    status_calls_per_job: float
    list_pages: int
    poison_messages: int
    submit_retries: int
    violations: list[str]
    jobs_without_webhook: int = 0
    timer_failures: dict[str, int] = field(default_factory=dict)
    #: SHA-256 of ``timeline``: equal for two runs of the same scenario and seed.
    timeline_sha256: str = ""

    @property
    def passed(self) -> bool:
        return not self.violations

    def to_dict(self) -> dict[str, object]:
        return {**asdict(self), "passed": self.passed}


def run_scenario(scenario: Scenario, *, settle_s: float | None = None) -> ScenarioReport:
    stack = play(scenario, settle_s=settle_s)
    return _report(stack, scenario, stack.emulators["eus2"])


def play(scenario: Scenario, *, settle_s: float | None = None) -> LocalStack:
    """Drive a scenario and return the stack, for reports and ``timeline``."""
    stack = LocalStack(
        delivery=scenario.delivery,
        seed=scenario.delivery.seed,
        file_processing_s=scenario.file_processing_s,
    )
    emulator = stack.emulators["eus2"]
    if scenario.files_failure_rate:
        emulator.chaos("files.upload", ChaosRule(scenario.files_failure_rate, Fault(status=500)))
    stack.register_webhooks()
    start = stack.clock.now()
    if scenario.receiver_outage_s:
        stack.receiver_down_until = start + timedelta(seconds=scenario.receiver_outage_s)
    if scenario.downstream_outage_s:
        stack.downstream_down_until = start + timedelta(seconds=scenario.downstream_outage_s)
    batch_ids: list[str] = []
    for index in range(scenario.jobs):
        emulator.plan_next(scenario.plans[index % len(scenario.plans)])
        batch_id = stack.submit()
        if batch_id:
            batch_ids.append(batch_id)
        stack.advance(30)
    stack.recover_submissions()
    stack.advance(settle_s if settle_s is not None else scenario.duration_s)
    return stack


def timeline(stack: LocalStack) -> list[str]:
    """Everything a run produced, in order: host and emulator journals, webhook deliveries,
    then every table row (job state, receipts, outbox attempts, leases, intents, demo
    receipts). Secrets are never included."""
    lines = [json.dumps(["host", *entry]) for entry in stack.journal]
    for key in sorted(stack.emulators):
        emulator = stack.emulators[key]
        lines += [json.dumps([key, *entry]) for entry in emulator.journal]
        lines += [json.dumps([key, asdict(d)], default=str) for d in emulator.deliveries]
    for name in TIMELINE_TABLES:
        table = getattr(stack.settings, name)
        for row in stack.store.rows(table):
            entry = [table, row.partition_key, row.row_key, row.etag, row.data]
            lines.append(json.dumps(entry, sort_keys=True, default=str))
    return lines


def _report(stack: LocalStack, scenario: Scenario, emulator: FoundryEmulator) -> ScenarioReport:
    registry = stack.runtime.registry
    jobs = [s.record for s in registry.terminal_jobs("eus2") + registry.open_jobs("eus2")]
    terminal = [j for j in jobs if j.outcome is not None]
    receipts = [e.data for e in stack.store.rows(stack.settings.table_demo_receipts)]
    first_effects = Counter(str(r["job_id"]) for r in receipts if not r.get("is_correction"))
    deliveries: Counter[str] = Counter(delivery.state for delivery in emulator.deliveries)
    deliveries["duplicates_sent"] = sum(1 for d in emulator.deliveries if d.duplicate_of)
    violations: list[str] = []
    provider_ids = set(emulator.batch_ids())
    tracked = {j.batch_id for j in jobs}
    if provider_ids - tracked:
        missing = sorted(provider_ids - tracked)
        violations.append(f"provider jobs missing from the registry: {missing}")
    for job in jobs:
        provider_status = emulator.batch(job.batch_id)["status"]
        if job.outcome is None:
            violations.append(f"{job.batch_id}: no final state (provider: {provider_status})")
        elif job.raw_status != provider_status:
            violations.append(f"{job.batch_id}: registry {job.raw_status} vs {provider_status}")
    for job in terminal:
        if first_effects.get(job.key.job_id, 0) < 1:
            violations.append(f"{job.batch_id}: final state never reached the downstream API")
    duplicates = sum(count - 1 for count in first_effects.values() if count > 1)
    if duplicates:
        violations.append(f"{duplicates} duplicate downstream business effects")
    if stack.poisoned:
        violations.append(f"poison messages: {dict(stack.poisoned)}")
    violations += _downstream_mismatches(receipts, jobs, emulator)
    silent = _jobs_without_webhook(provider_ids, emulator.deliveries)
    violations += _expectations(scenario, terminal, deliveries, silent)
    digest = hashlib.sha256("\n".join(timeline(stack)).encode("utf-8")).hexdigest()
    return ScenarioReport(
        scenario=scenario.name,
        label="SYNTHETIC: emulator behaviour under assumed delivery semantics, not Azure evidence",
        jobs_submitted=len(provider_ids),
        jobs_terminal=len(terminal),
        outcomes=dict(Counter(j.raw_status for j in terminal)),
        learned_from=dict(Counter(str(j.terminal_source) for j in terminal)),
        downstream_effects=len(receipts),
        downstream_duplicates_suppressed=stack.demo_responses[200],
        webhook_deliveries=dict(deliveries),
        status_calls_per_job=round(emulator.calls["batch.retrieve"] / max(len(jobs), 1), 2),
        list_pages=emulator.calls["batch.list"],
        poison_messages=sum(stack.poisoned.values()),
        submit_retries=stack.trigger_failures["submit_retry"],
        violations=violations,
        jobs_without_webhook=len(silent),
        timer_failures=dict(stack.timer_failures),
        timeline_sha256=digest,
    )


def _downstream_mismatches(
    receipts: Sequence[Mapping[str, Any]], jobs: Sequence[JobRecord], emulator: FoundryEmulator
) -> list[str]:
    """Every downstream effect must carry the provider's final status and its outcome."""
    batch_for = {job.key.job_id: job.batch_id for job in jobs}
    problems: list[str] = []
    for receipt in receipts:
        job_id = str(receipt["job_id"])
        batch_id = batch_for.get(job_id)
        final = emulator.batch(batch_id)["status"] if batch_id else None
        outcome = outcome_for(final)
        told = (receipt.get("outcome"), receipt.get("raw_status"))
        if outcome is None or told != (outcome.value, final):
            problems.append(f"{job_id}: downstream told {told[0]}/{told[1]}, provider {final}")
    return problems


def _jobs_without_webhook(provider_ids: set[str], deliveries: Sequence[Delivery]) -> set[str]:
    """Jobs whose terminal webhooks were all dropped before any delivery attempt."""
    sent = {d.batch_id for d in deliveries}
    return {batch_id for batch_id in provider_ids if batch_id not in sent}


def _expectations(
    scenario: Scenario,
    terminal: Sequence[JobRecord],
    deliveries: Counter[str],
    silent: set[str],
) -> list[str]:
    """How results must have arrived, per scenario (catches a dead or bypassed webhook path)."""
    problems: list[str] = []
    by_polling = {job.batch_id for job in terminal if job.terminal_source == "polling"}
    if scenario.webhook_only:
        other = sorted(job.batch_id for job in terminal if job.terminal_source != "webhook")
        if other:
            problems.append(f"expected every job from a webhook; not from one: {other}")
        if deliveries["abandoned"]:
            problems.append(f"{deliveries['abandoned']} webhook deliveries abandoned")
    if scenario.polling_matches_drops and by_polling != silent:
        problems.append(
            f"learned by polling: {len(by_polling)}, jobs whose webhooks were all dropped: "
            f"{len(silent)} (differ: {sorted(by_polling ^ silent)})"
        )
    return problems


def run_vault_rotation() -> dict[str, object]:
    """REQ-I03 lifecycle against the emulated client vault: capture, lost write, rotation,
    gated retirement and downstream retirement. Uses a synthetic destination."""
    from foundry_batch.emulator.client_vault import EmulatedClientVault

    stack = LocalStack(BATCH_CLIENT_VAULT_BACKEND="emulator")
    vault = stack.adapters.client_vault
    if not isinstance(vault, EmulatedClientVault):
        raise RuntimeError("Vault rotation scenario requires the emulated client vault")
    sync = VaultSyncService(
        stack.settings.environment,
        stack.store,
        stack.settings.table_control,
        stack.adapters.secrets,
        vault,
        stack.clock,
    )
    stack.registrar.sync_gate = sync.ready
    control, url = stack.runtime.control, stack.receiver_url("eus2")
    violations: list[str] = []
    steps: list[str] = []

    def run() -> None:
        stack.advance(60)
        sync.run("eus2", control.registrations("eus2"))

    stack.register_webhooks()
    first = next(r.endpoint_id for r in control.registrations("eus2"))
    vault.lose_next_put_response = True
    run()  # the write commits but its response is lost
    run()  # reconciled through the request token, not written twice
    steps.append(f"slot a captured and synchronized; destination writes={vault.writes}")
    if vault.writes != 1 or not sync.ready("eus2", [first]):
        violations.append("ambiguous write did not converge on one verified version")
    stack.registrar.rotate("eus2", url)
    second = next(r.endpoint_id for r in control.registrations("eus2") if r.endpoint_id != first)
    try:
        stack.registrar.retire("eus2", "a")
        violations.append("old slot retired before the new key reached the client vault")
    except RegistrationError:
        steps.append("retire of slot a refused until slot b is verified downstream")
    run()
    batch_id = stack.submit()
    stack.advance(600)
    job = stack.runtime.registry.load(job_key(batch_id)) if batch_id else None
    if job is None or job.record.terminal_source != "webhook":
        violations.append("webhook during the overlap was not verified")
    stack.registrar.retire("eus2", "a")
    run()
    old = next(r for r in sync.status("eus2") if r.endpoint_id == first)
    steps.append(f"slot a retired downstream: state={old.state}")
    if old.state != "retired" or vault.read(old.destination_name, old.destination_version or ""):
        violations.append("retired key is still readable in the client vault")
    if not sync.ready("eus2", [second]):
        violations.append("replacement key is not verified in the client vault")
    return {
        "scenario": "vault-rotation",
        "label": "SYNTHETIC: emulated external vault; no production destination adapter",
        "steps": steps,
        "violations": violations,
        "passed": not violations,
    }


def job_key(batch_id: str, resource_key: str = "eus2") -> JobKey:
    return JobKey(resource_key, batch_id)
