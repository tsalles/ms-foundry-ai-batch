"""Pipeline smoke test (US-7.2 AC2): submit a tiny synthetic job, wait for its demo receipt.

Exit codes: 0 delivered, 1 failed, 2 still running at the timeout (batch turnaround is not
bounded; the pipeline reports this as a warning, not a pass).
"""

from __future__ import annotations

import argparse
import json
import os
import time
from collections.abc import Callable

from foundry_batch.composition import Runtime, azure_adapters, build_runtime
from foundry_batch.config import Settings
from foundry_batch.domain.contract import idempotency_key
from foundry_batch.domain.models import JobKey
from foundry_batch.foundry.submission import SubmissionPending, SubmissionService
from foundry_batch.metrics.queued_tokens import CharacterHeuristicEstimator


def _azure_runtime() -> Runtime:  # pragma: no cover - needs Azure
    settings = Settings.from_env(os.environ)
    return build_runtime(settings, azure_adapters(settings))


def main(
    argv: list[str] | None = None,
    *,
    runtime_factory: Callable[[], Runtime] = _azure_runtime,
    sleep: Callable[[float], None] = time.sleep,
    monotonic: Callable[[], float] = time.monotonic,
) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--resource", required=True)
    parser.add_argument("--deployment", required=True)
    parser.add_argument("--timeout-minutes", type=int, default=45)
    parser.add_argument("--require-source", choices=("webhook", "polling"))
    args = parser.parse_args(argv)
    if args.timeout_minutes < 1:
        parser.error("--timeout-minutes must be positive")
    runtime = runtime_factory()
    settings = runtime.settings
    line = {
        "custom_id": "smoke-1",
        "method": "POST",
        "url": "/v1/chat/completions",
        "body": {
            "model": args.deployment,
            "messages": [{"role": "user", "content": "Reply with the word ok."}],
        },
    }
    service = SubmissionService(
        settings,
        runtime.adapters.store,
        runtime.registry,
        runtime.control,
        runtime.adapters.batch_apis,
        CharacterHeuristicEstimator(),
        runtime.clock,
        processor=runtime.processor,
    )
    try:
        job = service.submit(args.resource, args.deployment, json.dumps(line).encode())
    except SubmissionPending as exc:
        # The create may have committed: never resubmit blindly (runbooks/submission-recovery.md).
        print(f"pending: {exc.reason}; run `foundry-batch recover-submissions` first")
        return 3
    print(f"submitted {job.batch_id} covered={job.webhook_covered}")
    deadline = monotonic() + args.timeout_minutes * 60
    while monotonic() < deadline:
        sleep(60)  # respects the documented 60-second status floor
        stored = runtime.registry.load(JobKey(args.resource, job.batch_id))
        if stored is None or stored.record.outcome is None:
            continue
        key = idempotency_key(
            settings.environment, args.resource, job.batch_id, stored.record.outcome
        )
        receipt = runtime.adapters.store.get(
            settings.table_demo_receipts, settings.environment, key
        )
        if receipt is not None:
            source = receipt.data.get("source")
            print(f"delivered outcome={stored.record.outcome} source={source}")
            if args.require_source and source != args.require_source:
                print(f"delivery did not use the required {args.require_source} path")
                return 1
            return 0 if stored.record.outcome == "completed" else 1
    print("timeout: job not final or event not delivered yet")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
