"""Operator CLI (pipeline steps, runbooks, simulations).

Settings come from the same environment variables as the Function App (or ``--settings`` JSON
with non-secret values). Secrets are never accepted as arguments and never printed.

Exit codes: 0 done; 1 unhealthy or not converged; 2 usage (unknown resource, confirmation
mismatch, vault sync not configured); 3 pending, rerun after the named step; 4 refused or
rolled back by a safety rule (JSON ``reason``).
"""

from __future__ import annotations

import argparse
import base64
import json
import logging
import os
import secrets as pysecrets
import sys
from collections.abc import Callable, Iterable, Sequence
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Any

from foundry_batch.composition import Runtime, adapters_from_settings, build_runtime
from foundry_batch.config import Settings
from foundry_batch.errors import ProviderError
from foundry_batch.metrics.indicators import STANDARD_WINDOWS, compute_indicators
from foundry_batch.metrics.queued_tokens import CharacterHeuristicEstimator, queued_tokens
from foundry_batch.observability import JsonFormatter
from foundry_batch.ops.preflight import PreflightInput, exit_code, run_preflight, to_markdown
from foundry_batch.ports import Query
from foundry_batch.reconcile.polling_policy import (
    PollingPolicy,
    load_durations,
    reduction,
    simulate,
)
from foundry_batch.registration.registrar import RegistrationError, WebhookRegistrar
from foundry_batch.relay.replay import ReplayService, replay_poison_queue
from foundry_batch.vaultsync.service import SyncRecord, VaultSyncService

RuntimeFactory = Callable[[Settings, argparse.Namespace], Runtime]
SyncGate = Callable[[str, Sequence[str]], bool]

EXIT_OK, EXIT_UNHEALTHY, EXIT_USAGE, EXIT_PENDING, EXIT_REFUSED = 0, 1, 2, 3, 4


def _default_runtime(settings: Settings, args: argparse.Namespace) -> Runtime:  # pragma: no cover
    adapters = adapters_from_settings(settings, allow_api_key=args.allow_api_key)
    return build_runtime(settings, adapters)


def _aware_datetime(text: str) -> datetime:
    """argparse type: ISO 8601 with an explicit offset; a typo must never widen a replay."""
    try:
        value = datetime.fromisoformat(text.strip().replace("Z", "+00:00"))
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"not an ISO 8601 date-time: {text!r}") from exc
    if value.tzinfo is None:
        raise argparse.ArgumentTypeError(f"add a UTC offset (e.g. 'Z'): {text!r}")
    return value


def _load_settings(args: argparse.Namespace) -> Settings:
    env: dict[str, str] = dict(os.environ)
    if args.settings:
        data = json.loads(Path(args.settings).read_text(encoding="utf-8"))
        env.update({str(k): str(v) for k, v in data.items()})
    return Settings.from_env(env)


def _print(data: Any) -> None:
    print(json.dumps(data, indent=2, default=str, sort_keys=True))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="foundry-batch", description=__doc__)
    parser.add_argument("--settings", help="JSON file with non-secret settings (env names)")
    parser.add_argument(
        "--allow-api-key",
        action="store_true",
        help="use API keys for Foundry calls (requires the D-04 exception)",
    )
    parser.add_argument("--verbose", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)

    reg = sub.add_parser("register", help="webhook endpoint registration")
    reg_sub = reg.add_subparsers(dest="action", required=True)
    for name in ("ensure", "rotate"):
        p = reg_sub.add_parser(name)
        p.add_argument("--resource", required=True)
        p.add_argument(
            "--receiver-base-url",
            required=True,
            help="https://<host>/api/webhooks ; the resource key is appended",
        )
        if name == "ensure":
            p.add_argument("--repair", action="store_true")
            p.add_argument(
                "--force",
                action="store_true",
                help="with --repair: remove the unhealthy endpoint without waiting for the "
                "client vault to verify its replacement (REQ-I03)",
            )
    p = reg_sub.add_parser("retire")
    p.add_argument("--resource", required=True)
    p.add_argument("--slot", required=True, choices=["a", "b"])
    p.add_argument(
        "--force",
        action="store_true",
        help="retire without the other-slot health and client-vault checks (approved emergency)",
    )
    p = reg_sub.add_parser("remove")
    p.add_argument("--resource", required=True)
    p.add_argument("--confirm", required=True, help="type the resource key again to confirm")
    p = reg_sub.add_parser("health")
    p.add_argument("--resource", required=True)

    rs = sub.add_parser("relay-secret", help="create the relay signing secret if absent")
    rs.add_argument("action", choices=["ensure"])

    rp = sub.add_parser("replay", help="authorised, audited replay")
    rp_sub = rp.add_subparsers(dest="action", required=True)
    p = rp_sub.add_parser("outbox")
    p.add_argument("--status", default="quarantined", choices=["quarantined", "delivered"])
    p.add_argument("--since", type=_aware_datetime, help="ISO 8601 with offset, e.g. ...Z")
    p.add_argument("--until", type=_aware_datetime, help="ISO 8601 with offset, e.g. ...Z")
    for q in (p, rp_sub.add_parser("poison")):
        q.add_argument("--operator", required=True)
        q.add_argument("--reason", required=True)
        q.add_argument("--execute", action="store_true", help="without it, a dry run")
    rp_sub.choices["poison"].add_argument("--queue", required=True, choices=["incoming", "relay"])

    qu = sub.add_parser("quarantine", help="list quarantined messages")
    qu.add_argument("action", choices=["list"])

    rc = sub.add_parser("reconcile", help="run one sweep now")
    rc.add_argument("--resource")

    ind = sub.add_parser("indicators", help="indicator report from the registry")
    ind.add_argument("--window", choices=[w for w, _ in STANDARD_WINDOWS], default="24h")

    sub.add_parser("queued-tokens", help="queued input-token estimate per deployment")

    bf = sub.add_parser("backfill", help="store lifecycle records of past jobs (indicators only)")
    bf.add_argument("--resource", required=True)
    bf.add_argument("--max-pages", type=int, default=50)

    sm = sub.add_parser("simulate", help="polling policy vs fixed 60 s polling")
    sm.add_argument("--durations", required=True, help="file with one job duration (s) per line")
    sm.add_argument("--uncovered", action="store_true")

    pf = sub.add_parser("preflight", help="read-only target checks (Azure CLI)")
    pf.add_argument("--input", required=True, help="JSON with PreflightInput fields")
    pf.add_argument("--format", choices=["json", "md"], default="md")

    sb = sub.add_parser("submit", help="submit a small synthetic batch job")
    sb.add_argument("--resource", required=True)
    sb.add_argument("--deployment", required=True)
    sb.add_argument("--file", required=True)
    sb.add_argument("--correlation-id", help="reuse to make a retried submit idempotent")
    ld = sub.add_parser(
        "local-demo", help="run the whole PoC in-process against emulators (synthetic)"
    )
    ld.add_argument(
        "--scenario", default="all", help="happy | lossy | outages | vault-rotation | all"
    )
    vs = sub.add_parser("vault-sync", help="synchronize signing secrets to the client vault")
    vs.add_argument("action", choices=["run", "status"])
    vs.add_argument("--resource")
    sr = sub.add_parser("recover-submissions")
    sr.add_argument("--resource", required=True)
    sr.add_argument(
        "--min-age-seconds",
        type=int,
        default=900,
        help="leave younger intents alone (their submit may still be running)",
    )
    return parser


def main(
    argv: Sequence[str] | None = None,
    runtime_factory: RuntimeFactory | None = None,
    *,
    configure_logging: bool = True,
) -> int:
    args = build_parser().parse_args(argv)
    if configure_logging:  # pragma: no cover - process-level side effect
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(JsonFormatter())
        logging.basicConfig(
            level=logging.DEBUG if args.verbose else logging.INFO, handlers=[handler], force=True
        )

    # Commands that need no settings or runtime.
    if args.command == "simulate":
        durations = load_durations(Path(args.durations).read_text(encoding="utf-8").splitlines())
        covered = not args.uncovered
        baseline = simulate(durations, None)
        candidate = simulate(durations, PollingPolicy(), webhook_covered=covered)
        _print(
            {
                "baseline_fixed_60s": asdict(baseline),
                "adaptive": asdict(candidate),
                "reduction": reduction(baseline, candidate),
                "webhook_covered": covered,
            }
        )
        return 0
    if args.command == "preflight":
        data = json.loads(Path(args.input).read_text(encoding="utf-8"))
        findings = run_preflight(PreflightInput(**data))
        if args.format == "md":
            print(to_markdown(findings))
        else:
            _print([asdict(f) for f in findings])
        return exit_code(findings)

    if args.command == "local-demo":
        return _local_demo(args.scenario)

    settings = _load_settings(args)
    runtime = (runtime_factory or _default_runtime)(settings, args)
    return _dispatch(args, runtime)


def _unknown_resources(settings: Settings, keys: Iterable[str]) -> int | None:
    """Exit code 2 with a JSON error when a key is not configured; ``None`` when all are."""
    unknown = sorted(k for k in keys if settings.resource(k) is None)
    if not unknown:
        return None
    known = [r.key for r in settings.resources]
    _print({"status": "error", "error": f"unknown resource {unknown[0]!r}", "known": known})
    return EXIT_USAGE


def _sync_gate(rt: Runtime) -> SyncGate | None:
    return _vault_sync_service(rt).ready if rt.adapters.client_vault is not None else None


def _register(args: argparse.Namespace, rt: Runtime) -> int:
    s = rt.settings
    unknown = _unknown_resources(s, [args.resource])
    if unknown is not None:
        return unknown
    registrar = WebhookRegistrar(
        s, rt.adapters.endpoints_for, rt.adapters.secrets, rt.control, rt.clock
    )
    registrar.sync_gate = _sync_gate(rt)
    try:
        return _register_action(args, registrar)
    except RegistrationError as exc:
        _print({"status": "refused", "reason": exc.reason, "error": str(exc)})
        return EXIT_REFUSED


def _register_action(args: argparse.Namespace, registrar: WebhookRegistrar) -> int:
    if args.action == "health":
        health = registrar.health(args.resource)
        _print([asdict(h) | {"healthy": h.healthy} for h in health])
        return EXIT_OK if health and all(h.healthy for h in health) else EXIT_UNHEALTHY
    if args.action == "retire":
        _print(asdict(registrar.retire(args.resource, args.slot, force=args.force)))
        return EXIT_OK
    if args.action == "remove":
        if args.confirm != args.resource:
            print("confirmation does not match the resource key", file=sys.stderr)
            return EXIT_USAGE
        _print([asdict(o) for o in registrar.remove_all(args.resource)])
        return EXIT_OK
    url = args.receiver_base_url.rstrip("/") + "/" + args.resource
    if args.action == "rotate":
        _print(asdict(registrar.rotate(args.resource, url)))
        return EXIT_OK
    outcomes = registrar.ensure(args.resource, url, repair=args.repair, force=args.force)
    _print([asdict(o) for o in outcomes])
    if any(o.action == "repair_pending" for o in outcomes):
        return EXIT_PENDING
    return EXIT_OK if all(o.healthy for o in outcomes) else EXIT_UNHEALTHY


def _dispatch(args: argparse.Namespace, rt: Runtime) -> int:
    s = rt.settings
    if args.command == "register":
        return _register(args, rt)

    if args.command == "relay-secret":
        name = s.relay_signing_secret_name
        if rt.adapters.secrets.get_secret(name):
            _print({"secret_name": name, "action": "unchanged"})
            return 0
        value = "whsec_" + base64.b64encode(pysecrets.token_bytes(32)).decode()
        version = rt.adapters.secrets.set_secret(name, value, tags={"purpose": "relay-signing"})
        _print({"secret_name": name, "action": "created", "secret_version": version})
        return 0

    if args.command == "replay":
        if args.action == "outbox":
            report = ReplayService(rt.registry, rt.clock).replay_outbox(
                operator=args.operator,
                reason=args.reason,
                status=args.status,
                since=args.since,
                until=args.until,
                dry_run=not args.execute,
            )
            _print(asdict(report))
            return 0
        source = s.incoming_queue if args.queue == "incoming" else s.relay_queue
        poison = replay_poison_queue(
            rt.adapters.queue_admin,
            poison_queue=f"{source}-poison",
            target_queue=source,
            operator=args.operator,
            reason=args.reason,
            dry_run=not args.execute,
        )
        _print(asdict(poison))
        return 0

    if args.command == "quarantine":
        rows = rt.adapters.store.query(s.table_quarantine, Query(equals={"resolved": False}))
        _print(
            [
                {
                    "partition": e.partition_key,
                    "row": e.row_key,
                    "reason": e.data.get("reason"),
                    "batch_id": e.data.get("batch_id"),
                }
                for e in rows
            ]
        )
        return 0

    if args.command == "reconcile":
        keys = [args.resource] if args.resource else [r.key for r in s.resources]
        _print([asdict(rt.reconciler.run(k)) for k in keys])
        return 0

    if args.command == "indicators":
        window = dict(STANDARD_WINDOWS)[args.window]
        result = compute_indicators(
            [j.record for j in rt.registry.terminal_jobs()],
            [j.record for j in rt.registry.open_jobs()],
            now=rt.clock.now(),
            window=window,
            window_name=args.window,
        )
        _print([asdict(i) for i in result])
        return 0

    if args.command == "backfill":
        from foundry_batch.metrics.backfill import backfill

        backfill_report = backfill(
            s,
            rt.registry,
            rt.adapters.batch_apis,
            rt.clock,
            args.resource,
            max_pages=args.max_pages,
        )
        _print(asdict(backfill_report))
        return 0

    if args.command == "queued-tokens":
        _print([asdict(q) for q in queued_tokens(j.record for j in rt.registry.open_jobs())])
        return 0

    if args.command in ("submit", "recover-submissions"):
        from foundry_batch.foundry.submission import (
            SubmissionError,
            SubmissionPending,
            SubmissionService,
        )

        service = SubmissionService(
            s,
            rt.adapters.store,
            rt.registry,
            rt.control,
            rt.adapters.batch_apis,
            CharacterHeuristicEstimator(),
            rt.clock,
            processor=rt.processor,
        )
        if args.command == "submit":
            content = Path(args.file).read_bytes()
            try:
                job = service.submit(
                    args.resource, args.deployment, content, correlation_id=args.correlation_id
                )
            except SubmissionPending as exc:  # not definitely refused: never resubmit
                _print(
                    {
                        "status": "pending",
                        "correlation_id": exc.correlation_id,
                        "reason": exc.reason,
                        "next_step": "recover-submissions",
                    }
                )
                return 3
            except (SubmissionError, ProviderError) as exc:  # definitely not created
                _print({"status": "error", "error": str(exc)})
                return 1
            _print(
                {
                    "batch_id": job.batch_id,
                    "status": job.raw_status,
                    "correlation_id": job.correlation_id,
                    "webhook_covered": job.webhook_covered,
                    "token_estimate": job.token_estimate,
                }
            )
            return 0
        _print({"resolved": service.recover(args.resource, min_age_seconds=args.min_age_seconds)})
        return 0
    if args.command == "vault-sync":
        return _vault_sync(args, rt)
    return 2  # pragma: no cover - argparse guarantees a known command


def _vault_sync_service(rt: Runtime) -> VaultSyncService:
    if rt.adapters.client_vault is None:  # pragma: no cover - callers check first
        raise ValueError("no client vault configured")
    return VaultSyncService(
        rt.settings.environment,
        rt.adapters.store,
        rt.settings.table_control,
        rt.adapters.secrets,
        rt.adapters.client_vault,
        rt.clock,
    )


def _vault_sync(args: argparse.Namespace, rt: Runtime) -> int:
    s = rt.settings
    keys = [args.resource] if args.resource else [r.key for r in s.resources]
    unknown = _unknown_resources(s, keys)
    if unknown is not None:
        return unknown
    if rt.adapters.client_vault is None:
        _print(
            {
                "status": "not_configured",
                "reason": "no external vault adapter is configured; only the local "
                "emulator exists (BATCH_CLIENT_VAULT_BACKEND=emulator)",
            }
        )
        return EXIT_USAGE
    service = _vault_sync_service(rt)
    if args.action == "status":
        _print({key: [asdict(r) for r in service.status(key)] for key in keys})
        return EXIT_OK
    totals: dict[str, Any] = {}
    for key in keys:
        stats = service.run(key)
        unsettled = [_unsettled(r) for r in service.status(key) if not r.converged]
        converged = not unsettled and not stats.failed and key not in stats.skipped
        totals[key] = asdict(stats) | {"converged": converged, "unsettled": unsettled}
    _print(totals)
    return EXIT_OK if all(t["converged"] for t in totals.values()) else EXIT_UNHEALTHY


def _unsettled(record: SyncRecord) -> dict[str, Any]:
    return {
        "slot": record.slot,
        "endpoint_id": record.endpoint_id,
        "state": record.state,
        "last_error": record.last_error,
        "owed": len(record.owed),
    }


def _local_demo(name: str) -> int:
    from foundry_batch.emulator.local import SCENARIOS, run_scenario, run_vault_rotation

    names = [*SCENARIOS, "vault-rotation"] if name == "all" else [name]
    unknown = [n for n in names if n not in SCENARIOS and n != "vault-rotation"]
    if unknown:
        known = [*SCENARIOS, "vault-rotation"]
        _print({"error": f"unknown scenario {unknown[0]!r}", "known": known})
        return 2
    reports = [
        run_vault_rotation() if n == "vault-rotation" else run_scenario(SCENARIOS[n]).to_dict()
        for n in names
    ]
    _print(reports)
    return 0 if all(r["passed"] for r in reports) else 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
