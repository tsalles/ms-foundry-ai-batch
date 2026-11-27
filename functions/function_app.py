"""Azure Functions (Python v2 model) entry points. All logic lives in ``foundry_batch``.

Routes and triggers:
* POST /api/webhooks/{resource_key}  anonymous by design: authenticated by the signature
* queue %BATCH_INCOMING_QUEUE%        processor (defers with visibility delay when paced)
* queue %BATCH_RELAY_QUEUE%           relay nudge -> outbox dispatch
* timer %BATCH_SWEEP_SCHEDULE%        reconciliation sweep + accounting
* timer %BATCH_OUTBOX_SCHEDULE%       due outbox items + queue depth signals
* POST /api/demo/events              demo downstream (relay signature required)
* GET  /api/health                   readiness (function key), booleans only
"""

from __future__ import annotations

import json
import logging
import os

import azure.functions as func

from foundry_batch.composition import get_runtime
from foundry_batch.metrics.api_pressure import log_api_pressure
from foundry_batch.observability import log_event
from foundry_batch.webhook.http import HttpRequest, HttpResponse

logger = logging.getLogger("foundry_batch.functions")


def _configure_telemetry() -> None:
    if not os.environ.get("APPLICATIONINSIGHTS_CONNECTION_STRING"):
        return
    from azure.identity import ManagedIdentityCredential
    from azure.monitor.opentelemetry import configure_azure_monitor

    configure_azure_monitor(
        credential=ManagedIdentityCredential(client_id=os.environ.get("AZURE_CLIENT_ID")),
        logger_name="foundry_batch",
    )
    logging.getLogger("foundry_batch").setLevel(logging.INFO)


_configure_telemetry()
app = func.FunctionApp()


def _to_request(req: func.HttpRequest) -> HttpRequest:
    return HttpRequest(
        method=req.method,
        body=req.get_body(),
        headers={k.lower(): v for k, v in req.headers.items()},
        route_params=dict(req.route_params),
    )


def _to_response(resp: HttpResponse) -> func.HttpResponse:
    return func.HttpResponse(body=resp.body, status_code=resp.status, headers=dict(resp.headers))


@app.route(route="webhooks/{resource_key}", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
def webhook(req: func.HttpRequest) -> func.HttpResponse:
    return _to_response(get_runtime().receiver.handle(_to_request(req)))


@app.queue_trigger(arg_name="msg", queue_name="%BATCH_INCOMING_QUEUE%", connection="BATCH_STORAGE")
def process_event(msg: func.QueueMessage) -> None:
    get_runtime().processor.handle_queue_message(msg.get_body().decode("utf-8"))


@app.queue_trigger(arg_name="msg", queue_name="%BATCH_RELAY_QUEUE%", connection="BATCH_STORAGE")
def relay_event(msg: func.QueueMessage) -> None:
    get_runtime().dispatcher.dispatch_nudge(msg.get_body().decode("utf-8"))


@app.timer_trigger(arg_name="timer", schedule="%BATCH_SWEEP_SCHEDULE%", run_on_startup=False)
def reconcile(timer: func.TimerRequest) -> None:
    get_runtime().reconciler.run_all()


@app.timer_trigger(arg_name="timer", schedule="%BATCH_OUTBOX_SCHEDULE%", run_on_startup=False)
def outbox_and_queues(timer: func.TimerRequest) -> None:
    runtime = get_runtime()
    runtime.dispatcher.dispatch_due()
    settings = runtime.settings
    for queue in (
        settings.incoming_queue,
        f"{settings.incoming_queue}-poison",
        settings.relay_queue,
        f"{settings.relay_queue}-poison",
    ):
        try:
            depth = runtime.adapters.queue_admin.approximate_depth(queue)
        except Exception:  # noqa: BLE001 - a depth read must not stop the outbox timer
            log_event(logger, "ops.queue_depth_failed", level=logging.WARNING, queue=queue)
            continue
        log_event(logger, "ops.queue_depth", queue=queue, approximate_count=depth)
    log_api_pressure(runtime.adapters.counter, logger)


@app.route(route="demo/events", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
def demo_events(req: func.HttpRequest) -> func.HttpResponse:
    runtime = get_runtime()
    if runtime.settings.downstream_mode != "demo":
        return func.HttpResponse(status_code=404)
    return _to_response(runtime.demo_api.handle(_to_request(req)))


@app.route(route="health", methods=["GET"], auth_level=func.AuthLevel.FUNCTION)
def health(req: func.HttpRequest) -> func.HttpResponse:
    status = get_runtime().readiness()
    code = 200 if all(status.values()) else 503
    return func.HttpResponse(json.dumps(status), status_code=code, mimetype="application/json")
