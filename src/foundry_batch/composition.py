"""Composition root: wires settings, adapters and services.

``adapters_from_settings`` is the single switch between real services and local doubles.
With default settings it returns ``azure_adapters`` (identity-based Azure clients, imported
lazily so the core never needs the SDKs). Local doubles are selected only by explicit
``BATCH_*_BACKEND`` settings, which ``Settings`` refuses outside local environments.
``memory_adapters`` gives a fully local stack (tests, demos, simulations). The Functions entry
path (``runtime_from_env``) additionally needs Core Tools on loopback for any local double.
"""

from __future__ import annotations

import base64
import os
import secrets as pysecrets
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit

from foundry_batch.adapters.memory import (
    MemoryQueue,
    MemoryQueueAdmin,
    MemorySecrets,
    MemoryTableStore,
    RecordingDownstream,
)
from foundry_batch.clock import Clock, SystemClock
from foundry_batch.config import Settings, functions_host_is_local
from foundry_batch.control import ControlStore
from foundry_batch.errors import ConfigurationError, SecretUnavailableError
from foundry_batch.metrics.api_pressure import ApiCallCounter
from foundry_batch.ports import (
    BatchApiFactory,
    ClientVault,
    Downstream,
    MessageQueue,
    QueueAdmin,
    SecretWriter,
    TableStore,
    WebhookEndpointsApi,
)
from foundry_batch.processing.processor import EventProcessor
from foundry_batch.processing.registry import JobRegistry
from foundry_batch.reconcile.sweeper import Reconciler
from foundry_batch.relay.demo_api import DemoDownstreamApi
from foundry_batch.relay.dispatcher import OutboxDispatcher
from foundry_batch.webhook.receiver import WebhookReceiver
from foundry_batch.webhook.secrets import SigningSecretResolver

if TYPE_CHECKING:
    import random

    from foundry_batch.emulator.foundry import FoundryEmulator
    from foundry_batch.emulator.transport import FaultQueue, WireHandler
    from foundry_batch.foundry.auth import TokenCredentialLike


@dataclass
class Adapters:
    store: TableStore
    incoming_queue: MessageQueue
    relay_queue: MessageQueue
    queue_admin: QueueAdmin
    secrets: SecretWriter
    batch_apis: BatchApiFactory
    endpoints_for: Callable[[str], WebhookEndpointsApi]
    downstream: Downstream
    counter: ApiCallCounter = field(default_factory=ApiCallCounter)
    #: Optional destination for signing-secret synchronization.
    client_vault: ClientVault | None = None


@dataclass
class Runtime:
    settings: Settings
    adapters: Adapters
    clock: Clock
    registry: JobRegistry
    control: ControlStore
    receiver: WebhookReceiver
    processor: EventProcessor
    dispatcher: OutboxDispatcher
    reconciler: Reconciler
    demo_api: DemoDownstreamApi
    secret_resolver: SigningSecretResolver

    def relay_secret(self) -> str | None:
        return self.adapters.secrets.get_secret(self.settings.relay_signing_secret_name)

    def readiness(self) -> dict[str, bool]:
        """Per resource: at least one signing secret readable and decodable, the slot the
        receiver can verify with; the relay secret likewise. Never returns values.

        A secret-store outage reports ``False`` (503 from the health route), never an error.
        """
        from foundry_batch.webhook.signature import secret_is_usable

        resolver = self.secret_resolver
        status: dict[str, bool] = {}
        for r in self.settings.resources:
            try:
                slots = resolver.refresh_if_allowed(r.key) or resolver.secrets_for(r.key)
                status[f"signing_secret:{r.key}"] = any(secret_is_usable(s) for s in slots)
            except SecretUnavailableError:
                status[f"signing_secret:{r.key}"] = False
        try:
            relay = self.relay_secret()
            status["relay_secret"] = relay is not None and secret_is_usable(relay)
        except SecretUnavailableError:
            status["relay_secret"] = False
        return status


def build_runtime(
    settings: Settings,
    adapters: Adapters,
    clock: Clock | None = None,
    *,
    rng: random.Random | None = None,
) -> Runtime:
    """``rng`` seeds the relay's retry jitter so a local stack is deterministic; by default the
    jitter is random, as a cloud deployment needs."""
    clock = clock or SystemClock()
    registry = JobRegistry(adapters.store, settings, clock)
    control = ControlStore(adapters.store, settings.table_control, settings.environment, clock)
    resolver = SigningSecretResolver(adapters.secrets, settings, clock)
    processor = EventProcessor(
        settings,
        registry,
        adapters.store,
        adapters.batch_apis,
        adapters.relay_queue,
        clock,
        incoming_queue=adapters.incoming_queue,
    )
    secrets = adapters.secrets

    def relay_secret() -> str | None:
        return secrets.get_secret(settings.relay_signing_secret_name)

    return Runtime(
        settings=settings,
        adapters=adapters,
        clock=clock,
        registry=registry,
        control=control,
        receiver=WebhookReceiver(settings, adapters.incoming_queue, resolver, clock),
        processor=processor,
        dispatcher=OutboxDispatcher(settings, registry, adapters.downstream, clock, rng=rng),
        reconciler=Reconciler(settings, registry, processor, control, adapters.batch_apis, clock),
        demo_api=DemoDownstreamApi(
            adapters.store,
            settings.table_demo_receipts,
            relay_secret,
            clock,
            tolerance_seconds=settings.timestamp_tolerance_seconds,
        ),
        secret_resolver=resolver,
    )


def memory_adapters(
    batch_apis: BatchApiFactory,
    endpoints_for: Callable[[str], WebhookEndpointsApi],
    *,
    secrets: Mapping[str, str] | None = None,
    downstream: Downstream | None = None,
) -> Adapters:
    incoming, relay = MemoryQueue("batch-events"), MemoryQueue("relay-dispatch")
    poison_in = MemoryQueue("batch-events-poison")
    poison_relay = MemoryQueue("relay-dispatch-poison")
    return Adapters(
        store=MemoryTableStore(),
        incoming_queue=incoming,
        relay_queue=relay,
        queue_admin=MemoryQueueAdmin(
            {
                "batch-events": incoming,
                "relay-dispatch": relay,
                "batch-events-poison": poison_in,
                "relay-dispatch-poison": poison_relay,
            }
        ),
        secrets=MemorySecrets(secrets),
        batch_apis=batch_apis,
        endpoints_for=endpoints_for,
        downstream=downstream or RecordingDownstream(),
    )


def new_webhook_secret() -> str:
    """A random Standard Webhooks secret for local stacks (never for cloud environments)."""
    return "whsec_" + base64.b64encode(pysecrets.token_bytes(32)).decode("ascii")


def adapters_from_settings(
    settings: Settings,
    *,
    allow_api_key: bool = False,
    clock: Clock | None = None,
    emulators: Mapping[str, FoundryEmulator] | None = None,
    routes: Mapping[str, WireHandler] | None = None,
    faults: FaultQueue | None = None,
    credential_factory: Callable[[], TokenCredentialLike] | None = None,
) -> Adapters:
    """Real adapters by default; local doubles only when settings select them explicitly.

    Supported local combinations (all need ``Settings.local_profile``):

    * ``BATCH_PROVIDER_BACKEND=emulator``: in-process Foundry emulators behind the real REST
      client; ``emulators``/``routes`` let a ``LocalStack`` share them and its handlers.
    * ``BATCH_FOUNDRY_AUTH=emulator``: real REST client to a loopback ``EmulatorHttpServer``.
    * Neither: the real Foundry with Entra authentication, for spikes from a workstation.

    Local state (``memory`` storage and secrets) is required with any local double: mixing
    real Azure state with emulated services is refused rather than guessed.
    """
    if not settings.uses_local_doubles:
        return azure_adapters(settings, allow_api_key=allow_api_key)  # pragma: no cover
    if not settings.local_profile:  # defence in depth; Settings already refuses this
        raise ConfigurationError("local doubles are not allowed in this environment")
    if settings.storage_backend != "memory" or settings.secrets_backend != "memory":
        raise ConfigurationError(
            "local doubles need BATCH_STORAGE_BACKEND=memory and BATCH_SECRETS_BACKEND=memory"
        )
    if allow_api_key:
        raise ConfigurationError("API keys are not used with local doubles")
    import requests

    from foundry_batch.emulator.foundry import FoundryEmulator
    from foundry_batch.emulator.transport import foundry_handler, routed_session
    from foundry_batch.foundry.auth import AuthProvider, EntraTokenAuth, StaticTokenAuth
    from foundry_batch.foundry.rest_client import FoundryClientFactory, FoundryRestClient
    from foundry_batch.relay.http_downstream import HttpDownstream

    clock = clock or SystemClock()
    all_routes: dict[str, WireHandler] = dict(routes or {})
    auth: AuthProvider
    if settings.provider_backend == "emulator":
        hub = dict(emulators or {})
        for resource in settings.resources:
            emulator = hub.setdefault(
                resource.key,
                FoundryEmulator(resource.key, clock, deployments=resource.deployments or ()),
            )
            all_routes[urlsplit(resource.endpoint).hostname or ""] = foundry_handler(emulator)
        auth = StaticTokenAuth()
    elif settings.foundry_auth == "emulator":
        auth = StaticTokenAuth()  # Settings guarantee loopback endpoints
    else:  # pragma: no cover - needs azure-identity and a signed-in workstation
        from azure.identity import DefaultAzureCredential

        credential = credential_factory() if credential_factory else DefaultAzureCredential()
        auth = EntraTokenAuth(credential, settings.batch_api_token_scope)
    session = routed_session(all_routes, faults) if all_routes else requests.Session()
    counter = ApiCallCounter()
    clients = {
        r.key: FoundryRestClient(
            r.key, r.endpoint, auth, session=session, counter=counter, clock=clock
        )
        for r in settings.resources
    }
    relay_name = settings.relay_signing_secret_name
    adapters = memory_adapters(
        FoundryClientFactory(clients),
        lambda key: clients[key],
        secrets={relay_name: new_webhook_secret()},
    )
    adapters.downstream = HttpDownstream(
        settings.effective_downstream_url() or "https://downstream.emulator.invalid/",
        lambda: adapters.secrets.get_secret(relay_name),
        clock,
        session=session,
    )
    adapters.counter = counter
    if settings.client_vault_backend == "emulator":
        from foundry_batch.emulator.client_vault import EmulatedClientVault

        adapters.client_vault = EmulatedClientVault()
    return adapters


def azure_adapters(  # pragma: no cover - needs the Azure SDKs; exercised in integration
    settings: Settings, *, allow_api_key: bool = False
) -> Adapters:
    """Identity-based Azure clients. ``allow_api_key`` requires the D-04 exception."""
    from azure.data.tables import TableServiceClient
    from azure.keyvault.secrets import SecretClient
    from azure.storage.queue import QueueServiceClient

    from foundry_batch.adapters.azure_keyvault import KeyVaultSecrets
    from foundry_batch.adapters.azure_queues import (
        AzureMessageQueue,
        AzureQueueAdmin,
        receiver_queue_client,
    )
    from foundry_batch.adapters.azure_tables import AzureTableStore
    from foundry_batch.foundry.auth import ApiKeyAuth, AuthProvider, EntraTokenAuth
    from foundry_batch.foundry.rest_client import FoundryClientFactory, FoundryRestClient
    from foundry_batch.relay.http_downstream import HttpDownstream

    credential = azure_credential(settings, os.environ.get("AZURE_CLIENT_ID"))
    tables = TableServiceClient(endpoint=settings.table_service_uri, credential=credential)
    queues = QueueServiceClient(account_url=settings.queue_service_uri, credential=credential)
    # Own client with a small retry budget: the receiver acknowledges within seconds.
    incoming = receiver_queue_client(
        settings.queue_service_uri, settings.incoming_queue, credential
    )
    secrets = KeyVaultSecrets(SecretClient(vault_url=settings.key_vault_uri, credential=credential))
    counter = ApiCallCounter()

    def auth_for(resource_key: str) -> AuthProvider:
        if allow_api_key:
            name = f"foundry-key-{settings.environment}-{resource_key}"

            def key() -> str:
                value = secrets.get_secret(name)
                if not value:
                    raise RuntimeError(f"API key secret {name} missing")
                return value

            return ApiKeyAuth(key)
        return EntraTokenAuth(credential, settings.batch_api_token_scope)

    clock = SystemClock()
    clients = {
        r.key: FoundryRestClient(r.key, r.endpoint, auth_for(r.key), counter=counter, clock=clock)
        for r in settings.resources
    }
    downstream = HttpDownstream(
        settings.effective_downstream_url() or "https://localhost.invalid/",
        lambda: secrets.get_secret(settings.relay_signing_secret_name),
        clock,
    )
    return Adapters(
        store=AzureTableStore(tables),
        incoming_queue=AzureMessageQueue(incoming),
        relay_queue=AzureMessageQueue(queues.get_queue_client(settings.relay_queue)),
        queue_admin=AzureQueueAdmin(queues),
        secrets=secrets,
        batch_apis=FoundryClientFactory(clients),
        endpoints_for=lambda key: clients[key],
        downstream=downstream,
        counter=counter,
    )


def azure_credential(settings: Settings, client_id: str | None) -> Any:
    """The app's user-assigned managed identity when hosted on Azure (``Settings.hosted``
    covers Flex Consumption, where WEBSITE_INSTANCE_ID may be empty), else the developer chain."""
    from azure.identity import DefaultAzureCredential, ManagedIdentityCredential

    if client_id and settings.hosted:
        return ManagedIdentityCredential(client_id=client_id)
    return DefaultAzureCredential()


def runtime_from_env(env: Mapping[str, str]) -> Runtime:
    """The Functions entry path. Local doubles need Core Tools on loopback, not just a local
    ``BATCH_ENVIRONMENT``: one portal-added app setting must never swap a hosted app's state."""
    settings = Settings.from_env(env)
    if settings.uses_local_doubles and not functions_host_is_local(env):
        raise ConfigurationError(
            "local doubles in the Functions host need Core Tools on loopback "
            "(WEBSITE_HOSTNAME=localhost:<port>) and no Azure hosting variables"
        )
    return build_runtime(settings, adapters_from_settings(settings))


_RUNTIME: Runtime | None = None


def get_runtime() -> Runtime:  # pragma: no cover - process-level singleton for Functions
    global _RUNTIME
    if _RUNTIME is None:
        _RUNTIME = runtime_from_env(os.environ)
    return _RUNTIME
