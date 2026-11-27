"""Typed settings loaded from environment variables (the contract with Terraform).

Every name read here is set by ``infra/envs/poc`` on the Function App. ``Settings.from_env``
validates everything at start-up and reports all problems at once, without echoing values.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Literal
from urllib.parse import urlsplit

from foundry_batch.errors import ConfigurationError

_ENV_RE = re.compile(r"^[a-z][a-z0-9]{1,9}$")
_RESOURCE_KEY_RE = re.compile(r"^[a-z][a-z0-9]{1,11}$")
_TABLE_RE = re.compile(r"^[A-Za-z][A-Za-z0-9]{2,62}$")
_QUEUE_RE = re.compile(r"^[a-z0-9](?:[a-z0-9]|-(?=[a-z0-9])){2,62}$")

#: Documented minimum gap between status calls for one job (Microsoft Learn, batch how-to).
STATUS_CALL_FLOOR_SECONDS = 60
#: Storage Queue messages are limited to 64 KiB; keep room for the envelope metadata.
MAX_ALLOWED_BODY_BYTES = 48 * 1024

#: Environments where local doubles (emulators, in-memory state) may be selected. The cloud
#: environments (e.g. ``poc``) can never select them, and neither can an Azure-hosted process.
LOCAL_ENVIRONMENTS = frozenset({"local", "dev", "test", "ci"})
LOOPBACK_HOSTS = frozenset({"127.0.0.1", "localhost", "::1"})
#: Platform variables of App Service and Functions (Windows, Linux, Consumption, Flex). Any of
#: them means "hosted on Azure". Flex/Linux workers may leave WEBSITE_INSTANCE_ID empty.
HOSTING_VARIABLES = ("WEBSITE_INSTANCE_ID", "WEBSITE_SITE_NAME", "CONTAINER_NAME")
#: Special-use names (RFC 2606, RFC 6761) that Azure never assigns as WEBSITE_HOSTNAME; local
#: stacks use them for in-process routes (e.g. ``func.emulator.invalid``).
_RESERVED_SUFFIXES = (".invalid", ".test", ".example", ".localhost")
_RESERVED_DOMAINS = ("example.com", "example.net", "example.org")
_TRUE, _FALSE = frozenset({"true", "1", "yes"}), frozenset({"false", "0", "no"})

BATCH_EVENT_TYPES: tuple[str, ...] = (
    "batch.completed",
    "batch.failed",
    "batch.cancelled",
    "batch.expired",
)


@dataclass(frozen=True)
class ResourceConfig:
    """One Foundry / Azure OpenAI resource that sends webhooks and is reconciled."""

    key: str
    endpoint: str
    region: str
    deployments: tuple[str, ...] = ()

    def signing_secret_names(self, environment: str) -> tuple[str, str]:
        """Two Key Vault slots per resource so a secret can be rotated without downtime."""
        base = f"whsec-{environment}-{self.key}"
        return (f"{base}-a", f"{base}-b")

    def endpoint_name(self, environment: str, slot: str) -> str:
        return f"batch-{environment}-{self.key}-{slot}"


@dataclass(frozen=True)
class Settings:
    environment: str
    resources: tuple[ResourceConfig, ...]
    key_vault_uri: str = ""
    queue_service_uri: str = ""
    table_service_uri: str = ""
    incoming_queue: str = "batch-events"
    relay_queue: str = "relay-dispatch"
    table_jobs: str = "batchjobs"
    table_control: str = "batchcontrol"
    table_quarantine: str = "batchquarantine"
    table_submissions: str = "batchsubmissions"
    table_demo_receipts: str = "demoreceipts"
    downstream_mode: Literal["demo", "http"] = "demo"
    downstream_url: str = ""
    relay_signing_secret_name: str = ""
    max_body_bytes: int = 32 * 1024
    timestamp_tolerance_seconds: int = 300
    receiver_rate_per_second: float = 50.0
    receiver_burst: int = 200
    unknown_job_policy: Literal["quarantine", "adopt"] = "quarantine"
    #: How long an event for a job the registry does not know yet is deferred before quarantine
    #: (covers a webhook that beats submission linking or submission recovery).
    unknown_job_grace_seconds: int = 1800
    status_call_floor_seconds: int = STATUS_CALL_FLOOR_SECONDS
    sweep_page_size: int = 100
    sweep_max_calls: int = 200
    sweep_lease_seconds: int = 600
    sweep_overlap_seconds: int = 3600
    missed_event_grace_seconds: int = 1800
    long_running_threshold_seconds: int = 24 * 3600
    outbox_max_attempts: int = 12
    outbox_base_delay_seconds: float = 5.0
    outbox_max_delay_seconds: float = 900.0
    #: Transient failures are retried for at least this long before quarantine (TC-D05).
    outbox_retry_horizon_seconds: int = 6 * 3600
    secret_cache_ttl_seconds: int = 300
    batch_api_token_scope: str = "https://cognitiveservices.azure.com/.default"  # noqa: S105
    website_hostname: str = ""
    #: Backends: the defaults are the real services. Anything else is a local double.
    provider_backend: Literal["foundry", "emulator"] = "foundry"
    storage_backend: Literal["azure", "memory"] = "azure"
    secrets_backend: Literal["keyvault", "memory"] = "keyvault"
    foundry_auth: Literal["entra", "emulator"] = "entra"
    client_vault_backend: Literal["none", "emulator"] = "none"
    #: Running on Azure App Service/Functions (see ``hosting_signals``).
    hosted: bool = False
    #: REQ-I03: retire and repair wait for the vault-sync ledger even without a client-vault
    #: adapter on this side (e.g. an AWS pull worker). App setting only, never set by Terraform.
    vault_sync_required: bool = False
    extra: Mapping[str, str] = field(default_factory=dict)

    @property
    def local_profile(self) -> bool:
        """True only for a local environment outside Azure hosting."""
        return self.environment in LOCAL_ENVIRONMENTS and not self.hosted

    @property
    def requires_vault_verification(self) -> bool:
        return self.vault_sync_required or (not self.local_profile and self.environment != "poc")

    @property
    def uses_local_doubles(self) -> bool:
        return (
            self.provider_backend != "foundry"
            or self.storage_backend != "azure"
            or self.secrets_backend != "keyvault"
            or self.foundry_auth != "entra"
            or self.client_vault_backend != "none"
        )

    def resource(self, key: str) -> ResourceConfig | None:
        return next((r for r in self.resources if r.key == key), None)

    def effective_downstream_url(self) -> str:
        if self.downstream_url:
            return self.downstream_url
        if self.downstream_mode == "demo" and self.website_hostname:
            return f"https://{self.website_hostname}/api/demo/events"
        return ""

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> Settings:
        problems: list[str] = []

        def text(name: str, default: str = "", *, required: bool = False) -> str:
            value = env.get(name, default).strip()
            if required and not value:
                problems.append(f"{name} is required")
            return value

        def integer(name: str, default: int, low: int, high: int) -> int:
            raw = env.get(name)
            if raw is None or raw.strip() == "":
                return default
            try:
                value = int(raw)
            except ValueError:
                problems.append(f"{name} must be an integer")
                return default
            if not low <= value <= high:
                problems.append(f"{name} must be between {low} and {high}")
                return default
            return value

        def number(name: str, default: float, low: float, high: float) -> float:
            raw = env.get(name)
            if raw is None or raw.strip() == "":
                return default
            try:
                value = float(raw)
            except ValueError:
                problems.append(f"{name} must be a number")
                return default
            if not low <= value <= high:
                problems.append(f"{name} must be between {low} and {high}")
                return default
            return value

        def flag(name: str, default: bool) -> bool:
            value = text(name).lower()
            if not value:
                return default
            if value not in _TRUE | _FALSE:
                problems.append(f"{name} must be true or false")
                return default
            return value in _TRUE

        environment = text("BATCH_ENVIRONMENT", required=True)
        if environment and not _ENV_RE.match(environment):
            problems.append("BATCH_ENVIRONMENT must be 2-10 lowercase letters/digits")
        signals = hosting_signals(env)
        hosted = bool(signals)
        local = environment in LOCAL_ENVIRONMENTS and not hosted
        where = f" (Azure hosting detected: {', '.join(signals)})" if signals else ""

        def choice(name: str, default: str, allowed: tuple[str, ...]) -> str:
            value = text(name, default)
            if value not in allowed:
                problems.append(f"{name} must be one of: {', '.join(allowed)}")
                return default
            if value != default and not local:
                problems.append(
                    f"{name}={value} is a local double: allowed only when BATCH_ENVIRONMENT is "
                    f"one of {', '.join(sorted(LOCAL_ENVIRONMENTS))} outside Azure hosting{where}"
                )
                return default
            return value

        provider_backend = choice("BATCH_PROVIDER_BACKEND", "foundry", ("foundry", "emulator"))
        storage_backend = choice("BATCH_STORAGE_BACKEND", "azure", ("azure", "memory"))
        secrets_backend = choice("BATCH_SECRETS_BACKEND", "keyvault", ("keyvault", "memory"))
        foundry_auth = choice("BATCH_FOUNDRY_AUTH", "entra", ("entra", "emulator"))
        client_vault_backend = choice("BATCH_CLIENT_VAULT_BACKEND", "none", ("none", "emulator"))

        resources = _parse_resources(
            env.get("BATCH_RESOURCES_JSON", ""), problems, allow_loopback=local
        )
        if (
            foundry_auth == "emulator"
            and provider_backend == "foundry"
            and not all(is_loopback_url(resource.endpoint) for resource in resources)
        ):
            problems.append(
                "BATCH_FOUNDRY_AUTH=emulator needs loopback emulator endpoints for every "
                "resource: never send a placeholder token to a real service"
            )

        queues = {
            "BATCH_INCOMING_QUEUE": text("BATCH_INCOMING_QUEUE", "batch-events"),
            "BATCH_RELAY_QUEUE": text("BATCH_RELAY_QUEUE", "relay-dispatch"),
        }
        for name, value in queues.items():
            if not _QUEUE_RE.match(value):
                problems.append(f"{name} is not a valid queue name")
        tables = {
            "BATCH_TABLE_JOBS": text("BATCH_TABLE_JOBS", "batchjobs"),
            "BATCH_TABLE_CONTROL": text("BATCH_TABLE_CONTROL", "batchcontrol"),
            "BATCH_TABLE_QUARANTINE": text("BATCH_TABLE_QUARANTINE", "batchquarantine"),
            "BATCH_TABLE_SUBMISSIONS": text("BATCH_TABLE_SUBMISSIONS", "batchsubmissions"),
            "BATCH_TABLE_DEMO_RECEIPTS": text("BATCH_TABLE_DEMO_RECEIPTS", "demoreceipts"),
        }
        for name, value in tables.items():
            if not _TABLE_RE.match(value):
                problems.append(f"{name} is not a valid table name")

        mode = text("BATCH_DOWNSTREAM_MODE", "demo")
        if mode not in ("demo", "http"):
            problems.append("BATCH_DOWNSTREAM_MODE must be 'demo' or 'http'")
        downstream_url = text("BATCH_DOWNSTREAM_URL")
        loopback_ok = local and is_loopback_url(downstream_url)
        if downstream_url and not downstream_url.startswith("https://") and not loopback_ok:
            problems.append("BATCH_DOWNSTREAM_URL must use https")
        if mode == "http" and not downstream_url:
            problems.append("BATCH_DOWNSTREAM_URL is required when BATCH_DOWNSTREAM_MODE=http")

        policy = text("BATCH_UNKNOWN_JOB_POLICY", "quarantine")
        if policy not in ("quarantine", "adopt"):
            problems.append("BATCH_UNKNOWN_JOB_POLICY must be 'quarantine' or 'adopt'")

        floor = integer(
            "BATCH_STATUS_CALL_FLOOR_SECONDS",
            STATUS_CALL_FLOOR_SECONDS,
            STATUS_CALL_FLOOR_SECONDS,
            3600,
        )

        settings = cls(
            environment=environment,
            resources=resources,
            key_vault_uri=text("BATCH_KEY_VAULT_URI"),
            queue_service_uri=text("BATCH_STORAGE__queueServiceUri"),
            table_service_uri=text("BATCH_STORAGE__tableServiceUri"),
            incoming_queue=queues["BATCH_INCOMING_QUEUE"],
            relay_queue=queues["BATCH_RELAY_QUEUE"],
            table_jobs=tables["BATCH_TABLE_JOBS"],
            table_control=tables["BATCH_TABLE_CONTROL"],
            table_quarantine=tables["BATCH_TABLE_QUARANTINE"],
            table_submissions=tables["BATCH_TABLE_SUBMISSIONS"],
            table_demo_receipts=tables["BATCH_TABLE_DEMO_RECEIPTS"],
            downstream_mode="http" if mode == "http" else "demo",
            downstream_url=downstream_url,
            relay_signing_secret_name=text(
                "BATCH_RELAY_SIGNING_SECRET_NAME", f"relay-signing-{environment}"
            ),
            max_body_bytes=integer("BATCH_MAX_BODY_BYTES", 32 * 1024, 1024, MAX_ALLOWED_BODY_BYTES),
            timestamp_tolerance_seconds=integer("BATCH_TIMESTAMP_TOLERANCE_SECONDS", 300, 30, 900),
            receiver_rate_per_second=number("BATCH_RECEIVER_RATE_PER_SECOND", 50.0, 1.0, 10_000.0),
            receiver_burst=integer("BATCH_RECEIVER_BURST", 200, 1, 100_000),
            unknown_job_policy="adopt" if policy == "adopt" else "quarantine",
            unknown_job_grace_seconds=integer("BATCH_UNKNOWN_JOB_GRACE_SECONDS", 1800, 0, 21600),
            status_call_floor_seconds=floor,
            sweep_page_size=integer("BATCH_SWEEP_PAGE_SIZE", 100, 1, 100),
            sweep_max_calls=integer("BATCH_SWEEP_MAX_CALLS", 200, 1, 10_000),
            sweep_lease_seconds=integer("BATCH_SWEEP_LEASE_SECONDS", 600, 60, 3600),
            sweep_overlap_seconds=integer("BATCH_SWEEP_OVERLAP_SECONDS", 3600, 0, 7 * 86400),
            missed_event_grace_seconds=integer("BATCH_MISSED_EVENT_GRACE_SECONDS", 1800, 60, 86400),
            long_running_threshold_seconds=integer(
                "BATCH_LONG_RUNNING_THRESHOLD_SECONDS", 86400, 3600, 7 * 86400
            ),
            outbox_max_attempts=integer("BATCH_OUTBOX_MAX_ATTEMPTS", 12, 1, 100),
            outbox_base_delay_seconds=number("BATCH_OUTBOX_BASE_DELAY_SECONDS", 5.0, 0.1, 600.0),
            outbox_max_delay_seconds=number("BATCH_OUTBOX_MAX_DELAY_SECONDS", 900.0, 1.0, 86400.0),
            outbox_retry_horizon_seconds=integer(
                "BATCH_OUTBOX_RETRY_HORIZON_SECONDS", 6 * 3600, 0, 14 * 86400
            ),
            secret_cache_ttl_seconds=integer("BATCH_SECRET_CACHE_TTL_SECONDS", 300, 0, 3600),
            batch_api_token_scope=text(
                "BATCH_API_TOKEN_SCOPE", "https://cognitiveservices.azure.com/.default"
            ),
            website_hostname=text("WEBSITE_HOSTNAME"),
            provider_backend="emulator" if provider_backend == "emulator" else "foundry",
            storage_backend="memory" if storage_backend == "memory" else "azure",
            secrets_backend="memory" if secrets_backend == "memory" else "keyvault",
            foundry_auth="emulator" if foundry_auth == "emulator" else "entra",
            client_vault_backend="emulator" if client_vault_backend == "emulator" else "none",
            hosted=hosted,
            vault_sync_required=flag("BATCH_VAULT_SYNC_REQUIRED", not local),
        )
        if problems:
            raise ConfigurationError("Invalid settings: " + "; ".join(problems))
        return settings


def _parse_resources(
    raw: str, problems: list[str], *, allow_loopback: bool = False
) -> tuple[ResourceConfig, ...]:
    if not raw.strip():
        problems.append("BATCH_RESOURCES_JSON is required")
        return ()
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        problems.append("BATCH_RESOURCES_JSON is not valid JSON")
        return ()
    if not isinstance(data, list) or not data:
        problems.append("BATCH_RESOURCES_JSON must be a non-empty JSON array")
        return ()
    resources: list[ResourceConfig] = []
    seen: set[str] = set()
    for index, item in enumerate(data):
        if not isinstance(item, dict):
            problems.append(f"BATCH_RESOURCES_JSON[{index}] must be an object")
            continue
        key = str(item.get("key", ""))
        endpoint = str(item.get("endpoint", ""))
        region = str(item.get("region", ""))
        deployments = item.get("deployments", [])
        if not _RESOURCE_KEY_RE.match(key):
            problems.append(
                f"BATCH_RESOURCES_JSON[{index}].key must match {_RESOURCE_KEY_RE.pattern}"
            )
        elif key in seen:
            problems.append(f"BATCH_RESOURCES_JSON has a duplicate key '{key}'")
        seen.add(key)
        loopback = allow_loopback and is_loopback_url(endpoint)
        if not endpoint.startswith("https://") and not loopback:
            problems.append(f"BATCH_RESOURCES_JSON[{index}].endpoint must use https")
        if not region:
            problems.append(f"BATCH_RESOURCES_JSON[{index}].region is required")
        if not isinstance(deployments, list) or not all(isinstance(d, str) for d in deployments):
            problems.append(f"BATCH_RESOURCES_JSON[{index}].deployments must be a list of strings")
            deployments = []
        resources.append(
            ResourceConfig(
                key=key,
                endpoint=endpoint if endpoint.endswith("/") else endpoint + "/",
                region=region,
                deployments=tuple(deployments),
            )
        )
    return tuple(resources)


def is_loopback_url(url: str) -> bool:
    """``http(s)://`` to 127.0.0.1, localhost or ::1 on any port. Decided on the parsed host:
    no userinfo (``127.0.0.1@host`` goes to ``host``) and a numeric port, so the authority is
    exactly ``host[:port]`` and no HTTP client can read another host out of it."""
    try:
        parts = urlsplit(url)
        _ = parts.port  # raises ValueError for a port that is not a number
    except ValueError:
        return False
    return (
        parts.scheme in ("http", "https")
        and "@" not in parts.netloc
        and (parts.hostname or "") in LOOPBACK_HOSTS
    )


def hosting_signals(env: Mapping[str, str]) -> tuple[str, ...]:
    """Names (never values) of the variables showing that the process runs on Azure hosting.

    ``WEBSITE_HOSTNAME`` counts unless it is loopback (Core Tools sets ``localhost:7071``) or a
    special-use name Azure never assigns; an unparsable value counts (fail closed).
    """
    found = [name for name in HOSTING_VARIABLES if env.get(name, "").strip()]
    hostname = env.get("WEBSITE_HOSTNAME", "").strip()
    if hostname:
        host = _host_of(hostname)
        if host is None or not (host in LOOPBACK_HOSTS or _special_use(host)):
            found.append("WEBSITE_HOSTNAME")
    return tuple(found)


def functions_host_is_local(env: Mapping[str, str]) -> bool:
    """The Functions host runs under Core Tools on this machine: ``WEBSITE_HOSTNAME`` is
    loopback and no Azure platform variable is set. Stricter than ``hosting_signals``."""
    if any(env.get(name, "").strip() for name in HOSTING_VARIABLES):
        return False
    host = _host_of(env.get("WEBSITE_HOSTNAME", "").strip())
    return host is not None and host in LOOPBACK_HOSTS


def _host_of(value: str) -> str | None:
    """Host of a ``host[:port]`` value, or ``None`` when it is anything else."""
    try:
        parts = urlsplit("//" + value)
        _ = parts.port
    except ValueError:
        return None
    if parts.netloc != value or "@" in value:
        return None  # a path, query, userinfo or stripped characters: not a plain host
    return parts.hostname or None


def _special_use(host: str) -> bool:
    return host.endswith(_RESERVED_SUFFIXES) or any(
        host == domain or host.endswith("." + domain) for domain in _RESERVED_DOMAINS
    )
