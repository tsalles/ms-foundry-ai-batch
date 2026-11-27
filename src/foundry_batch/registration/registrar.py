"""Idempotent webhook registration with one-time secret capture into Key Vault.

Rules (US-2.1, US-2.2, plan section 3.4 item 5):

* endpoint names are deterministic (``batch-<env>-<resource>-<slot>``), so a rerun finds its own
  endpoints and never duplicates them; a changed URL or event list is applied as an update;
* the signing secret exists only in memory between the create response and the Key Vault
  write; it is never logged, printed, returned or passed on a command line;
* if the secret write fails, the new endpoint is deleted (compensation); if that fails too,
  ``CompensationFailedError`` is raised and logged as an error for an operator;
* an endpoint whose Key Vault slot does not carry its endpoint id is unhealthy; ``repair``
  replaces it through the rotation path because the secret cannot be read back;
* removing an endpoint (``retire``, ``repair``) waits for ``sync_gate`` (REQ-I03): the client's
  vault must verify the remaining key first, and a gate that cannot answer refuses;
* coverage of an endpoint that no longer exists is ended by every run (an interrupted retire
  must not leave a stale record active);
* concurrent runs for one resource are serialised with a lease;
* an ambiguous create (timeout) is resolved by listing before any retry.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, replace

from foundry_batch.clock import Clock, to_iso
from foundry_batch.config import BATCH_EVENT_TYPES, ResourceConfig, Settings
from foundry_batch.control import ControlStore, Lease, RegistrationRecord
from foundry_batch.errors import (
    CompensationFailedError,
    EndpointSecretMissingError,
    FoundryBatchError,
    ProviderAmbiguousError,
)
from foundry_batch.observability import log_event
from foundry_batch.ports import (
    CreatedWebhookEndpoint,
    SecretWriter,
    WebhookEndpoint,
    WebhookEndpointsApi,
)

logger = logging.getLogger(__name__)

SLOTS = ("a", "b")


class RegistrationError(FoundryBatchError):
    """Refused or rolled back; nothing was left half-done. ``reason`` is a stable code."""

    def __init__(self, message: str, *, reason: str = "registration_failed") -> None:
        super().__init__(message)
        self.reason = reason


@dataclass(frozen=True)
class SlotHealth:
    slot: str
    endpoint_id: str
    url: str
    event_types: tuple[str, ...]
    secret_present: bool
    secret_matches: bool

    @property
    def healthy(self) -> bool:
        return self.secret_present and self.secret_matches


@dataclass(frozen=True)
class RegistrationOutcome:
    resource_key: str
    action: str
    slot: str | None
    endpoint_id: str | None
    secret_name: str | None
    secret_version: str | None
    healthy: bool


class WebhookRegistrar:
    def __init__(
        self,
        settings: Settings,
        endpoints_for: Callable[[str], WebhookEndpointsApi],
        secrets: SecretWriter,
        control: ControlStore,
        clock: Clock,
        *,
        holder: str | None = None,
        sync_gate: Callable[[str, Sequence[str]], bool] | None = None,
    ) -> None:
        self._settings = settings
        self._endpoints_for = endpoints_for
        self._secrets = secrets
        self._control = control
        self._clock = clock
        self._holder = holder or f"registrar-{uuid.uuid4().hex[:8]}"
        self.sync_gate = sync_gate

    # -- public operations ----------------------------------------------------------------

    def ensure(
        self,
        resource_key: str,
        receiver_url: str,
        *,
        event_types: Sequence[str] = BATCH_EVENT_TYPES,
        repair: bool = False,
        force: bool = False,
    ) -> list[RegistrationOutcome]:
        """Create or update this resource's endpoints. ``repair`` replaces an unhealthy one; the
        old endpoint stays until ``sync_gate`` passes (``repair_pending``) unless ``force``."""
        resource = self._resource(resource_key)
        _check_url(receiver_url)
        with self._locked(resource_key):
            api = self._endpoints_for(resource_key)
            ours = self._ours(api, resource)
            for slot in SLOTS:
                name = self._secret_name(resource, slot)
                if slot not in ours and self._secrets.get_secret_tags(name) is not None:
                    self._secrets.disable_secret(name)
            self._end_stale_coverage(resource_key, [e.endpoint_id for e in ours.values()])
            if not ours:
                return [self._create(api, resource, "a", receiver_url, event_types, "created")]
            actions: dict[str, str] = {}
            for slot, endpoint in sorted(ours.items()):
                ours[slot], actions[slot] = self._apply(
                    api, endpoint, slot_url(receiver_url, slot), event_types
                )
            health = {slot: self._health(resource, slot, e) for slot, e in ours.items()}
            unhealthy = [slot for slot in sorted(ours) if not health[slot].healthy]
            if repair and unhealthy:
                return self._repair(
                    api, resource, ours, health, actions, receiver_url, event_types, force=force
                )
            return [
                self._report(resource, slot, ours[slot], actions[slot], health[slot])
                for slot in sorted(ours)
            ]

    def _apply(
        self,
        api: WebhookEndpointsApi,
        endpoint: WebhookEndpoint,
        url: str,
        event_types: Sequence[str],
    ) -> tuple[WebhookEndpoint, str]:
        if endpoint.url == url and set(endpoint.event_types) == set(event_types):
            return endpoint, "unchanged"
        updated = api.update_endpoint(
            endpoint.endpoint_id, name=endpoint.name, url=url, event_types=list(event_types)
        )
        return updated, "updated"

    def _report(
        self,
        resource: ResourceConfig,
        slot: str,
        endpoint: WebhookEndpoint,
        action: str,
        health: SlotHealth,
    ) -> RegistrationOutcome:
        if health.healthy:
            self._ensure_record(resource, slot, endpoint)
        log_event(
            logger,
            f"registration.{action}",
            resource_key=resource.key,
            slot=slot,
            endpoint_id=endpoint.endpoint_id,
            healthy=health.healthy,
        )
        return self._outcome(resource, slot, endpoint, action, health.healthy)

    def _ensure_record(
        self, resource: ResourceConfig, slot: str, endpoint: WebhookEndpoint
    ) -> None:
        """Re-create a missing coverage record (e.g. the first write failed after the secret
        was stored), so missed-event accounting and coverage windows stay correct."""
        for record in self._control.registrations(resource.key):
            if record.endpoint_id == endpoint.endpoint_id and record.active_until is None:
                return
        self._control.save_registration(
            RegistrationRecord(
                resource_key=resource.key,
                slot=slot,
                endpoint_id=endpoint.endpoint_id,
                endpoint_name=endpoint.name,
                url=endpoint.url,
                secret_name=self._secret_name(resource, slot),
                secret_version="unknown",  # noqa: S106  # nosec B106: version metadata, not a key
                active_from=self._clock.now(),
            )
        )
        log_event(
            logger,
            "registration.record_repaired",
            level=logging.WARNING,
            resource_key=resource.key,
            slot=slot,
            endpoint_id=endpoint.endpoint_id,
        )

    def rotate(
        self,
        resource_key: str,
        receiver_url: str,
        *,
        event_types: Sequence[str] = BATCH_EVENT_TYPES,
    ) -> RegistrationOutcome:
        """Add an endpoint in the free slot. The receiver accepts both secrets until ``retire``."""
        resource = self._resource(resource_key)
        _check_url(receiver_url)
        with self._locked(resource_key):
            api = self._endpoints_for(resource_key)
            ours = self._ours(api, resource)
            free = [s for s in SLOTS if s not in ours]
            if not free:
                raise RegistrationError(
                    "both slots are in use; retire one before rotating", reason="slots_in_use"
                )
            self._end_stale_coverage(resource_key, [e.endpoint_id for e in ours.values()])
            return self._create(api, resource, free[0], receiver_url, event_types, "rotated")

    def retire(self, resource_key: str, slot: str, *, force: bool = False) -> RegistrationOutcome:
        """Delete the endpoint in ``slot``, end its coverage, then disable its secret. A rerun
        after a partial retire finishes the coverage and secret steps."""
        if slot not in SLOTS:
            raise RegistrationError(f"unknown slot {slot!r}", reason="unknown_slot")
        resource = self._resource(resource_key)
        with self._locked(resource_key):
            api = self._endpoints_for(resource_key)
            ours = self._ours(api, resource)
            others = [s for s in ours if s != slot and self._health(resource, s, ours[s]).healthy]
            if not force and not others:
                raise RegistrationError(
                    "refusing to retire the only healthy endpoint", reason="only_healthy_endpoint"
                )
            remaining = [ours[s].endpoint_id for s in others]
            if not force and not self._gate_allows(resource_key, remaining):
                # REQ-I03: an AWS verifier still needs the old key until the new one is synced.
                raise RegistrationError(
                    "refusing to retire: the remaining key is not yet verified in the client vault",
                    reason="vault_sync_gate",
                )
            endpoint = ours.pop(slot, None)
            if endpoint is not None:
                api.delete_endpoint(endpoint.endpoint_id)
            self._end_stale_coverage(resource_key, [e.endpoint_id for e in ours.values()])
            self._secrets.disable_secret(self._secret_name(resource, slot))
            log_event(
                logger,
                "registration.retired",
                level=logging.WARNING,
                resource_key=resource_key,
                slot=slot,
                endpoint_id=endpoint.endpoint_id if endpoint else None,
            )
            return RegistrationOutcome(
                resource_key,
                "retired",
                slot,
                endpoint.endpoint_id if endpoint else None,
                self._secret_name(resource, slot),
                None,
                True,
            )

    def remove_all(self, resource_key: str) -> list[RegistrationOutcome]:
        """Teardown: unregister before compute or Key Vault removal (US-7.2 AC5)."""
        return [self.retire(resource_key, slot, force=True) for slot in SLOTS]

    def health(self, resource_key: str) -> list[SlotHealth]:
        resource = self._resource(resource_key)
        api = self._endpoints_for(resource_key)
        return [self._health(resource, s, e) for s, e in sorted(self._ours(api, resource).items())]

    # -- internals ------------------------------------------------------------------------

    def _create(
        self,
        api: WebhookEndpointsApi,
        resource: ResourceConfig,
        slot: str,
        url: str,
        event_types: Sequence[str],
        action: str,
    ) -> RegistrationOutcome:
        name = resource.endpoint_name(self._settings.environment, slot)
        url = slot_url(url, slot)
        try:
            created = self._create_endpoint(api, name, url, event_types)
        except EndpointSecretMissingError as exc:
            # Created without a usable secret: it would deliver events nobody can verify.
            if exc.endpoint_id:
                self._compensate(api, resource.key, exc.endpoint_id)
            else:
                self._delete_named(api, name)
            raise RegistrationError(
                "create returned no signing secret; endpoint removed", reason="create_compensated"
            ) from exc
        secret_name = self._secret_name(resource, slot)
        tags = {
            "endpoint_id": created.endpoint.endpoint_id,
            "endpoint_name": name,
            "resource_key": resource.key,
            "environment": self._settings.environment,
            "created_at": to_iso(self._clock.now()) or "",
        }
        endpoint = created.endpoint
        try:
            version = self._secrets.set_secret(secret_name, created.signing_secret, tags=tags)
        except Exception as exc:
            self._compensate(api, resource.key, endpoint.endpoint_id)
            raise RegistrationError(
                "secret write failed; new endpoint removed", reason="create_compensated"
            ) from exc
        finally:
            del created
        self._control.save_registration(
            RegistrationRecord(
                resource_key=resource.key,
                slot=slot,
                endpoint_id=endpoint.endpoint_id,
                endpoint_name=name,
                url=url,
                secret_name=secret_name,
                secret_version=version,
                active_from=self._clock.now(),
            )
        )
        log_event(
            logger,
            f"registration.{action}",
            resource_key=resource.key,
            slot=slot,
            endpoint_id=endpoint.endpoint_id,
            secret_name=secret_name,
            secret_version=version,
        )
        return RegistrationOutcome(
            resource.key, action, slot, endpoint.endpoint_id, secret_name, version, True
        )

    def _create_endpoint(
        self, api: WebhookEndpointsApi, name: str, url: str, event_types: Sequence[str]
    ) -> CreatedWebhookEndpoint:
        try:
            return api.create_endpoint(name=name, url=url, event_types=list(event_types))
        except ProviderAmbiguousError:
            # The create may have succeeded, but its one-time secret is lost: remove any
            # endpoint with our name, then create once more.
            self._delete_named(api, name)
            try:
                return api.create_endpoint(name=name, url=url, event_types=list(event_types))
            except ProviderAmbiguousError as exc:
                self._delete_named(api, name)
                raise RegistrationError(
                    "repeated ambiguous create compensated; rerun ensure",
                    reason="create_compensated",
                ) from exc

    def _delete_named(self, api: WebhookEndpointsApi, name: str) -> None:
        for endpoint in api.list_endpoints():
            if endpoint.name == name:
                api.delete_endpoint(endpoint.endpoint_id)
                log_event(
                    logger,
                    "registration.ambiguous_create_cleaned",
                    level=logging.WARNING,
                    endpoint_id=endpoint.endpoint_id,
                )

    def _compensate(self, api: WebhookEndpointsApi, resource_key: str, endpoint_id: str) -> None:
        try:
            api.delete_endpoint(endpoint_id)
        except Exception as exc:
            log_event(
                logger,
                "registration.compensation_failed",
                level=logging.ERROR,
                resource_key=resource_key,
                endpoint_id=endpoint_id,
            )
            raise CompensationFailedError(
                f"endpoint {endpoint_id} has no stored secret and could not be deleted"
            ) from exc
        log_event(
            logger,
            "registration.compensated",
            level=logging.WARNING,
            resource_key=resource_key,
            endpoint_id=endpoint_id,
        )

    def _repair(
        self,
        api: WebhookEndpointsApi,
        resource: ResourceConfig,
        ours: dict[str, WebhookEndpoint],
        health: dict[str, SlotHealth],
        actions: dict[str, str],
        url: str,
        event_types: Sequence[str],
        *,
        force: bool,
    ) -> list[RegistrationOutcome]:
        """Replace the unhealthy endpoint through the rotation path, gated like ``retire``.

        The replacement is the healthy other slot when there is one (e.g. created by an earlier
        ``repair_pending`` run), else a new endpoint in the free slot. The unhealthy endpoint
        is removed only once ``sync_gate`` verifies the replacement downstream, or ``force``.
        """
        broken = [slot for slot in sorted(ours) if not health[slot].healthy]
        kept = [slot for slot in sorted(ours) if health[slot].healthy]
        free = [slot for slot in SLOTS if slot not in ours]
        if not (kept or free):  # both slots exist and both are unhealthy
            raise RegistrationError(
                "cannot repair: no healthy or free slot; retire one slot first",
                reason="no_free_slot",
            )
        slot, endpoint = broken[0], ours[broken[0]]
        if kept:
            other = kept[0]
            replacement = self._report(resource, other, ours[other], actions[other], health[other])
        else:
            replacement = self._create(api, resource, free[0], url, event_types, "created")
        if not force and not self._gate_allows(resource.key, [replacement.endpoint_id or ""]):
            log_event(
                logger,
                "registration.repair_pending",
                level=logging.WARNING,
                resource_key=resource.key,
                slot=slot,
                endpoint_id=endpoint.endpoint_id,
                replacement_endpoint_id=replacement.endpoint_id,
            )
            pending = self._outcome(resource, slot, endpoint, "repair_pending", False)
            return [replacement, pending]
        api.delete_endpoint(endpoint.endpoint_id)
        live = [e.endpoint_id for s, e in ours.items() if s != slot]
        self._end_stale_coverage(resource.key, [*live, replacement.endpoint_id or ""])
        self._secrets.disable_secret(self._secret_name(resource, slot))
        log_event(
            logger,
            "registration.repaired",
            level=logging.WARNING,
            resource_key=resource.key,
            slot=slot,
            endpoint_id=endpoint.endpoint_id,
            replacement_endpoint_id=replacement.endpoint_id,
            forced=force,
        )
        return [replace(replacement, action="repaired")]

    def _gate_allows(self, resource_key: str, endpoint_ids: Sequence[str]) -> bool:
        """REQ-I03 gate. Missing or unavailable required verification fails closed."""
        if self.sync_gate is None:
            return not self._settings.requires_vault_verification
        try:
            return self.sync_gate(resource_key, list(endpoint_ids))
        except Exception as exc:
            log_event(
                logger,
                "registration.gate_unavailable",
                level=logging.ERROR,
                resource_key=resource_key,
                error=type(exc).__name__,
            )
            raise RegistrationError(
                "refusing: client-vault verification is unavailable",
                reason="vault_sync_verification_unavailable",
            ) from exc

    def _end_stale_coverage(self, resource_key: str, live_ids: Iterable[str]) -> None:
        """End the coverage window of every record whose endpoint is not live."""
        live = set(live_ids)
        for record in self._control.registrations(resource_key):
            if record.endpoint_id not in live and record.active_until is None:
                self._control.save_registration(replace(record, active_until=self._clock.now()))

    def _health(self, resource: ResourceConfig, slot: str, endpoint: WebhookEndpoint) -> SlotHealth:
        name = self._secret_name(resource, slot)
        tags = self._secrets.get_secret_tags(name)
        present = tags is not None and bool(self._secrets.get_secret(name))
        matches = tags is not None and tags.get("endpoint_id") == endpoint.endpoint_id
        return SlotHealth(
            slot, endpoint.endpoint_id, endpoint.url, endpoint.event_types, present, matches
        )

    def _ours(
        self, api: WebhookEndpointsApi, resource: ResourceConfig
    ) -> dict[str, WebhookEndpoint]:
        by_name = {resource.endpoint_name(self._settings.environment, s): s for s in SLOTS}
        found: dict[str, WebhookEndpoint] = {}
        for endpoint in api.list_endpoints():
            slot = by_name.get(endpoint.name)
            if slot is None:
                continue
            if slot in found:
                raise RegistrationError(
                    f"duplicate endpoints named {endpoint.name}; fix manually",
                    reason="duplicate_endpoints",
                )
            found[slot] = endpoint
        return found

    def _outcome(
        self,
        resource: ResourceConfig,
        slot: str,
        endpoint: WebhookEndpoint,
        action: str,
        healthy: bool,
    ) -> RegistrationOutcome:
        return RegistrationOutcome(
            resource.key,
            action,
            slot,
            endpoint.endpoint_id,
            self._secret_name(resource, slot),
            None,
            healthy,
        )

    def _secret_name(self, resource: ResourceConfig, slot: str) -> str:
        names = resource.signing_secret_names(self._settings.environment)
        return names[SLOTS.index(slot)]

    def _resource(self, resource_key: str) -> ResourceConfig:
        resource = self._settings.resource(resource_key)
        if resource is None:
            raise RegistrationError(
                f"resource {resource_key!r} is not in BATCH_RESOURCES_JSON",
                reason="unknown_resource",
            )
        return resource

    def _locked(self, resource_key: str) -> _LeaseContext:
        return _LeaseContext(self._control, f"register|{resource_key}", self._holder)


class _LeaseContext:
    def __init__(self, control: ControlStore, name: str, holder: str) -> None:
        self._control = control
        self._name = name
        self._holder = holder
        self._lease: Lease | None = None

    def __enter__(self) -> None:
        lease = self._control.acquire(self._name, self._holder, ttl_seconds=300)
        if lease is None:
            raise RegistrationError(
                "another registration run holds the lease; try again later", reason="lease_held"
            )
        self._lease = lease

    def __exit__(self, *_: object) -> None:
        if self._lease is not None:
            self._control.release(self._lease)


def _check_url(url: str) -> None:
    if not url.startswith("https://"):
        raise RegistrationError("receiver URL must use https", reason="invalid_url")


def slot_url(url: str, slot: str) -> str:
    """Slot a keeps the plain URL; other slots add ``slot=<slot>``. The provider rejects a second
    endpoint with an identical URL, and the receiver route ignores the query string."""
    if slot == "a":
        return url
    return f"{url}{'&' if '?' in url else '?'}slot={slot}"
