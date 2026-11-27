"""Signing-secret lookup per resource with a short cache and a throttled refresh."""

from __future__ import annotations

import logging
from dataclasses import dataclass, replace
from datetime import datetime

from foundry_batch.clock import Clock
from foundry_batch.config import Settings
from foundry_batch.errors import SecretUnavailableError
from foundry_batch.observability import log_event
from foundry_batch.ports import SecretReader

logger = logging.getLogger(__name__)


@dataclass
class _CacheEntry:
    secrets: tuple[str, ...]
    loaded_at: datetime
    attempted_at: datetime


class SigningSecretResolver:
    """Return every configured slot value for a resource (current and previous during rotation).

    A failed verification may force one refresh, at most once per ``min_refresh_seconds`` per
    resource, so a newly rotated secret is picked up without letting bad requests hammer Key
    Vault. If Key Vault cannot be read, the last known values keep serving (stale-while-error)
    and the next read is attempted after ``min_refresh_seconds``; with nothing cached the
    ``SecretUnavailableError`` error propagates so the receiver answers 503.
    """

    def __init__(
        self,
        reader: SecretReader,
        settings: Settings,
        clock: Clock,
        *,
        min_refresh_seconds: int = 30,
    ) -> None:
        self._reader = reader
        self._settings = settings
        self._clock = clock
        self._min_refresh = min_refresh_seconds
        self._cache: dict[str, _CacheEntry] = {}

    def secrets_for(self, resource_key: str) -> tuple[str, ...]:
        entry = self._cache.get(resource_key)
        if entry is None:
            return self._load(resource_key)
        now = self._clock.now()
        expired = (now - entry.loaded_at).total_seconds() >= self._settings.secret_cache_ttl_seconds
        if expired and (now - entry.attempted_at).total_seconds() >= self._min_refresh:
            return self._load(resource_key)
        return entry.secrets

    def refresh_if_allowed(self, resource_key: str) -> tuple[str, ...] | None:
        entry = self._cache.get(resource_key)
        now = self._clock.now()
        if entry is not None and (now - entry.attempted_at).total_seconds() < self._min_refresh:
            return None
        return self._load(resource_key)

    def _load(self, resource_key: str) -> tuple[str, ...]:
        resource = self._settings.resource(resource_key)
        if resource is None:
            return ()
        now = self._clock.now()
        values: list[str] = []
        try:
            for name in resource.signing_secret_names(self._settings.environment):
                value = self._reader.get_secret(name)
                if value:
                    values.append(value)
        except SecretUnavailableError:
            entry = self._cache.get(resource_key)
            if entry is None:
                raise
            self._cache[resource_key] = replace(entry, attempted_at=now)
            log_event(
                logger,
                "webhook.secrets_stale",
                level=logging.WARNING,
                resource_key=resource_key,
                age_s=round((now - entry.loaded_at).total_seconds(), 1),
            )
            return entry.secrets
        self._cache[resource_key] = _CacheEntry(tuple(values), now, now)
        log_event(
            logger,
            "webhook.secrets_loaded",
            level=logging.DEBUG,
            resource_key=resource_key,
            slots_available=len(values),
        )
        return tuple(values)
