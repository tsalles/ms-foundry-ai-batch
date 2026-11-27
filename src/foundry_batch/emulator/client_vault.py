"""Emulated external secret store for local synchronization and rotation tests.

Models the semantics the synchronization design needs from any product: versioned writes,
idempotent replays of a request token, readback, and retirement that keeps history but makes
a version unusable. Faults cover an outage and a write that commits before its response is lost.
"""

from __future__ import annotations

import itertools
import threading
from collections.abc import Mapping
from dataclasses import dataclass, field

from foundry_batch.ports import VaultRecord


class ClientVaultUnavailableError(Exception):
    """The destination could not be reached or did not answer (outcome may be unknown)."""


@dataclass
class _Version:
    version: str
    value: str = field(repr=False)
    metadata: dict[str, str]
    request_token: str
    enabled: bool = True


class EmulatedClientVault:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._secrets: dict[str, list[_Version]] = {}
        self._ids = itertools.count(1)
        self.unavailable = False
        self.lose_next_put_response = False  # commit, then raise like a timeout
        self.writes = 0

    def put(self, name: str, value: str, *, request_token: str, metadata: Mapping[str, str]) -> str:
        with self._lock:
            if self.unavailable:
                raise ClientVaultUnavailableError("emulated outage")
            versions = self._secrets.setdefault(name, [])
            for existing in versions:
                if existing.request_token == request_token:
                    if existing.value != value:
                        raise ValueError("request token reused for a different value")
                    return existing.version  # idempotent replay
            version = f"dv{next(self._ids)}"
            versions.append(_Version(version, value, dict(metadata), request_token))
            self.writes += 1
            if self.lose_next_put_response:
                self.lose_next_put_response = False
                raise ClientVaultUnavailableError("emulated: committed, response lost")
            return version

    def find(self, name: str, request_token: str) -> VaultRecord | None:
        with self._lock:
            self._check()
            for item in self._secrets.get(name, []):
                if item.request_token == request_token:
                    return _record(name, item)
            return None

    def current(self, name: str) -> VaultRecord | None:
        with self._lock:
            self._check()
            versions = self._secrets.get(name)
            return _record(name, versions[-1]) if versions else None

    def read(self, name: str, version: str) -> str | None:
        with self._lock:
            self._check()
            for item in self._secrets.get(name, []):
                if item.version == version:
                    return item.value if item.enabled else None
            return None

    def retire(self, name: str, version: str) -> None:
        with self._lock:
            self._check()
            for item in self._secrets.get(name, []):
                if item.version == version:
                    item.enabled = False

    def names(self) -> list[str]:
        with self._lock:
            return sorted(self._secrets)

    def _check(self) -> None:
        if self.unavailable:
            raise ClientVaultUnavailableError("emulated outage")


def _record(name: str, item: _Version) -> VaultRecord:
    return VaultRecord(name, item.version, dict(item.metadata), item.enabled)
