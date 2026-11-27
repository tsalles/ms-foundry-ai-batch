"""Request authentication for the Foundry data plane.

Microsoft Entra ID is the default. API keys are allowed only through an explicit exception
(decision D-04, spike SP-3) and are never an automatic fallback.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from typing import Protocol


class AccessTokenLike(Protocol):
    @property
    def token(self) -> str: ...

    @property
    def expires_on(self) -> int: ...


class TokenCredentialLike(Protocol):
    def get_token(self, *scopes: str) -> AccessTokenLike: ...


class AuthProvider(Protocol):
    def headers(self) -> dict[str, str]: ...


class EntraTokenAuth:
    """Bearer token from an azure-identity credential, refreshed five minutes before expiry."""

    def __init__(
        self,
        credential: TokenCredentialLike,
        scope: str,
        *,
        now: Callable[[], float] = time.time,
    ) -> None:
        self._credential = credential
        self._scope = scope
        self._now = now
        self._token: AccessTokenLike | None = None
        self._lock = threading.Lock()

    def headers(self) -> dict[str, str]:
        with self._lock:
            if self._token is None or self._token.expires_on - 300 <= self._now():
                self._token = self._credential.get_token(self._scope)
            return {"authorization": f"Bearer {self._token.token}"}


class ApiKeyAuth:
    """Only constructed when the D-04 exception is approved (``--allow-api-key``)."""

    def __init__(self, key_provider: Callable[[], str]) -> None:
        self._key_provider = key_provider

    def headers(self) -> dict[str, str]:
        return {"api-key": self._key_provider()}


class StaticTokenAuth:
    """Placeholder bearer token for emulators only.

    Settings allow it only in a local environment with loopback or in-process emulator
    endpoints, so it can never be sent to a real service.
    """

    def __init__(self, token: str = "emulator-local-token") -> None:  # noqa: S107  # nosec B107
        self._token = token

    def headers(self) -> dict[str, str]:
        return {"authorization": "Bearer " + self._token}
