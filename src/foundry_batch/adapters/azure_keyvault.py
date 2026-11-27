"""Key Vault adapter. Values are never logged; disabled or missing secrets read as ``None``.

A 403 means "disabled" only when Key Vault says so with the error code ``SecretDisabled``. Every
other 403 (RBAC, firewall, "Public access is disabled ...", private link) is an outage: reading
it as "no secret" would drop cached secrets and block vault sync.
"""

from __future__ import annotations

import json
from collections.abc import Iterator, Mapping
from typing import Any

from azure.core.exceptions import AzureError, HttpResponseError, ResourceNotFoundError
from azure.keyvault.secrets import SecretClient

from foundry_batch.errors import SecretUnavailableError

DISABLED_ERROR_CODE = "SecretDisabled"


class KeyVaultSecrets:
    def __init__(self, client: SecretClient) -> None:
        self._client = client

    def get_secret(self, name: str) -> str | None:
        return self._value(name, None)

    def get_secret_version(self, name: str, version: str) -> str | None:
        """Exact version for synchronization: the latest may already be a newer rotation."""
        return self._value(name, version)

    def _value(self, name: str, version: str | None) -> str | None:
        try:
            secret = self._client.get_secret(name, version)
        except ResourceNotFoundError:
            return None
        except HttpResponseError as exc:
            if is_secret_disabled(exc):
                return None
            raise SecretUnavailableError(f"get_secret failed ({exc.status_code})") from exc
        except AzureError as exc:  # network, timeout, credential
            raise SecretUnavailableError(f"get_secret failed ({type(exc).__name__})") from exc
        if secret.properties.enabled is False:
            return None
        return secret.value

    def set_secret(self, name: str, value: str, *, tags: Mapping[str, str]) -> str:
        result = self._client.set_secret(
            name, value, tags=dict(tags), content_type="application/vnd.batch.webhook-secret"
        )
        return str(result.properties.version)

    def get_secret_tags(self, name: str) -> Mapping[str, str] | None:
        try:
            secret = self._client.get_secret(name)
        except ResourceNotFoundError:
            return None
        except HttpResponseError as exc:
            if is_secret_disabled(exc):
                return None
            # Throttling or an outage is not "no secret": health checks must not trigger repair.
            raise SecretUnavailableError(f"get_secret failed ({exc.status_code})") from exc
        except AzureError as exc:
            raise SecretUnavailableError(f"get_secret failed ({type(exc).__name__})") from exc
        if secret.properties.enabled is False:
            return None
        return dict(secret.properties.tags or {})

    def disable_secret(self, name: str) -> None:
        try:
            self._client.update_secret_properties(name, enabled=False)
        except ResourceNotFoundError:
            return


def is_secret_disabled(exc: HttpResponseError) -> bool:
    """403 with the error code ``SecretDisabled`` (top-level or inner), read from the parsed
    OData error (``exc.error``), the generated model (``exc.model``) or the JSON body."""
    if exc.status_code != 403:
        return False
    sources = (getattr(exc, "error", None), getattr(exc, "model", None), _body(exc))
    return any(DISABLED_ERROR_CODE in _codes(source) for source in sources)


def _codes(node: Any, depth: int = 0) -> Iterator[str]:
    """Error codes of an OData error: ``code``, then ``innererror``/``inner_error``, nested."""
    if node is None or depth > 5:
        return
    if isinstance(node, Mapping):
        if "error" in node and depth == 0:
            yield from _codes(node["error"], depth + 1)
            return
        code, inner = node.get("code"), node.get("innererror") or node.get("inner_error")
    else:
        if getattr(node, "error", None) is not None and depth == 0:  # generated KeyVaultError
            yield from _codes(node.error, depth + 1)
            return
        code = getattr(node, "code", None)
        inner = getattr(node, "innererror", None) or getattr(node, "inner_error", None)
    if isinstance(code, str):
        yield code
    yield from _codes(inner, depth + 1)


def _body(exc: HttpResponseError) -> Any:
    response = getattr(exc, "response", None)
    try:
        text = response.text() if response is not None else ""
        return json.loads(text) if text else None
    except Exception:  # noqa: BLE001 - an unreadable body is simply not "disabled"
        return None
