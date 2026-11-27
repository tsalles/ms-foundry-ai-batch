"""Stand-ins for the Azure SDK names the adapters import (azure-core, azure-data-tables,
azure-storage-queue, azure-keyvault-secrets). Only the surface the adapters use is modelled;
the Azurite integration tests and CI exercise the real SDKs.
"""

from __future__ import annotations

import enum
import types
from typing import Any


class AzureError(Exception):
    pass


class HttpResponseError(AzureError):
    def __init__(
        self, message: str = "", *, status_code: int | None = None, error_code: str | None = None
    ) -> None:
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.error_code = error_code


class ResourceNotFoundError(HttpResponseError):
    pass


class ResourceExistsError(HttpResponseError):
    pass


class ResourceModifiedError(HttpResponseError):
    pass


class ServiceRequestError(AzureError):
    pass


class TableTransactionError(HttpResponseError):
    pass


class MatchConditions(enum.Enum):
    Unconditionally = 1
    IfNotModified = 2


class EdmType(enum.Enum):
    INT64 = "Edm.Int64"


class EntityProperty:
    def __init__(self, value: Any, edm_type: Any) -> None:
        self.value = value
        self.edm_type = edm_type


class UpdateMode(enum.Enum):
    REPLACE = "replace"
    MERGE = "merge"


class TableEntity(dict):  # type: ignore[type-arg]
    metadata: dict[str, Any]


def table_entity(data: dict[str, Any], etag: str) -> TableEntity:
    entity = TableEntity(data)
    entity.metadata = {"etag": etag}
    return entity


class _Placeholder:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.args, self.kwargs = args, kwargs


def modules() -> dict[str, types.ModuleType]:
    def module(name: str, **attrs: Any) -> types.ModuleType:
        mod = types.ModuleType(name)
        mod.__path__ = []  # type: ignore[attr-defined]
        for key, value in attrs.items():
            setattr(mod, key, value)
        return mod

    return {
        "azure": module("azure"),
        "azure.core": module("azure.core", MatchConditions=MatchConditions),
        "azure.core.exceptions": module(
            "azure.core.exceptions",
            AzureError=AzureError,
            HttpResponseError=HttpResponseError,
            ResourceNotFoundError=ResourceNotFoundError,
            ResourceExistsError=ResourceExistsError,
            ResourceModifiedError=ResourceModifiedError,
            ServiceRequestError=ServiceRequestError,
        ),
        "azure.data": module("azure.data"),
        "azure.data.tables": module(
            "azure.data.tables",
            EdmType=EdmType,
            EntityProperty=EntityProperty,
            TableServiceClient=_Placeholder,
            TableTransactionError=TableTransactionError,
            UpdateMode=UpdateMode,
        ),
        "azure.storage": module("azure.storage"),
        "azure.storage.queue": module(
            "azure.storage.queue", QueueClient=_Placeholder, QueueServiceClient=_Placeholder
        ),
        "azure.keyvault": module("azure.keyvault"),
        "azure.keyvault.secrets": module("azure.keyvault.secrets", SecretClient=_Placeholder),
    }
