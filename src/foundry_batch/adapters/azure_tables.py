"""Azure Table Storage adapter (identity-based; shared keys are disabled on the account)."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from azure.core import MatchConditions
from azure.core.exceptions import (
    AzureError,
    ResourceExistsError,
    ResourceModifiedError,
    ResourceNotFoundError,
)
from azure.data.tables import (
    EdmType,
    EntityProperty,
    TableServiceClient,
    TableTransactionError,
    UpdateMode,
)

from foundry_batch.errors import ConcurrencyConflictError, StoreUnavailableError
from foundry_batch.ports import Entity, Query, TxOp

_CONFLICT_CODES = frozenset(
    {"EntityAlreadyExists", "UpdateConditionNotSatisfied", "ResourceNotFound", "EntityNotFound"}
)
_INT32 = range(-(2**31), 2**31)
_IF_MATCH = MatchConditions.IfNotModified


def _to_table(data: Mapping[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in data.items():
        if isinstance(value, int) and not isinstance(value, bool) and value not in _INT32:
            out[key] = EntityProperty(value, EdmType.INT64)
        elif value is not None:
            out[key] = value
    return out


def _from_table(entity: Mapping[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in entity.items():
        if key in ("PartitionKey", "RowKey"):
            continue
        out[key] = value.value if isinstance(value, EntityProperty) else value
    return out


def _odata_prefix_bounds(prefix: str) -> tuple[str, str]:
    return prefix, prefix[:-1] + chr(ord(prefix[-1]) + 1)


class AzureTableStore:
    def __init__(self, service: TableServiceClient) -> None:
        self._service = service

    def get(self, table: str, partition_key: str, row_key: str) -> Entity | None:
        client = self._service.get_table_client(table)
        try:
            entity = client.get_entity(partition_key, row_key)
        except ResourceNotFoundError:
            return None
        except AzureError as exc:
            raise StoreUnavailableError(f"get {table}: {type(exc).__name__}") from exc
        return Entity(partition_key, row_key, _from_table(entity), entity.metadata.get("etag"))

    def query(self, table: str, query: Query) -> list[Entity]:
        clauses: list[str] = []
        params: dict[str, Any] = {}
        if query.partition_key is not None:
            clauses.append("PartitionKey eq @pk")
            params["pk"] = query.partition_key
        if query.row_key_prefix:
            low, high = _odata_prefix_bounds(query.row_key_prefix)
            clauses.append("RowKey ge @rklow and RowKey lt @rkhigh")
            params.update(rklow=low, rkhigh=high)
        for index, (name, value) in enumerate(sorted(query.equals.items())):
            clauses.append(f"{name} eq @eq{index}")
            params[f"eq{index}"] = value
        for index, (name, value) in enumerate(sorted(query.less_or_equal.items())):
            clauses.append(f"{name} le @le{index}")
            params[f"le{index}"] = value
        client = self._service.get_table_client(table)
        results: list[Entity] = []
        try:
            pages = (
                client.query_entities(" and ".join(clauses), parameters=params)
                if clauses
                else client.list_entities()
            )
            for entity in pages:
                results.append(
                    Entity(
                        str(entity["PartitionKey"]),
                        str(entity["RowKey"]),
                        _from_table(entity),
                        entity.metadata.get("etag"),
                    )
                )
                if query.limit is not None and len(results) >= query.limit:
                    break
        except AzureError as exc:
            raise StoreUnavailableError(f"query {table}: {type(exc).__name__}") from exc
        return results

    def submit(self, table: str, ops: Sequence[TxOp]) -> dict[str, str]:
        if not ops:
            return {}
        operations: list[Any] = []
        for op in ops:
            body = {
                "PartitionKey": op.entity.partition_key,
                "RowKey": op.entity.row_key,
                **_to_table(op.entity.data),
            }
            if op.kind == "create":
                operations.append(("create", body))
            elif op.kind == "upsert":
                operations.append(("upsert", body, {"mode": UpdateMode.REPLACE}))
            elif op.kind == "update":
                kwargs: dict[str, Any] = {"mode": UpdateMode.REPLACE}
                if not op.force:
                    kwargs.update(etag=op.entity.etag, match_condition=_IF_MATCH)
                operations.append(("update", body, kwargs))
            else:
                kwargs = {}
                if not op.force:
                    kwargs.update(etag=op.entity.etag, match_condition=_IF_MATCH)
                operations.append(("delete", body, kwargs))
        client = self._service.get_table_client(table)
        try:
            result = client.submit_transaction(operations)
        except TableTransactionError as exc:
            error_code = getattr(exc, "error_code", None)
            if error_code in _CONFLICT_CODES:
                raise ConcurrencyConflictError(f"{table}: {error_code}") from exc
            raise StoreUnavailableError(f"{table}: {error_code}") from exc
        except (ResourceExistsError, ResourceModifiedError, ResourceNotFoundError) as exc:
            raise ConcurrencyConflictError(f"{table}: {type(exc).__name__}") from exc
        except AzureError as exc:
            raise StoreUnavailableError(f"{table}: {type(exc).__name__}") from exc
        etags: dict[str, str] = {}
        for op, metadata in zip(ops, result, strict=False):
            if op.kind != "delete" and isinstance(metadata, Mapping) and metadata.get("etag"):
                etags[op.entity.row_key] = str(metadata["etag"])
        return etags
