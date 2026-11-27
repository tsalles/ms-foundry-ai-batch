"""Client-side counters of provider calls by operation and status class (US-6.5).

These do not depend on platform metrics (SP-6): every list page, retrieve, enrichment, retry,
upload and cancel is counted where the call is made.
"""

from __future__ import annotations

import logging
import threading
from collections import Counter
from dataclasses import dataclass

from foundry_batch.observability import log_event

logger = logging.getLogger(__name__)


def status_class(status_code: int | None) -> str:
    if status_code is None:
        return "network"
    if status_code == 429:
        return "429"
    if status_code >= 500:
        return "5xx"
    if status_code >= 400:
        return "4xx"
    return "2xx"


@dataclass(frozen=True)
class CallKey:
    resource_key: str
    operation: str
    status_class: str


class ApiCallCounter:
    def __init__(self) -> None:
        self._counts: Counter[CallKey] = Counter()
        self._lock = threading.Lock()

    def record(self, resource_key: str, operation: str, status_code: int | None) -> None:
        classification = status_class(status_code)
        with self._lock:
            self._counts[CallKey(resource_key, operation, classification)] += 1
        log_event(
            logger,
            "api.calls",
            resource_key=resource_key,
            operation=operation,
            status_class=classification,
            count=1,
        )

    def total(
        self,
        *,
        resource_key: str | None = None,
        operation_prefix: str | None = None,
        status: str | None = None,
    ) -> int:
        with self._lock:
            return sum(
                count
                for key, count in self._counts.items()
                if (resource_key is None or key.resource_key == resource_key)
                and (operation_prefix is None or key.operation.startswith(operation_prefix))
                and (status is None or key.status_class == status)
            )

    def snapshot(self) -> dict[CallKey, int]:
        with self._lock:
            return dict(self._counts)

    def reset(self) -> dict[CallKey, int]:
        with self._lock:
            current = dict(self._counts)
            self._counts.clear()
            return current


def log_api_pressure(counter: ApiCallCounter, logger: logging.Logger) -> int:
    """Emit process-local summaries separately from the per-call measurement events."""
    snapshot = counter.reset()
    for key, count in sorted(snapshot.items(), key=lambda kv: tuple(vars(kv[0]).values())):
        log_event(
            logger,
            "api.calls.summary",
            resource_key=key.resource_key,
            operation=key.operation,
            status_class=key.status_class,
            count=count,
        )
    return len(snapshot)
