"""Input-file readiness for batch submission (US-5.1, CON-B08).

Microsoft Learn (batch how-to, checked 17 Nov 2026) says to track an uploaded input file until
its status is ``processed`` before creating the batch. That creating a batch from a file that is
still ``pending`` is refused (HTTP 400) is an assumption to confirm in a spike; waiting first is
correct either way.

``FileApi`` is the Files capability the submission harness needs beyond ``ports.BatchApi``.
``FoundryRestClient`` implements it; port doubles that do not are treated as "no file status".
"""

from __future__ import annotations

import math
from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta
from typing import Literal, Protocol, runtime_checkable

from foundry_batch.clock import Clock
from foundry_batch.errors import ProviderNotFoundError, ProviderRetryableError

#: The file can be used to create a batch.
FILE_READY = frozenset({"processed"})
#: The file will never be usable. The service documents ``error``; the rest are defensive.
FILE_UNUSABLE = frozenset({"error", "failed", "deleting", "deleted", "not_found"})
#: Microsoft Learn's sample polls the file status every 15 seconds.
FILE_POLL_SECONDS = 15.0
#: How long ``submit`` waits for ``processed`` before reporting a recoverable pending outcome.
FILE_WAIT_TIMEOUT_SECONDS = 600.0

FileReadiness = Literal["processed", "unusable", "timeout"]


@dataclass(frozen=True)
class ProviderFile:
    file_id: str
    status: str
    expires_at: int | None = None


@runtime_checkable
class FileApi(Protocol):
    def retrieve_file(self, file_id: str) -> ProviderFile: ...

    def delete_file(self, file_id: str) -> None: ...


@dataclass(frozen=True)
class FileWait:
    outcome: FileReadiness
    status: str  # last status read; "unknown" if every read failed transiently
    polls: int


def wait_until_processed(
    api: FileApi,
    file_id: str,
    clock: Clock,
    sleep: Callable[[float], None],
    *,
    timeout_s: float = FILE_WAIT_TIMEOUT_SECONDS,
    poll_s: float = FILE_POLL_SECONDS,
) -> FileWait:
    """Poll the file until it is processed or unusable, or the clock passes ``timeout_s``.

    Transient read failures (throttling, 5xx, network) keep polling; other request errors
    propagate. The number of polls is bounded even if ``sleep`` does not move ``clock``.
    """
    if poll_s <= 0 or timeout_s < 0:
        raise ValueError("poll_s must be positive and timeout_s not negative")
    deadline = clock.now() + timedelta(seconds=timeout_s)
    status = "unknown"
    polls = 0
    for polls in range(1, math.ceil(timeout_s / poll_s) + 2):
        status = _read_status(api, file_id, status)
        if status in FILE_READY:
            return FileWait("processed", status, polls)
        if status in FILE_UNUSABLE:
            return FileWait("unusable", status, polls)
        remaining = (deadline - clock.now()).total_seconds()
        if remaining <= 0:
            break
        sleep(min(poll_s, remaining))
    return FileWait("timeout", status, polls)


def _read_status(api: FileApi, file_id: str, previous: str) -> str:
    try:
        return api.retrieve_file(file_id).status
    except ProviderNotFoundError:
        return "not_found"
    except ProviderRetryableError:
        return previous
