"""Exception hierarchy shared by the core and the adapters."""

from __future__ import annotations


class FoundryBatchError(Exception):
    """Base class for all errors raised by this package."""


class ConfigurationError(FoundryBatchError):
    """Settings are missing or invalid. Fail fast at start-up."""


class ConcurrencyConflictError(FoundryBatchError):
    """An optimistic-concurrency check failed (ETag mismatch or entity already exists)."""


class StoreUnavailableError(FoundryBatchError):
    """The durable store could not be reached or the outcome of a write is unknown."""


class QueueUnavailableError(FoundryBatchError):
    """A queue send failed or its outcome is unknown. Callers must treat it as retryable."""


class SecretUnavailableError(FoundryBatchError):
    """The secret store could not be read (throttling, outage, network). Retryable."""


class ProviderError(FoundryBatchError):
    """Base class for errors returned by the Azure OpenAI (Foundry) data plane."""

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class ProviderRetryableError(ProviderError):
    """408, 429, 5xx or a network failure where retrying later is safe."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        retry_after_seconds: float | None = None,
    ) -> None:
        super().__init__(message, status_code=status_code)
        self.retry_after_seconds = retry_after_seconds


class ProviderAmbiguousError(ProviderRetryableError):
    """The request may or may not have taken effect (for example a timeout after sending)."""


class ProviderRequestError(ProviderError):
    """A non-retryable client error (4xx other than 408/429)."""


class ProviderNotFoundError(ProviderRequestError):
    """The requested provider object does not exist (404)."""


class EndpointSecretMissingError(ProviderRequestError):
    """A webhook endpoint was created but the response carried no signing secret.

    ``endpoint_id`` identifies the orphan so the caller can delete it (compensation).
    """

    def __init__(self, message: str, *, endpoint_id: str, status_code: int | None = None) -> None:
        super().__init__(message, status_code=status_code)
        self.endpoint_id = endpoint_id


class RetryLater(FoundryBatchError):  # noqa: N818 - a control-flow signal, not an error (PEP 8)
    """Processing must be retried after ``delay_seconds`` (pacing floor, transient dependency)."""

    def __init__(self, reason: str, delay_seconds: float) -> None:
        super().__init__(f"{reason} (retry in {delay_seconds:.0f}s)")
        self.reason = reason
        self.delay_seconds = delay_seconds


class CompensationFailedError(FoundryBatchError):
    """A compensating action failed and left external state that needs an operator."""
