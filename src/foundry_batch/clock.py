"""Time source abstraction so every time-dependent rule is testable."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Protocol


class Clock(Protocol):
    def now(self) -> datetime:
        """Return the current time as an aware UTC datetime."""
        ...


class SystemClock:
    def now(self) -> datetime:
        return datetime.now(UTC)


class ManualClock:
    """Deterministic clock for tests and simulations."""

    def __init__(self, start: datetime) -> None:
        if start.tzinfo is None:
            raise ValueError("ManualClock needs an aware datetime")
        self._now = start.astimezone(UTC)

    def now(self) -> datetime:
        return self._now

    def advance(self, seconds: float) -> datetime:
        self._now = self._now + timedelta(seconds=seconds)
        return self._now

    def set(self, value: datetime) -> None:
        self._now = value.astimezone(UTC)


def from_epoch(value: object) -> datetime | None:
    """Convert a provider epoch-seconds value to UTC; ``None``/invalid/zero become ``None``."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int | float) and value > 0:
        return datetime.fromtimestamp(float(value), tz=UTC)
    if isinstance(value, str) and value.strip().isdigit() and int(value) > 0:
        return datetime.fromtimestamp(int(value), tz=UTC)
    return None


ISO_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"


def to_iso(value: datetime | None) -> str | None:
    """Fixed-precision UTC text so stored timestamps sort correctly as strings."""
    return None if value is None else value.astimezone(UTC).strftime(ISO_FORMAT)


def from_iso(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(UTC)
