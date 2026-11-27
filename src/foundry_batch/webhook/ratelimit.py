"""Per-instance token bucket. Platform-level limits depend on the hosting decision (D-02)."""

from __future__ import annotations

from datetime import datetime

from foundry_batch.clock import Clock


class TokenBucket:
    def __init__(self, rate_per_second: float, burst: int, clock: Clock) -> None:
        if rate_per_second <= 0 or burst <= 0:
            raise ValueError("rate and burst must be positive")
        self._rate = rate_per_second
        self._burst = float(burst)
        self._clock = clock
        self._tokens: dict[str, float] = {}
        self._updated: dict[str, datetime] = {}

    def try_acquire(self, key: str) -> float:
        """Take one token. Return 0 when allowed, else the seconds until a token is available."""
        now = self._clock.now()
        tokens = self._tokens.get(key, self._burst)
        last = self._updated.get(key, now)
        tokens = min(self._burst, tokens + (now - last).total_seconds() * self._rate)
        self._updated[key] = now
        if tokens >= 1.0:
            self._tokens[key] = tokens - 1.0
            return 0.0
        self._tokens[key] = tokens
        return (1.0 - tokens) / self._rate
