"""Bounded exponential backoff with full jitter and Retry-After support."""

from __future__ import annotations

import random
import re
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime

#: Upper bound for a server-requested Retry-After (protects against absurd values).
MAX_RETRY_AFTER_SECONDS = 6 * 3600.0

#: RFC 9110 delta-seconds: ASCII digits only (``str.isdigit`` also accepts e.g. "²").
_DELTA_SECONDS = re.compile(r"[0-9]+")


def backoff_delay(
    attempt: int,
    *,
    base_seconds: float,
    max_seconds: float,
    rng: random.Random | None = None,
    retry_after_seconds: float | None = None,
) -> float:
    """Delay before retry number ``attempt`` (1-based): full jitter, Retry-After honoured.

    Full jitter draws uniformly from ``[0, min(max, base * 2^(attempt-1))]`` (AWS
    architecture guidance), which spreads synchronized retries best. A server's Retry-After
    is a floor and is honoured even above ``max_seconds``, up to ``MAX_RETRY_AFTER_SECONDS``.
    """
    if attempt < 1:
        raise ValueError("attempt is 1-based")
    ceiling = min(max_seconds, base_seconds * (2 ** min(attempt - 1, 32)))
    generator = rng or random.SystemRandom()
    delay = generator.uniform(0.0, ceiling)
    if retry_after_seconds is not None and retry_after_seconds > 0:
        delay = max(delay, min(retry_after_seconds, MAX_RETRY_AFTER_SECONDS))
    return delay


def parse_retry_after(value: str | None, *, now: datetime | None = None) -> float | None:
    """Parse ``Retry-After`` as delta-seconds or an HTTP date. Invalid values give ``None``.

    The result is capped at ``MAX_RETRY_AFTER_SECONDS`` so an absurd header can never overflow
    a later conversion (``float`` of a 400-digit number is ``inf``).
    """
    if not value:
        return None
    text = value.strip()
    if _DELTA_SECONDS.fullmatch(text):
        return float(min(int(text), int(MAX_RETRY_AFTER_SECONDS)))
    try:
        when = parsedate_to_datetime(text)
    except (TypeError, ValueError):
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=UTC)
    reference = now or datetime.now(UTC)
    return min(MAX_RETRY_AFTER_SECONDS, max(0.0, (when - reference).total_seconds()))
