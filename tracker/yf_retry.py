"""yfinance rate-limit awareness — small shared retry helper.

Centralised so the daily sync's three yfinance call sites (pricing
refresh in tracker.enrich, sync_security_prices, sync_indices) all
handle 429s the same way: detect, back off, retry, give up gracefully.

Exposes:
  - is_rate_limit(exc): bool — heuristic match against YFRateLimitError
    (when the installed yfinance is recent enough to expose it) plus a
    string-pattern fallback for older versions.
  - with_yf_retry(label, fn, backoffs=...): T — calls fn(); on a
    rate-limit-shaped error, sleeps and retries. Non-rate-limit
    exceptions propagate immediately so callers' own "ticker doesn't
    exist" handling still works.
"""

from __future__ import annotations

import time
from typing import Callable, TypeVar

# YFRateLimitError was added in yfinance >= 0.2.40; older installs lack
# the symbol. The string-pattern fallback below covers older versions
# (which raise generic Exception("Too Many Requests") on a 429).
try:
    from yfinance.exceptions import YFRateLimitError  # type: ignore

    _RATE_LIMIT_TYPES: tuple[type, ...] = (YFRateLimitError,)
except ImportError:  # pragma: no cover — depends on installed yfinance
    _RATE_LIMIT_TYPES = ()

_RATE_LIMIT_PHRASES = ("rate limit", "too many requests", "429")

# Backoffs in seconds. Yahoo's rate window resets in roughly a minute,
# so 30s gets us out the door fast; the longer fallbacks cover the
# stickier-throttle case where the limiter is upset with our IP.
DEFAULT_BACKOFFS: tuple[int, ...] = (30, 90, 180)

T = TypeVar("T")


def is_rate_limit(exc: BaseException) -> bool:
    """True if the exception looks like a yfinance rate-limit signal."""
    if _RATE_LIMIT_TYPES and isinstance(exc, _RATE_LIMIT_TYPES):
        return True
    msg = str(exc).lower()
    return any(p in msg for p in _RATE_LIMIT_PHRASES)


def with_yf_retry(
    label: str,
    fn: Callable[[], T],
    backoffs: tuple[int, ...] = DEFAULT_BACKOFFS,
) -> T:
    """Run fn(); on rate-limit errors, sleep and retry.

    The initial attempt has no delay. After each rate-limit failure the
    helper sleeps `backoffs[i]` seconds before the next try. Exhausting
    all attempts re-raises the last rate-limit exception so callers can
    fall through to their own failure handling (typically "record 0 and
    move on" for per-ticker syncs, or "mark batch as failed" in
    _fetch_yf).
    """
    last: BaseException | None = None
    attempts = len(backoffs) + 1
    for i, wait in enumerate((0, *backoffs)):
        if wait:
            print(
                f"    rate-limited on {label} — sleeping {wait}s "
                f"(attempt {i + 1}/{attempts})",
                flush=True,
            )
            time.sleep(wait)
        try:
            return fn()
        except Exception as exc:
            if not is_rate_limit(exc):
                raise
            last = exc
    assert last is not None  # loop always sets last on rate-limit failure
    raise last
