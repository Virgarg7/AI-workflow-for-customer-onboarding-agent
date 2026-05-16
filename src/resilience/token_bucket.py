"""
src/resilience/token_bucket.py
Thread-safe token bucket for per-service rate limiting.
"""

from __future__ import annotations

import threading
import time


class TokenBucket:
    """
    Token bucket algorithm.

    Refills at `rate` tokens/second up to a burst capacity of `rate`.
    Call `consume()` before each outbound request.  It blocks (sleeps)
    until a token is available — no request is ever silently dropped.

    Args:
        rate: sustained requests per second allowed
        burst: optional burst capacity (defaults to rate)
    """

    def __init__(self, rate: float, burst: float | None = None) -> None:
        if rate <= 0:
            raise ValueError("rate must be positive")
        self._rate = rate
        self._capacity = burst if burst is not None else rate
        self._tokens: float = self._capacity
        self._last_refill: float = time.monotonic()
        self._lock = threading.Lock()

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(self._capacity, self._tokens + elapsed * self._rate)
        self._last_refill = now

    def consume(self, tokens: float = 1.0) -> None:
        """Block until `tokens` are available, then consume them."""
        while True:
            with self._lock:
                self._refill()
                if self._tokens >= tokens:
                    self._tokens -= tokens
                    return
                # Calculate how long until enough tokens accumulate
                wait = (tokens - self._tokens) / self._rate
            time.sleep(wait)

    def try_consume(self, tokens: float = 1.0) -> bool:
        """Non-blocking attempt. Returns True if tokens were consumed."""
        with self._lock:
            self._refill()
            if self._tokens >= tokens:
                self._tokens -= tokens
                return True
            return False
