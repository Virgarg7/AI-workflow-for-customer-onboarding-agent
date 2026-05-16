"""
src/resilience/circuit_breaker.py
Three-state circuit breaker: CLOSED → OPEN → HALF_OPEN → CLOSED.

State transitions:
  CLOSED     → OPEN       when error_rate >= threshold over the last `window` seconds
  OPEN       → HALF_OPEN  after `recovery_timeout` seconds
  HALF_OPEN  → CLOSED     on first successful probe call
  HALF_OPEN  → OPEN       on failed probe call
"""

from __future__ import annotations

import threading
import time
from collections import deque
from enum import Enum, auto
from typing import Callable, TypeVar

from src.monitoring.logger import get_logger

log = get_logger("circuit_breaker")

T = TypeVar("T")


class CircuitState(Enum):
    CLOSED = auto()
    OPEN = auto()
    HALF_OPEN = auto()


class CircuitBreakerOpenError(RuntimeError):
    """Raised when a call is rejected because the circuit is OPEN."""


class CircuitBreaker:
    """
    Args:
        name:             identifier used in logs/metrics
        error_threshold:  fraction of calls that must fail to trip (e.g. 0.5 = 50%)
        window_seconds:   sliding time window for error rate calculation
        min_calls:        minimum calls in window before tripping is allowed
        recovery_timeout: seconds to wait in OPEN state before probing
    """

    def __init__(
        self,
        name: str,
        error_threshold: float = 0.5,
        window_seconds: float = 60.0,
        min_calls: int = 10,
        recovery_timeout: float = 60.0,
    ) -> None:
        self.name = name
        self._threshold = error_threshold
        self._window = window_seconds
        self._min_calls = min_calls
        self._recovery_timeout = recovery_timeout

        self._state = CircuitState.CLOSED
        self._opened_at: float | None = None
        # deque of (timestamp, success: bool)
        self._calls: deque[tuple[float, bool]] = deque()
        self._lock = threading.Lock()

    # ── Public API ──────────────────────────────────────────

    @property
    def state(self) -> CircuitState:
        with self._lock:
            return self._state

    def call(self, fn: Callable[[], T]) -> T:
        """
        Execute `fn` through the breaker.
        Raises CircuitBreakerOpenError if the circuit is OPEN.
        """
        with self._lock:
            self._maybe_transition()
            if self._state == CircuitState.OPEN:
                raise CircuitBreakerOpenError(
                    f"Circuit '{self.name}' is OPEN — call rejected"
                )

        try:
            result = fn()
            self._record(success=True)
            if self._state == CircuitState.HALF_OPEN:
                self._close()
            return result
        except Exception:
            self._record(success=False)
            self._maybe_trip()
            raise

    # ── Internal ────────────────────────────────────────────

    def _maybe_transition(self) -> None:
        """Check if OPEN → HALF_OPEN transition is due."""
        if (
            self._state == CircuitState.OPEN
            and self._opened_at is not None
            and time.monotonic() - self._opened_at >= self._recovery_timeout
        ):
            self._state = CircuitState.HALF_OPEN
            log.info("circuit_half_open", circuit=self.name)

    def _record(self, *, success: bool) -> None:
        now = time.monotonic()
        with self._lock:
            self._calls.append((now, success))
            # Evict entries outside the sliding window
            cutoff = now - self._window
            while self._calls and self._calls[0][0] < cutoff:
                self._calls.popleft()

    def _maybe_trip(self) -> None:
        with self._lock:
            if self._state != CircuitState.CLOSED:
                return
            total = len(self._calls)
            if total < self._min_calls:
                return
            errors = sum(1 for _, ok in self._calls if not ok)
            rate = errors / total
            if rate >= self._threshold:
                self._state = CircuitState.OPEN
                self._opened_at = time.monotonic()
                log.warning(
                    "circuit_opened",
                    circuit=self.name,
                    error_rate=round(rate, 3),
                    total_calls=total,
                )

    def _close(self) -> None:
        with self._lock:
            self._state = CircuitState.CLOSED
            self._calls.clear()
            log.info("circuit_closed", circuit=self.name)
