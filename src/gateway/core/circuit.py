from __future__ import annotations

import time
from enum import IntEnum


class BreakerState(IntEnum):
    CLOSED = 0
    HALF_OPEN = 1
    OPEN = 2


class CircuitBreaker:
    """Minimal async-safe (single event loop) circuit breaker.

    CLOSED   -> calls flow; consecutive failures counted.
    OPEN     -> calls short-circuit immediately (no network wait) until
                `recovery_seconds` elapse.
    HALF_OPEN-> exactly one probe call is let through; success closes the
                breaker, failure re-opens it.
    """

    def __init__(self, name: str, failure_threshold: int, recovery_seconds: float) -> None:
        self.name = name
        self.failure_threshold = max(1, failure_threshold)
        self.recovery_seconds = recovery_seconds
        self.state = BreakerState.CLOSED
        self._failures = 0
        self._opened_at = 0.0
        self._probe_in_flight = False

    def allow(self) -> bool:
        if self.state is BreakerState.CLOSED:
            return True
        if self.state is BreakerState.OPEN:
            if time.monotonic() - self._opened_at >= self.recovery_seconds:
                self.state = BreakerState.HALF_OPEN
                self._probe_in_flight = False
            else:
                return False
        # HALF_OPEN: single probe
        if self._probe_in_flight:
            return False
        self._probe_in_flight = True
        return True

    def record_success(self) -> None:
        self._failures = 0
        self._probe_in_flight = False
        self.state = BreakerState.CLOSED

    def record_failure(self) -> None:
        self._probe_in_flight = False
        self._failures += 1
        if self.state is BreakerState.HALF_OPEN or self._failures >= self.failure_threshold:
            self.state = BreakerState.OPEN
            self._opened_at = time.monotonic()

    def force_open(self) -> None:  # used by tests / ops
        self.state = BreakerState.OPEN
        self._opened_at = time.monotonic()
