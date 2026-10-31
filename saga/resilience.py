from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List


class CircuitState(str, Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class CircuitBreakerOpenError(RuntimeError):
    pass


class ZombieRequestFencedError(RuntimeError):
    pass


class CircuitBreaker:
    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        recovery_timeout_sec: float = 0.05,
        half_open_max_calls: int = 1,
    ) -> None:
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout_sec = recovery_timeout_sec
        self.half_open_max_calls = half_open_max_calls
        self.state: CircuitState = CircuitState.CLOSED
        self.consecutive_failures: int = 0
        self.last_failure_time: float = 0.0
        self.half_open_calls: int = 0
        self.fast_rejected_calls: int = 0
        self.state_transitions: int = 0

    def allow_request(self) -> bool:
        now = time.perf_counter()
        if self.state == CircuitState.CLOSED:
            return True
        if self.state == CircuitState.OPEN:
            if (now - self.last_failure_time) >= self.recovery_timeout_sec:
                self._transition(CircuitState.HALF_OPEN)
                self.half_open_calls = 1
                return True
            self.fast_rejected_calls += 1
            return False
        if self.state == CircuitState.HALF_OPEN:
            if self.half_open_calls < self.half_open_max_calls:
                self.half_open_calls += 1
                return True
            self.fast_rejected_calls += 1
            return False
        return True

    def record_success(self) -> None:
        self.consecutive_failures = 0
        if self.state != CircuitState.CLOSED:
            self._transition(CircuitState.CLOSED)
        self.half_open_calls = 0

    def record_failure(self) -> None:
        self.consecutive_failures += 1
        self.last_failure_time = time.perf_counter()
        if self.state == CircuitState.HALF_OPEN:
            self._transition(CircuitState.OPEN)
        elif (
            self.state == CircuitState.CLOSED
            and self.consecutive_failures >= self.failure_threshold
        ):
            self._transition(CircuitState.OPEN)

    def _transition(self, new_state: CircuitState) -> None:
        if self.state != new_state:
            self.state = new_state
            self.state_transitions += 1


@dataclass
class DeadLetterEntry:
    saga_id: str
    step_index: int
    step_name: str
    context: Dict[str, Any]
    last_error: str
    attempts: int
    enqueued_at_ns: int = field(default_factory=time.perf_counter_ns)
    resolved: bool = False


class DeadLetterQueue:
    def __init__(self) -> None:
        self.entries: List[DeadLetterEntry] = []

    def enqueue(self, entry: DeadLetterEntry) -> None:
        self.entries.append(entry)

    def pending(self) -> List[DeadLetterEntry]:
        return [e for e in self.entries if not e.resolved]
