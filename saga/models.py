from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Awaitable, Callable, Dict, List, Optional


class SagaStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    COMPENSATING = "COMPENSATING"
    COMPENSATED = "COMPENSATED"
    FAILED = "FAILED"


class StepStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    COMPENSATING = "COMPENSATING"
    COMPENSATED = "COMPENSATED"
    COMPENSATION_FAILED = "COMPENSATION_FAILED"


ActionFn = Callable[[str, Dict[str, Any]], Awaitable[Dict[str, Any]]]


@dataclass
class SagaStep:
    name: str
    action: ActionFn
    compensation: ActionFn
    timeout_sec: float = 2.0
    max_compensation_retries: int = 5
    retry_backoff_sec: float = 0.002


@dataclass
class StepResult:
    step_name: str
    status: StepStatus
    started_at_ns: int
    ended_at_ns: int
    output: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None
    compensation_started_at_ns: Optional[int] = None
    compensation_ended_at_ns: Optional[int] = None
    compensation_attempts: int = 0


@dataclass
class StepExecutionRecord:
    saga_id: str
    step_index: int
    step_name: str
    status: StepStatus
    updated_at_ns: int
    detail: Optional[str] = None


@dataclass
class SagaExecutionRecord:
    saga_id: str
    status: SagaStatus
    context: Dict[str, Any]
    started_at_ns: int
    ended_at_ns: int = 0
    failed_step: Optional[str] = None
    failure_detected_at_ns: Optional[int] = None
    compensation_triggered_at_ns: Optional[int] = None
    compensation_completed_at_ns: Optional[int] = None
    steps: List[StepResult] = field(default_factory=list)
    error: Optional[str] = None

    @property
    def total_duration_ms(self) -> float:
        if self.ended_at_ns <= self.started_at_ns:
            return 0.0
        return (self.ended_at_ns - self.started_at_ns) / 1_000_000.0

    @property
    def failure_detection_latency_ms(self) -> Optional[float]:
        if (
            self.failure_detected_at_ns is not None
            and self.compensation_triggered_at_ns is not None
        ):
            return (
                self.compensation_triggered_at_ns - self.failure_detected_at_ns
            ) / 1_000_000.0
        return None

    @property
    def compensation_duration_ms(self) -> Optional[float]:
        if (
            self.compensation_triggered_at_ns is not None
            and self.compensation_completed_at_ns is not None
        ):
            return (
                self.compensation_completed_at_ns - self.compensation_triggered_at_ns
            ) / 1_000_000.0
        return None
