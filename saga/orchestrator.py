from __future__ import annotations

import asyncio
import time
from typing import Any, Dict, List, Optional

from .models import (
    SagaExecutionRecord,
    SagaStatus,
    SagaStep,
    StepResult,
    StepStatus,
)
from .resilience import (
    CircuitBreaker,
    CircuitBreakerOpenError,
    DeadLetterEntry,
    DeadLetterQueue,
)
from .store import SagaStateStore


class SagaOrchestrator:
    def __init__(
        self,
        steps: List[SagaStep],
        store: Optional[SagaStateStore] = None,
        persist_state: bool = True,
        circuit_breakers: Optional[Dict[str, CircuitBreaker]] = None,
        dlq: Optional[DeadLetterQueue] = None,
    ) -> None:
        if not steps:
            raise ValueError("SagaOrchestrator requires at least one SagaStep")
        self.steps = steps
        self.persist_state = persist_state
        self.store = (
            store
            if store is not None
            else (SagaStateStore() if persist_state else None)
        )
        self.circuit_breakers: Dict[str, CircuitBreaker] = circuit_breakers or {}
        self.dlq = dlq if dlq is not None else DeadLetterQueue()

    def close(self) -> None:
        if self.store is not None:
            self.store.close()

    async def execute(
        self, saga_id: str, initial_context: Dict[str, Any]
    ) -> SagaExecutionRecord:
        context = dict(initial_context)
        started_at_ns = time.perf_counter_ns()
        record = SagaExecutionRecord(
            saga_id=saga_id,
            status=SagaStatus.RUNNING,
            context=context,
            started_at_ns=started_at_ns,
        )

        if self.persist_state and self.store is not None:
            self.store.create_saga(saga_id, context, started_at_ns)

        completed_indices: List[int] = []

        for idx, step in enumerate(self.steps):
            step_start_ns = time.perf_counter_ns()
            step_result = StepResult(
                step_name=step.name,
                status=StepStatus.RUNNING,
                started_at_ns=step_start_ns,
                ended_at_ns=step_start_ns,
            )
            record.steps.append(step_result)

            if self.persist_state and self.store is not None:
                self.store.record_step_event(
                    saga_id, idx, step.name, StepStatus.RUNNING, step_start_ns
                )

            cb = self.circuit_breakers.get(step.name)
            try:
                if cb is not None and not cb.allow_request():
                    raise CircuitBreakerOpenError(
                        f"CircuitBreaker[{step.name}] is OPEN — fast-failing request"
                    )

                output = await asyncio.wait_for(
                    step.action(saga_id, context), timeout=step.timeout_sec
                )
                if cb is not None:
                    cb.record_success()

                step_end_ns = time.perf_counter_ns()
                if output:
                    context.update(output)
                step_result.output = output or {}
                step_result.status = StepStatus.COMPLETED
                step_result.ended_at_ns = step_end_ns
                completed_indices.append(idx)

                if self.persist_state and self.store is not None:
                    self.store.record_step_event(
                        saga_id, idx, step.name, StepStatus.COMPLETED, step_end_ns
                    )
                    self.store.update_saga_status(
                        saga_id,
                        SagaStatus.RUNNING,
                        context=context,
                        updated_at_ns=step_end_ns,
                    )

            except Exception as exc:
                failure_detected_ns = time.perf_counter_ns()
                is_timeout = isinstance(exc, asyncio.TimeoutError)
                if cb is not None and not isinstance(exc, CircuitBreakerOpenError):
                    cb.record_failure()

                error_msg = (
                    f"Timeout after {step.timeout_sec}s"
                    if is_timeout
                    else str(exc)
                )
                step_result.status = StepStatus.FAILED
                step_result.ended_at_ns = failure_detected_ns
                step_result.error = error_msg

                # If the step timed out, fence off any delayed/zombie in-flight forward RPC
                # by compensating this step first (planting a tombstone on the target service).
                if is_timeout:
                    completed_indices.append(idx)

                record.failed_step = step.name
                record.error = error_msg
                record.failure_detected_at_ns = failure_detected_ns
                record.status = SagaStatus.COMPENSATING

                if self.persist_state and self.store is not None:
                    self.store.record_step_event(
                        saga_id,
                        idx,
                        step.name,
                        StepStatus.FAILED,
                        failure_detected_ns,
                        detail=error_msg,
                    )
                    self.store.update_saga_status(
                        saga_id,
                        SagaStatus.COMPENSATING,
                        context=context,
                        failed_step=step.name,
                        error=error_msg,
                        updated_at_ns=failure_detected_ns,
                    )

                comp_triggered_ns = time.perf_counter_ns()
                record.compensation_triggered_at_ns = comp_triggered_ns

                compensation_ok = await self._compensate(
                    saga_id, context, completed_indices, record
                )
                comp_completed_ns = time.perf_counter_ns()
                record.compensation_completed_at_ns = comp_completed_ns
                record.ended_at_ns = comp_completed_ns
                record.status = (
                    SagaStatus.COMPENSATED if compensation_ok else SagaStatus.FAILED
                )

                if self.persist_state and self.store is not None:
                    self.store.update_saga_status(
                        saga_id,
                        record.status,
                        context=context,
                        updated_at_ns=comp_completed_ns,
                    )
                return record

        ended_at_ns = time.perf_counter_ns()
        record.status = SagaStatus.COMPLETED
        record.ended_at_ns = ended_at_ns
        if self.persist_state and self.store is not None:
            self.store.update_saga_status(
                saga_id, SagaStatus.COMPLETED, context=context, updated_at_ns=ended_at_ns
            )
        return record

    async def execute_dag(
        self,
        saga_id: str,
        stages: List[List[SagaStep]],
        initial_context: Dict[str, Any],
    ) -> SagaExecutionRecord:
        flat_steps = [s for stage in stages for s in stage]
        step_to_idx = {id(s): i for i, s in enumerate(flat_steps)}
        prev_steps = self.steps
        self.steps = flat_steps
        try:
            context = dict(initial_context)
            started_at_ns = time.perf_counter_ns()
            record = SagaExecutionRecord(
                saga_id=saga_id,
                status=SagaStatus.RUNNING,
                context=context,
                started_at_ns=started_at_ns,
                steps=[
                    StepResult(
                        step_name=s.name,
                        status=StepStatus.PENDING,
                        started_at_ns=started_at_ns,
                        ended_at_ns=started_at_ns,
                    )
                    for s in flat_steps
                ],
            )
            completed_indices: List[int] = []

            for stage in stages:
                async def _run_stage_step(step: SagaStep) -> tuple[int, Optional[Dict[str, Any]], Optional[Exception]]:
                    idx = step_to_idx[id(step)]
                    s_start = time.perf_counter_ns()
                    record.steps[idx].status = StepStatus.RUNNING
                    record.steps[idx].started_at_ns = s_start
                    try:
                        out = await asyncio.wait_for(
                            step.action(saga_id, context), timeout=step.timeout_sec
                        )
                        record.steps[idx].ended_at_ns = time.perf_counter_ns()
                        return idx, out, None
                    except Exception as exc:
                        record.steps[idx].ended_at_ns = time.perf_counter_ns()
                        return idx, None, exc

                stage_outcomes = await asyncio.gather(*[_run_stage_step(s) for s in stage])
                stage_failed_exc: Optional[Exception] = None
                failed_step_name: Optional[str] = None

                for idx, out, exc in stage_outcomes:
                    if exc is None:
                        record.steps[idx].status = StepStatus.COMPLETED
                        record.steps[idx].output = out or {}
                        if out:
                            context.update(out)
                        completed_indices.append(idx)
                    else:
                        record.steps[idx].status = StepStatus.FAILED
                        record.steps[idx].error = str(exc)
                        if isinstance(exc, asyncio.TimeoutError):
                            completed_indices.append(idx)
                        if stage_failed_exc is None:
                            stage_failed_exc = exc
                            failed_step_name = flat_steps[idx].name

                if stage_failed_exc is not None:
                    now_ns = time.perf_counter_ns()
                    record.failed_step = failed_step_name
                    record.error = str(stage_failed_exc)
                    record.failure_detected_at_ns = now_ns
                    record.compensation_triggered_at_ns = time.perf_counter_ns()
                    ok = await self._compensate(saga_id, context, completed_indices, record)
                    end_ns = time.perf_counter_ns()
                    record.compensation_completed_at_ns = end_ns
                    record.ended_at_ns = end_ns
                    record.status = SagaStatus.COMPENSATED if ok else SagaStatus.FAILED
                    return record

            record.status = SagaStatus.COMPLETED
            record.ended_at_ns = time.perf_counter_ns()
            return record
        finally:
            self.steps = prev_steps

    async def _compensate(
        self,
        saga_id: str,
        context: Dict[str, Any],
        completed_indices: List[int],
        record: SagaExecutionRecord,
    ) -> bool:
        all_compensated = True
        for idx in reversed(completed_indices):
            step = self.steps[idx]
            step_result = record.steps[idx]
            comp_start_ns = time.perf_counter_ns()
            step_result.compensation_started_at_ns = comp_start_ns
            step_result.status = StepStatus.COMPENSATING

            if self.persist_state and self.store is not None:
                self.store.record_step_event(
                    saga_id, idx, step.name, StepStatus.COMPENSATING, comp_start_ns
                )

            compensated = False
            last_err: Optional[str] = None
            for attempt in range(1, step.max_compensation_retries + 1):
                step_result.compensation_attempts = attempt
                try:
                    comp_out = await asyncio.wait_for(
                        step.compensation(saga_id, context), timeout=step.timeout_sec
                    )
                    if comp_out:
                        context.update(comp_out)
                    compensated = True
                    break
                except Exception as exc:
                    last_err = str(exc)
                    if attempt < step.max_compensation_retries:
                        await asyncio.sleep(step.retry_backoff_sec * (2 ** (attempt - 1)))

            comp_end_ns = time.perf_counter_ns()
            step_result.compensation_ended_at_ns = comp_end_ns

            if compensated:
                step_result.status = StepStatus.COMPENSATED
                if self.persist_state and self.store is not None:
                    self.store.record_step_event(
                        saga_id, idx, step.name, StepStatus.COMPENSATED, comp_end_ns
                    )
            else:
                step_result.status = StepStatus.COMPENSATION_FAILED
                step_result.error = f"Compensation failed: {last_err}"
                all_compensated = False
                self.dlq.enqueue(
                    DeadLetterEntry(
                        saga_id=saga_id,
                        step_index=idx,
                        step_name=step.name,
                        context=dict(context),
                        last_error=last_err or "unknown error",
                        attempts=step.max_compensation_retries,
                    )
                )
                if self.persist_state and self.store is not None:
                    self.store.record_step_event(
                        saga_id,
                        idx,
                        step.name,
                        StepStatus.COMPENSATION_FAILED,
                        comp_end_ns,
                        detail=last_err,
                    )

        return all_compensated

    async def replay_dlq(self) -> int:
        resolved_count = 0
        for entry in self.dlq.pending():
            step = self.steps[entry.step_index]
            try:
                await asyncio.wait_for(
                    step.compensation(entry.saga_id, entry.context),
                    timeout=step.timeout_sec,
                )
                entry.resolved = True
                resolved_count += 1
                if self.persist_state and self.store is not None:
                    self.store.record_step_event(
                        entry.saga_id,
                        entry.step_index,
                        entry.step_name,
                        StepStatus.COMPENSATED,
                    )
                    self.store.update_saga_status(
                        entry.saga_id, SagaStatus.COMPENSATED, context=entry.context
                    )
            except Exception:
                entry.attempts += 1
        return resolved_count

    async def recover_incomplete_sagas(self) -> List[SagaExecutionRecord]:
        if self.store is None:
            return []
        incomplete = self.store.list_incomplete_sagas()
        recovered: List[SagaExecutionRecord] = []

        for row in incomplete:
            saga_id = row["saga_id"]
            context = row["context"]
            events = self.store.get_step_events(saga_id)

            completed_steps: Dict[int, str] = {}
            compensated_steps: set[int] = set()
            for ev in events:
                if ev.status == StepStatus.COMPLETED:
                    completed_steps[ev.step_index] = ev.step_name
                elif ev.status == StepStatus.COMPENSATED:
                    compensated_steps.add(ev.step_index)

            to_compensate = sorted(
                idx for idx in completed_steps if idx not in compensated_steps
            )
            now_ns = time.perf_counter_ns()
            rec = SagaExecutionRecord(
                saga_id=saga_id,
                status=SagaStatus.COMPENSATING,
                context=context,
                started_at_ns=row["started_at_ns"],
                failure_detected_at_ns=now_ns,
                compensation_triggered_at_ns=now_ns,
                steps=[
                    StepResult(
                        step_name=self.steps[i].name,
                        status=(
                            StepStatus.COMPLETED
                            if i in completed_steps
                            else StepStatus.PENDING
                        ),
                        started_at_ns=row["started_at_ns"],
                        ended_at_ns=now_ns,
                    )
                    for i in range(len(self.steps))
                ],
            )
            ok = await self._compensate(saga_id, context, to_compensate, rec)
            end_ns = time.perf_counter_ns()
            rec.compensation_completed_at_ns = end_ns
            rec.ended_at_ns = end_ns
            rec.status = SagaStatus.COMPENSATED if ok else SagaStatus.FAILED
            self.store.update_saga_status(
                saga_id, rec.status, context=context, updated_at_ns=end_ns
            )
            recovered.append(rec)

        return recovered
