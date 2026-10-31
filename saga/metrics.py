from __future__ import annotations

import asyncio
import statistics
import time
from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional

from .models import SagaExecutionRecord, SagaStatus
from .orchestrator import SagaOrchestrator
from .resilience import CircuitBreaker
from .services import OrderCluster
from .store import SagaStateStore


@dataclass
class BenchmarkMetrics:
    scenario_name: str
    total_transactions: int
    succeeded_transactions: int
    compensated_transactions: int
    failed_uncompensated_transactions: int
    success_rate_pct: float
    throughput_tps: float
    avg_rollback_time_ms: float
    p95_rollback_time_ms: float
    avg_failure_detection_latency_ms: float
    p95_failure_detection_latency_ms: float
    inconsistent_state_pct: float
    stock_conservation_valid: bool
    fenced_zombie_requests: int
    circuit_breaker_fast_rejects: int
    wall_time_sec: float

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _percentile(values: List[float], q: float) -> float:
    if not values:
        return 0.0
    sorted_vals = sorted(values)
    idx = min(len(sorted_vals) - 1, max(0, int(round(q * (len(sorted_vals) - 1)))))
    return sorted_vals[idx]


async def run_scenario(
    scenario_name: str,
    num_transactions: int = 1000,
    concurrency: int = 50,
    inventory_fail_rate: float = 0.05,
    payment_fail_rate: float = 0.10,
    shipment_fail_rate: float = 0.10,
    compensation_fail_rate: float = 0.02,
    latency_sec: float = 0.0005,
    zombie_rate: float = 0.0,
    zombie_delay_sec: float = 0.008,
    step_timeout_sec: float = 2.0,
    enable_circuit_breaker: bool = False,
    persist_state: bool = True,
    seed: int = 42,
) -> BenchmarkMetrics:
    cluster = OrderCluster(
        inventory_fail_rate=inventory_fail_rate,
        payment_fail_rate=payment_fail_rate,
        shipment_fail_rate=shipment_fail_rate,
        compensation_fail_rate=compensation_fail_rate,
        latency_sec=latency_sec,
        zombie_rate=zombie_rate,
        zombie_delay_sec=zombie_delay_sec,
        seed=seed,
    )
    store = SagaStateStore(":memory:") if persist_state else None
    cbs: Optional[Dict[str, CircuitBreaker]] = None
    if enable_circuit_breaker:
        cbs = {
            "reserve_inventory": CircuitBreaker(
                "reserve_inventory", failure_threshold=3, recovery_timeout_sec=0.004
            ),
            "charge_payment": CircuitBreaker(
                "charge_payment", failure_threshold=3, recovery_timeout_sec=0.004
            ),
            "create_shipment": CircuitBreaker(
                "create_shipment", failure_threshold=3, recovery_timeout_sec=0.004
            ),
        }

    orchestrator = SagaOrchestrator(
        steps=cluster.build_steps(timeout_sec=step_timeout_sec),
        store=store,
        persist_state=persist_state,
        circuit_breakers=cbs,
    )

    sem = asyncio.Semaphore(concurrency)
    skus = ["SKU-LAPTOP", "SKU-PHONE", "SKU-MONITOR"]

    async def _worker(i: int) -> SagaExecutionRecord:
        async with sem:
            ctx = {
                "customer_id": f"cust-{i % 200}",
                "sku": skus[i % len(skus)],
                "quantity": (i % 3) + 1,
                "amount_cents": ((i % 5) + 1) * 2500,
                "address": f"{100 + i} Distributed Way, Mountain View, CA",
            }
            return await orchestrator.execute(f"saga-{i:06d}", ctx)

    start_wall = time.perf_counter()
    records = await asyncio.gather(*[_worker(i) for i in range(num_transactions)])
    wall_time_sec = max(1e-9, time.perf_counter() - start_wall)

    await cluster.drain_background_zombies()
    audit = cluster.audit_consistency(records)
    orchestrator.close()

    succeeded = sum(1 for r in records if r.status == SagaStatus.COMPLETED)
    compensated = sum(1 for r in records if r.status == SagaStatus.COMPENSATED)
    failed_uncomp = sum(1 for r in records if r.status == SagaStatus.FAILED)

    rollback_times_ms = [
        r.compensation_duration_ms
        for r in records
        if r.compensation_duration_ms is not None
    ]
    detection_latencies_ms = [
        r.failure_detection_latency_ms
        for r in records
        if r.failure_detection_latency_ms is not None
    ]
    cb_fast_rejects = (
        sum(cb.fast_rejected_calls for cb in cbs.values()) if cbs else 0
    )

    return BenchmarkMetrics(
        scenario_name=scenario_name,
        total_transactions=num_transactions,
        succeeded_transactions=succeeded,
        compensated_transactions=compensated,
        failed_uncompensated_transactions=failed_uncomp,
        success_rate_pct=round(succeeded / num_transactions * 100.0, 2),
        throughput_tps=round(num_transactions / wall_time_sec, 2),
        avg_rollback_time_ms=round(
            statistics.mean(rollback_times_ms) if rollback_times_ms else 0.0, 4
        ),
        p95_rollback_time_ms=round(_percentile(rollback_times_ms, 0.95), 4),
        avg_failure_detection_latency_ms=round(
            statistics.mean(detection_latencies_ms) if detection_latencies_ms else 0.0,
            4,
        ),
        p95_failure_detection_latency_ms=round(
            _percentile(detection_latencies_ms, 0.95), 4
        ),
        inconsistent_state_pct=round(audit.inconsistent_pct, 4),
        stock_conservation_valid=audit.stock_conservation_valid,
        fenced_zombie_requests=audit.fenced_zombie_requests,
        circuit_breaker_fast_rejects=cb_fast_rejects,
        wall_time_sec=round(wall_time_sec, 4),
    )


async def run_benchmark_suite() -> List[BenchmarkMetrics]:
    scenarios = [
        {
            "scenario_name": "Low Failure (2% per-step fault rate, Durable WAL)",
            "num_transactions": 1000,
            "concurrency": 64,
            "inventory_fail_rate": 0.02,
            "payment_fail_rate": 0.02,
            "shipment_fail_rate": 0.02,
            "compensation_fail_rate": 0.01,
            "latency_sec": 0.0004,
            "persist_state": True,
        },
        {
            "scenario_name": "Moderate Failure (22% aggregate failure rate, Durable WAL)",
            "num_transactions": 1000,
            "concurrency": 64,
            "inventory_fail_rate": 0.05,
            "payment_fail_rate": 0.10,
            "shipment_fail_rate": 0.10,
            "compensation_fail_rate": 0.03,
            "latency_sec": 0.0004,
            "persist_state": True,
        },
        {
            "scenario_name": "Zombie RPC Fencing + Circuit Breaker Chaos (Delayed Forward RPCs vs. Rollback)",
            "num_transactions": 1000,
            "concurrency": 64,
            "inventory_fail_rate": 0.05,
            "payment_fail_rate": 0.20,
            "shipment_fail_rate": 0.25,
            "compensation_fail_rate": 0.02,
            "latency_sec": 0.0003,
            "zombie_rate": 0.08,
            "zombie_delay_sec": 0.008,
            "step_timeout_sec": 0.002,
            "enable_circuit_breaker": True,
            "persist_state": True,
        },
        {
            "scenario_name": "Peak Throughput Stress (In-Memory Fast Path, 3,000 txns)",
            "num_transactions": 3000,
            "concurrency": 128,
            "inventory_fail_rate": 0.05,
            "payment_fail_rate": 0.05,
            "shipment_fail_rate": 0.05,
            "compensation_fail_rate": 0.01,
            "latency_sec": 0.0,
            "persist_state": False,
        },
    ]
    results: List[BenchmarkMetrics] = []
    for cfg in scenarios:
        res = await run_scenario(**cfg)
        results.append(res)
    return results
