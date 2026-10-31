from __future__ import annotations

import argparse
import asyncio
import json
from typing import List

from saga.metrics import BenchmarkMetrics, run_benchmark_suite, run_scenario


def _print_report(results: List[BenchmarkMetrics]) -> None:
    print("=" * 112)
    print("SAGA PATTERN TRANSACTION COORDINATOR — BENCHMARK, FENCING & CONSISTENCY REPORT")
    print("=" * 112)
    for m in results:
        print(f"\nScenario: {m.scenario_name}")
        print("-" * 112)
        print(
            f"  Transactions          : {m.total_transactions:,} total | "
            f"{m.succeeded_transactions:,} succeeded | "
            f"{m.compensated_transactions:,} compensated (rolled back) | "
            f"{m.failed_uncompensated_transactions:,} uncompensated"
        )
        print(f"  Success Rate          : {m.success_rate_pct:.2f}%")
        print(f"  Throughput            : {m.throughput_tps:,.2f} txns/sec (wall: {m.wall_time_sec:.3f}s)")
        print(
            f"  Rollback Latency      : avg = {m.avg_rollback_time_ms:.4f} ms | "
            f"p95 = {m.p95_rollback_time_ms:.4f} ms"
        )
        print(
            f"  Failure Detection     : avg = {m.avg_failure_detection_latency_ms:.4f} ms | "
            f"p95 = {m.p95_failure_detection_latency_ms:.4f} ms"
        )
        print(
            f"  Resilience Guards     : {m.fenced_zombie_requests} zombie RPCs fenced | "
            f"{m.circuit_breaker_fast_rejects} circuit-breaker fast-rejects"
        )
        print(
            f"  Inconsistent State %  : {m.inconsistent_state_pct:.4f}% "
            f"(Stock conservation valid: {m.stock_conservation_valid})"
        )
    print("\n" + "=" * 112)


async def _async_main() -> None:
    parser = argparse.ArgumentParser(
        description="Distributed Saga Transaction Coordinator & Benchmark Suite"
    )
    parser.add_argument(
        "--transactions",
        type=int,
        default=0,
        help="Run a custom scenario with N transactions (0 runs the full benchmark suite)",
    )
    parser.add_argument(
        "--fail-rate",
        type=float,
        default=0.10,
        help="Per-step simulated failure rate for custom run (0.0 to 1.0)",
    )
    parser.add_argument(
        "--json-out",
        type=str,
        default="",
        help="Optional path to export benchmark results as JSON",
    )
    args = parser.parse_args()

    if args.transactions > 0:
        res = await run_scenario(
            scenario_name=f"Custom Run ({args.transactions} txns, step fail rate={args.fail_rate})",
            num_transactions=args.transactions,
            inventory_fail_rate=args.fail_rate,
            payment_fail_rate=args.fail_rate,
            shipment_fail_rate=args.fail_rate,
        )
        results = [res]
    else:
        results = await run_benchmark_suite()

    _print_report(results)

    if args.json_out:
        with open(args.json_out, "w", encoding="utf-8") as f:
            json.dump([r.to_dict() for r in results], f, indent=2)


if __name__ == "__main__":
    asyncio.run(_async_main())
