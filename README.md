# Distributed Saga Pattern Transaction Coordinator

A distributed Saga transaction orchestrator with a durable SQLite Write-Ahead Log (WAL) state store, reverse-order compensating transaction engine, **Tombstone Zombie-RPC Fencing**, **Per-Service Circuit Breakers**, **Dead-Letter Queue (DLQ) replay**, **Parallel DAG stage execution**, crash recovery, and consistency verification suite.

## Problem Statement

Long-running, multi-step transactions across microservices (`InventoryService` -> `PaymentService` -> `ShipmentService`) can fail partway through due to service outages, timeouts, or business rule rejections. Traditional two-phase commit (2PC) / distributed ACID transactions do not scale across independent service boundaries due to lock contention and blocking coordinator availability. The **Saga Pattern** maintains eventual consistency by executing a sequence of local transactions and triggering **compensating transactions** in reverse order whenever any step fails.

---

## Architecture & Standout Engineering Mechanisms

```
                         +----------------------------------------+
                         |        SagaOrchestrator (Central)      |
                         |  - Sequential & Parallel DAG Execution |
                         |  - Per-Service Circuit Breakers        |
                         |  - Reverse Compensation + DLQ Replay   |
                         +-------------------+--------------------+
                                             |
                                             | Durable State Transitions
                                             v
                         +----------------------------------------+
                         |      SQLite WAL State Store (Log)      |
                         |   sagas | saga_step_events | dlq       |
                         +-------------------+--------------------+
                                             |
            +--------------------------------+--------------------------------+
            |                                |                                |
            v                                v                                v
+-------------------------+      +-------------------------+      +-------------------------+
|    InventoryService     |      |     PaymentService      |      |     ShipmentService     |
|-------------------------|      |-------------------------|      |-------------------------|
| + reserve_inventory()   | ---> | + charge_payment()      | ---> | + create_shipment()     |
| - release_inventory()   | <--- | - refund_payment()      | <--- | - cancel_shipment()     |
| * Tombstone Zombie Fence|      | * Tombstone Zombie Fence|      | * Tombstone Zombie Fence|
+-------------------------+      +-------------------------+      +-------------------------+
```

### 1. Out-of-Order / "Zombie" Request Fencing (Tombstone Ledger)
A subtle distributed race condition occurs when `reserve_inventory` times out at the orchestrator (e.g., after `2ms`) while the underlying RPC packet is still delayed in transit (`8ms`). The orchestrator immediately triggers `release_inventory` at `2.1ms`. Without fencing, the delayed `reserve_inventory` arrives at `8ms` *after* the rollback has already run and permanently leaks reserved inventory.
* **Solution**: When a step times out, the orchestrator immediately executes that step's compensation to plant an **Aborted-Saga Tombstone** (`tombstoned_sagas`) on the service. When the delayed zombie forward RPC eventually arrives, the service detects the tombstone, increments `fenced_zombie_count`, and rejects the mutation (`ZombieRequestFencedError`), preserving `0.0000%` inconsistency.

### 2. Per-Service Circuit Breakers & Dead-Letter Queue (DLQ)
* **Circuit Breakers (`CLOSED` -> `OPEN` -> `HALF_OPEN`)**: Tracks consecutive step failures and fast-fails requests in `< 1 µs` when a downstream service is degraded, preventing cascading thread/coroutine pool exhaustion.
* **Dead-Letter Queue (`DeadLetterQueue` + `replay_dlq()`)**: If a compensating action exhausts its exponential-backoff retry budget during a prolonged outage, the payload is captured in a DLQ for automated or operator-triggered replay once the service recovers.

### 3. Why Central Orchestration Over Event-Driven Choreography?
1. **Single Source of Truth & Observability**: Persists every step transition (`PENDING` -> `RUNNING` -> `COMPLETED` / `FAILED` -> `COMPENSATING` -> `COMPENSATED`) in a single queryable WAL store.
2. **Acyclic Dependency Graph**: Downstream services never subscribe to each other's domain events, preventing cyclic rollback loops as workflow steps grow.
3. **Deterministic Reverse Rollback & DAG Support**: Supports both linear pipelines and concurrent stage DAGs (`execute_dag`) with deterministic reverse-stage rollback.

---

## Collected Metrics (Benchmark Results)

Generated via `python3 main.py`:

| Scenario | Total Txns | Success Rate | Throughput (txns/sec) | Avg / p95 Rollback Time | Avg Failure Detection | Zombie RPCs Fenced / CB Fast-Rejects | Inconsistent State % |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **Low Failure** (2%/step fault, Durable WAL) | 1,000 | **93.00%** | 14,079 txns/s | 1.17 ms / 2.92 ms | 0.0090 ms (9.0 µs) | 0 / 0 | **0.0000%** |
| **Moderate Failure** (22% agg fault, Durable WAL) | 1,000 | **73.80%** | 13,535 txns/s | 1.56 ms / 2.86 ms | 0.0088 ms (8.8 µs) | 0 / 0 | **0.0000%** |
| **Zombie RPC + Circuit Breaker Chaos** | 1,000 | **50.60%** | 12,971 txns/s | 1.81 ms / 2.89 ms | 0.0089 ms (8.9 µs) | **75 fenced / 31 fast-rejected** | **0.0000%** |
| **Peak Throughput** (In-Memory Fast Path) | 3,000 | **85.50%** | **65,243 txns/s** | 0.29 ms / 0.006 ms | 0.0002 ms (0.2 µs) | 0 / 0 | **0.0000%** |

---

## Resume Highlights (Google XYZ Format)

* **Engineered a fault-tolerant Saga transaction coordinator** in Python (`asyncio`, SQLite WAL) achieving **14,000+ durable txns/sec** (**65,000+ txns/sec** in-memory) and **0.0000% state inconsistency** across 5,000+ chaos-tested multi-service workflows by implementing reverse-order compensating transactions with exponential backoff and crash recovery.
* **Eliminated late-arriving RPC race conditions ("zombie reservations")** under network timeout chaos—fencing **100% of delayed post-timeout forward requests** with **8.9 µs average failure detection latency**—by designing an idempotent tombstone ledger and per-service circuit breakers with Dead-Letter Queue (DLQ) replay.

---

## Usage

```bash
# Run full benchmark & chaos verification suite
python3 main.py

# Run unit & consistency tests (9 test suites)
python3 -m unittest discover -s tests -v
```
