from __future__ import annotations

import asyncio
import unittest
from typing import Any, Dict, List

from saga.metrics import run_scenario
from saga.models import SagaStatus, SagaStep, StepStatus
from saga.orchestrator import SagaOrchestrator
from saga.resilience import CircuitBreaker, CircuitState
from saga.services import OrderCluster, ServiceFailureError
from saga.store import SagaStateStore


class SagaCoordinatorTests(unittest.IsolatedAsyncioTestCase):
    async def test_happy_path_executes_all_steps(self) -> None:
        cluster = OrderCluster(
            inventory_fail_rate=0.0,
            payment_fail_rate=0.0,
            shipment_fail_rate=0.0,
        )
        orchestrator = SagaOrchestrator(steps=cluster.build_steps())
        ctx = {
            "customer_id": "cust-1",
            "sku": "SKU-LAPTOP",
            "quantity": 2,
            "amount_cents": 199900,
            "address": "1600 Amphitheatre Pkwy, Mountain View, CA",
        }
        rec = await orchestrator.execute("saga-happy-1", ctx)
        self.assertEqual(rec.status, SagaStatus.COMPLETED)
        self.assertEqual(len(rec.steps), 3)
        self.assertTrue(all(s.status == StepStatus.COMPLETED for s in rec.steps))

        audit = cluster.audit_consistency([rec])
        self.assertEqual(audit.inconsistent_transactions, 0)
        self.assertEqual(audit.inconsistent_pct, 0.0)
        self.assertTrue(audit.stock_conservation_valid)
        orchestrator.close()

    async def test_failure_at_step_3_triggers_reverse_compensation(self) -> None:
        compensation_order: List[str] = []

        async def step1_act(sid: str, ctx: Dict[str, Any]) -> Dict[str, Any]:
            return {"s1": True}

        async def step1_comp(sid: str, ctx: Dict[str, Any]) -> Dict[str, Any]:
            compensation_order.append("comp_step_1")
            return {}

        async def step2_act(sid: str, ctx: Dict[str, Any]) -> Dict[str, Any]:
            return {"s2": True}

        async def step2_comp(sid: str, ctx: Dict[str, Any]) -> Dict[str, Any]:
            compensation_order.append("comp_step_2")
            return {}

        async def step3_act(sid: str, ctx: Dict[str, Any]) -> Dict[str, Any]:
            raise ServiceFailureError("shipment failure")

        async def step3_comp(sid: str, ctx: Dict[str, Any]) -> Dict[str, Any]:
            compensation_order.append("comp_step_3")
            return {}

        orchestrator = SagaOrchestrator(
            steps=[
                SagaStep("step_1", step1_act, step1_comp),
                SagaStep("step_2", step2_act, step2_comp),
                SagaStep("step_3", step3_act, step3_comp),
            ]
        )
        rec = await orchestrator.execute("saga-rev-1", {})
        self.assertEqual(rec.status, SagaStatus.COMPENSATED)
        self.assertEqual(rec.failed_step, "step_3")
        self.assertEqual(compensation_order, ["comp_step_2", "comp_step_1"])
        self.assertIsNotNone(rec.failure_detection_latency_ms)
        self.assertIsNotNone(rec.compensation_duration_ms)
        orchestrator.close()

    async def test_compensation_retries_on_transient_failure(self) -> None:
        attempts = 0

        async def s1_act(sid: str, ctx: Dict[str, Any]) -> Dict[str, Any]:
            return {"reserved": True}

        async def s1_comp(sid: str, ctx: Dict[str, Any]) -> Dict[str, Any]:
            nonlocal attempts
            attempts += 1
            if attempts < 3:
                raise ServiceFailureError("transient network error during release")
            return {"released": True}

        async def s2_act(sid: str, ctx: Dict[str, Any]) -> Dict[str, Any]:
            raise ServiceFailureError("payment declined")

        async def s2_comp(sid: str, ctx: Dict[str, Any]) -> Dict[str, Any]:
            return {}

        orchestrator = SagaOrchestrator(
            steps=[
                SagaStep(
                    "reserve_inventory",
                    s1_act,
                    s1_comp,
                    max_compensation_retries=5,
                    retry_backoff_sec=0.001,
                ),
                SagaStep("charge_payment", s2_act, s2_comp),
            ]
        )
        rec = await orchestrator.execute("saga-retry-1", {})
        self.assertEqual(rec.status, SagaStatus.COMPENSATED)
        self.assertEqual(attempts, 3)
        self.assertEqual(rec.steps[0].compensation_attempts, 3)
        orchestrator.close()

    async def test_zombie_rpc_fenced_by_tombstone_after_timeout(self) -> None:
        cluster = OrderCluster(
            zombie_rate=1.0,
            zombie_delay_sec=0.03,
        )
        orchestrator = SagaOrchestrator(
            steps=cluster.build_steps(timeout_sec=0.005),
            persist_state=False,
        )
        ctx = {
            "customer_id": "cust-zombie",
            "sku": "SKU-LAPTOP",
            "quantity": 5,
            "amount_cents": 500000,
            "address": "1 Zombie Way",
        }
        rec = await orchestrator.execute("saga-zombie-1", ctx)
        self.assertEqual(rec.status, SagaStatus.COMPENSATED)
        self.assertIn("saga-zombie-1", cluster.inventory.tombstoned_sagas)

        # Wait for the delayed zombie forward request to arrive at InventoryService
        await cluster.drain_background_zombies()
        self.assertEqual(cluster.inventory.fenced_zombie_count, 1)

        audit = cluster.audit_consistency([rec])
        self.assertEqual(audit.inconsistent_transactions, 0)
        self.assertTrue(audit.stock_conservation_valid)
        orchestrator.close()

    async def test_circuit_breaker_trips_and_fast_fails(self) -> None:
        cluster = OrderCluster(payment_fail_rate=1.0)
        cb = CircuitBreaker(
            "charge_payment", failure_threshold=3, recovery_timeout_sec=10.0
        )
        orchestrator = SagaOrchestrator(
            steps=cluster.build_steps(),
            persist_state=False,
            circuit_breakers={"charge_payment": cb},
        )
        ctx = {
            "customer_id": "cust-cb",
            "sku": "SKU-PHONE",
            "quantity": 1,
            "amount_cents": 50000,
            "address": "10 Circuit Rd",
        }
        records = []
        for i in range(6):
            records.append(await orchestrator.execute(f"saga-cb-{i}", ctx))

        self.assertEqual(cb.state, CircuitState.OPEN)
        self.assertEqual(cb.fast_rejected_calls, 3)
        audit = cluster.audit_consistency(records)
        self.assertEqual(audit.inconsistent_transactions, 0)
        self.assertTrue(audit.stock_conservation_valid)

    async def test_dead_letter_queue_and_replay(self) -> None:
        comp_Broken = True

        async def s1_act(sid: str, ctx: Dict[str, Any]) -> Dict[str, Any]:
            return {"ok": True}

        async def s1_comp(sid: str, ctx: Dict[str, Any]) -> Dict[str, Any]:
            if comp_Broken:
                raise ServiceFailureError("downstream DB offline during rollback")
            return {"rolled_back": True}

        async def s2_act(sid: str, ctx: Dict[str, Any]) -> Dict[str, Any]:
            raise ServiceFailureError("step 2 failed")

        async def s2_comp(sid: str, ctx: Dict[str, Any]) -> Dict[str, Any]:
            return {}

        orchestrator = SagaOrchestrator(
            steps=[
                SagaStep(
                    "step_1",
                    s1_act,
                    s1_comp,
                    max_compensation_retries=2,
                    retry_backoff_sec=0.001,
                ),
                SagaStep("step_2", s2_act, s2_comp),
            ]
        )
        rec = await orchestrator.execute("saga-dlq-1", {})
        self.assertEqual(rec.status, SagaStatus.FAILED)
        self.assertEqual(len(orchestrator.dlq.pending()), 1)

        comp_Broken = False
        replayed = await orchestrator.replay_dlq()
        self.assertEqual(replayed, 1)
        self.assertEqual(len(orchestrator.dlq.pending()), 0)
        orchestrator.close()

    async def test_parallel_dag_execution_and_rollback(self) -> None:
        compensated: List[str] = []

        async def fraud_act(sid: str, ctx: Dict[str, Any]) -> Dict[str, Any]:
            await asyncio.sleep(0.002)
            return {"fraud_cleared": True}

        async def fraud_comp(sid: str, ctx: Dict[str, Any]) -> Dict[str, Any]:
            compensated.append("fraud_check")
            return {}

        async def inv_act(sid: str, ctx: Dict[str, Any]) -> Dict[str, Any]:
            await asyncio.sleep(0.002)
            return {"inv_reserved": True}

        async def inv_comp(sid: str, ctx: Dict[str, Any]) -> Dict[str, Any]:
            compensated.append("reserve_inventory")
            return {}

        async def pay_act(sid: str, ctx: Dict[str, Any]) -> Dict[str, Any]:
            raise ServiceFailureError("payment declined in stage 2")

        async def pay_comp(sid: str, ctx: Dict[str, Any]) -> Dict[str, Any]:
            compensated.append("charge_payment")
            return {}

        s_fraud = SagaStep("fraud_check", fraud_act, fraud_comp)
        s_inv = SagaStep("reserve_inventory", inv_act, inv_comp)
        s_pay = SagaStep("charge_payment", pay_act, pay_comp)

        orchestrator = SagaOrchestrator(
            steps=[s_fraud, s_inv, s_pay], persist_state=False
        )
        rec = await orchestrator.execute_dag(
            "saga-dag-1", stages=[[s_fraud, s_inv], [s_pay]], initial_context={}
        )
        self.assertEqual(rec.status, SagaStatus.COMPENSATED)
        self.assertEqual(set(compensated), {"fraud_check", "reserve_inventory"})

    async def test_crash_recovery_from_durable_store(self) -> None:
        cluster = OrderCluster()
        store = SagaStateStore(":memory:")
        ctx = {
            "customer_id": "cust-crash",
            "sku": "SKU-MONITOR",
            "quantity": 3,
            "amount_cents": 90000,
            "address": "500 Recovery Rd",
        }
        saga_id = "saga-crash-001"
        store.create_saga(saga_id, ctx)
        await cluster.inventory.reserve_inventory(saga_id, ctx)
        store.record_step_event(saga_id, 0, "reserve_inventory", StepStatus.COMPLETED)
        await cluster.payment.charge_payment(saga_id, ctx)
        store.record_step_event(saga_id, 1, "charge_payment", StepStatus.COMPLETED)
        store.record_step_event(saga_id, 2, "create_shipment", StepStatus.RUNNING)

        orchestrator = SagaOrchestrator(steps=cluster.build_steps(), store=store)
        recovered = await orchestrator.recover_incomplete_sagas()
        self.assertEqual(len(recovered), 1)
        self.assertEqual(recovered[0].status, SagaStatus.COMPENSATED)

        audit = cluster.audit_consistency(recovered)
        self.assertEqual(audit.inconsistent_transactions, 0)
        self.assertTrue(audit.stock_conservation_valid)
        store.close()

    async def test_randomized_load_zero_inconsistent_state(self) -> None:
        metrics = await run_scenario(
            scenario_name="Unit Test Randomized Load",
            num_transactions=300,
            concurrency=32,
            inventory_fail_rate=0.10,
            payment_fail_rate=0.15,
            shipment_fail_rate=0.15,
            compensation_fail_rate=0.05,
            latency_sec=0.0001,
            zombie_rate=0.05,
            zombie_delay_sec=0.006,
            step_timeout_sec=0.002,
            enable_circuit_breaker=True,
            persist_state=True,
        )
        self.assertEqual(metrics.inconsistent_state_pct, 0.0)
        self.assertTrue(metrics.stock_conservation_valid)
        self.assertEqual(metrics.failed_uncompensated_transactions, 0)
        self.assertGreater(metrics.fenced_zombie_requests, 0)


if __name__ == "__main__":
    unittest.main()
