from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set

from .models import SagaExecutionRecord, SagaStatus, SagaStep
from .resilience import ZombieRequestFencedError


class ServiceFailureError(RuntimeError):
    pass


@dataclass
class ReservationRecord:
    saga_id: str
    sku: str
    quantity: int
    status: str = "RESERVED"


@dataclass
class PaymentRecord:
    saga_id: str
    customer_id: str
    amount_cents: int
    status: str = "CHARGED"


@dataclass
class ShipmentRecord:
    saga_id: str
    address: str
    sku: str
    quantity: int
    status: str = "SCHEDULED"


class InventoryService:
    def __init__(
        self,
        initial_stock: Optional[Dict[str, int]] = None,
        fail_rate: float = 0.0,
        compensation_fail_rate: float = 0.0,
        latency_sec: float = 0.0,
        zombie_rate: float = 0.0,
        zombie_delay_sec: float = 0.02,
        rng: Optional[random.Random] = None,
    ) -> None:
        self.initial_stock: Dict[str, int] = dict(
            initial_stock
            or {"SKU-LAPTOP": 100_000, "SKU-PHONE": 100_000, "SKU-MONITOR": 100_000}
        )
        self.available_stock: Dict[str, int] = dict(self.initial_stock)
        self.reservations: Dict[str, ReservationRecord] = {}
        self.tombstoned_sagas: Set[str] = set()
        self.fenced_zombie_count: int = 0
        self.background_tasks: Set[asyncio.Task[Any]] = set()
        self.fail_rate = fail_rate
        self.compensation_fail_rate = compensation_fail_rate
        self.latency_sec = latency_sec
        self.zombie_rate = zombie_rate
        self.zombie_delay_sec = zombie_delay_sec
        self._rng = rng or random.Random()

    async def _commit_reservation(
        self, saga_id: str, sku: str, qty: int, delay_sec: float
    ) -> Dict[str, Any]:
        if delay_sec > 0:
            await asyncio.sleep(delay_sec)

        if saga_id in self.tombstoned_sagas:
            self.fenced_zombie_count += 1
            raise ZombieRequestFencedError(
                f"InventoryService: fenced late zombie request for aborted saga {saga_id}"
            )

        if saga_id in self.reservations and self.reservations[saga_id].status == "RESERVED":
            return {"reservation_id": f"res-{saga_id}"}

        if self.available_stock.get(sku, 0) < qty:
            raise ServiceFailureError(f"InventoryService: insufficient stock for {sku}")

        self.available_stock[sku] -= qty
        self.reservations[saga_id] = ReservationRecord(
            saga_id=saga_id, sku=sku, quantity=qty, status="RESERVED"
        )
        return {"reservation_id": f"res-{saga_id}"}

    async def reserve_inventory(
        self, saga_id: str, context: Dict[str, Any]
    ) -> Dict[str, Any]:
        sku = context["sku"]
        qty = int(context["quantity"])

        if self.zombie_rate > 0 and self._rng.random() < self.zombie_rate:
            task = asyncio.create_task(
                self._commit_reservation(saga_id, sku, qty, self.zombie_delay_sec)
            )
            self.background_tasks.add(task)
            task.add_done_callback(
                lambda t: (
                    self.background_tasks.discard(t),
                    t.exception() if not t.cancelled() else None,
                )
            )
            return await asyncio.shield(task)

        if self.latency_sec > 0:
            await asyncio.sleep(self.latency_sec)

        if saga_id in self.tombstoned_sagas:
            self.fenced_zombie_count += 1
            raise ZombieRequestFencedError(
                f"InventoryService: fenced late zombie request for aborted saga {saga_id}"
            )

        if self._rng.random() < self.fail_rate:
            raise ServiceFailureError("InventoryService: simulated node/lock error")

        return await self._commit_reservation(saga_id, sku, qty, 0.0)

    async def release_inventory(
        self, saga_id: str, context: Dict[str, Any]
    ) -> Dict[str, Any]:
        if self.latency_sec > 0:
            await asyncio.sleep(self.latency_sec)

        if self._rng.random() < self.compensation_fail_rate:
            raise ServiceFailureError("InventoryService: transient release failure")

        self.tombstoned_sagas.add(saga_id)
        rec = self.reservations.get(saga_id)
        if rec is not None and rec.status == "RESERVED":
            self.available_stock[rec.sku] += rec.quantity
            rec.status = "RELEASED"
        return {"inventory_released": True}


class PaymentService:
    def __init__(
        self,
        fail_rate: float = 0.0,
        compensation_fail_rate: float = 0.0,
        latency_sec: float = 0.0,
        rng: Optional[random.Random] = None,
    ) -> None:
        self.payments: Dict[str, PaymentRecord] = {}
        self.tombstoned_sagas: Set[str] = set()
        self.fenced_zombie_count: int = 0
        self.fail_rate = fail_rate
        self.compensation_fail_rate = compensation_fail_rate
        self.latency_sec = latency_sec
        self._rng = rng or random.Random()

    async def charge_payment(
        self, saga_id: str, context: Dict[str, Any]
    ) -> Dict[str, Any]:
        if self.latency_sec > 0:
            await asyncio.sleep(self.latency_sec)

        if saga_id in self.tombstoned_sagas:
            self.fenced_zombie_count += 1
            raise ZombieRequestFencedError(
                f"PaymentService: fenced late zombie charge for aborted saga {saga_id}"
            )

        if saga_id in self.payments and self.payments[saga_id].status == "CHARGED":
            return {"payment_id": f"pay-{saga_id}"}

        if self._rng.random() < self.fail_rate:
            raise ServiceFailureError("PaymentService: gateway declined / timeout")

        customer_id = context["customer_id"]
        amount_cents = int(context["amount_cents"])
        if amount_cents <= 0:
            raise ServiceFailureError("PaymentService: invalid payment amount")

        self.payments[saga_id] = PaymentRecord(
            saga_id=saga_id,
            customer_id=customer_id,
            amount_cents=amount_cents,
            status="CHARGED",
        )
        return {"payment_id": f"pay-{saga_id}"}

    async def refund_payment(
        self, saga_id: str, context: Dict[str, Any]
    ) -> Dict[str, Any]:
        if self.latency_sec > 0:
            await asyncio.sleep(self.latency_sec)

        if self._rng.random() < self.compensation_fail_rate:
            raise ServiceFailureError("PaymentService: transient refund gateway error")

        self.tombstoned_sagas.add(saga_id)
        rec = self.payments.get(saga_id)
        if rec is not None and rec.status == "CHARGED":
            rec.status = "REFUNDED"
        return {"payment_refunded": True}


class ShipmentService:
    def __init__(
        self,
        fail_rate: float = 0.0,
        compensation_fail_rate: float = 0.0,
        latency_sec: float = 0.0,
        rng: Optional[random.Random] = None,
    ) -> None:
        self.shipments: Dict[str, ShipmentRecord] = {}
        self.tombstoned_sagas: Set[str] = set()
        self.fenced_zombie_count: int = 0
        self.fail_rate = fail_rate
        self.compensation_fail_rate = compensation_fail_rate
        self.latency_sec = latency_sec
        self._rng = rng or random.Random()

    async def create_shipment(
        self, saga_id: str, context: Dict[str, Any]
    ) -> Dict[str, Any]:
        if self.latency_sec > 0:
            await asyncio.sleep(self.latency_sec)

        if saga_id in self.tombstoned_sagas:
            self.fenced_zombie_count += 1
            raise ZombieRequestFencedError(
                f"ShipmentService: fenced late zombie shipment for aborted saga {saga_id}"
            )

        if saga_id in self.shipments and self.shipments[saga_id].status == "SCHEDULED":
            return {"shipment_id": f"shp-{saga_id}"}

        if self._rng.random() < self.fail_rate:
            raise ServiceFailureError("ShipmentService: carrier dispatch unavailable")

        self.shipments[saga_id] = ShipmentRecord(
            saga_id=saga_id,
            address=context["address"],
            sku=context["sku"],
            quantity=int(context["quantity"]),
            status="SCHEDULED",
        )
        return {"shipment_id": f"shp-{saga_id}"}

    async def cancel_shipment(
        self, saga_id: str, context: Dict[str, Any]
    ) -> Dict[str, Any]:
        if self.latency_sec > 0:
            await asyncio.sleep(self.latency_sec)

        if self._rng.random() < self.compensation_fail_rate:
            raise ServiceFailureError("ShipmentService: transient carrier cancel error")

        self.tombstoned_sagas.add(saga_id)
        rec = self.shipments.get(saga_id)
        if rec is not None and rec.status == "SCHEDULED":
            rec.status = "CANCELLED"
        return {"shipment_cancelled": True}


@dataclass
class ConsistencyAuditResult:
    total_transactions: int
    consistent_transactions: int
    inconsistent_transactions: int
    inconsistent_pct: float
    stock_conservation_valid: bool
    fenced_zombie_requests: int = 0
    violations: List[str] = field(default_factory=list)


class OrderCluster:
    def __init__(
        self,
        inventory_fail_rate: float = 0.0,
        payment_fail_rate: float = 0.0,
        shipment_fail_rate: float = 0.0,
        compensation_fail_rate: float = 0.0,
        latency_sec: float = 0.0,
        zombie_rate: float = 0.0,
        zombie_delay_sec: float = 0.015,
        seed: int = 42,
    ) -> None:
        self.inventory = InventoryService(
            fail_rate=inventory_fail_rate,
            compensation_fail_rate=compensation_fail_rate,
            latency_sec=latency_sec,
            zombie_rate=zombie_rate,
            zombie_delay_sec=zombie_delay_sec,
            rng=random.Random(seed + 1),
        )
        self.payment = PaymentService(
            fail_rate=payment_fail_rate,
            compensation_fail_rate=compensation_fail_rate,
            latency_sec=latency_sec,
            rng=random.Random(seed + 2),
        )
        self.shipment = ShipmentService(
            fail_rate=shipment_fail_rate,
            compensation_fail_rate=compensation_fail_rate,
            latency_sec=latency_sec,
            rng=random.Random(seed + 3),
        )

    def build_steps(self, timeout_sec: float = 2.0) -> List[SagaStep]:
        return [
            SagaStep(
                name="reserve_inventory",
                action=self.inventory.reserve_inventory,
                compensation=self.inventory.release_inventory,
                timeout_sec=timeout_sec,
            ),
            SagaStep(
                name="charge_payment",
                action=self.payment.charge_payment,
                compensation=self.payment.refund_payment,
                timeout_sec=timeout_sec,
            ),
            SagaStep(
                name="create_shipment",
                action=self.shipment.create_shipment,
                compensation=self.shipment.cancel_shipment,
                timeout_sec=timeout_sec,
            ),
        ]

    async def drain_background_zombies(self) -> None:
        if self.inventory.background_tasks:
            await asyncio.gather(
                *list(self.inventory.background_tasks), return_exceptions=True
            )

    def audit_consistency(
        self, records: List[SagaExecutionRecord]
    ) -> ConsistencyAuditResult:
        violations: List[str] = []
        inconsistent_count = 0
        expected_reserved_by_sku: Dict[str, int] = {
            sku: 0 for sku in self.inventory.initial_stock
        }

        for rec in records:
            sid = rec.saga_id
            inv = self.inventory.reservations.get(sid)
            pay = self.payment.payments.get(sid)
            shp = self.shipment.shipments.get(sid)

            if rec.status == SagaStatus.COMPLETED:
                ok = (
                    inv is not None
                    and inv.status == "RESERVED"
                    and pay is not None
                    and pay.status == "CHARGED"
                    and shp is not None
                    and shp.status == "SCHEDULED"
                )
                if not ok:
                    inconsistent_count += 1
                    violations.append(
                        f"Saga {sid} COMPLETED but service state mismatch: inv={inv}, pay={pay}, shp={shp}"
                    )
                else:
                    expected_reserved_by_sku[inv.sku] += inv.quantity
            else:
                inv_ok = inv is None or inv.status == "RELEASED"
                pay_ok = pay is None or pay.status == "REFUNDED"
                shp_ok = shp is None or shp.status == "CANCELLED"
                if not (inv_ok and pay_ok and shp_ok):
                    inconsistent_count += 1
                    violations.append(
                        f"Saga {sid} {rec.status.value} left dangling resources: inv={inv}, pay={pay}, shp={shp}"
                    )

        stock_valid = True
        for sku, initial_qty in self.inventory.initial_stock.items():
            actual_avail = self.inventory.available_stock.get(sku, 0)
            expected_avail = initial_qty - expected_reserved_by_sku.get(sku, 0)
            if actual_avail != expected_avail:
                stock_valid = False
                violations.append(
                    f"Stock conservation violated for {sku}: expected={expected_avail}, actual={actual_avail}"
                )

        total = len(records)
        consistent = total - inconsistent_count
        pct = (inconsistent_count / total * 100.0) if total > 0 else 0.0
        total_fenced = (
            self.inventory.fenced_zombie_count
            + self.payment.fenced_zombie_count
            + self.shipment.fenced_zombie_count
        )
        return ConsistencyAuditResult(
            total_transactions=total,
            consistent_transactions=consistent,
            inconsistent_transactions=inconsistent_count,
            inconsistent_pct=pct,
            stock_conservation_valid=stock_valid,
            fenced_zombie_requests=total_fenced,
            violations=violations,
        )
