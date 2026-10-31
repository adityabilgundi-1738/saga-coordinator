from __future__ import annotations

import json
import sqlite3
import threading
import time
from typing import Any, Dict, List, Optional

from .models import SagaStatus, StepExecutionRecord, StepStatus


class SagaStateStore:
    def __init__(self, db_path: str = ":memory:") -> None:
        self.db_path = db_path
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("PRAGMA journal_mode=WAL;")
            cur.execute("PRAGMA synchronous=NORMAL;")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS sagas (
                    saga_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    context_json TEXT NOT NULL,
                    started_at_ns INTEGER NOT NULL,
                    updated_at_ns INTEGER NOT NULL,
                    failed_step TEXT,
                    error TEXT
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS saga_step_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    saga_id TEXT NOT NULL,
                    step_index INTEGER NOT NULL,
                    step_name TEXT NOT NULL,
                    status TEXT NOT NULL,
                    timestamp_ns INTEGER NOT NULL,
                    detail TEXT,
                    FOREIGN KEY (saga_id) REFERENCES sagas(saga_id)
                )
                """
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_step_events_saga ON saga_step_events(saga_id);"
            )
            self._conn.commit()

    def create_saga(
        self, saga_id: str, context: Dict[str, Any], started_at_ns: Optional[int] = None
    ) -> None:
        now_ns = started_at_ns if started_at_ns is not None else time.perf_counter_ns()
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO sagas (saga_id, status, context_json, started_at_ns, updated_at_ns)
                VALUES (?, ?, ?, ?, ?)
                """,
                (saga_id, SagaStatus.RUNNING.value, json.dumps(context), now_ns, now_ns),
            )
            self._conn.commit()

    def update_saga_status(
        self,
        saga_id: str,
        status: SagaStatus,
        context: Optional[Dict[str, Any]] = None,
        failed_step: Optional[str] = None,
        error: Optional[str] = None,
        updated_at_ns: Optional[int] = None,
    ) -> None:
        now_ns = updated_at_ns if updated_at_ns is not None else time.perf_counter_ns()
        with self._lock:
            if context is not None:
                self._conn.execute(
                    """
                    UPDATE sagas
                    SET status = ?, context_json = ?, updated_at_ns = ?,
                        failed_step = COALESCE(?, failed_step),
                        error = COALESCE(?, error)
                    WHERE saga_id = ?
                    """,
                    (
                        status.value,
                        json.dumps(context),
                        now_ns,
                        failed_step,
                        error,
                        saga_id,
                    ),
                )
            else:
                self._conn.execute(
                    """
                    UPDATE sagas
                    SET status = ?, updated_at_ns = ?,
                        failed_step = COALESCE(?, failed_step),
                        error = COALESCE(?, error)
                    WHERE saga_id = ?
                    """,
                    (status.value, now_ns, failed_step, error, saga_id),
                )
            self._conn.commit()

    def record_step_event(
        self,
        saga_id: str,
        step_index: int,
        step_name: str,
        status: StepStatus,
        timestamp_ns: Optional[int] = None,
        detail: Optional[str] = None,
    ) -> None:
        now_ns = timestamp_ns if timestamp_ns is not None else time.perf_counter_ns()
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO saga_step_events (saga_id, step_index, step_name, status, timestamp_ns, detail)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (saga_id, step_index, step_name, status.value, now_ns, detail),
            )
            self._conn.commit()

    def get_saga(self, saga_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM sagas WHERE saga_id = ?", (saga_id,)
            ).fetchone()
            if row is None:
                return None
            return {
                "saga_id": row["saga_id"],
                "status": SagaStatus(row["status"]),
                "context": json.loads(row["context_json"]),
                "started_at_ns": row["started_at_ns"],
                "updated_at_ns": row["updated_at_ns"],
                "failed_step": row["failed_step"],
                "error": row["error"],
            }

    def get_step_events(self, saga_id: str) -> List[StepExecutionRecord]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT saga_id, step_index, step_name, status, timestamp_ns, detail
                FROM saga_step_events
                WHERE saga_id = ?
                ORDER BY id ASC
                """,
                (saga_id,),
            ).fetchall()
            return [
                StepExecutionRecord(
                    saga_id=r["saga_id"],
                    step_index=r["step_index"],
                    step_name=r["step_name"],
                    status=StepStatus(r["status"]),
                    updated_at_ns=r["timestamp_ns"],
                    detail=r["detail"],
                )
                for r in rows
            ]

    def list_incomplete_sagas(self) -> List[Dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT * FROM sagas
                WHERE status IN (?, ?, ?)
                ORDER BY started_at_ns ASC
                """,
                (
                    SagaStatus.PENDING.value,
                    SagaStatus.RUNNING.value,
                    SagaStatus.COMPENSATING.value,
                ),
            ).fetchall()
            return [
                {
                    "saga_id": r["saga_id"],
                    "status": SagaStatus(r["status"]),
                    "context": json.loads(r["context_json"]),
                    "started_at_ns": r["started_at_ns"],
                    "updated_at_ns": r["updated_at_ns"],
                    "failed_step": r["failed_step"],
                    "error": r["error"],
                }
                for r in rows
            ]

    def close(self) -> None:
        with self._lock:
            self._conn.close()
