"""Tests for the async token bucket and the DB-backed budget store."""

from __future__ import annotations

import asyncio
import time
from datetime import UTC, date, datetime

import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session

from headcount.db.enums import (
    RunKind,
    RunStatus,
    SourceBudgetStatus,
    SourceName,
)
from headcount.ingest.rate_limit import (
    BudgetExhaustedError,
    BudgetTrippedError,
    CircuitBreaker,
    SourceBudgetStore,
    TokenBucket,
)
from headcount.models import Base, Run


@pytest.fixture()
def session() -> Session:
    engine = create_engine("sqlite:///:memory:", future=True)

    @event.listens_for(engine, "connect")
    def _enable_fk(dbapi_conn, _):  # type: ignore[no-untyped-def]
        dbapi_conn.execute("PRAGMA foreign_keys=ON")

    Base.metadata.create_all(engine)
    with Session(engine, future=True) as s:
        yield s


@pytest.fixture()
def run_id(session: Session) -> str:
    run = Run(
        kind=RunKind.full,
        status=RunStatus.running,
        started_at=datetime.now(tz=UTC),
        cutoff_month=date(2026, 4, 1),
        method_version="hc-v1",
        anchor_policy_version="anchor-v1",
        coverage_curve_version="coverage-v1",
        config_hash="test-config",
    )
    session.add(run)
    session.commit()
    return run.id


@pytest.mark.asyncio
async def test_token_bucket_blocks_until_refill() -> None:
    bucket = TokenBucket(capacity=2, refill_per_second=50.0)
    start = time.monotonic()
    await bucket.acquire()
    await bucket.acquire()
    await bucket.acquire()
    elapsed = time.monotonic() - start
    assert elapsed >= 0.01
    assert elapsed < 0.2


@pytest.mark.asyncio
async def test_token_bucket_parallel_waiters_serialize() -> None:
    bucket = TokenBucket(capacity=1, refill_per_second=100.0)
    results: list[float] = []

    async def consumer() -> None:
        await bucket.acquire()
        results.append(time.monotonic())

    start = time.monotonic()
    await asyncio.gather(consumer(), consumer(), consumer(), consumer())
    assert len(results) == 4
    assert all(t >= start for t in results)


@pytest.mark.asyncio
async def test_token_bucket_rejects_oversized_request() -> None:
    bucket = TokenBucket(capacity=2, refill_per_second=1.0)
    with pytest.raises(ValueError):
        await bucket.acquire(5)


def test_budget_store_reserve_and_outcome(session: Session, run_id: str) -> None:
    store = SourceBudgetStore(session, run_id=run_id, default_allowed=3)
    row = store.reserve(SourceName.company_web)
    assert row.requests_used == 0
    store.record_outcome(SourceName.company_web, outcome="ok")
    store.record_outcome(SourceName.company_web, outcome="ok")
    assert store.remaining(SourceName.company_web) == 1
    store.record_outcome(SourceName.company_web, outcome="ok")
    with pytest.raises(BudgetExhaustedError):
        store.reserve(SourceName.company_web)
    assert store.status(SourceName.company_web) is SourceBudgetStatus.exhausted


def test_budget_store_circuit_breaker_trips(session: Session, run_id: str) -> None:
    store = SourceBudgetStore(session, run_id=run_id, default_allowed=100, trip_after_n_failures=3)
    breaker = CircuitBreaker(store=store, source=SourceName.sec)
    store.reserve(SourceName.sec)
    for _ in range(3):
        breaker.record("error")
    assert breaker.is_open() is True
    with pytest.raises(BudgetTrippedError):
        store.reserve(SourceName.sec)


def test_budget_store_reset_on_ok(session: Session, run_id: str) -> None:
    store = SourceBudgetStore(session, run_id=run_id, default_allowed=100, trip_after_n_failures=3)
    store.reserve(SourceName.sec)
    store.record_outcome(SourceName.sec, outcome="error")
    store.record_outcome(SourceName.sec, outcome="error")
    store.record_outcome(SourceName.sec, outcome="ok")
    store.record_outcome(SourceName.sec, outcome="error")
    store.record_outcome(SourceName.sec, outcome="error")
    assert store.status(SourceName.sec) is SourceBudgetStatus.open


def test_cache_hit_does_not_consume_budget(session: Session, run_id: str) -> None:
    store = SourceBudgetStore(session, run_id=run_id, default_allowed=2)
    store.reserve(SourceName.sec)
    store.record_outcome(SourceName.sec, outcome="cache_hit")
    store.record_outcome(SourceName.sec, outcome="cache_hit")
    assert store.remaining(SourceName.sec) == 2
