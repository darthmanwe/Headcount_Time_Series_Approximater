"""Forward-ref stub tests for the review writer/reader."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session

from headcount.db.enums import (
    CompanyStatus,
    OverrideField,
    PriorityTier,
    ReviewReason,
)
from headcount.ingest.policy import CircuitBreaker, TokenBucket
from headcount.models import Base, Company, ManualOverride
from headcount.review import EnqueueRequest, enqueue, get_active_overrides


@pytest.fixture()
def session() -> Session:
    engine = create_engine("sqlite:///:memory:", future=True)

    @event.listens_for(engine, "connect")
    def _enable_fk(dbapi_conn, _):  # type: ignore[no-untyped-def]
        dbapi_conn.execute("PRAGMA foreign_keys=ON")

    Base.metadata.create_all(engine)
    with Session(engine, future=True) as s:
        yield s


def test_enqueue_writes_review_item(session: Session) -> None:
    company = Company(
        canonical_name="Acme",
        status=CompanyStatus.active,
        priority_tier=PriorityTier.P1,
    )
    session.add(company)
    session.flush()
    item_id = enqueue(
        session,
        EnqueueRequest(
            company_id=company.id,
            reason=ReviewReason.resolution_ambiguity,
            priority=90,
            detail="two matches",
        ),
    )
    session.commit()
    assert item_id


def test_get_active_overrides_respects_expiry(session: Session) -> None:
    company = Company(
        canonical_name="Acme",
        status=CompanyStatus.active,
        priority_tier=PriorityTier.P1,
    )
    session.add(company)
    session.flush()
    now = datetime.now(tz=UTC)
    session.add_all(
        [
            ManualOverride(
                company_id=company.id,
                field_name=OverrideField.current_anchor,
                override_value_json={"point": 400},
                expires_at=None,
            ),
            ManualOverride(
                company_id=company.id,
                field_name=OverrideField.event_segment,
                override_value_json={"month": "2023-06"},
                expires_at=now - timedelta(days=1),
            ),
        ]
    )
    session.commit()
    active = get_active_overrides(session, company.id)
    assert len(active) == 1
    assert active[0].field_name is OverrideField.current_anchor


def test_policy_stubs_raise_not_implemented() -> None:
    bucket = TokenBucket(capacity=10, refill_per_second=1.0)
    with pytest.raises(NotImplementedError):
        bucket.acquire()
    breaker = CircuitBreaker(trip_after_n=5)
    with pytest.raises(NotImplementedError):
        breaker.record("error")
