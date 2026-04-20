"""Integration tests for review queue + manual overrides + audit log.

These tests run against an in-memory SQLite database so they exercise
the real SQLAlchemy relationships, enums, and JSON columns.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import create_engine, event, select
from sqlalchemy.orm import Session

from headcount.db.enums import (
    EventType,
    OverrideField,
    PriorityTier,
    ReviewReason,
    ReviewStatus,
)
from headcount.models import (
    Base,
    Company,
    ManualOverride,
    ReviewQueueItem,
)
from headcount.review.audit import record_audit
from headcount.review.overrides import load_active_overrides
from headcount.review.queue import QueueCandidate, upsert_review_items


@pytest.fixture()
def session() -> Session:
    engine = create_engine("sqlite:///:memory:", future=True)

    @event.listens_for(engine, "connect")
    def _enable_fk(dbapi_conn, _):  # type: ignore[no-untyped-def]
        dbapi_conn.execute("PRAGMA foreign_keys=ON")

    Base.metadata.create_all(engine)
    with Session(engine, future=True) as s:
        yield s


def _make_company(session: Session, name: str = "Acme Inc") -> Company:
    c = Company(
        canonical_name=name,
        canonical_domain=f"{name.lower().replace(' ', '')}.com",
        priority_tier=PriorityTier.P1,
    )
    session.add(c)
    session.flush()
    return c


# --- review queue upsert ---------------------------------------------------


def test_queue_insert_then_refresh_dedupes_on_key(session: Session) -> None:
    company = _make_company(session)
    key_args = {
        "company_id": company.id,
        "estimate_version_id": None,
        "review_reason": ReviewReason.low_confidence,
    }
    first = QueueCandidate(
        **key_args,
        detail="3 months below medium (min_score=0.25)",
        severity=0.75,
        confidence_score=0.25,
    )
    counts = upsert_review_items(session, [first])
    assert counts == {"inserted": 1, "refreshed": 0, "skipped": 0}

    refreshed = QueueCandidate(
        **key_args,
        detail="5 months below medium (min_score=0.18)",
        severity=0.82,
        confidence_score=0.18,
    )
    counts = upsert_review_items(session, [refreshed])
    assert counts == {"inserted": 0, "refreshed": 1, "skipped": 0}

    rows = session.execute(select(ReviewQueueItem)).scalars().all()
    assert len(rows) == 1
    assert rows[0].detail.startswith("5 months")


def test_queue_skips_resolved_rows(session: Session) -> None:
    company = _make_company(session)
    cand = QueueCandidate(
        company_id=company.id,
        estimate_version_id=None,
        review_reason=ReviewReason.anomaly,
        detail="original",
        severity=0.5,
    )
    upsert_review_items(session, [cand])

    row = session.execute(select(ReviewQueueItem)).scalars().one()
    row.status = ReviewStatus.resolved
    session.flush()

    upsert_review_items(
        session,
        [
            QueueCandidate(
                company_id=company.id,
                estimate_version_id=None,
                review_reason=ReviewReason.anomaly,
                detail="newer",
                severity=0.9,
            )
        ],
    )
    row = session.execute(select(ReviewQueueItem)).scalars().one()
    assert row.status is ReviewStatus.resolved
    assert row.detail == "original"


def test_queue_priority_reflects_severity_and_confidence(session: Session) -> None:
    company_a = _make_company(session, "Low Sev Co")
    company_b = _make_company(session, "High Sev Co")
    low_sev = QueueCandidate(
        company_id=company_a.id,
        estimate_version_id=None,
        review_reason=ReviewReason.low_confidence,
        detail="low severity",
        severity=0.0,
        confidence_score=0.9,
    )
    high_sev = QueueCandidate(
        company_id=company_b.id,
        estimate_version_id=None,
        review_reason=ReviewReason.low_confidence,
        detail="high severity",
        severity=1.0,
        confidence_score=0.05,
    )
    upsert_review_items(session, [low_sev, high_sev])
    rows = (
        session.execute(
            select(ReviewQueueItem).order_by(ReviewQueueItem.priority.desc())
        )
        .scalars()
        .all()
    )
    assert len(rows) == 2
    assert rows[0].priority > rows[1].priority


# --- manual overrides ------------------------------------------------------


def test_overrides_loader_returns_anchor_pin_and_suppress_window(session: Session) -> None:
    company = _make_company(session)
    session.add(
        ManualOverride(
            company_id=company.id,
            field_name=OverrideField.current_anchor,
            override_value_json={
                "anchor_month": "2023-06-01",
                "value_min": 950,
                "value_point": 1000,
                "value_max": 1050,
                "confidence": 0.95,
            },
            reason="analyst pin",
        )
    )
    session.add(
        ManualOverride(
            company_id=company.id,
            field_name=OverrideField.estimate_suppress_window,
            override_value_json={
                "start_month": "2023-01-01",
                "end_month": "2023-03-01",
            },
            reason="data quality pause",
        )
    )
    session.add(
        ManualOverride(
            company_id=company.id,
            field_name=OverrideField.event_segment,
            override_value_json={
                "event_month": "2023-07-01",
                "event_type": "acquisition",
            },
            reason="undocumented acquisition",
        )
    )
    session.flush()

    active = load_active_overrides(session, company.id)
    assert len(active.anchor_pins) == 1
    assert active.anchor_pins[0].value_point == 1000
    assert len(active.suppress_windows) == 1
    assert active.is_suppressed(datetime(2023, 2, 1).date()) is not None
    assert active.is_suppressed(datetime(2023, 4, 1).date()) is None
    assert len(active.synthetic_events) == 1
    assert active.synthetic_events[0].event_type is EventType.acquisition


def test_expired_overrides_are_ignored(session: Session) -> None:
    company = _make_company(session)
    past = datetime.now(tz=UTC) - timedelta(days=10)
    session.add(
        ManualOverride(
            company_id=company.id,
            field_name=OverrideField.current_anchor,
            override_value_json={
                "anchor_month": "2022-06-01",
                "value_min": 900,
                "value_point": 950,
                "value_max": 1000,
            },
            reason="expired",
            expires_at=past,
        )
    )
    session.flush()
    active = load_active_overrides(session, company.id)
    assert active.anchor_pins == ()


# --- audit log -------------------------------------------------------------


def test_audit_log_writes_row(session: Session) -> None:
    company = _make_company(session)
    row = record_audit(
        session,
        actor_type="cli",
        actor_id="alice",
        action="override_created",
        target_type="manual_override",
        target_id=company.id,
        payload={"note": "hello"},
    )
    assert row.id is not None
    assert row.payload_json is not None
    assert dict(row.payload_json)["note"] == "hello"
