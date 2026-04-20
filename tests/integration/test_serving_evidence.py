"""Integration tests for the structured evidence builder."""

from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session

from headcount.db.enums import (
    AnchorType,
    EventSourceClass,
    EventType,
    HeadcountValueKind,
    OverrideField,
    PriorityTier,
    SourceName,
)
from headcount.estimate.pipeline import estimate_series
from headcount.models import (
    Base,
    Company,
    CompanyAnchorObservation,
    CompanyEvent,
    ManualOverride,
    Person,
    PersonEmploymentObservation,
)
from headcount.serving.evidence import (
    EVIDENCE_VERSION,
    EvidenceNotFoundError,
    build_evidence,
)


@pytest.fixture()
def session() -> Session:
    engine = create_engine("sqlite:///:memory:", future=True)

    @event.listens_for(engine, "connect")
    def _enable_fk(dbapi_conn, _):  # type: ignore[no-untyped-def]
        dbapi_conn.execute("PRAGMA foreign_keys=ON")

    Base.metadata.create_all(engine)
    with Session(engine, future=True) as s:
        yield s


def _make_company(session: Session) -> Company:
    c = Company(
        canonical_name="Acme Inc",
        canonical_domain="acme.com",
        priority_tier=PriorityTier.P1,
    )
    session.add(c)
    session.flush()
    return c


def _seed_series(session: Session) -> Company:
    company = _make_company(session)
    session.add(
        CompanyAnchorObservation(
            company_id=company.id,
            anchor_type=AnchorType.historical_statement,
            anchor_month=date(2023, 6, 1),
            headcount_value_point=1000,
            headcount_value_min=980,
            headcount_value_max=1020,
            headcount_value_kind=HeadcountValueKind.exact,
            confidence=0.9,
        )
    )
    for i in range(10):
        p = Person(
            source_name=SourceName.manual,
            source_person_key=f"manual::p{i}",
            display_name=f"Person {i}",
        )
        session.add(p)
        session.flush()
        session.add(
            PersonEmploymentObservation(
                person_id=p.id,
                company_id=company.id,
                start_month=date(2023, 1, 1),
                end_month=None,
                is_current_role=True,
            )
        )
    session.flush()

    estimate_series(
        session,
        start_month=date(2023, 1, 1),
        end_month=date(2023, 12, 1),
        as_of_month=date(2023, 12, 1),
        sample_floor=1,
    )
    return company


def test_build_evidence_populates_all_sections(session: Session) -> None:
    company = _seed_series(session)

    tree = build_evidence(session, company_id=company.id, month=date(2023, 6, 1))

    assert tree["evidence_version"] == EVIDENCE_VERSION
    assert tree["company"]["id"] == company.id
    assert tree["company"]["canonical_name"] == "Acme Inc"
    assert tree["month"] == "2023-06-01"
    assert tree["estimate"]["month"] == "2023-06-01"
    assert tree["estimate"]["value_point"] > 0
    assert tree["estimate_version"]["method_version"]

    assert tree["segment"]["start_month"] is None
    assert tree["segment"]["end_month_exclusive"] is None
    assert tree["segment"]["opening_events"] == []

    assert len(tree["inputs"]["anchors"]) == 1
    assert tree["inputs"]["anchors"][0]["value_point"] == 1000.0
    assert tree["inputs"]["events"] == []
    assert tree["inputs"]["employment_snapshot"]["public_profile_count"] == 10

    assert tree["reconciled_anchors"]
    assert tree["confidence"]["band"] in {"high", "medium", "low", "manual_review_required"}
    assert "components" in tree["confidence"]["components"]


def test_build_evidence_respects_segments(session: Session) -> None:
    company = _seed_series(session)
    session.add(
        CompanyEvent(
            company_id=company.id,
            event_type=EventType.acquisition,
            event_month=date(2023, 8, 1),
            source_class=EventSourceClass.manual,
            confidence=1.0,
            description="Acme acquired BetaCorp",
        )
    )
    session.flush()

    estimate_series(
        session,
        start_month=date(2023, 1, 1),
        end_month=date(2023, 12, 1),
        as_of_month=date(2023, 12, 1),
        sample_floor=1,
    )

    pre = build_evidence(session, company_id=company.id, month=date(2023, 6, 1))
    post = build_evidence(session, company_id=company.id, month=date(2023, 10, 1))

    assert pre["segment"]["end_month_exclusive"] == "2023-08-01"
    assert post["segment"]["start_month"] == "2023-08-01"
    assert post["segment"]["end_month_exclusive"] is None
    assert any(ev["event_type"] == "acquisition" for ev in post["segment"]["opening_events"])
    # Anchors from before the break should not bleed into the post segment.
    assert all(a["anchor_month"] >= "2023-08-01" for a in post["inputs"]["anchors"])


def test_build_evidence_includes_overrides_and_audit(session: Session) -> None:
    company = _seed_series(session)
    session.add(
        ManualOverride(
            company_id=company.id,
            field_name=OverrideField.estimate_suppress_window,
            override_value_json={"start_month": "2023-02-01", "end_month": "2023-04-01"},
            reason="test",
        )
    )
    session.flush()
    estimate_series(
        session,
        start_month=date(2023, 1, 1),
        end_month=date(2023, 12, 1),
        as_of_month=date(2023, 12, 1),
        sample_floor=1,
    )

    tree = build_evidence(session, company_id=company.id, month=date(2023, 3, 1))
    assert len(tree["overrides_applied"]) == 1
    assert tree["overrides_applied"][0]["field_name"] == "estimate_suppress_window"
    assert tree["audit"]
    assert any(a["action"] == "overrides_applied" for a in tree["audit"])


def test_build_evidence_404s_for_unknown_company(session: Session) -> None:
    with pytest.raises(EvidenceNotFoundError):
        build_evidence(session, company_id="nope", month=date(2023, 6, 1))


def test_build_evidence_404s_for_unknown_month(session: Session) -> None:
    company = _seed_series(session)
    with pytest.raises(EvidenceNotFoundError):
        build_evidence(session, company_id=company.id, month=date(2019, 1, 1))
