"""Integration tests for benchmark event promotion + canonical event merge."""

from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import create_engine, event, select
from sqlalchemy.orm import Session

from headcount.db.enums import (
    BenchmarkEventCandidateStatus,
    BenchmarkEventHintType,
    EventSourceClass,
    EventType,
    PriorityTier,
)
from headcount.models import Base, Company
from headcount.models.benchmark import BenchmarkEventCandidate
from headcount.models.company_event import CompanyEvent
from headcount.parsers import (
    BENCHMARK_EVENT_DEFAULT_CONFIDENCE,
    map_hint_to_event_type,
    merge_events,
    promote_benchmark_events,
)

# ---------------------------------------------------------------------------
# Fixtures + helpers
# ---------------------------------------------------------------------------


@pytest.fixture()
def session() -> Session:
    engine = create_engine("sqlite:///:memory:", future=True)

    @event.listens_for(engine, "connect")
    def _enable_fk(dbapi_conn, _):  # type: ignore[no-untyped-def]
        dbapi_conn.execute("PRAGMA foreign_keys=ON")

    Base.metadata.create_all(engine)
    with Session(engine, future=True) as s:
        yield s


def _make_company(session: Session, *, name: str = "Acme Inc") -> Company:
    company = Company(
        canonical_name=name,
        canonical_domain=f"{name.lower().replace(' ', '')}.com",
        priority_tier=PriorityTier.P1,
    )
    session.add(company)
    session.flush()
    return company


def _make_candidate(
    session: Session,
    *,
    company_id: str | None,
    hint_type: BenchmarkEventHintType,
    event_month_hint: date | None,
    description: str,
    row_index: int = 1,
    status: BenchmarkEventCandidateStatus = BenchmarkEventCandidateStatus.pending_merge,
    workbook: str = "wb.xlsx",
    sheet: str = "Sheet1",
) -> BenchmarkEventCandidate:
    cand = BenchmarkEventCandidate(
        company_id=company_id,
        source_workbook=workbook,
        source_sheet=sheet,
        source_row_index=row_index,
        hint_type=hint_type,
        event_month_hint=event_month_hint,
        description=description,
        status=status,
    )
    session.add(cand)
    session.flush()
    return cand


# ---------------------------------------------------------------------------
# Hint mapping
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("hint", "expected"),
    [
        (BenchmarkEventHintType.acquisition, EventType.acquisition),
        (BenchmarkEventHintType.rebrand, EventType.rebrand),
        (BenchmarkEventHintType.merger, EventType.merger),
        (BenchmarkEventHintType.unknown, None),
    ],
)
def test_map_hint_to_event_type(hint: BenchmarkEventHintType, expected) -> None:
    assert map_hint_to_event_type(hint) is expected


# ---------------------------------------------------------------------------
# promote_benchmark_events
# ---------------------------------------------------------------------------


def test_promote_happy_path_creates_event(session: Session) -> None:
    co = _make_company(session)
    cand = _make_candidate(
        session,
        company_id=co.id,
        hint_type=BenchmarkEventHintType.acquisition,
        event_month_hint=date(2023, 6, 15),
        description="Acquired by Symphony AI in June 2023",
    )

    res = promote_benchmark_events(session)

    assert res.candidates_considered == 1
    assert res.promoted == 1
    assert res.skipped_unresolved == 0

    events = session.execute(select(CompanyEvent)).scalars().all()
    assert len(events) == 1
    ev = events[0]
    assert ev.company_id == co.id
    assert ev.event_type is EventType.acquisition
    assert ev.event_month == date(2023, 6, 1)  # month-floored
    assert ev.source_class is EventSourceClass.benchmark
    assert ev.confidence == BENCHMARK_EVENT_DEFAULT_CONFIDENCE
    assert ev.description == cand.description

    session.refresh(cand)
    assert cand.status is BenchmarkEventCandidateStatus.merged


def test_promote_skips_unresolved_candidate(session: Session) -> None:
    cand = _make_candidate(
        session,
        company_id=None,
        hint_type=BenchmarkEventHintType.acquisition,
        event_month_hint=date(2023, 6, 1),
        description="Acquired sometime",
    )
    res = promote_benchmark_events(session)
    assert res.promoted == 0
    assert res.skipped_unresolved == 1
    assert session.execute(select(CompanyEvent)).scalars().first() is None
    session.refresh(cand)
    assert cand.status is BenchmarkEventCandidateStatus.pending_merge


def test_promote_skips_unknown_hint(session: Session) -> None:
    co = _make_company(session)
    _make_candidate(
        session,
        company_id=co.id,
        hint_type=BenchmarkEventHintType.unknown,
        event_month_hint=date(2023, 6, 1),
        description="Something happened in June 2023",
    )
    res = promote_benchmark_events(session)
    assert res.promoted == 0
    assert res.skipped_unknown_hint == 1


def test_promote_skips_missing_event_month(session: Session) -> None:
    co = _make_company(session)
    _make_candidate(
        session,
        company_id=co.id,
        hint_type=BenchmarkEventHintType.merger,
        event_month_hint=None,
        description="Merger of equals",
    )
    res = promote_benchmark_events(session)
    assert res.promoted == 0
    assert res.skipped_missing_month == 1


def test_promote_is_idempotent_across_runs(session: Session) -> None:
    co = _make_company(session)
    _make_candidate(
        session,
        company_id=co.id,
        hint_type=BenchmarkEventHintType.acquisition,
        event_month_hint=date(2023, 6, 1),
        description="Acquired by Symphony AI in June 2023",
    )

    first = promote_benchmark_events(session)
    second = promote_benchmark_events(session)

    assert first.promoted == 1
    assert second.promoted == 0
    assert second.candidates_considered == 0  # status flipped, only_pending=True
    events = session.execute(select(CompanyEvent)).scalars().all()
    assert len(events) == 1


def test_promote_with_only_pending_false_dedupes_via_existing_event(
    session: Session,
) -> None:
    """A second candidate (different row, same event) should not double-insert."""

    co = _make_company(session)
    _make_candidate(
        session,
        company_id=co.id,
        hint_type=BenchmarkEventHintType.acquisition,
        event_month_hint=date(2023, 6, 1),
        description="Acquired by Symphony AI in June 2023",
        row_index=1,
    )
    _make_candidate(
        session,
        company_id=co.id,
        hint_type=BenchmarkEventHintType.acquisition,
        event_month_hint=date(2023, 6, 1),
        description="Acquired in June 2023 (other workbook)",
        row_index=2,
        workbook="other.xlsx",
    )

    res = promote_benchmark_events(session)

    assert res.candidates_considered == 2
    assert res.promoted == 1
    assert res.duplicates_of_existing_event == 1

    events = session.execute(select(CompanyEvent)).scalars().all()
    assert len(events) == 1


# ---------------------------------------------------------------------------
# merge_events precedence + dedup
# ---------------------------------------------------------------------------


def _add_event(
    session: Session,
    *,
    company_id: str,
    event_type: EventType,
    event_month: date,
    source_class: EventSourceClass,
    confidence: float,
    description: str | None = None,
) -> CompanyEvent:
    ev = CompanyEvent(
        company_id=company_id,
        event_type=event_type,
        event_month=event_month,
        source_class=source_class,
        confidence=confidence,
        description=description,
    )
    session.add(ev)
    session.flush()
    return ev


def test_merge_keeps_first_party_over_benchmark(session: Session) -> None:
    co = _make_company(session)
    benchmark = _add_event(
        session,
        company_id=co.id,
        event_type=EventType.acquisition,
        event_month=date(2023, 6, 1),
        source_class=EventSourceClass.benchmark,
        confidence=0.5,
        description="benchmark note",
    )
    first_party = _add_event(
        session,
        company_id=co.id,
        event_type=EventType.acquisition,
        event_month=date(2023, 6, 1),
        source_class=EventSourceClass.first_party,
        confidence=0.7,
        description="SEC 8-K",
    )

    res = merge_events(session)
    assert res.groups_considered == 1
    assert res.groups_collapsed == 1
    assert res.rows_deleted == 1

    survivors = session.execute(select(CompanyEvent)).scalars().all()
    assert len(survivors) == 1
    assert survivors[0].id == first_party.id
    # Confidence is bumped to max of the group.
    assert survivors[0].confidence == pytest.approx(0.7)
    # Original benchmark row is gone.
    assert session.get(CompanyEvent, benchmark.id) is None


def test_merge_manual_always_wins(session: Session) -> None:
    co = _make_company(session)
    _add_event(
        session,
        company_id=co.id,
        event_type=EventType.merger,
        event_month=date(2022, 1, 1),
        source_class=EventSourceClass.first_party,
        confidence=0.95,
        description="Filing says merger",
    )
    manual = _add_event(
        session,
        company_id=co.id,
        event_type=EventType.merger,
        event_month=date(2022, 1, 1),
        source_class=EventSourceClass.manual,
        confidence=0.4,
        description="analyst override",
    )
    merge_events(session)
    survivors = session.execute(select(CompanyEvent)).scalars().all()
    assert len(survivors) == 1
    assert survivors[0].id == manual.id
    # Confidence bumped to max of the group, not capped down to manual's.
    assert survivors[0].confidence == pytest.approx(0.95)


def test_merge_does_not_touch_distinct_groups(session: Session) -> None:
    co = _make_company(session)
    _add_event(
        session,
        company_id=co.id,
        event_type=EventType.acquisition,
        event_month=date(2023, 1, 1),
        source_class=EventSourceClass.benchmark,
        confidence=0.5,
    )
    _add_event(
        session,
        company_id=co.id,
        event_type=EventType.rebrand,
        event_month=date(2023, 1, 1),
        source_class=EventSourceClass.benchmark,
        confidence=0.5,
    )
    _add_event(
        session,
        company_id=co.id,
        event_type=EventType.acquisition,
        event_month=date(2024, 5, 1),
        source_class=EventSourceClass.benchmark,
        confidence=0.5,
    )

    res = merge_events(session)
    assert res.groups_considered == 3
    assert res.groups_collapsed == 0
    assert res.rows_deleted == 0
    assert len(session.execute(select(CompanyEvent)).scalars().all()) == 3


def test_merge_scopes_to_company(session: Session) -> None:
    a = _make_company(session, name="Acme")
    b = _make_company(session, name="BetaCo")
    _add_event(
        session,
        company_id=a.id,
        event_type=EventType.acquisition,
        event_month=date(2023, 6, 1),
        source_class=EventSourceClass.benchmark,
        confidence=0.5,
    )
    _add_event(
        session,
        company_id=a.id,
        event_type=EventType.acquisition,
        event_month=date(2023, 6, 1),
        source_class=EventSourceClass.first_party,
        confidence=0.8,
    )
    _add_event(
        session,
        company_id=b.id,
        event_type=EventType.acquisition,
        event_month=date(2023, 6, 1),
        source_class=EventSourceClass.benchmark,
        confidence=0.5,
    )
    _add_event(
        session,
        company_id=b.id,
        event_type=EventType.acquisition,
        event_month=date(2023, 6, 1),
        source_class=EventSourceClass.first_party,
        confidence=0.9,
    )

    merge_events(session, company_id=a.id)
    a_rows = (
        session.execute(select(CompanyEvent).where(CompanyEvent.company_id == a.id)).scalars().all()
    )
    b_rows = (
        session.execute(select(CompanyEvent).where(CompanyEvent.company_id == b.id)).scalars().all()
    )
    assert len(a_rows) == 1
    assert len(b_rows) == 2  # untouched


def test_merge_backfills_description_from_loser(session: Session) -> None:
    co = _make_company(session)
    survivor_seed = _add_event(
        session,
        company_id=co.id,
        event_type=EventType.acquisition,
        event_month=date(2023, 6, 1),
        source_class=EventSourceClass.first_party,
        confidence=0.7,
        description=None,
    )
    _add_event(
        session,
        company_id=co.id,
        event_type=EventType.acquisition,
        event_month=date(2023, 6, 1),
        source_class=EventSourceClass.benchmark,
        confidence=0.5,
        description="Acquired by Symphony AI in June 2023",
    )
    merge_events(session)
    session.refresh(survivor_seed)
    assert survivor_seed.description == "Acquired by Symphony AI in June 2023"


def test_merge_is_idempotent(session: Session) -> None:
    co = _make_company(session)
    _add_event(
        session,
        company_id=co.id,
        event_type=EventType.acquisition,
        event_month=date(2023, 6, 1),
        source_class=EventSourceClass.benchmark,
        confidence=0.5,
    )
    _add_event(
        session,
        company_id=co.id,
        event_type=EventType.acquisition,
        event_month=date(2023, 6, 1),
        source_class=EventSourceClass.first_party,
        confidence=0.7,
    )

    first = merge_events(session)
    second = merge_events(session)

    assert first.rows_deleted == 1
    assert second.rows_deleted == 0
    assert second.groups_collapsed == 0
    assert len(session.execute(select(CompanyEvent)).scalars().all()) == 1


# ---------------------------------------------------------------------------
# End-to-end: promote + merge
# ---------------------------------------------------------------------------


def test_promote_then_merge_with_existing_first_party_event(
    session: Session,
) -> None:
    """Realistic flow: a first_party event already exists; benchmark candidate
    promotes into a benchmark event; merge collapses them with first_party
    surviving."""

    co = _make_company(session)
    first_party = _add_event(
        session,
        company_id=co.id,
        event_type=EventType.acquisition,
        event_month=date(2023, 6, 1),
        source_class=EventSourceClass.first_party,
        confidence=0.85,
        description="Filed in 8-K",
    )
    _make_candidate(
        session,
        company_id=co.id,
        hint_type=BenchmarkEventHintType.acquisition,
        event_month_hint=date(2023, 6, 15),
        description="Acquired by Symphony AI in June 2023",
    )

    promote_res = promote_benchmark_events(session)
    assert promote_res.promoted == 1
    assert len(session.execute(select(CompanyEvent)).scalars().all()) == 2  # both rows present

    merge_res = merge_events(session, company_id=co.id)
    assert merge_res.groups_collapsed == 1
    assert merge_res.rows_deleted == 1

    rows = session.execute(select(CompanyEvent)).scalars().all()
    assert len(rows) == 1
    assert rows[0].id == first_party.id
    assert rows[0].confidence == pytest.approx(0.85)
