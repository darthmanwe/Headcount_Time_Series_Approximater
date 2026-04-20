"""Promote benchmark event candidates into canonical ``CompanyEvent`` rows.

:class:`~headcount.models.benchmark.BenchmarkEventCandidate` rows are created
by the benchmark loader when a note column contains phrases like "Acquired by
Symphony AI in June 2023". They carry a coarse
:class:`~headcount.db.enums.BenchmarkEventHintType`, an optional
``event_month_hint``, and free-text ``description``. Once the resolver has
linked the candidate to a canonical ``Company`` we can *promote* them into
``company_event`` rows so downstream estimation / event segmentation can use
them.

Design invariants for the promotion step
----------------------------------------

1. **Read-through by company_id.** Only candidates whose ``company_id`` is
   already populated are eligible. Unresolved candidates are skipped (they'll
   be promoted on a subsequent pass after resolution finishes).
2. **Hint type must map.** ``BenchmarkEventHintType.unknown`` is never
   promoted - those stay in ``pending_merge`` forever until an analyst either
   reclassifies them or marks them ``rejected``. Conservative by design.
3. **Event month must be known.** If we can't peg the hint to a month we
   don't invent one; the candidate stays ``pending_merge``.
4. **Idempotent.** Re-running the promoter after candidates are already
   promoted MUST NOT create duplicate ``CompanyEvent`` rows or move statuses
   around unexpectedly. We look up an existing event by
   ``(company_id, event_type, event_month, source_class=benchmark)`` before
   inserting, and we only promote candidates whose status is
   ``pending_merge``.
5. **Source class is always ``benchmark``.** Promotion doesn't itself touch
   ``first_party`` / ``press`` / ``manual`` events - those are owned by other
   pipelines. Collapsing multi-provenance duplicates into a single row is the
   job of :mod:`headcount.parsers.event_merge`.
6. **Confidence starts low.** Benchmark notes are analyst-readable hints, not
   primary evidence, so promoted events get ``confidence=0.5`` by default.
   Event merge can then bump confidence once a higher-provenance source
   agrees.

Everything in this module is pure-functional in spirit - the only side
effects are SQLAlchemy ``session.add`` / field assignments; no commits.
Callers own the unit of work.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

from sqlalchemy import select
from sqlalchemy.orm import Session

from headcount.db.enums import (
    BenchmarkEventCandidateStatus,
    BenchmarkEventHintType,
    EventSourceClass,
    EventType,
)
from headcount.models.benchmark import BenchmarkEventCandidate
from headcount.models.company_event import CompanyEvent

EVENTS_PARSER_VERSION = "events_v1"
"""Bumped when the hint->event mapping or default-confidence policy changes."""

BENCHMARK_EVENT_DEFAULT_CONFIDENCE = 0.5

_HINT_TO_EVENT: dict[BenchmarkEventHintType, EventType] = {
    BenchmarkEventHintType.acquisition: EventType.acquisition,
    BenchmarkEventHintType.rebrand: EventType.rebrand,
    BenchmarkEventHintType.merger: EventType.merger,
}


@dataclass(slots=True)
class PromoteResult:
    """Summary of a single :func:`promote_benchmark_events` call."""

    candidates_considered: int = 0
    promoted: int = 0
    skipped_unresolved: int = 0
    skipped_unknown_hint: int = 0
    skipped_missing_month: int = 0
    already_merged: int = 0
    duplicates_of_existing_event: int = 0
    rejected_candidate_ids: list[str] = field(default_factory=list)
    promoted_event_ids: list[str] = field(default_factory=list)


def map_hint_to_event_type(hint: BenchmarkEventHintType) -> EventType | None:
    """Translate a :class:`BenchmarkEventHintType` to an :class:`EventType`.

    Returns ``None`` for :attr:`BenchmarkEventHintType.unknown` so callers can
    leave the candidate in ``pending_merge`` without inventing a type.
    """

    return _HINT_TO_EVENT.get(hint)


def _month_floor(value: date) -> date:
    return value.replace(day=1)


def _find_existing_benchmark_event(
    session: Session,
    *,
    company_id: str,
    event_type: EventType,
    event_month: date,
) -> CompanyEvent | None:
    stmt = (
        select(CompanyEvent)
        .where(
            CompanyEvent.company_id == company_id,
            CompanyEvent.event_type == event_type,
            CompanyEvent.event_month == event_month,
            CompanyEvent.source_class == EventSourceClass.benchmark,
        )
        .limit(1)
    )
    return session.execute(stmt).scalars().first()


def promote_benchmark_events(
    session: Session,
    *,
    only_pending: bool = True,
    default_confidence: float = BENCHMARK_EVENT_DEFAULT_CONFIDENCE,
) -> PromoteResult:
    """Promote eligible :class:`BenchmarkEventCandidate` rows into events.

    Parameters
    ----------
    only_pending:
        When ``True`` (default) we only consider candidates whose ``status``
        is :attr:`BenchmarkEventCandidateStatus.pending_merge`. Pass ``False``
        to re-scan everything, which is useful after changing the mapping.
    default_confidence:
        Confidence stamped on promoted events. Downstream event-merge can
        raise this when a higher-precedence source agrees.

    Returns
    -------
    PromoteResult
        Counts + promoted event IDs for logging / review UI surfacing.
    """

    result = PromoteResult()

    stmt = select(BenchmarkEventCandidate)
    if only_pending:
        stmt = stmt.where(
            BenchmarkEventCandidate.status == BenchmarkEventCandidateStatus.pending_merge
        )

    candidates = session.execute(stmt).scalars().all()
    result.candidates_considered = len(candidates)

    for candidate in candidates:
        if candidate.status == BenchmarkEventCandidateStatus.merged:
            result.already_merged += 1
            continue
        if candidate.company_id is None:
            result.skipped_unresolved += 1
            continue

        event_type = map_hint_to_event_type(candidate.hint_type)
        if event_type is None:
            result.skipped_unknown_hint += 1
            continue

        if candidate.event_month_hint is None:
            result.skipped_missing_month += 1
            continue

        event_month = _month_floor(candidate.event_month_hint)

        existing = _find_existing_benchmark_event(
            session,
            company_id=candidate.company_id,
            event_type=event_type,
            event_month=event_month,
        )
        if existing is not None:
            # Idempotent re-run or duplicate sibling candidate. Mark this
            # candidate as merged but don't double-insert.
            candidate.status = BenchmarkEventCandidateStatus.merged
            result.duplicates_of_existing_event += 1
            result.promoted_event_ids.append(existing.id)
            continue

        event = CompanyEvent(
            company_id=candidate.company_id,
            event_type=event_type,
            event_month=event_month,
            source_observation_id=None,
            source_class=EventSourceClass.benchmark,
            confidence=default_confidence,
            description=candidate.description,
        )
        session.add(event)
        session.flush()  # assign PK so we can record it + run event_merge next.

        candidate.status = BenchmarkEventCandidateStatus.merged
        result.promoted += 1
        result.promoted_event_ids.append(event.id)

    return result


__all__ = [
    "BENCHMARK_EVENT_DEFAULT_CONFIDENCE",
    "EVENTS_PARSER_VERSION",
    "PromoteResult",
    "map_hint_to_event_type",
    "promote_benchmark_events",
]
