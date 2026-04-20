"""Deterministic merge policy for :class:`CompanyEvent` rows.

A single real-world event (e.g. "Acme acquired BetaCorp in June 2023") can
turn up in multiple provenances:

- A first-party SEC 8-K filing (``source_class=first_party``),
- A press release scanned in Phase 6.5 (``source_class=press``),
- A benchmark note promoted by :mod:`headcount.parsers.events`
  (``source_class=benchmark``),
- An analyst override (``source_class=manual``).

Without a merge pass, event segmentation sees three or four rows for the same
event month and over-weights it. :func:`merge_events` collapses those into
exactly one canonical row per
``(company_id, event_type, event_month)`` key using a fixed precedence ladder
and deterministic tie-breaks, so repeated runs produce byte-identical output.

Precedence
----------

Higher wins, and a higher-class winner **absorbs** lower-class siblings:

1. ``manual``       - analyst intent, never overridden.
2. ``first_party``  - the company said it themselves (SEC, about page, etc.).
3. ``press``        - third-party reporting.
4. ``benchmark``    - analyst-readable hints from validation workbooks.
5. ``manual_hint``  - low-weight manual seed.

Within the same class the survivor is picked by (in order):

- Highest ``confidence``.
- Earliest ``created_at`` (deterministic given schema defaults).
- Lowest ``id`` as a final tie-break.

Merge rules
-----------

- Confidence of the survivor is bumped to ``max`` across the group, capped
  at 1.0. Corroboration by multiple sources is evidence of agreement.
- Description prefers the survivor's; if empty, fall back to the first
  non-empty description in precedence order.
- ``source_observation_id`` on the survivor is preserved. The absorbed rows
  are deleted - their provenance remains in ``source_observation`` because
  we never delete that table here.

Pure-in-spirit: the function mutates SQLAlchemy rows and issues ``delete``
through the session, but never commits. Callers own the transaction.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from headcount.db.enums import EventSourceClass, EventType
from headcount.models.company_event import CompanyEvent

EVENT_MERGE_PARSER_VERSION = "event_merge_v1"
"""Bumped when precedence or tie-break rules change."""

_PRECEDENCE: dict[EventSourceClass, int] = {
    EventSourceClass.manual: 5,
    EventSourceClass.first_party: 4,
    EventSourceClass.press: 3,
    EventSourceClass.benchmark: 2,
    EventSourceClass.manual_hint: 1,
}


@dataclass(slots=True)
class MergeResult:
    """Summary of a :func:`merge_events` pass."""

    groups_considered: int = 0
    groups_collapsed: int = 0
    rows_deleted: int = 0
    rows_updated: int = 0
    survivor_event_ids: list[str] = field(default_factory=list)


def _precedence(cls: EventSourceClass) -> int:
    return _PRECEDENCE.get(cls, 0)


def _pick_survivor(rows: list[CompanyEvent]) -> CompanyEvent:
    # Highest precedence, then highest confidence, then earliest created_at,
    # then smallest id. We negate the "smaller is earlier/better" fields by
    # inverting their contribution.
    def _key(ev: CompanyEvent) -> tuple[int, float, str, str]:
        created_at = getattr(ev, "created_at", None)
        # Prefer earlier created_at (ascending). Use a large sentinel for None
        # so rows without a timestamp sort last.
        created_token = created_at.isoformat() if created_at else "\uffff"
        return (
            -_precedence(ev.source_class),
            -float(ev.confidence or 0.0),
            created_token,
            ev.id or "",
        )

    return sorted(rows, key=_key)[0]


def _merge_group(session: Session, rows: list[CompanyEvent]) -> tuple[CompanyEvent, int, int]:
    """Collapse a single group.

    Returns ``(survivor, rows_updated, rows_deleted)``. For single-row groups
    the survivor is the input row and both counters are zero.
    """

    survivor = _pick_survivor(rows)
    if len(rows) == 1:
        return (survivor, 0, 0)

    losers = [r for r in rows if r.id != survivor.id]

    max_conf = max((float(r.confidence or 0.0) for r in rows), default=0.0)
    new_conf = min(1.0, max_conf)
    if float(survivor.confidence or 0.0) != new_conf:
        survivor.confidence = new_conf

    if not (survivor.description and survivor.description.strip()):
        for candidate in sorted(losers, key=lambda r: (-_precedence(r.source_class), r.id or "")):
            if candidate.description and candidate.description.strip():
                survivor.description = candidate.description
                break

    loser_ids = [loser.id for loser in losers if loser.id is not None]
    if loser_ids:
        session.execute(delete(CompanyEvent).where(CompanyEvent.id.in_(loser_ids)))

    return (survivor, 1, len(loser_ids))


def merge_events(
    session: Session,
    *,
    company_id: str | None = None,
) -> MergeResult:
    """Collapse duplicate events per ``(company_id, event_type, event_month)``.

    Parameters
    ----------
    company_id:
        Optional filter - when set we only merge within that company's
        events, which is cheaper for incremental runs (e.g. right after
        :func:`promote_benchmark_events` for a single company).

    Returns
    -------
    MergeResult
        Counts + surviving event IDs for telemetry.
    """

    result = MergeResult()

    stmt = select(CompanyEvent)
    if company_id is not None:
        stmt = stmt.where(CompanyEvent.company_id == company_id)

    rows = list(session.execute(stmt).scalars().all())

    groups: dict[tuple[str, EventType, date], list[CompanyEvent]] = defaultdict(list)
    for row in rows:
        groups[(row.company_id, row.event_type, row.event_month)].append(row)

    result.groups_considered = len(groups)

    for members in groups.values():
        survivor, updated, deleted = _merge_group(session, members)
        result.rows_updated += updated
        result.rows_deleted += deleted
        if deleted > 0:
            result.groups_collapsed += 1
        if survivor.id is not None:
            result.survivor_event_ids.append(survivor.id)

    return result


__all__ = [
    "EVENT_MERGE_PARSER_VERSION",
    "MergeResult",
    "merge_events",
]
