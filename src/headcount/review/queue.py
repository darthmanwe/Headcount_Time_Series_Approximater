"""Build and upsert analyst review queue rows.

Queue items are deduplicated on
``(company_id, estimate_version_id, review_reason)`` because one month
can have two *different* reasons to review (e.g., sample floor AND
benchmark disagreement) and we want one row per reason so analysts
resolve them independently.

Priority scoring
----------------

``priority`` is a 0..99 integer used purely for sort order. The queue
currently uses a simple additive ladder:

    base per reason (e.g., benchmark_disagreement 70, anomaly 60)
    + severity bonus (0..15) scaled to the signal
    + confidence penalty (0..15) scaled to 1 - confidence_score

We intentionally keep this dumb so the UI can explain "priority is the
sum of these three inputs". Tuning happens by editing the
``_BASE_PRIORITY`` / bonus helpers and bumping ``QUEUE_VERSION``.

Upsert semantics
----------------

- If no row exists for the key, we insert it in ``status=open``.
- If a row exists and is already resolved/dismissed, we leave it alone
  (analysts moved past it on an earlier run; re-opening would lose
  context).
- Otherwise we refresh ``priority`` and ``detail`` with the latest
  signal - the newest estimate version wins for priority, but we
  preserve ``assigned_to``.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime

from sqlalchemy import and_, select
from sqlalchemy.orm import Session

from headcount.db.enums import ReviewReason, ReviewStatus
from headcount.models.review_queue_item import ReviewQueueItem

QUEUE_VERSION = "queue_v1"
"""Bumped when priority math or upsert semantics change."""


_BASE_PRIORITY: dict[ReviewReason, int] = {
    ReviewReason.benchmark_disagreement: 70,
    ReviewReason.anchor_disagreement: 65,
    ReviewReason.anomaly: 55,
    ReviewReason.low_confidence: 50,
    ReviewReason.degraded_run: 45,
    ReviewReason.linkedin_gated: 40,
    ReviewReason.resolution_ambiguity: 35,
    ReviewReason.manual: 30,
}


@dataclass(frozen=True, slots=True)
class QueueCandidate:
    """Everything needed to upsert one :class:`ReviewQueueItem`."""

    company_id: str
    estimate_version_id: str | None
    review_reason: ReviewReason
    detail: str
    severity: float = 0.5
    confidence_score: float | None = None
    extra: dict[str, object] = field(default_factory=dict)


def _clip_priority(value: int) -> int:
    return max(0, min(99, value))


def _priority(candidate: QueueCandidate) -> int:
    base = _BASE_PRIORITY.get(candidate.review_reason, 30)
    severity_bonus = round(min(max(candidate.severity, 0.0), 1.0) * 15)
    conf = candidate.confidence_score
    confidence_penalty = round(min(max(1.0 - conf, 0.0), 1.0) * 15) if conf is not None else 0
    return _clip_priority(base + severity_bonus + confidence_penalty)


def _find_existing(
    session: Session,
    candidate: QueueCandidate,
) -> ReviewQueueItem | None:
    stmt = select(ReviewQueueItem).where(
        and_(
            ReviewQueueItem.company_id == candidate.company_id,
            ReviewQueueItem.review_reason == candidate.review_reason,
            ReviewQueueItem.estimate_version_id == candidate.estimate_version_id,
        )
    )
    return session.execute(stmt).scalars().first()


def upsert_review_items(
    session: Session,
    candidates: Iterable[QueueCandidate],
) -> dict[str, int]:
    """Insert or refresh review queue rows.

    Returns a counter ``{'inserted': n, 'refreshed': n, 'skipped': n}``
    for telemetry. The caller owns the transaction.
    """

    counts = {"inserted": 0, "refreshed": 0, "skipped": 0}
    now = datetime.now(tz=UTC)

    for cand in candidates:
        existing = _find_existing(session, cand)
        priority = _priority(cand)
        detail = cand.detail[:2048]

        if existing is None:
            session.add(
                ReviewQueueItem(
                    company_id=cand.company_id,
                    estimate_version_id=cand.estimate_version_id,
                    review_reason=cand.review_reason,
                    priority=priority,
                    status=ReviewStatus.open,
                    detail=detail,
                )
            )
            counts["inserted"] += 1
            continue

        if existing.status in (ReviewStatus.resolved, ReviewStatus.dismissed):
            counts["skipped"] += 1
            continue

        changed = False
        if existing.priority != priority:
            existing.priority = priority
            changed = True
        if existing.detail != detail:
            existing.detail = detail
            changed = True
        if changed:
            existing.updated_at = now
            counts["refreshed"] += 1
        else:
            counts["skipped"] += 1

    session.flush()
    return counts


__all__ = [
    "QUEUE_VERSION",
    "QueueCandidate",
    "upsert_review_items",
]
