"""Minimal review-queue writer callable from earlier phases.

Phase 3 and Phase 5 must be able to enqueue review items (resolution
ambiguity, LinkedIn gated) before the full review module lands in Phase 8.
This module owns the write path; Phase 8 extends it with triggers,
priority policy, and analyst workflows.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from sqlalchemy.orm import Session

from headcount.db.enums import ReviewReason, ReviewStatus
from headcount.models.review_queue_item import ReviewQueueItem
from headcount.utils.logging import get_logger

_log = get_logger("headcount.review.queue_writer")


@dataclass(slots=True)
class EnqueueRequest:
    company_id: str
    reason: ReviewReason
    estimate_version_id: str | None = None
    priority: int = 50
    detail: str | None = None


class ReviewQueueWriter(Protocol):
    def enqueue(self, request: EnqueueRequest) -> str: ...


class DbReviewQueueWriter:
    """Default writer backed by the SQLAlchemy session."""

    def __init__(self, session: Session) -> None:
        self._session = session

    def enqueue(self, request: EnqueueRequest) -> str:
        item = ReviewQueueItem(
            company_id=request.company_id,
            estimate_version_id=request.estimate_version_id,
            review_reason=request.reason,
            priority=request.priority,
            status=ReviewStatus.open,
            detail=request.detail,
        )
        self._session.add(item)
        self._session.flush()
        _log.info(
            "review_enqueued",
            company_id=request.company_id,
            reason=request.reason.value,
            priority=request.priority,
            estimate_version_id=request.estimate_version_id,
        )
        return item.id


def enqueue(session: Session, request: EnqueueRequest) -> str:
    """Convenience wrapper used by Phase 3/5 callers."""
    return DbReviewQueueWriter(session).enqueue(request)
