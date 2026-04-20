"""Analyst review queue row."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, Enum, ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from headcount.db.base import Base, Timestamped, UUIDPk
from headcount.db.enums import ReviewReason, ReviewStatus


class ReviewQueueItem(UUIDPk, Timestamped, Base):
    __tablename__ = "review_queue_item"

    company_id: Mapped[str] = mapped_column(
        ForeignKey("company.id", ondelete="CASCADE"),
        nullable=False,
    )
    estimate_version_id: Mapped[str | None] = mapped_column(
        ForeignKey("estimate_version.id", ondelete="SET NULL"),
        nullable=True,
    )
    review_reason: Mapped[ReviewReason] = mapped_column(
        Enum(ReviewReason, name="review_reason"),
        nullable=False,
    )
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=50)
    status: Mapped[ReviewStatus] = mapped_column(
        Enum(ReviewStatus, name="review_status"),
        nullable=False,
        default=ReviewStatus.open,
    )
    assigned_to: Mapped[str | None] = mapped_column(String(256), nullable=True)
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    detail: Mapped[str | None] = mapped_column(String(2048), nullable=True)
