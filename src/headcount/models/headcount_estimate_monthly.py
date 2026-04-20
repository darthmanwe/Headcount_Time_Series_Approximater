"""Monthly headcount estimate with interval output."""

from __future__ import annotations

from datetime import date

from sqlalchemy import (
    JSON,
    Boolean,
    CheckConstraint,
    Date,
    Enum,
    Float,
    ForeignKey,
    Index,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from headcount.db.base import Base, Timestamped, UUIDPk
from headcount.db.enums import ConfidenceBand, EstimateMethod


class HeadcountEstimateMonthly(UUIDPk, Timestamped, Base):
    __tablename__ = "headcount_estimate_monthly"
    __table_args__ = (
        UniqueConstraint(
            "estimate_version_id",
            "month",
            name="uq_headcount_estimate_version_month",
        ),
        CheckConstraint(
            "estimated_headcount_min <= estimated_headcount AND "
            "estimated_headcount <= estimated_headcount_max",
            name="ck_headcount_estimate_interval_monotonic",
        ),
        Index(
            "ix_headcount_estimate_company_month_version",
            "company_id",
            "month",
            "estimate_version_id",
        ),
    )

    company_id: Mapped[str] = mapped_column(
        ForeignKey("company.id", ondelete="CASCADE"),
        nullable=False,
    )
    estimate_version_id: Mapped[str] = mapped_column(
        ForeignKey("estimate_version.id", ondelete="CASCADE"),
        nullable=False,
    )
    month: Mapped[date] = mapped_column(Date, nullable=False)
    estimated_headcount: Mapped[float] = mapped_column(Float, nullable=False)
    estimated_headcount_min: Mapped[float] = mapped_column(Float, nullable=False)
    estimated_headcount_max: Mapped[float] = mapped_column(Float, nullable=False)
    public_profile_count: Mapped[int] = mapped_column(nullable=False, default=0)
    scaled_from_anchor_value: Mapped[float] = mapped_column(Float, nullable=False)
    method: Mapped[EstimateMethod] = mapped_column(
        Enum(EstimateMethod, name="estimate_method"),
        nullable=False,
    )
    confidence_band: Mapped[ConfidenceBand] = mapped_column(
        Enum(ConfidenceBand, name="confidence_band"),
        nullable=False,
    )
    needs_review: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    suppression_reason: Mapped[str | None] = mapped_column(String(256), nullable=True)
    # Phase 8: numeric 0..1 confidence, plus structured per-month breakdown
    # of the components that produced the band.
    confidence_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    confidence_components_json: Mapped[dict[str, object] | None] = mapped_column(
        JSON, nullable=True
    )
