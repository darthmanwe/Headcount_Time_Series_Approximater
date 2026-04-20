"""Interval-valued anchor observations.

The benchmark spreadsheets show anchor values come in three flavours:
``exact`` scalars (e.g. ``602``), visible ``range`` strings (``201-500``),
and opaque ``bucket`` labels. Storing ``min/point/max`` plus a ``kind``
preserves uncertainty end-to-end so estimation can propagate it rather
than silently pick a midpoint.
"""

from __future__ import annotations

from datetime import date

from sqlalchemy import CheckConstraint, Date, Enum, Float, ForeignKey, Index, String
from sqlalchemy.orm import Mapped, mapped_column

from headcount.db.base import Base, Timestamped, UUIDPk
from headcount.db.enums import AnchorType, HeadcountValueKind


class CompanyAnchorObservation(UUIDPk, Timestamped, Base):
    __tablename__ = "company_anchor_observation"
    __table_args__ = (
        CheckConstraint(
            "headcount_value_min <= headcount_value_point AND "
            "headcount_value_point <= headcount_value_max",
            name="ck_company_anchor_interval_monotonic",
        ),
        Index(
            "ix_company_anchor_company_month",
            "company_id",
            "anchor_month",
        ),
    )

    company_id: Mapped[str] = mapped_column(
        ForeignKey("company.id", ondelete="CASCADE"),
        nullable=False,
    )
    source_observation_id: Mapped[str | None] = mapped_column(
        ForeignKey("source_observation.id", ondelete="SET NULL"),
        nullable=True,
    )
    anchor_type: Mapped[AnchorType] = mapped_column(
        Enum(AnchorType, name="anchor_type"),
        nullable=False,
    )
    headcount_value_min: Mapped[float] = mapped_column(Float, nullable=False)
    headcount_value_point: Mapped[float] = mapped_column(Float, nullable=False)
    headcount_value_max: Mapped[float] = mapped_column(Float, nullable=False)
    headcount_value_kind: Mapped[HeadcountValueKind] = mapped_column(
        Enum(HeadcountValueKind, name="headcount_value_kind"),
        nullable=False,
    )
    anchor_month: Mapped[date] = mapped_column(Date, nullable=False)
    confidence: Mapped[float] = mapped_column(Float, nullable=False, default=0.5)
    note: Mapped[str | None] = mapped_column(String(1024), nullable=True)
