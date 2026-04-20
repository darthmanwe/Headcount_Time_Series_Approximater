"""Company-level event used for event-aware series segmentation."""

from __future__ import annotations

from datetime import date

from sqlalchemy import Date, Enum, Float, ForeignKey, Index, String
from sqlalchemy.orm import Mapped, mapped_column

from headcount.db.base import Base, Timestamped, UUIDPk
from headcount.db.enums import EventSourceClass, EventType


class CompanyEvent(UUIDPk, Timestamped, Base):
    __tablename__ = "company_event"
    __table_args__ = (
        Index(
            "ix_company_event_company_month_type",
            "company_id",
            "event_month",
            "event_type",
        ),
    )

    company_id: Mapped[str] = mapped_column(
        ForeignKey("company.id", ondelete="CASCADE"),
        nullable=False,
    )
    event_type: Mapped[EventType] = mapped_column(
        Enum(EventType, name="event_type"),
        nullable=False,
    )
    event_month: Mapped[date] = mapped_column(Date, nullable=False)
    source_observation_id: Mapped[str | None] = mapped_column(
        ForeignKey("source_observation.id", ondelete="SET NULL"),
        nullable=True,
    )
    source_class: Mapped[EventSourceClass] = mapped_column(
        Enum(EventSourceClass, name="event_source_class"),
        nullable=False,
    )
    confidence: Mapped[float] = mapped_column(Float, nullable=False, default=0.5)
    description: Mapped[str | None] = mapped_column(String(2048), nullable=True)
