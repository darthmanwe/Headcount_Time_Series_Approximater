"""Per-run, per-source budget and circuit-breaker state."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, Enum, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from headcount.db.base import Base, Timestamped, UUIDPk
from headcount.db.enums import SourceBudgetStatus, SourceName


class SourceBudget(UUIDPk, Timestamped, Base):
    __tablename__ = "source_budget"
    __table_args__ = (
        UniqueConstraint("run_id", "source_name", name="uq_source_budget_run_source"),
    )

    run_id: Mapped[str] = mapped_column(
        ForeignKey("run.id", ondelete="CASCADE"),
        nullable=False,
    )
    source_name: Mapped[SourceName] = mapped_column(
        Enum(SourceName, name="source_name_budget"),
        nullable=False,
    )
    requests_used: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    requests_allowed: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[SourceBudgetStatus] = mapped_column(
        Enum(SourceBudgetStatus, name="source_budget_status"),
        nullable=False,
        default=SourceBudgetStatus.open,
    )
    tripped_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    trip_reason: Mapped[str | None] = mapped_column(String(256), nullable=True)
    consecutive_failures: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
