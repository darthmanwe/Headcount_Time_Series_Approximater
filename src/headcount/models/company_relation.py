"""Parent/sub relationship between two canonical companies.

Populated by Phase 3 when acquisitions, renames, or spinouts are detected.
Consumed by Phase 7 event-aware segmentation so the 1010data -> Symphony AI
case produces two clean series segments instead of one muddled continuous
series.
"""

from __future__ import annotations

from datetime import date

from sqlalchemy import Date, Enum, Float, ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column

from headcount.db.base import Base, Timestamped, UUIDPk
from headcount.db.enums import RelationKind


class CompanyRelation(UUIDPk, Timestamped, Base):
    __tablename__ = "company_relation"

    parent_id: Mapped[str] = mapped_column(
        ForeignKey("company.id", ondelete="CASCADE"),
        nullable=False,
    )
    child_id: Mapped[str] = mapped_column(
        ForeignKey("company.id", ondelete="CASCADE"),
        nullable=False,
    )
    kind: Mapped[RelationKind] = mapped_column(
        Enum(RelationKind, name="relation_kind"),
        nullable=False,
    )
    effective_month: Mapped[date | None] = mapped_column(Date, nullable=True)
    confidence: Mapped[float] = mapped_column(Float, nullable=False, default=0.5)
    source_observation_id: Mapped[str | None] = mapped_column(
        ForeignKey("source_observation.id", ondelete="SET NULL"),
        nullable=True,
    )
    note: Mapped[str | None] = mapped_column(String(1024), nullable=True)
