"""Per-component confidence breakdown for an estimate version."""

from __future__ import annotations

from sqlalchemy import Float, ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column

from headcount.db.base import Base, Timestamped, UUIDPk


class ConfidenceComponentScore(UUIDPk, Timestamped, Base):
    __tablename__ = "confidence_component_score"

    estimate_version_id: Mapped[str] = mapped_column(
        ForeignKey("estimate_version.id", ondelete="CASCADE"),
        nullable=False,
    )
    component_name: Mapped[str] = mapped_column(String(128), nullable=False)
    component_score: Mapped[float] = mapped_column(Float, nullable=False)
    note: Mapped[str | None] = mapped_column(String(1024), nullable=True)
