"""Preferred external source URLs for a canonical company."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import Boolean, DateTime, Enum, Float, ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column

from headcount.db.base import Base, Timestamped, UUIDPk
from headcount.db.enums import SourceName


class CompanySourceLink(UUIDPk, Timestamped, Base):
    __tablename__ = "company_source_link"

    company_id: Mapped[str] = mapped_column(
        ForeignKey("company.id", ondelete="CASCADE"),
        nullable=False,
    )
    source_name: Mapped[SourceName] = mapped_column(
        Enum(SourceName, name="source_name"),
        nullable=False,
    )
    source_url: Mapped[str] = mapped_column(String(2048), nullable=False)
    source_external_id: Mapped[str | None] = mapped_column(String(512), nullable=True)
    is_primary: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    confidence: Mapped[float] = mapped_column(Float, nullable=False, default=0.5)
    last_verified_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
