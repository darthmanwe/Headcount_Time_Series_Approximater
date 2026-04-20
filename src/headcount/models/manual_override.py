"""Analyst-written override, auditable and optionally expiring."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import JSON, DateTime, Enum, ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column

from headcount.db.base import Base, Timestamped, UUIDPk
from headcount.db.enums import OverrideField


class ManualOverride(UUIDPk, Timestamped, Base):
    __tablename__ = "manual_override"

    company_id: Mapped[str] = mapped_column(
        ForeignKey("company.id", ondelete="CASCADE"),
        nullable=False,
    )
    field_name: Mapped[OverrideField] = mapped_column(
        Enum(OverrideField, name="override_field"),
        nullable=False,
    )
    override_value_json: Mapped[dict[str, object]] = mapped_column(JSON, nullable=False)
    reason: Mapped[str | None] = mapped_column(String(2048), nullable=True)
    entered_by: Mapped[str | None] = mapped_column(String(256), nullable=True)
    expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
