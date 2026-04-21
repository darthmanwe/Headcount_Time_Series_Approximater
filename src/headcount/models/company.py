"""Canonical ``company`` aggregate."""

from __future__ import annotations

from sqlalchemy import Enum, Index, String
from sqlalchemy.orm import Mapped, mapped_column

from headcount.db.base import Base, Timestamped, UUIDPk
from headcount.db.enums import CompanyStatus, PriorityTier


class Company(UUIDPk, Timestamped, Base):
    __tablename__ = "company"
    __table_args__ = (
        Index("ix_company_canonical_domain", "canonical_domain"),
        Index("ix_company_canonical_name", "canonical_name"),
    )

    canonical_name: Mapped[str] = mapped_column(String(512), nullable=False)
    canonical_domain: Mapped[str | None] = mapped_column(String(255), nullable=True)
    linkedin_company_url: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    linkedin_resolved_url: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    country: Mapped[str | None] = mapped_column(String(64), nullable=True)
    state_or_region: Mapped[str | None] = mapped_column(String(128), nullable=True)
    status: Mapped[CompanyStatus] = mapped_column(
        Enum(CompanyStatus, name="company_status"),
        nullable=False,
        default=CompanyStatus.active,
    )
    priority_tier: Mapped[PriorityTier] = mapped_column(
        Enum(PriorityTier, name="priority_tier"),
        nullable=False,
        default=PriorityTier.P1,
    )
    status_reason: Mapped[str | None] = mapped_column(String(1024), nullable=True)
