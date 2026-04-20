"""Known alias for a canonical ``company``."""

from __future__ import annotations

from sqlalchemy import Enum, Float, ForeignKey, Index, String
from sqlalchemy.orm import Mapped, mapped_column

from headcount.db.base import Base, Timestamped, UUIDPk
from headcount.db.enums import AliasType


class CompanyAlias(UUIDPk, Timestamped, Base):
    __tablename__ = "company_alias"
    __table_args__ = (Index("ix_company_alias_name", "alias_name"),)

    company_id: Mapped[str] = mapped_column(
        ForeignKey("company.id", ondelete="CASCADE"),
        nullable=False,
    )
    alias_name: Mapped[str] = mapped_column(String(512), nullable=False)
    alias_type: Mapped[AliasType] = mapped_column(
        Enum(AliasType, name="alias_type"),
        nullable=False,
    )
    confidence: Mapped[float] = mapped_column(Float, nullable=False, default=0.5)
    source: Mapped[str | None] = mapped_column(String(128), nullable=True)
