"""Public profile identity used for monthly active counts."""

from __future__ import annotations

from sqlalchemy import Enum, Index, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from headcount.db.base import Base, Timestamped, UUIDPk
from headcount.db.enums import SourceName


class Person(UUIDPk, Timestamped, Base):
    __tablename__ = "person"
    __table_args__ = (
        UniqueConstraint("source_name", "source_person_key", name="uq_person_source_key"),
        Index("ix_person_source_key", "source_person_key"),
    )

    source_name: Mapped[SourceName] = mapped_column(
        Enum(SourceName, name="source_name_person"),
        nullable=False,
    )
    source_person_key: Mapped[str] = mapped_column(String(512), nullable=False)
    display_name: Mapped[str | None] = mapped_column(String(512), nullable=True)
    profile_url: Mapped[str | None] = mapped_column(String(2048), nullable=True)
