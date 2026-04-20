"""Analyst-verified merges across profile-slug variants."""

from __future__ import annotations

from sqlalchemy import CheckConstraint, ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column

from headcount.db.base import Base, Timestamped, UUIDPk


class PersonIdentityMerge(UUIDPk, Timestamped, Base):
    __tablename__ = "person_identity_merge"
    __table_args__ = (
        CheckConstraint(
            "primary_person_id <> duplicate_person_id",
            name="ck_person_identity_merge_distinct",
        ),
    )

    primary_person_id: Mapped[str] = mapped_column(
        ForeignKey("person.id", ondelete="CASCADE"),
        nullable=False,
    )
    duplicate_person_id: Mapped[str] = mapped_column(
        ForeignKey("person.id", ondelete="CASCADE"),
        nullable=False,
    )
    reason: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    entered_by: Mapped[str | None] = mapped_column(String(256), nullable=True)
