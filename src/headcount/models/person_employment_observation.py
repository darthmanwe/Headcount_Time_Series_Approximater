"""Public employment-history observation tied to a company and a person."""

from __future__ import annotations

from datetime import date

from sqlalchemy import Boolean, CheckConstraint, Date, Float, ForeignKey, Index, String
from sqlalchemy.orm import Mapped, mapped_column

from headcount.db.base import Base, Timestamped, UUIDPk


class PersonEmploymentObservation(UUIDPk, Timestamped, Base):
    __tablename__ = "person_employment_observation"
    __table_args__ = (
        CheckConstraint(
            "end_month IS NULL OR end_month >= start_month",
            name="ck_employment_end_after_start",
        ),
        Index(
            "ix_employment_company_start",
            "company_id",
            "start_month",
        ),
        Index(
            "ix_employment_person",
            "person_id",
        ),
    )

    person_id: Mapped[str] = mapped_column(
        ForeignKey("person.id", ondelete="CASCADE"),
        nullable=False,
    )
    company_id: Mapped[str] = mapped_column(
        ForeignKey("company.id", ondelete="CASCADE"),
        nullable=False,
    )
    source_observation_id: Mapped[str | None] = mapped_column(
        ForeignKey("source_observation.id", ondelete="SET NULL"),
        nullable=True,
    )
    observed_company_name: Mapped[str | None] = mapped_column(String(512), nullable=True)
    job_title: Mapped[str | None] = mapped_column(String(512), nullable=True)
    start_month: Mapped[date] = mapped_column(Date, nullable=False)
    end_month: Mapped[date | None] = mapped_column(Date, nullable=True)
    is_current_role: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    confidence: Mapped[float] = mapped_column(Float, nullable=False, default=0.5)
