"""Raw seed rows imported before canonical resolution (Phase 2).

Only the resolver promotes these to ``company`` rows; keeping them in a
separate table prevents seed noise from polluting canonical identity.
"""

from __future__ import annotations

from sqlalchemy import Enum, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from headcount.db.base import Base, Timestamped, UUIDPk
from headcount.db.enums import CandidateStatus


class CompanyCandidate(UUIDPk, Timestamped, Base):
    __tablename__ = "company_candidate"
    __table_args__ = (
        UniqueConstraint(
            "source_workbook",
            "source_sheet",
            "source_row_index",
            name="uq_company_candidate_source_row",
        ),
    )

    company_id: Mapped[str | None] = mapped_column(
        ForeignKey("company.id", ondelete="SET NULL"),
        nullable=True,
    )

    source_workbook: Mapped[str] = mapped_column(String(512), nullable=False)
    source_sheet: Mapped[str] = mapped_column(String(128), nullable=False)
    source_row_index: Mapped[int] = mapped_column(Integer, nullable=False)

    company_name: Mapped[str] = mapped_column(String(512), nullable=False)
    domain: Mapped[str | None] = mapped_column(String(255), nullable=True)

    status: Mapped[CandidateStatus] = mapped_column(
        Enum(CandidateStatus, name="candidate_status"),
        nullable=False,
        default=CandidateStatus.pending_resolution,
    )
    note: Mapped[str | None] = mapped_column(String(1024), nullable=True)
