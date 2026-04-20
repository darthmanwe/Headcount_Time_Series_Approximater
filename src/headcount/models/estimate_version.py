"""One-per-run, per-company estimate version row.

Stores everything needed to reconstruct an output offline: which run,
which method/anchor/coverage version, and the snapshot cutoff.
"""

from __future__ import annotations

from datetime import date

from sqlalchemy import Date, Enum, ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column

from headcount.db.base import Base, Timestamped, UUIDPk
from headcount.db.enums import EstimateVersionStatus


class EstimateVersion(UUIDPk, Timestamped, Base):
    __tablename__ = "estimate_version"

    company_id: Mapped[str] = mapped_column(
        ForeignKey("company.id", ondelete="CASCADE"),
        nullable=False,
    )
    estimation_run_id: Mapped[str] = mapped_column(
        ForeignKey("run.id", ondelete="CASCADE"),
        nullable=False,
    )
    method_version: Mapped[str] = mapped_column(String(64), nullable=False)
    anchor_policy_version: Mapped[str] = mapped_column(String(64), nullable=False)
    coverage_curve_version: Mapped[str] = mapped_column(String(64), nullable=False)
    source_snapshot_cutoff: Mapped[date] = mapped_column(Date, nullable=False)
    status: Mapped[EstimateVersionStatus] = mapped_column(
        Enum(EstimateVersionStatus, name="estimate_version_status"),
        nullable=False,
        default=EstimateVersionStatus.draft,
    )
