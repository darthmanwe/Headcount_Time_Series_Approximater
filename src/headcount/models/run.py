"""Orchestration tables for multi-stage batch runs."""

from __future__ import annotations

from datetime import date, datetime

from sqlalchemy import (
    DateTime,
    Enum,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from headcount.db.base import Base, Timestamped, UUIDPk
from headcount.db.enums import (
    CompanyRunStage,
    CompanyRunStageStatus,
    PriorityTier,
    RunKind,
    RunStatus,
)


class Run(UUIDPk, Timestamped, Base):
    __tablename__ = "run"

    kind: Mapped[RunKind] = mapped_column(Enum(RunKind, name="run_kind"), nullable=False)
    status: Mapped[RunStatus] = mapped_column(
        Enum(RunStatus, name="run_status"),
        nullable=False,
        default=RunStatus.started,
    )
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    cutoff_month: Mapped[date] = mapped_column(nullable=False)
    method_version: Mapped[str] = mapped_column(String(64), nullable=False)
    anchor_policy_version: Mapped[str] = mapped_column(String(64), nullable=False)
    coverage_curve_version: Mapped[str] = mapped_column(String(64), nullable=False)
    config_hash: Mapped[str] = mapped_column(String(128), nullable=False)
    priority_tier: Mapped[PriorityTier | None] = mapped_column(
        Enum(PriorityTier, name="run_priority_tier"),
        nullable=True,
    )
    note: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    # Human-readable tag so many overlapping runs can share the one
    # canonical DB without collapsing into an anonymous soup. Examples:
    # "harmonic_cohort:postleak", "retry:arable",
    # "wayback_backfill:2026-04-21", "ad_hoc". Not unique (reruns reuse
    # the label intentionally) and not required (legacy rows keep
    # NULL).
    label: Mapped[str | None] = mapped_column(String(128), nullable=True)


class CompanyRunStatus(UUIDPk, Timestamped, Base):
    __tablename__ = "company_run_status"
    __table_args__ = (
        UniqueConstraint("run_id", "company_id", "stage", name="uq_company_run_stage"),
    )

    run_id: Mapped[str] = mapped_column(
        ForeignKey("run.id", ondelete="CASCADE"),
        nullable=False,
    )
    company_id: Mapped[str] = mapped_column(
        ForeignKey("company.id", ondelete="CASCADE"),
        nullable=False,
    )
    stage: Mapped[CompanyRunStage] = mapped_column(
        Enum(CompanyRunStage, name="company_run_stage"),
        nullable=False,
    )
    status: Mapped[CompanyRunStageStatus] = mapped_column(
        Enum(CompanyRunStageStatus, name="company_run_stage_status"),
        nullable=False,
        default=CompanyRunStageStatus.pending,
    )
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_error: Mapped[str | None] = mapped_column(String(2048), nullable=True)
    last_progress_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
