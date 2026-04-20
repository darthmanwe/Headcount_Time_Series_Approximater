"""Benchmark-sourced observations and event-hint candidates.

``test_source/`` spreadsheets are validation inputs, not live sources, so we
preserve full cell-level provenance (workbook, sheet, row, column) on every
row. ``benchmark_event_candidate`` holds textual hints like "Acquired by
Symphony AI in June 2023"; Phase 6 merges those into canonical
``company_event`` rows after resolution has promoted the candidates.
"""

from __future__ import annotations

from datetime import date

from sqlalchemy import (
    CheckConstraint,
    Date,
    Enum,
    Float,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from headcount.db.base import Base, Timestamped, UUIDPk
from headcount.db.enums import (
    BenchmarkEventCandidateStatus,
    BenchmarkEventHintType,
    BenchmarkMetric,
    BenchmarkProvider,
    HeadcountValueKind,
)


class BenchmarkObservation(UUIDPk, Timestamped, Base):
    __tablename__ = "benchmark_observation"
    __table_args__ = (
        CheckConstraint(
            "value_min IS NULL OR value_max IS NULL OR value_point IS NULL OR "
            "(value_min <= value_point AND value_point <= value_max)",
            name="ck_benchmark_observation_interval_monotonic",
        ),
        UniqueConstraint(
            "source_workbook",
            "source_sheet",
            "source_row_index",
            "provider",
            "metric",
            name="uq_benchmark_observation_cell",
        ),
    )

    company_candidate_id: Mapped[str | None] = mapped_column(
        ForeignKey("company_candidate.id", ondelete="SET NULL"),
        nullable=True,
    )
    company_id: Mapped[str | None] = mapped_column(
        ForeignKey("company.id", ondelete="SET NULL"),
        nullable=True,
    )

    source_workbook: Mapped[str] = mapped_column(String(512), nullable=False)
    source_sheet: Mapped[str] = mapped_column(String(128), nullable=False)
    source_row_index: Mapped[int] = mapped_column(Integer, nullable=False)
    source_cell_address: Mapped[str | None] = mapped_column(String(16), nullable=True)
    source_column_name: Mapped[str | None] = mapped_column(String(256), nullable=True)

    company_name_raw: Mapped[str] = mapped_column(String(512), nullable=False)
    company_domain_raw: Mapped[str | None] = mapped_column(String(255), nullable=True)
    linkedin_url_raw: Mapped[str | None] = mapped_column(String(2048), nullable=True)

    provider: Mapped[BenchmarkProvider] = mapped_column(
        Enum(BenchmarkProvider, name="benchmark_provider"),
        nullable=False,
    )
    metric: Mapped[BenchmarkMetric] = mapped_column(
        Enum(BenchmarkMetric, name="benchmark_metric"),
        nullable=False,
    )
    as_of_month: Mapped[date | None] = mapped_column(Date, nullable=True)
    value_min: Mapped[float | None] = mapped_column(Float, nullable=True)
    value_point: Mapped[float | None] = mapped_column(Float, nullable=True)
    value_max: Mapped[float | None] = mapped_column(Float, nullable=True)
    value_kind: Mapped[HeadcountValueKind | None] = mapped_column(
        Enum(HeadcountValueKind, name="benchmark_value_kind"),
        nullable=True,
    )
    raw_value_text: Mapped[str | None] = mapped_column(String(512), nullable=True)
    note: Mapped[str | None] = mapped_column(String(2048), nullable=True)


class BenchmarkEventCandidate(UUIDPk, Timestamped, Base):
    __tablename__ = "benchmark_event_candidate"
    __table_args__ = (
        UniqueConstraint(
            "source_workbook",
            "source_sheet",
            "source_row_index",
            "description",
            name="uq_benchmark_event_candidate_source",
        ),
    )

    company_candidate_id: Mapped[str | None] = mapped_column(
        ForeignKey("company_candidate.id", ondelete="SET NULL"),
        nullable=True,
    )
    company_id: Mapped[str | None] = mapped_column(
        ForeignKey("company.id", ondelete="SET NULL"),
        nullable=True,
    )

    source_workbook: Mapped[str] = mapped_column(String(512), nullable=False)
    source_sheet: Mapped[str] = mapped_column(String(128), nullable=False)
    source_row_index: Mapped[int] = mapped_column(Integer, nullable=False)

    hint_type: Mapped[BenchmarkEventHintType] = mapped_column(
        Enum(BenchmarkEventHintType, name="benchmark_event_hint_type"),
        nullable=False,
    )
    event_month_hint: Mapped[date | None] = mapped_column(Date, nullable=True)
    description: Mapped[str] = mapped_column(String(2048), nullable=False)
    status: Mapped[BenchmarkEventCandidateStatus] = mapped_column(
        Enum(
            BenchmarkEventCandidateStatus,
            name="benchmark_event_candidate_status",
        ),
        nullable=False,
        default=BenchmarkEventCandidateStatus.pending_merge,
    )
