"""Evaluation run: scoreboard of pipeline output vs. benchmark workbooks.

Each row is a self-contained, immutable snapshot of how the estimator
performed at a point in time, comparing the latest
:class:`HeadcountEstimateMonthly` rows against :class:`BenchmarkObservation`
data loaded from ``test_source/`` spreadsheets. Phase 11 introduces this
as the canonical regression / accuracy ledger.

Key design choices
------------------

* **Immutable**: we insert one row per ``hc evaluate`` invocation and
  never mutate. The Streamlit history view reads these rows directly.
* **Self-contained payload**: the full scoreboard (coverage, per-provider
  MAPE/MAE, top disagreements, confidence distribution) lives in
  ``scoreboard_json`` so the UI and acceptance-gate test don't have to
  re-derive it from the raw tables.
* **Denormalized summary fields**: the most common filter/sort columns
  (``as_of_month``, ``companies_evaluated``, ``mape_headcount_current``)
  are promoted to typed columns for efficient list queries; the full
  payload remains authoritative.
* **Optional ``estimate_run_id`` FK**: when an evaluation is tied to a
  specific pipeline run we record it; for ad-hoc evaluations against
  the current DB state it can be ``None``.
"""

from __future__ import annotations

from datetime import date

from sqlalchemy import JSON, Date, Float, ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from headcount.db.base import Base, Timestamped, UUIDPk


class EvaluationRun(UUIDPk, Timestamped, Base):
    __tablename__ = "evaluation_run"

    estimate_run_id: Mapped[str | None] = mapped_column(
        ForeignKey("run.id", ondelete="SET NULL"),
        nullable=True,
    )
    as_of_month: Mapped[date] = mapped_column(Date, nullable=False)
    evaluation_version: Mapped[str] = mapped_column(String(64), nullable=False)

    companies_in_scope: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    companies_evaluated: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    companies_with_benchmark: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0
    )

    # Coverage ratio: estimates_written / in_scope (0..1).
    coverage_in_scope: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    # Coverage ratio on the subset that has *any* benchmark row.
    coverage_with_benchmark: Mapped[float] = mapped_column(
        Float, nullable=False, default=0.0
    )

    # Headline accuracy metric: MAPE of estimate vs analyst (``zeeshan``)
    # ``headcount_current`` value. Promoted because the acceptance gate
    # test filters on this.
    mape_headcount_current: Mapped[float | None] = mapped_column(Float, nullable=True)
    mae_growth_1y_pct: Mapped[float | None] = mapped_column(Float, nullable=True)

    review_queue_open: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    high_confidence_disagreements: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0
    )

    # Full JSON scoreboard. Authoritative source of truth; promoted
    # columns above are denormalized accessors.
    scoreboard_json: Mapped[dict[str, object]] = mapped_column(JSON, nullable=False)
    note: Mapped[str | None] = mapped_column(String(1024), nullable=True)
