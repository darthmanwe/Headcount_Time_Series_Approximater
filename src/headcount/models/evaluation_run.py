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
  MAPE/MAE, rank correlation, top disagreements, confidence distribution)
  lives in ``scoreboard_json`` so the UI and acceptance-gate test don't
  have to re-derive it from the raw tables.
* **Denormalized summary fields**: the headline accuracy columns
  (Harmonic MAPE, Harmonic 1y/6m growth MAE, Spearman on Harmonic
  growth) are promoted to typed columns for efficient list queries and
  trend plots; the full payload remains authoritative.
* **Primary provider is Harmonic by design.** The ``primary_provider``
  column exists so a future shift of the target signal doesn't silently
  break historical trend plots - every row records which provider was
  the KPI source at the time it was persisted.
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
    # Provider whose numbers drive the headline KPIs on this row. Kept
    # as a column (not just JSON) so historical rows remain legible
    # even if the primary provider later changes.
    primary_provider: Mapped[str] = mapped_column(String(32), nullable=False, default="harmonic")

    companies_in_scope: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    companies_evaluated: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    companies_with_benchmark: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # Harmonic cohort: the (small) calibration set of companies that
    # have at least one Harmonic benchmark row. Tracked separately so
    # the calibration coverage can never get diluted by the larger
    # population of benchmark-less companies.
    harmonic_cohort_size: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    harmonic_cohort_evaluated: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # Coverage ratio: estimates_written / in_scope (0..1).
    coverage_in_scope: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    # Coverage ratio on the subset that has *any* benchmark row.
    coverage_with_benchmark: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)

    # --- Headline KPIs (primary provider = Harmonic) -------------------
    # MAPE of our ``headcount_current`` vs Harmonic's ``Headcount``.
    mape_headcount_current: Mapped[float | None] = mapped_column(Float, nullable=True)
    # MAE of our growth ratio vs Harmonic's ``Headcount %`` rate.
    mae_growth_6m_pct: Mapped[float | None] = mapped_column(Float, nullable=True)
    mae_growth_1y_pct: Mapped[float | None] = mapped_column(Float, nullable=True)
    # 2y falls back to Zeeshan because Harmonic does not emit it.
    mae_growth_2y_pct: Mapped[float | None] = mapped_column(Float, nullable=True)
    # Spearman rank correlation between our and Harmonic's growth
    # ordering across the Harmonic cohort.
    spearman_growth_6m: Mapped[float | None] = mapped_column(Float, nullable=True)
    spearman_growth_1y: Mapped[float | None] = mapped_column(Float, nullable=True)

    # --- Supporting-provider MAPEs (Zeeshan / LinkedIn) ---------------
    mape_headcount_current_zeeshan: Mapped[float | None] = mapped_column(Float, nullable=True)
    mape_headcount_current_linkedin: Mapped[float | None] = mapped_column(Float, nullable=True)

    review_queue_open: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    # High-confidence disagreements against the primary provider
    # (Harmonic). Trips the acceptance gate when > 0.
    high_confidence_disagreements: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    # High-confidence disagreements against any supporting provider.
    # Diagnostic only; never trips the gate.
    supporting_disagreements: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # Full JSON scoreboard. Authoritative source of truth; promoted
    # columns above are denormalized accessors.
    scoreboard_json: Mapped[dict[str, object]] = mapped_column(JSON, nullable=False)
    note: Mapped[str | None] = mapped_column(String(1024), nullable=True)
