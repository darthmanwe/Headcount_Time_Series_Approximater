"""harmonic_primary_eval_columns

Revision ID: c5f2b1e9e0a8
Revises: b4e1a9d83f55
Create Date: 2026-04-20 23:00:00.000000+00:00

Add Harmonic-primary headline columns and supporting-provider MAPE
columns to ``evaluation_run``. The Phase 11 initial schema treated
Zeeshan as the headline provider; this migration flips the target
signal to Harmonic (the external feed we are explicitly trying to
approximate) and adds:

* ``primary_provider`` - records which provider the headline columns
  reflect. Historical rows were written against Zeeshan and get
  backfilled to ``'zeeshan'`` so their headline numbers remain
  interpretable; new rows default to ``'harmonic'``.
* ``harmonic_cohort_size`` / ``harmonic_cohort_evaluated`` - the
  calibration-cohort split (companies with at least one Harmonic
  benchmark row) so the scoreboard can report calibration coverage
  separately from full-population coverage.
* ``mae_growth_6m_pct`` / ``mae_growth_2y_pct`` - sibling headline
  columns next to the existing ``mae_growth_1y_pct``.
* ``spearman_growth_6m`` / ``spearman_growth_1y`` - rank correlation
  between our growth ordering and Harmonic's.
* ``mape_headcount_current_zeeshan`` / ``mape_headcount_current_linkedin``
  - supporting-provider accuracy reported alongside the primary KPI.
* ``supporting_disagreements`` - count of high-confidence
  disagreements against supporting providers. Diagnostic only.

All new columns are nullable or have server defaults so the migration
is safe to run against an existing DB.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "c5f2b1e9e0a8"
down_revision: str | None = "b4e1a9d83f55"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("evaluation_run") as batch:
        batch.add_column(
            sa.Column(
                "primary_provider",
                sa.String(length=32),
                nullable=False,
                server_default="zeeshan",
            )
        )
        batch.add_column(
            sa.Column(
                "harmonic_cohort_size",
                sa.Integer(),
                nullable=False,
                server_default="0",
            )
        )
        batch.add_column(
            sa.Column(
                "harmonic_cohort_evaluated",
                sa.Integer(),
                nullable=False,
                server_default="0",
            )
        )
        batch.add_column(sa.Column("mae_growth_6m_pct", sa.Float(), nullable=True))
        batch.add_column(sa.Column("mae_growth_2y_pct", sa.Float(), nullable=True))
        batch.add_column(sa.Column("spearman_growth_6m", sa.Float(), nullable=True))
        batch.add_column(sa.Column("spearman_growth_1y", sa.Float(), nullable=True))
        batch.add_column(sa.Column("mape_headcount_current_zeeshan", sa.Float(), nullable=True))
        batch.add_column(sa.Column("mape_headcount_current_linkedin", sa.Float(), nullable=True))
        batch.add_column(
            sa.Column(
                "supporting_disagreements",
                sa.Integer(),
                nullable=False,
                server_default="0",
            )
        )
    # Existing historical rows were written with Zeeshan as the
    # headline provider; keep their ``primary_provider`` sticky at
    # ``zeeshan`` (that's the server default above). New inserts flip
    # to ``harmonic`` via the application-level default in the model.
    # We deliberately do not rewrite the stored ``scoreboard_json`` of
    # historical rows - those blobs remain authoritative snapshots of
    # what ``eval_v1`` produced.


def downgrade() -> None:
    with op.batch_alter_table("evaluation_run") as batch:
        batch.drop_column("supporting_disagreements")
        batch.drop_column("mape_headcount_current_linkedin")
        batch.drop_column("mape_headcount_current_zeeshan")
        batch.drop_column("spearman_growth_1y")
        batch.drop_column("spearman_growth_6m")
        batch.drop_column("mae_growth_2y_pct")
        batch.drop_column("mae_growth_6m_pct")
        batch.drop_column("harmonic_cohort_evaluated")
        batch.drop_column("harmonic_cohort_size")
        batch.drop_column("primary_provider")
