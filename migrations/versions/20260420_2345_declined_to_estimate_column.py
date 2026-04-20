"""declined_to_estimate_column

Revision ID: d7a3c4b1f920
Revises: c5f2b1e9e0a8
Create Date: 2026-04-20 23:45:00.000000+00:00

Adds ``companies_declined_to_estimate`` to ``evaluation_run``.

Background: BUG-B in docs/HARMONIC_COHORT_LIVE_RUN.md. The estimator
emits a 0.0 placeholder for months it cannot produce a real value for
(``method`` in ``{suppressed_low_sample, degraded_current_only}``). The
evaluation harness now skips those rows when computing MAPE so a thin
free-data day does not drag the headline KPI to ~1.0, and reports the
coverage gap via this column instead. Column is non-nullable with a
server default of 0 so the migration is safe against existing data.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "d7a3c4b1f920"
down_revision: str | None = "c5f2b1e9e0a8"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("evaluation_run") as batch:
        batch.add_column(
            sa.Column(
                "companies_declined_to_estimate",
                sa.Integer(),
                nullable=False,
                server_default="0",
            )
        )


def downgrade() -> None:
    with op.batch_alter_table("evaluation_run") as batch:
        batch.drop_column("companies_declined_to_estimate")
