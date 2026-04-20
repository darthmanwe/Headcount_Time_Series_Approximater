"""phase_11_evaluation_run

Revision ID: b4e1a9d83f55
Revises: a3c21f7e9d04
Create Date: 2026-04-20 21:00:00.000000+00:00

Create the ``evaluation_run`` table. Each row is an immutable scoreboard
comparing pipeline output against the benchmark workbooks in
``test_source/``. Promoted columns support list / sort queries; the full
payload lives in ``scoreboard_json``.
"""
from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "b4e1a9d83f55"
down_revision: str | None = "a3c21f7e9d04"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "evaluation_run",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("estimate_run_id", sa.String(length=36), nullable=True),
        sa.Column("as_of_month", sa.Date(), nullable=False),
        sa.Column("evaluation_version", sa.String(length=64), nullable=False),
        sa.Column(
            "companies_in_scope", sa.Integer(), nullable=False, server_default="0"
        ),
        sa.Column(
            "companies_evaluated", sa.Integer(), nullable=False, server_default="0"
        ),
        sa.Column(
            "companies_with_benchmark",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
        sa.Column(
            "coverage_in_scope", sa.Float(), nullable=False, server_default="0.0"
        ),
        sa.Column(
            "coverage_with_benchmark",
            sa.Float(),
            nullable=False,
            server_default="0.0",
        ),
        sa.Column("mape_headcount_current", sa.Float(), nullable=True),
        sa.Column("mae_growth_1y_pct", sa.Float(), nullable=True),
        sa.Column(
            "review_queue_open", sa.Integer(), nullable=False, server_default="0"
        ),
        sa.Column(
            "high_confidence_disagreements",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
        sa.Column("scoreboard_json", sa.JSON(), nullable=False),
        sa.Column("note", sa.String(length=1024), nullable=True),
        sa.ForeignKeyConstraint(
            ["estimate_run_id"], ["run.id"], ondelete="SET NULL"
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_evaluation_run_as_of_month",
        "evaluation_run",
        ["as_of_month"],
        unique=False,
    )
    op.create_index(
        "ix_evaluation_run_created_at",
        "evaluation_run",
        ["created_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_evaluation_run_created_at", table_name="evaluation_run")
    op.drop_index("ix_evaluation_run_as_of_month", table_name="evaluation_run")
    op.drop_table("evaluation_run")
