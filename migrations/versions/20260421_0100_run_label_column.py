"""run_label_column

Revision ID: f1c8b2a90e44
Revises: e9b4d72a5f01
Create Date: 2026-04-21 01:00:00.000000+00:00

Adds a nullable ``run.label`` column so the canonical long-lived DB can
host many overlapping runs (Harmonic cohort, retries, Wayback backfill,
ad-hoc probes) without collapsing them into an anonymous run kind/status
pair. Back-fill semantics: leaving ``NULL`` for existing rows matches
"legacy / unlabelled" and code treats it as such.

Additive + nullable, so this is a forward-only safe migration on
SQLite (ALTER TABLE ADD COLUMN) and Postgres alike. No index is added
- the column is a tag, not a lookup key.
"""
from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "f1c8b2a90e44"
down_revision: str | None = "e9b4d72a5f01"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "run",
        sa.Column("label", sa.String(length=128), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("run", "label")
