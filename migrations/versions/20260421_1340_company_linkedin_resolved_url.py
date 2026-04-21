"""company_linkedin_resolved_url

Revision ID: b3c1a8d5e902
Revises: a7f0c39b1e82
Create Date: 2026-04-21 13:40:00.000000+00:00

Persist resolver outcomes in a dedicated company-level URL column so
slug discovery does not need to be repeated across runs when we already
verified the LinkedIn company page.
"""
from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "b3c1a8d5e902"
down_revision: str | None = "a7f0c39b1e82"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "company",
        sa.Column("linkedin_resolved_url", sa.String(length=1024), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("company", "linkedin_resolved_url")
