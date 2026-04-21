"""raw_response_archive

Revision ID: a7f0c39b1e82
Revises: f1c8b2a90e44
Create Date: 2026-04-21 02:00:00.000000+00:00

Plan C: durable archive of every live HTTP response the fetcher
receives. Keyed ``(url, content_hash)`` so refetches of an unchanged
page collapse into a single row with bumped ``seen_count`` /
``last_seen_at`` rather than fanning out into duplicates. Bodies are
stored gzip-compressed to keep the SQLite file manageable at cohort
scale.
"""
from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "a7f0c39b1e82"
down_revision: str | None = "f1c8b2a90e44"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "raw_response",
        sa.Column("id", sa.String(), primary_key=True, nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.Column("method", sa.String(length=16), nullable=False, server_default="GET"),
        sa.Column("url", sa.String(length=2048), nullable=False),
        sa.Column("status_code", sa.Integer(), nullable=False),
        sa.Column("content_hash", sa.String(length=64), nullable=False),
        sa.Column("body_gz", sa.LargeBinary(), nullable=True),
        sa.Column(
            "body_encoding",
            sa.String(length=16),
            nullable=False,
            server_default="gzip",
        ),
        sa.Column(
            "body_length_bytes",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
        sa.Column("content_type", sa.String(length=256), nullable=True),
        sa.Column("headers_json", sa.JSON(), nullable=True),
        sa.Column("source_hint", sa.String(length=64), nullable=True),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "seen_count", sa.Integer(), nullable=False, server_default="1"
        ),
        sa.Column("note", sa.Text(), nullable=True),
        sa.UniqueConstraint("url", "content_hash", name="uq_raw_response_url_hash"),
    )
    op.create_index("ix_raw_response_url", "raw_response", ["url"])
    op.create_index(
        "ix_raw_response_source_hint", "raw_response", ["source_hint"]
    )
    op.create_index(
        "ix_raw_response_fetched_at", "raw_response", ["first_seen_at"]
    )


def downgrade() -> None:
    op.drop_index("ix_raw_response_fetched_at", table_name="raw_response")
    op.drop_index("ix_raw_response_source_hint", table_name="raw_response")
    op.drop_index("ix_raw_response_url", table_name="raw_response")
    op.drop_table("raw_response")
