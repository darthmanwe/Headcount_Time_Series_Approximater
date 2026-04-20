"""Declarative base and cross-cutting column mixins.

Every ORM model inherits from ``Base`` so Alembic autogenerate sees a
single metadata. Timestamps are tracked on all rows for audit and freshness
queries; primary keys are UUIDs so merges across environments stay safe.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

from sqlalchemy import DateTime
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Shared declarative base."""


def _utcnow() -> datetime:
    return datetime.now(tz=UTC)


class UUIDPk:
    """Mixin providing a ``uuid`` primary key.

    Stored as ``CHAR(36)`` string form so SQLite and Postgres both index it
    with no driver-specific fuss.
    """

    id: Mapped[str] = mapped_column(
        primary_key=True,
        default=lambda: str(uuid.uuid4()),
    )


class Timestamped:
    """Mixin providing ``created_at`` / ``updated_at``."""

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=_utcnow,
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=_utcnow,
        onupdate=_utcnow,
        nullable=False,
    )
