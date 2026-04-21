"""source_name_wayback

Revision ID: e9b4d72a5f01
Revises: d7a3c4b1f920
Create Date: 2026-04-21 00:00:00.000000+00:00

Adds ``wayback`` to the ``SourceName`` enum so the Internet-Archive-backed
historical observer can persist ``source_observation`` rows under its own
provenance tag instead of piggy-backing on ``company_web`` /
``linkedin_public``.

Postgres requires ``ALTER TYPE ... ADD VALUE`` on every named enum type
that references ``SourceName``:

- ``source_name_obs``   (source_observation.source_name)
- ``source_name``       (company_source_link.source_name)
- ``source_name_person`` (person.source_name)
- ``source_name_budget`` (source_budget.source_name)

SQLite stores enums as a ``VARCHAR`` + CHECK constraint. We don't tighten
the CHECK here because application writes go through SQLAlchemy which
coerces to the string ``'wayback'`` and round-trips cleanly.
"""
from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "e9b4d72a5f01"
down_revision: str | None = "d7a3c4b1f920"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


_ENUM_TYPES: tuple[str, ...] = (
    "source_name_obs",
    "source_name",
    "source_name_person",
    "source_name_budget",
)


def upgrade() -> None:
    bind = op.get_bind()
    dialect = bind.dialect.name if bind is not None else ""
    if dialect == "postgresql":
        for enum_name in _ENUM_TYPES:
            op.execute(
                f"ALTER TYPE {enum_name} ADD VALUE IF NOT EXISTS 'wayback'"
            )
    # SQLite: string round-trips cleanly through the Enum(..) VARCHAR.


def downgrade() -> None:
    # Postgres does not support removing a value from an enum type
    # without a full type swap. Downgrade is a no-op; removing the value
    # is left as an operational task if ever required.
    pass
