"""phase_10_5_interpolated_method

Revision ID: a3c21f7e9d04
Revises: 1c438df3c2eb
Create Date: 2026-04-20 19:45:00.000000+00:00

Adds ``interpolated_multi_anchor`` to the ``estimate_method`` enum so the
anchor-interpolation path in ``headcount.estimate.reconcile`` can persist
its output.

Postgres requires ``ALTER TYPE ... ADD VALUE``; SQLite stores enums as
strings so the upgrade is a no-op there.
"""
from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "a3c21f7e9d04"
down_revision: str | None = "1c438df3c2eb"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    bind = op.get_bind()
    dialect = bind.dialect.name if bind is not None else ""
    if dialect == "postgresql":
        op.execute(
            "ALTER TYPE estimate_method ADD VALUE IF NOT EXISTS "
            "'interpolated_multi_anchor'"
        )
    # SQLite stores Enum as VARCHAR with a CHECK constraint that is
    # dropped/recreated on schema changes via batch_alter_table. Since the
    # monthly row value is only written by application code, the string
    # round-trips cleanly. If a future SQLite deployment needs the CHECK
    # tightened, a batch_alter_table migration can refresh it.


def downgrade() -> None:
    # Postgres does not support removing a value from an enum type
    # without a full type swap. Downgrade is a no-op; removing the value
    # is left as an operational task if ever required.
    pass
