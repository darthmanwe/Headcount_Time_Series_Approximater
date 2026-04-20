"""Read active ``manual_override`` rows for a company.

The resolver (Phase 3) and estimation pipeline (Phase 7) both need to honor
analyst-entered overrides before they do any deterministic work. Phase 8
owns the write side; this reader lets earlier phases see overrides without
waiting on it.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from headcount.db.enums import OverrideField
from headcount.models.manual_override import ManualOverride


def get_active_overrides(
    session: Session,
    company_id: str,
    *,
    fields: Iterable[OverrideField] | None = None,
    at: datetime | None = None,
) -> list[ManualOverride]:
    """Return all non-expired overrides for ``company_id``.

    ``fields`` filters to a specific subset; ``at`` lets callers evaluate
    historical state for reproducibility (defaults to ``now(UTC)``).
    """
    as_of = at or datetime.now(tz=UTC)
    stmt = select(ManualOverride).where(ManualOverride.company_id == company_id)
    if fields is not None:
        stmt = stmt.where(ManualOverride.field_name.in_(list(fields)))
    rows = session.execute(stmt).scalars().all()

    def _is_active(row: ManualOverride) -> bool:
        if row.expires_at is None:
            return True
        expires = row.expires_at
        if expires.tzinfo is None:
            expires = expires.replace(tzinfo=UTC)
        return expires > as_of

    return [row for row in rows if _is_active(row)]
