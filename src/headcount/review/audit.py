"""Tiny audit-log writer for decision-changing mutations.

Any mutation that a human could realistically want to inspect later -
manual override creation, review queue transition, confidence-threshold
change - gets a row in :class:`AuditLog`. The writer is intentionally
thin so callers don't have to remember column shapes.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy.orm import Session

from headcount.models.audit_log import AuditLog

AUDIT_VERSION = "audit_v1"
"""Semantic version for audit-payload shape. Bump if fields change."""


def record_audit(
    session: Session,
    *,
    actor_type: str,
    action: str,
    target_type: str,
    target_id: str | None,
    actor_id: str | None = None,
    payload: dict[str, Any] | None = None,
) -> AuditLog:
    """Append one audit-log row. Caller owns the transaction."""

    row = AuditLog(
        actor_type=actor_type,
        actor_id=actor_id,
        action=action,
        target_type=target_type,
        target_id=target_id,
        payload_json=dict(payload or {}),
    )
    session.add(row)
    session.flush()
    return row


__all__ = [
    "AUDIT_VERSION",
    "record_audit",
]
