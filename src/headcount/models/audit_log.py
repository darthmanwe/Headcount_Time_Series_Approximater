"""Immutable audit log entry for decision-changing actions."""

from __future__ import annotations

from sqlalchemy import JSON, String
from sqlalchemy.orm import Mapped, mapped_column

from headcount.db.base import Base, Timestamped, UUIDPk


class AuditLog(UUIDPk, Timestamped, Base):
    __tablename__ = "audit_log"

    actor_type: Mapped[str] = mapped_column(String(64), nullable=False)
    actor_id: Mapped[str | None] = mapped_column(String(256), nullable=True)
    action: Mapped[str] = mapped_column(String(128), nullable=False)
    target_type: Mapped[str] = mapped_column(String(128), nullable=False)
    target_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    payload_json: Mapped[dict[str, object] | None] = mapped_column(JSON, nullable=True)
