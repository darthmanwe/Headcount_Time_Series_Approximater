"""Per-estimate record of how the reconciled anchor was chosen.

Phase 7 writes one of these per ``estimate_version`` so analysts can audit
which raw anchor candidates fed the output, what weights were applied, and
what the final interval was. The JSON payload carries a list of
``{source, point, min, max, weight, confidence}`` entries plus a free-text
rationale string.
"""

from __future__ import annotations

from sqlalchemy import JSON, Float, ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column

from headcount.db.base import Base, Timestamped, UUIDPk


class AnchorReconciliation(UUIDPk, Timestamped, Base):
    __tablename__ = "anchor_reconciliation"

    estimate_version_id: Mapped[str] = mapped_column(
        ForeignKey("estimate_version.id", ondelete="CASCADE"),
        nullable=False,
    )
    chosen_point: Mapped[float] = mapped_column(Float, nullable=False)
    chosen_min: Mapped[float] = mapped_column(Float, nullable=False)
    chosen_max: Mapped[float] = mapped_column(Float, nullable=False)
    inputs_json: Mapped[list[dict[str, object]]] = mapped_column(JSON, nullable=False)
    weights_json: Mapped[dict[str, float]] = mapped_column(JSON, nullable=False)
    rationale: Mapped[str | None] = mapped_column(String(2048), nullable=True)
