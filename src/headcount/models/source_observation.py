"""Raw or normalized observation from any adapter.

``raw_content_hash`` is the cache key used by ``http_cache.py`` so the same
page fetched twice never creates two rows. ``parser_version`` is stored so
re-parses can be compared against historical output without ambiguity.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import JSON, DateTime, Enum, Index, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from headcount.db.base import Base, Timestamped, UUIDPk
from headcount.db.enums import ParseStatus, SourceEntityType, SourceName


class SourceObservation(UUIDPk, Timestamped, Base):
    __tablename__ = "source_observation"
    __table_args__ = (
        Index("ix_source_observation_source_hash", "source_name", "raw_content_hash"),
        Index("ix_source_observation_observed_at", "observed_at"),
    )

    source_name: Mapped[SourceName] = mapped_column(
        Enum(SourceName, name="source_name_obs"),
        nullable=False,
    )
    entity_type: Mapped[SourceEntityType] = mapped_column(
        Enum(SourceEntityType, name="source_entity_type"),
        nullable=False,
    )
    source_url: Mapped[str | None] = mapped_column(String(2048), nullable=True)
    observed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    raw_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    raw_html_path: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    raw_content_hash: Mapped[str] = mapped_column(String(128), nullable=False)
    parser_version: Mapped[str] = mapped_column(String(128), nullable=False)
    parse_status: Mapped[ParseStatus] = mapped_column(
        Enum(ParseStatus, name="parse_status"),
        nullable=False,
        default=ParseStatus.ok,
    )
    normalized_payload_json: Mapped[dict[str, object] | None] = mapped_column(JSON, nullable=True)
