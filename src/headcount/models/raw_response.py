"""Raw HTTP response archive.

Plan C of the data-persistence arc: every successful live fetch the
``HttpClient`` performs is mirrored into this table so parser changes,
new extractors, or forensic audits can replay the original bytes
without re-hitting the (rate-limited, bot-walled) upstream source.

Design notes
------------

- **Why not just reuse the file cache?** The file cache
  (``ingest/http.py:FileCache``) is keyed on the *request envelope*
  (method + url + params + body hash) and lives on disk. That is fine
  for intra-run reuse but is not queryable, not transactional with
  the observation tables, and can be purged / rotated. This table is
  the SOR for "we saw these bytes on this URL at this time".

- **Compression.** Bodies are stored gzip-compressed in
  :attr:`body_gz`. A typical LinkedIn / company-web / Wayback HTML
  response compresses to ~15-20 % of its original size, so even a
  full 2,000-company cohort run keeps the canonical SQLite DB well
  under 5 GB.

- **Dedup.** ``(url, content_hash)`` is the natural-key dedup -
  Wikipedia-style refetches of an unchanged page are cheap (they
  update ``last_seen_at`` but do not re-write the blob). The unique
  constraint is enforced at the DB level and the sink catches
  IntegrityError on collisions.

- **Source hint.** ``source_hint`` is the :class:`SourceName` that
  initiated the fetch. It is best-effort observability: a redirect
  chain that lands on a third-party host keeps the originating source
  for traceability. It is NOT a foreign key into the source enum
  because the redirector may be host-agnostic (Wayback follows into
  arbitrary origin hosts).
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    DateTime,
    Index,
    Integer,
    LargeBinary,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy import JSON as _JSON
from sqlalchemy.orm import Mapped, mapped_column

from headcount.db.base import Base, Timestamped, UUIDPk


class RawResponse(UUIDPk, Timestamped, Base):
    __tablename__ = "raw_response"
    __table_args__ = (
        UniqueConstraint(
            "url", "content_hash", name="uq_raw_response_url_hash"
        ),
        Index("ix_raw_response_url", "url"),
        Index("ix_raw_response_source_hint", "source_hint"),
        Index("ix_raw_response_fetched_at", "first_seen_at"),
    )

    method: Mapped[str] = mapped_column(String(16), nullable=False, default="GET")
    url: Mapped[str] = mapped_column(String(2048), nullable=False)
    status_code: Mapped[int] = mapped_column(Integer, nullable=False)
    # Canonical sha256 over the raw decoded body bytes. Joined with the
    # ``source_observation.raw_content_hash`` semantics in two cases:
    #   1. Observers that hash ``text`` directly (benchmark loaders) -
    #      trivial join after decompression.
    #   2. ``RawAnchorSignal.raw_content_hash`` hashes url + body, so
    #      the reparse script recomputes that value on read rather
    #      than storing it twice.
    content_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    # Gzipped UTF-8 body. NULL only if the upstream returned no body
    # (eg 304 Not Modified); we still keep the row for timing.
    body_gz: Mapped[bytes | None] = mapped_column(LargeBinary, nullable=True)
    # Encoding tag so a future switch to zstd / brotli stays additive.
    body_encoding: Mapped[str] = mapped_column(
        String(16), nullable=False, default="gzip"
    )
    body_length_bytes: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    content_type: Mapped[str | None] = mapped_column(String(256), nullable=True)
    headers_json: Mapped[dict[str, object] | None] = mapped_column(_JSON, nullable=True)
    source_hint: Mapped[str | None] = mapped_column(String(64), nullable=True)
    # Parsed-out useful metadata for fast inspection without
    # decompressing every blob.
    first_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    last_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    seen_count: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    note: Mapped[str | None] = mapped_column(Text, nullable=True)
