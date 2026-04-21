"""Sink abstraction + default DB implementation for raw HTTP responses.

The :class:`HttpClient` write-through contract is:

    on every **live** (non-cache-hit) HTTP response, call
    ``sink.write(RawResponseWrite)`` with the decoded body.

A :class:`RawResponseSink` is any object that implements that one
method. The default :class:`DbRawResponseSink` persists rows into the
canonical ``raw_response`` table. Tests use :class:`NullRawResponseSink`
to opt out entirely.

Invariants enforced by the DB sink
----------------------------------

- ``(url, content_hash)`` is the dedup key (matching
  ``uq_raw_response_url_hash``). A refetch that returns unchanged
  bytes bumps ``last_seen_at`` and ``seen_count`` instead of adding a
  new row.
- Bodies are gzip-compressed before insert. ``body_length_bytes`` is
  the *uncompressed* length so forensic queries don't have to
  decompress to size-bucket traffic.
- Sink errors never bubble back into the HTTP path. The fetcher logs
  the failure and continues; raw archival is best-effort so a DB
  hiccup can never break a live run.
"""

from __future__ import annotations

import gzip
import hashlib
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Protocol

from sqlalchemy import Engine, select
from sqlalchemy.orm import Session, sessionmaker

from headcount.models.raw_response import RawResponse
from headcount.utils.logging import get_logger

_log = get_logger("headcount.ingest.raw_response_store")


@dataclass(slots=True)
class RawResponseWrite:
    """In-flight write payload.

    Kept separate from the ORM model so the sink abstraction does not
    depend on SQLAlchemy and tests can exercise the contract with a
    lightweight dataclass.
    """

    url: str
    method: str
    status_code: int
    body_text: str
    headers: Mapping[str, str]
    source_hint: str | None = None
    fetched_at: datetime = field(default_factory=lambda: datetime.now(tz=UTC))
    note: str | None = None


def compute_body_hash(body_text: str) -> str:
    """Canonical hash for :attr:`RawResponse.content_hash`.

    Sha256 over the UTF-8-encoded body. Matches the dedup behaviour
    of ``uq_raw_response_url_hash`` - same bytes, same hash.
    """

    return hashlib.sha256(body_text.encode("utf-8")).hexdigest()


class RawResponseSink(Protocol):
    """One-method contract used by :class:`HttpClient`."""

    def write(self, payload: RawResponseWrite) -> None:  # pragma: no cover - protocol
        ...


class NullRawResponseSink:
    """No-op sink for tests / offline runs. Always cheap, never raises."""

    def write(self, payload: RawResponseWrite) -> None:  # noqa: D401 - trivial
        return None


class DbRawResponseSink:
    """Persist raw responses to the canonical ``raw_response`` table.

    The sink opens a short-lived transactional session per write via
    the supplied ``sessionmaker`` so it can safely be called from an
    async context (each call is synchronous against its own session)
    and from multiple observers in parallel without fighting over a
    shared SQLAlchemy session.

    All write paths swallow exceptions after logging. Raw archival is
    an observability / reparse affordance, not part of the critical
    path; a DB error here must not fail a live fetch.
    """

    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        self._session_factory = session_factory

    def write(self, payload: RawResponseWrite) -> None:
        try:
            self._persist(payload)
        except Exception as exc:
            _log.warning(
                "raw_response_sink_error",
                url=payload.url,
                source_hint=payload.source_hint,
                error=repr(exc),
            )

    def _persist(self, payload: RawResponseWrite) -> None:
        content_hash = compute_body_hash(payload.body_text)
        body_bytes = payload.body_text.encode("utf-8")
        body_gz = gzip.compress(body_bytes, compresslevel=6) if body_bytes else None
        body_length = len(body_bytes)
        content_type = None
        for key, value in payload.headers.items():
            if key.lower() == "content-type":
                content_type = str(value)[:256]
                break
        headers_json = {str(k)[:128]: str(v)[:1024] for k, v in payload.headers.items()}
        with self._session_factory() as session:
            existing = session.execute(
                select(RawResponse).where(
                    RawResponse.url == payload.url,
                    RawResponse.content_hash == content_hash,
                )
            ).scalar_one_or_none()
            if existing is not None:
                # Dedup: refresh sighting timestamps + counter; skip
                # the expensive blob write. The status_code may have
                # legitimately flipped (eg 200 -> 304) but we keep the
                # first-seen status for archival fidelity.
                existing.last_seen_at = payload.fetched_at
                existing.seen_count = (existing.seen_count or 0) + 1
                session.commit()
                return

            row = RawResponse(
                url=payload.url,
                method=payload.method,
                status_code=payload.status_code,
                content_hash=content_hash,
                body_gz=body_gz,
                body_encoding="gzip",
                body_length_bytes=body_length,
                content_type=content_type,
                headers_json=headers_json,
                source_hint=payload.source_hint,
                first_seen_at=payload.fetched_at,
                last_seen_at=payload.fetched_at,
                seen_count=1,
                note=payload.note,
            )
            session.add(row)
            session.commit()


def build_sink_from_session(session: Session) -> RawResponseSink:
    """Construct a DB sink bound to the same engine as ``session``.

    The sink opens its own short-lived sessions so observer code can
    call it from within an active ``session.flush()`` / ``commit()``
    cycle without fighting the caller's SQLAlchemy state. Returns a
    :class:`NullRawResponseSink` if the session is not bound to an
    engine (eg some in-memory smoke tests).
    """

    bind = session.get_bind()
    if not isinstance(bind, Engine):
        return NullRawResponseSink()
    factory = sessionmaker(bind=bind, expire_on_commit=False, future=True)
    return DbRawResponseSink(factory)


def decompress_body(row: RawResponse) -> str:
    """Decode the on-disk body back to str. Reparse utility.

    Raises on an unexpected encoding so a future migration to zstd /
    brotli forces a callsite update rather than silently returning
    bytes the caller can't parse.
    """

    if row.body_gz is None:
        return ""
    if row.body_encoding == "gzip":
        return gzip.decompress(row.body_gz).decode("utf-8", errors="replace")
    raise ValueError(
        f"unsupported raw_response body_encoding: {row.body_encoding!r}"
    )
