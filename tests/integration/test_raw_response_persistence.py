"""Plan C: HttpClient write-through + reparse-all replay.

Covers the three contracts that keep raw_response useful in
production:

1. Every live (non-cache-hit) successful fetch is archived.
2. A refetch of unchanged bytes collapses onto the existing row
   (``seen_count`` + ``last_seen_at`` bumped; no duplicate body blob).
3. ``scripts/reparse_raw_responses.py`` can drive extraction off the
   archived bytes alone - no network, no HttpClient - and produces
   the same ``source_observation`` + ``company_anchor_observation``
   shape the live observer would have produced, with content-hash
   dedup so a repeated reparse is a no-op.

If any of these regress, the value proposition of Plan C evaporates:
- (1) regressed => we're back to "the data we fetched is stuck in
  the file cache, not queryable, not reparseable".
- (2) regressed => the archive balloons with redundant blobs.
- (3) regressed => a parser bump can't be backfilled without
  re-fetching the internet.
"""

from __future__ import annotations

import gzip
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
from sqlalchemy import create_engine, event, select
from sqlalchemy.orm import Session, sessionmaker

from headcount.db.base import Base
from headcount.db.enums import SourceName
from headcount.ingest.http import FileCache, HttpClient
from headcount.ingest.raw_response_store import (
    DbRawResponseSink,
    NullRawResponseSink,
    RawResponseWrite,
    build_sink_from_session,
    compute_body_hash,
    decompress_body,
)
from headcount.models.company import Company
from headcount.models.company_anchor_observation import CompanyAnchorObservation
from headcount.models.raw_response import RawResponse
from headcount.models.source_observation import SourceObservation


# ---------------------------------------------------------------------------
# Fixture plumbing
# ---------------------------------------------------------------------------


@pytest.fixture()
def db(tmp_path: Path) -> Iterator[tuple[Path, sessionmaker[Session]]]:
    db_path = tmp_path / "canonical.sqlite"
    engine = create_engine(
        f"sqlite:///{db_path.as_posix()}",
        future=True,
        connect_args={"check_same_thread": False},
    )

    @event.listens_for(engine, "connect")
    def _pragmas(dbapi_conn, _):  # type: ignore[no-untyped-def]
        cur = dbapi_conn.cursor()
        try:
            cur.execute("PRAGMA foreign_keys=ON")
        finally:
            cur.close()

    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine, expire_on_commit=False, future=True)
    yield db_path, factory
    engine.dispose()


# ---------------------------------------------------------------------------
# (1) DbRawResponseSink semantics
# ---------------------------------------------------------------------------


def test_sink_writes_and_dedupes_same_bytes(
    db: tuple[Path, sessionmaker[Session]],
) -> None:
    _, factory = db
    sink = DbRawResponseSink(factory)

    payload = RawResponseWrite(
        url="https://acme.example/about",
        method="GET",
        status_code=200,
        body_text="<html>acme has 500 employees</html>",
        headers={"Content-Type": "text/html"},
        source_hint=SourceName.company_web.value,
        fetched_at=datetime(2026, 4, 1, 12, 0, tzinfo=UTC),
    )
    sink.write(payload)

    with factory() as session:
        rows = session.execute(select(RawResponse)).scalars().all()
        assert len(rows) == 1
        row = rows[0]
        assert row.url == "https://acme.example/about"
        assert row.status_code == 200
        assert row.source_hint == SourceName.company_web.value
        assert row.content_hash == compute_body_hash(payload.body_text)
        assert row.body_encoding == "gzip"
        assert row.body_gz is not None
        assert gzip.decompress(row.body_gz).decode("utf-8") == payload.body_text
        assert row.content_type == "text/html"
        assert row.seen_count == 1
        assert row.first_seen_at == row.last_seen_at

    # Second write, identical bytes -> no new row, seen_count bumped,
    # last_seen_at pushed forward.
    later = datetime(2026, 4, 2, 12, 0, tzinfo=UTC)
    sink.write(
        RawResponseWrite(
            url=payload.url,
            method=payload.method,
            status_code=payload.status_code,
            body_text=payload.body_text,
            headers=dict(payload.headers),
            source_hint=payload.source_hint,
            fetched_at=later,
        )
    )
    with factory() as session:
        rows = session.execute(select(RawResponse)).scalars().all()
        assert len(rows) == 1
        row = rows[0]
        assert row.seen_count == 2
        # SQLite drops tzinfo on read, so compare against the naive
        # equivalent of ``later``. The semantics we care about - that
        # the "last seen" timestamp advanced past "first seen" - still
        # holds either way.
        assert row.last_seen_at.replace(tzinfo=None) == later.replace(tzinfo=None)
        assert row.first_seen_at < row.last_seen_at


def test_sink_creates_second_row_when_body_differs(
    db: tuple[Path, sessionmaker[Session]],
) -> None:
    _, factory = db
    sink = DbRawResponseSink(factory)

    sink.write(
        RawResponseWrite(
            url="https://acme.example/about",
            method="GET",
            status_code=200,
            body_text="<html>v1</html>",
            headers={},
            source_hint=SourceName.company_web.value,
        )
    )
    sink.write(
        RawResponseWrite(
            url="https://acme.example/about",
            method="GET",
            status_code=200,
            body_text="<html>v2-newer</html>",
            headers={},
            source_hint=SourceName.company_web.value,
        )
    )
    with factory() as session:
        rows = session.execute(select(RawResponse)).scalars().all()
        assert len(rows) == 2
        hashes = {r.content_hash for r in rows}
        assert len(hashes) == 2


def test_null_sink_swallows_all(db: tuple[Path, sessionmaker[Session]]) -> None:
    _, factory = db
    sink = NullRawResponseSink()
    sink.write(
        RawResponseWrite(
            url="https://x",
            method="GET",
            status_code=200,
            body_text="",
            headers={},
        )
    )
    with factory() as session:
        rows = session.execute(select(RawResponse)).scalars().all()
        assert rows == []


# ---------------------------------------------------------------------------
# (2) HttpClient write-through on live fetch
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_http_client_archives_live_fetch_but_not_cache_hit(
    db: tuple[Path, sessionmaker[Session]],
    tmp_path: Path,
) -> None:
    _, factory = db

    # Count how many times the transport was actually called so we can
    # tell a cache hit from a live fetch without leaking HttpClient
    # internals into the test.
    call_count = {"n": 0}

    def _handler(request: httpx.Request) -> httpx.Response:
        call_count["n"] += 1
        return httpx.Response(
            200,
            text="<html>500 employees</html>",
            headers={"Content-Type": "text/html; charset=utf-8"},
        )

    cache = FileCache(tmp_path / "cache")
    transport = httpx.MockTransport(_handler)
    with factory() as sink_session:
        sink = build_sink_from_session(sink_session)
    # build_sink_from_session returns a DbRawResponseSink bound to the
    # same engine as sink_session. The sink opens its own sessions so
    # closing sink_session above is fine.

    client = HttpClient(
        cache=cache,
        configs={},
        transport=transport,
        raw_response_sink=sink,
    )

    async with client:
        r1 = await client.get(SourceName.company_web, "https://acme.example/about")
        r2 = await client.get(SourceName.company_web, "https://acme.example/about")

    assert r1.status_code == 200
    assert r2.status_code == 200
    # Transport was hit exactly once - second call served from cache.
    assert call_count["n"] == 1

    with factory() as session:
        rows = session.execute(select(RawResponse)).scalars().all()
        # Only the live fetch is archived. Cache hits skip the sink
        # because the body is already in the file cache and archival
        # is a live-response mirror, not a re-hydration loop.
        assert len(rows) == 1
        row = rows[0]
        assert row.url.startswith("https://acme.example/about")
        assert row.seen_count == 1
        assert row.source_hint == SourceName.company_web.value
        assert decompress_body(row) == "<html>500 employees</html>"


@pytest.mark.asyncio
async def test_http_client_skips_archive_on_error_responses(
    db: tuple[Path, sessionmaker[Session]],
    tmp_path: Path,
) -> None:
    _, factory = db

    def _handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            404, text="<html>not found</html>", headers={"Content-Type": "text/html"}
        )

    cache = FileCache(tmp_path / "cache")
    transport = httpx.MockTransport(_handler)
    with factory() as sink_session:
        sink = build_sink_from_session(sink_session)
    client = HttpClient(
        cache=cache, configs={}, transport=transport, raw_response_sink=sink
    )
    async with client:
        r = await client.get(SourceName.company_web, "https://acme.example/missing")
    assert r.status_code == 404
    with factory() as session:
        # Error responses are intentionally skipped to keep the archive
        # focused on parseable successes.
        assert session.execute(select(RawResponse)).scalars().all() == []


# ---------------------------------------------------------------------------
# (3) Reparse script replays stored bodies offline
# ---------------------------------------------------------------------------


def test_reparse_from_archived_company_web(
    db: tuple[Path, sessionmaker[Session]],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    db_path, factory = db

    # Seed one Company and one archived company-web response whose HTML
    # contains a pattern the live parser will pick up ("500 employees").
    # The reparse script has to match the URL's hostname to the
    # canonical_domain, run the parser, and persist the signal exactly
    # like the live observer would.
    body = (
        "<html><body>"
        "<p>Acme Corp has <strong>500 employees</strong> worldwide.</p>"
        "</body></html>"
    )
    with factory() as session:
        session.add(
            Company(
                id="acme",
                canonical_name="Acme Corp",
                canonical_domain="acme.example",
            )
        )
        session.add(
            RawResponse(
                url="https://acme.example/about",
                method="GET",
                status_code=200,
                content_hash=compute_body_hash(body),
                body_gz=gzip.compress(body.encode("utf-8")),
                body_encoding="gzip",
                body_length_bytes=len(body.encode("utf-8")),
                content_type="text/html",
                source_hint=SourceName.company_web.value,
                first_seen_at=datetime(2026, 4, 1, tzinfo=UTC),
                last_seen_at=datetime(2026, 4, 1, tzinfo=UTC),
                seen_count=1,
            )
        )
        session.commit()

    # Point the reparse script at this DB via ``session_scope``.
    # ``session_scope`` reads the engine from ``get_settings().db_url``
    # so we patch both the settings and the engine-factory call site to
    # bind to our tmp DB.
    monkeypatch.setenv("DB_URL", f"sqlite:///{db_path.as_posix()}")
    # Force a fresh engine read in engine.py.
    import headcount.config.settings as settings_module

    settings_module.get_settings.cache_clear()  # type: ignore[attr-defined]
    import headcount.db.engine as engine_module

    # Replace session_scope so our reparse script binds to the test DB.
    # We do this by patching session_scope to return a scope over our
    # test factory. This keeps the script pure while letting the test
    # supply the DB binding.
    from contextlib import contextmanager

    @contextmanager
    def _scoped():
        s = factory()
        try:
            yield s
            s.commit()
        except Exception:
            s.rollback()
            raise
        finally:
            s.close()

    import scripts.reparse_raw_responses as reparse_module

    monkeypatch.setattr(reparse_module, "session_scope", _scoped)

    stats = reparse_module.run(
        source_hints=[SourceName.company_web.value],
        dry_run=False,
    )
    assert stats.raw_rows_considered == 1
    assert stats.raw_rows_matched == 1
    assert stats.signals_written >= 1, (
        f"reparse produced no signals; stats={stats.as_dict()}"
    )
    assert stats.anchors_written >= 1

    with factory() as session:
        obs = session.execute(
            select(SourceObservation).where(
                SourceObservation.source_name == SourceName.company_web
            )
        ).scalars().all()
        assert len(obs) == stats.signals_written
        anchors = session.execute(
            select(CompanyAnchorObservation)
        ).scalars().all()
        assert len(anchors) == stats.anchors_written
        for a in anchors:
            assert a.headcount_value_point == 500.0

    # Rerun with the same inputs -> full dedup, zero new rows.
    stats2 = reparse_module.run(
        source_hints=[SourceName.company_web.value],
        dry_run=False,
    )
    assert stats2.raw_rows_considered == 1
    assert stats2.signals_written == 0, (
        f"rerun wrote new signals despite dedup; stats={stats2.as_dict()}"
    )
    assert stats2.anchors_written == 0
    assert stats2.dedup_hits >= 1
