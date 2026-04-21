"""Engine and session factory.

Keeps all session construction in one place so tests and the application
share the same wiring. The module is intentionally small: nothing else in
the codebase should instantiate engines.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import Engine, create_engine, event
from sqlalchemy.orm import Session, sessionmaker

from headcount.config import get_settings


_SQLITE_BUSY_TIMEOUT_MS = 15_000
_SQLITE_WAL_ENABLED = True
def _is_in_memory_sqlite(url: str) -> bool:
    # ``sqlite://`` (no path) and ``sqlite:///:memory:`` are the two
    # shapes SQLAlchemy recognises as the :memory: database. WAL does
    # not apply to :memory: (no file to journal) so we skip the PRAGMA
    # to avoid a harmless-but-noisy warning from SQLite.
    if not url:
        return False
    return url in {"sqlite://", "sqlite:///:memory:"} or ":memory:" in url


def _make_engine(url: str) -> Engine:
    connect_args: dict[str, object] = {}
    is_sqlite = url.startswith("sqlite")
    if is_sqlite:
        connect_args["check_same_thread"] = False

    engine = create_engine(url, future=True, connect_args=connect_args)

    if is_sqlite:
        in_memory = _is_in_memory_sqlite(url)

        @event.listens_for(engine, "connect")
        def _set_sqlite_pragmas(dbapi_conn, _: object) -> None:  # type: ignore[no-untyped-def]
            # Called once per physical connection. The canonical DB is
            # shared across every cohort run / retry script / observer
            # backfill, so we need three things:
            #   1. FKs on, same as before.
            #   2. WAL so two processes (cohort runner + ad-hoc retry)
            #      can write concurrently without corrupting each other.
            #   3. busy_timeout so short contention doesn't surface as
            #      "database is locked" mid-fetch - the blocker is
            #      almost always a fast metadata write.
            cursor = dbapi_conn.cursor()
            try:
                cursor.execute("PRAGMA foreign_keys=ON")
                cursor.execute(f"PRAGMA busy_timeout={_SQLITE_BUSY_TIMEOUT_MS}")
                if _SQLITE_WAL_ENABLED and not in_memory:
                    # journal_mode is persistent across connections so
                    # issuing this on every connect is cheap - SQLite
                    # no-ops when already WAL - but leaving it here
                    # means the very first connect on a fresh DB also
                    # flips the mode, which matters for the backfill
                    # migration.
                    cursor.execute("PRAGMA journal_mode=WAL")
                    # synchronous=NORMAL is the WAL-recommended pairing;
                    # FULL is overkill when we're journaling anyway.
                    cursor.execute("PRAGMA synchronous=NORMAL")
            finally:
                cursor.close()

    return engine


def get_engine() -> Engine:
    settings = get_settings()
    return _make_engine(settings.db_url)


def get_sessionmaker(engine: Engine | None = None) -> sessionmaker[Session]:
    return sessionmaker(bind=engine or get_engine(), expire_on_commit=False, future=True)


@contextmanager
def session_scope(engine: Engine | None = None) -> Iterator[Session]:
    """Transactional session context manager."""
    factory = get_sessionmaker(engine)
    session = factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
