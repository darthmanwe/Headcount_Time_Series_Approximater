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


def _make_engine(url: str) -> Engine:
    connect_args: dict[str, object] = {}
    is_sqlite = url.startswith("sqlite")
    if is_sqlite:
        connect_args["check_same_thread"] = False

    engine = create_engine(url, future=True, connect_args=connect_args)

    if is_sqlite:

        @event.listens_for(engine, "connect")
        def _set_sqlite_pragmas(dbapi_conn, _: object) -> None:  # type: ignore[no-untyped-def]
            cursor = dbapi_conn.cursor()
            try:
                cursor.execute("PRAGMA foreign_keys=ON")
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
