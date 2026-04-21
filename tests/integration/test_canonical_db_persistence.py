"""Smoke test for Plan B1: observations persist across ``collect_anchors``
invocations on the same canonical DB.

Contract being verified
-----------------------

1. A clean canonical SQLite DB starts empty.
2. First ``collect_anchors`` run with a mocked adapter writes one
   ``source_observation`` + one ``company_anchor_observation`` per
   company.
3. A *second* ``collect_anchors`` run against the **same DB** with the
   **same mock** does not produce a duplicate observation - the
   content-hash dedup logic kicks in - and the row count of the
   observation tables is unchanged.
4. The second run does create its own ``Run`` row (different id, same
   or different ``label``), demonstrating that multiple runs can
   legitimately coexist in one canonical DB.

If this ever regresses, every successful public-source fetch in
production would start duplicating on rerun, which is the exact bug
Plan B1 is meant to prevent.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from datetime import UTC, date, datetime
from pathlib import Path

import pytest
from sqlalchemy import create_engine, event, select
from sqlalchemy.orm import Session, sessionmaker

from headcount.db.base import Base
from headcount.db.enums import (
    AnchorType,
    HeadcountValueKind,
    ParseStatus,
    SourceEntityType,
    SourceName,
)
from headcount.ingest.base import (
    AnchorSourceAdapter,
    CompanyTarget,
    FetchContext,
    RawAnchorSignal,
)
from headcount.ingest.collect import collect_anchors
from headcount.ingest.http import FileCache, HttpClient
from headcount.models.company import Company
from headcount.models.company_anchor_observation import CompanyAnchorObservation
from headcount.models.run import Run
from headcount.models.source_observation import SourceObservation


# ---------------------------------------------------------------------------
# Fixture plumbing
# ---------------------------------------------------------------------------


@pytest.fixture()
def canonical_db(tmp_path: Path) -> Iterator[tuple[Path, sessionmaker[Session]]]:
    """Spin up a file-backed SQLite DB at ``<tmp>/canonical.sqlite`` and
    return ``(path, sessionmaker)``.

    File-backed (not ``:memory:``) so the WAL / busy_timeout PRAGMAs the
    app sets at connect time have something real to journal against,
    and so a "second run" can legitimately pick up the same file.
    """

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
            cur.execute("PRAGMA journal_mode=WAL")
        finally:
            cur.close()

    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine, expire_on_commit=False, future=True)
    yield db_path, factory
    engine.dispose()


class _StubAdapter(AnchorSourceAdapter):
    """Deterministic adapter that always emits one signal per company.

    The signal's ``raw_text`` is derived from the company id so the
    content hash is stable across reruns - which is the precondition
    the dedup logic relies on.
    """

    source_name = SourceName.manual
    parser_version = "smoke-v1"

    async def fetch_current_anchor(  # noqa: D401 - stub
        self, target: CompanyTarget, *, context: FetchContext
    ) -> list[RawAnchorSignal]:
        return [
            RawAnchorSignal(
                source_name=self.source_name,
                entity_type=SourceEntityType.company,
                source_url=f"https://test.example/{target.company_id}",
                observed_at=datetime(2026, 4, 1, tzinfo=UTC),
                anchor_month=date(2026, 4, 1),
                anchor_type=AnchorType.current_headcount_anchor,
                headcount_value_min=100.0,
                headcount_value_point=100.0,
                headcount_value_max=100.0,
                headcount_value_kind=HeadcountValueKind.exact,
                confidence=0.8,
                raw_text=f"smoke body for {target.company_id}",
                parser_version=self.parser_version,
                parse_status=ParseStatus.ok,
                note="smoke-test",
                normalized_payload={"stub": True},
            )
        ]


def _make_http(cache_dir: Path) -> HttpClient:
    # No adapter issues a real request in this test, but collect_anchors
    # still builds an HttpClient lifecycle so we hand it a working one.
    return HttpClient(cache=FileCache(cache_dir), configs={}, transport=None)


# ---------------------------------------------------------------------------
# The actual smoke test
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_two_runs_against_same_canonical_db_dedupe_observations(
    canonical_db: tuple[Path, sessionmaker[Session]],
    tmp_path: Path,
) -> None:
    db_path, factory = canonical_db
    assert db_path.exists()

    # Seed two companies directly - the candidate/resolve flow is
    # covered elsewhere; this test only cares about observation dedup.
    with factory() as session:
        for cid, name, domain in (
            ("c1", "Acme", "acme.example"),
            ("c2", "Beta", "beta.example"),
        ):
            session.add(
                Company(
                    id=cid,
                    canonical_name=name,
                    canonical_domain=domain,
                )
            )
        session.commit()

    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    adapter = _StubAdapter()

    # --- Run 1 -------------------------------------------------------
    with factory() as session:
        client = _make_http(cache_dir)
        result1 = await collect_anchors(
            session,
            adapters=[adapter],
            companies=list(session.execute(select(Company)).scalars()),
            http_client=client,
            run_label="smoke:first",
        )
        session.commit()
        run1_signals = result1.signals_written
        run1_anchors = result1.anchors_written

    assert run1_signals == 2
    assert run1_anchors == 2

    # --- Run 2 (same DB, same stub) -----------------------------------
    with factory() as session:
        client = _make_http(cache_dir)
        result2 = await collect_anchors(
            session,
            adapters=[adapter],
            companies=list(session.execute(select(Company)).scalars()),
            http_client=client,
            run_label="smoke:second",
        )
        session.commit()
        run2_signals = result2.signals_written
        run2_anchors = result2.anchors_written

    assert run2_signals == 0, (
        "second run wrote source_observation rows despite identical content "
        f"hashes; got {run2_signals}"
    )
    assert run2_anchors == 0, (
        "second run wrote company_anchor_observation rows despite dedup; got "
        f"{run2_anchors}"
    )

    # Final state: 2 observation rows total; 2 Run rows with distinct
    # labels (both runs recorded).
    with factory() as session:
        obs_count = session.execute(
            select(SourceObservation)
        ).unique().scalars().all()
        assert len(obs_count) == 2

        anchor_count = session.execute(
            select(CompanyAnchorObservation)
        ).unique().scalars().all()
        assert len(anchor_count) == 2

        runs = list(session.execute(select(Run).order_by(Run.started_at)).scalars())
        assert len(runs) == 2
        labels = [r.label for r in runs]
        assert labels == ["smoke:first", "smoke:second"]
