"""Integration tests for the company-candidate seed importer."""

from __future__ import annotations

from pathlib import Path

import pytest
from openpyxl import Workbook
from sqlalchemy import create_engine, event, select
from sqlalchemy.orm import Session

from headcount.ingest.seeds import import_candidates
from headcount.models import Base, CompanyCandidate


def _make_workbook(path: Path, rows: list[tuple[str | None, str | None]]) -> None:
    wb = Workbook()
    ws = wb.active
    ws.title = "Sheet1"
    ws.append(("Company name", "Company Domain Name"))
    for row in rows:
        ws.append(row)
    wb.save(path)


@pytest.fixture()
def session() -> Session:
    engine = create_engine("sqlite:///:memory:", future=True)

    @event.listens_for(engine, "connect")
    def _enable_fk(dbapi_conn, _):  # type: ignore[no-untyped-def]
        dbapi_conn.execute("PRAGMA foreign_keys=ON")

    Base.metadata.create_all(engine)
    with Session(engine, future=True) as s:
        yield s


def test_seed_imports_names_and_domains(tmp_path: Path, session: Session) -> None:
    wb_path = tmp_path / "seed.xlsx"
    _make_workbook(wb_path, [("1010data", "1010data.com"), ("6sense", "6sense.com")])
    result = import_candidates(session, wb_path)
    session.commit()

    assert result.rows_scanned == 2
    assert result.rows_imported == 2
    stored = (
        session.execute(select(CompanyCandidate).order_by(CompanyCandidate.source_row_index))
        .scalars()
        .all()
    )
    assert [(c.company_name, c.domain, c.source_row_index) for c in stored] == [
        ("1010data", "1010data.com", 1),
        ("6sense", "6sense.com", 2),
    ]
    assert all(c.source_workbook == "seed.xlsx" for c in stored)
    assert all(c.source_sheet == "Sheet1" for c in stored)


def test_seed_is_idempotent(tmp_path: Path, session: Session) -> None:
    wb_path = tmp_path / "seed.xlsx"
    _make_workbook(wb_path, [("Acme", "acme.com")])
    import_candidates(session, wb_path)
    session.commit()
    result = import_candidates(session, wb_path)
    session.commit()
    assert result.rows_imported == 0
    assert result.rows_updated == 0
    assert session.execute(select(CompanyCandidate)).scalars().all().__len__() == 1


def test_seed_updates_changed_domain(tmp_path: Path, session: Session) -> None:
    wb_path = tmp_path / "seed.xlsx"
    _make_workbook(wb_path, [("Acme", "acme.com")])
    import_candidates(session, wb_path)
    session.commit()
    _make_workbook(wb_path, [("Acme", "acme.io")])
    result = import_candidates(session, wb_path)
    session.commit()
    assert result.rows_imported == 0
    assert result.rows_updated == 1
    assert session.execute(select(CompanyCandidate)).scalar_one().domain == "acme.io"


def test_seed_skips_blank_rows(tmp_path: Path, session: Session) -> None:
    wb_path = tmp_path / "seed.xlsx"
    _make_workbook(wb_path, [("", None), ("Acme", None), ("   ", "acme.com")])
    result = import_candidates(session, wb_path)
    session.commit()
    assert result.rows_scanned == 3
    assert result.rows_imported == 1
    assert result.rows_skipped == 2


def test_seed_requires_name_header(tmp_path: Path, session: Session) -> None:
    wb_path = tmp_path / "bad.xlsx"
    wb = Workbook()
    ws = wb.active
    ws.append(("wrong header", "another"))
    ws.append(("Acme", "acme.com"))
    wb.save(wb_path)
    with pytest.raises(ValueError):
        import_candidates(session, wb_path)
