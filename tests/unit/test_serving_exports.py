"""Unit tests for :mod:`headcount.serving.exports`."""

from __future__ import annotations

import csv
import json
from datetime import date
from pathlib import Path

import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session

from headcount.db.enums import (
    AnchorType,
    ConfidenceBand,
    EstimateMethod,
    EstimateVersionStatus,
    HeadcountValueKind,
    PriorityTier,
    ReviewReason,
    ReviewStatus,
    RunKind,
    RunStatus,
)
from headcount.models import (
    Base,
    Company,
    CompanyAnchorObservation,
    EstimateVersion,
    HeadcountEstimateMonthly,
    ReviewQueueItem,
    Run,
)
from headcount.serving.exports import ExportFormatError, export_table


@pytest.fixture()
def session() -> Session:
    engine = create_engine("sqlite:///:memory:", future=True)

    @event.listens_for(engine, "connect")
    def _enable_fk(dbapi_conn, _):  # type: ignore[no-untyped-def]
        dbapi_conn.execute("PRAGMA foreign_keys=ON")

    Base.metadata.create_all(engine)
    with Session(engine, future=True) as s:
        yield s


def _seed(session: Session) -> Company:
    company = Company(
        canonical_name="Acme Inc",
        canonical_domain="acme.com",
        priority_tier=PriorityTier.P1,
    )
    session.add(company)
    session.flush()

    from datetime import UTC, datetime

    run = Run(
        kind=RunKind.full,
        status=RunStatus.succeeded,
        started_at=datetime.now(tz=UTC),
        cutoff_month=date(2023, 12, 1),
        method_version="m1",
        anchor_policy_version="a1",
        coverage_curve_version="c1",
        config_hash="test",
    )
    session.add(run)
    session.flush()

    version = EstimateVersion(
        company_id=company.id,
        estimation_run_id=run.id,
        method_version="m1",
        anchor_policy_version="a1",
        coverage_curve_version="c1",
        source_snapshot_cutoff=date(2023, 12, 1),
        status=EstimateVersionStatus.published,
    )
    session.add(version)
    session.flush()

    session.add(
        CompanyAnchorObservation(
            company_id=company.id,
            anchor_type=AnchorType.historical_statement,
            anchor_month=date(2023, 6, 1),
            headcount_value_point=1000,
            headcount_value_min=980,
            headcount_value_max=1020,
            headcount_value_kind=HeadcountValueKind.exact,
            confidence=0.9,
        )
    )
    for month in (date(2023, 6, 1), date(2023, 7, 1), date(2023, 8, 1)):
        session.add(
            HeadcountEstimateMonthly(
                company_id=company.id,
                estimate_version_id=version.id,
                month=month,
                estimated_headcount=1000.0,
                estimated_headcount_min=950.0,
                estimated_headcount_max=1050.0,
                public_profile_count=50,
                scaled_from_anchor_value=1000.0,
                method=EstimateMethod.scaled_ratio,
                confidence_band=ConfidenceBand.high,
                confidence_score=0.8,
                confidence_components_json={"components": {}, "scoring_version": "scoring_v1"},
            )
        )
    session.add(
        ReviewQueueItem(
            company_id=company.id,
            estimate_version_id=version.id,
            review_reason=ReviewReason.low_confidence,
            priority=80,
            status=ReviewStatus.open,
            detail="borderline sample",
        )
    )
    session.flush()
    return company


def test_export_monthly_series_csv(tmp_path: Path, session: Session) -> None:
    company = _seed(session)
    out = tmp_path / "series.csv"
    result = export_table(session, table="monthly_series", path=out, fmt="csv")
    assert result.rows == 3
    assert result.path == out
    with out.open(encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    assert {r["month"] for r in rows} == {"2023-06-01", "2023-07-01", "2023-08-01"}
    assert rows[0]["company_id"] == company.id
    assert rows[0]["canonical_name"] == "Acme Inc"


def test_export_anchors_json(tmp_path: Path, session: Session) -> None:
    _seed(session)
    out = tmp_path / "anchors.json"
    result = export_table(session, table="anchors", path=out, fmt="json")
    assert result.rows == 1
    payload = json.loads(out.read_text(encoding="utf-8"))
    assert payload[0]["anchor_month"] == "2023-06-01"
    assert payload[0]["value_point"] == 1000.0


def test_export_review_queue_open_only(tmp_path: Path, session: Session) -> None:
    _seed(session)
    out = tmp_path / "queue.json"
    result = export_table(session, table="review_queue", path=out, fmt="json")
    payload = json.loads(out.read_text(encoding="utf-8"))
    assert result.rows == 1
    assert payload[0]["review_reason"] == "low_confidence"
    assert payload[0]["priority"] == 80


def test_export_growth_windows_csv(tmp_path: Path, session: Session) -> None:
    _seed(session)
    out = tmp_path / "growth.csv"
    result = export_table(session, table="growth_windows", path=out, fmt="csv")
    # 3 horizons per company, 1 company
    assert result.rows == 3
    with out.open(encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    windows = {r["window"] for r in rows}
    assert windows == {"6m", "1y", "2y"}
    # Fixture has 3 months of estimates (2023-06..2023-08). 6m/1y/2y
    # windows all have no start-month estimate, so they're suppressed.
    assert {r["suppressed"] for r in rows} == {"True"}


def test_export_rejects_unknown_format(tmp_path: Path, session: Session) -> None:
    with pytest.raises(ExportFormatError):
        export_table(session, table="monthly_series", path=tmp_path / "x.xml", fmt="xml")


def test_export_rejects_unknown_table(tmp_path: Path, session: Session) -> None:
    with pytest.raises(ExportFormatError):
        export_table(session, table="nope", path=tmp_path / "x.csv", fmt="csv")
