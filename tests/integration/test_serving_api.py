"""Integration tests for the FastAPI surface.

Each test builds a fresh in-memory SQLite engine, wires a custom
``sessionmaker`` into :func:`create_app`, and drives the app via
``TestClient``. This keeps the suite fully offline and avoids touching
the developer's real DB.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, date, datetime

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from headcount.db.enums import (
    AnchorType,
    BenchmarkMetric,
    BenchmarkProvider,
    HeadcountValueKind,
    OverrideField,
    PriorityTier,
    ReviewReason,
    ReviewStatus,
    SourceName,
)
from headcount.estimate.pipeline import estimate_series
from headcount.models import (
    Base,
    BenchmarkObservation,
    Company,
    CompanyAnchorObservation,
    Person,
    PersonEmploymentObservation,
    ReviewQueueItem,
)
from headcount.serving.api import create_app


@pytest.fixture()
def engine() -> Iterator[Engine]:
    # StaticPool + check_same_thread=False so the in-memory DB is shared
    # across the test thread and the TestClient's request thread.
    eng = create_engine(
        "sqlite:///:memory:",
        future=True,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    @event.listens_for(eng, "connect")
    def _enable_fk(dbapi_conn, _):  # type: ignore[no-untyped-def]
        dbapi_conn.execute("PRAGMA foreign_keys=ON")

    Base.metadata.create_all(eng)
    yield eng
    eng.dispose()


@pytest.fixture()
def session_factory(engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(bind=engine, expire_on_commit=False, future=True)


@pytest.fixture()
def client(session_factory: sessionmaker[Session]) -> TestClient:
    app = create_app(session_factory=session_factory)
    return TestClient(app)


def _seed(session: Session) -> Company:
    company = Company(
        canonical_name="Acme Inc",
        canonical_domain="acme.com",
        priority_tier=PriorityTier.P1,
    )
    session.add(company)
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
    for i in range(10):
        p = Person(
            source_name=SourceName.manual,
            source_person_key=f"manual::p{i}",
            display_name=f"Person {i}",
        )
        session.add(p)
        session.flush()
        session.add(
            PersonEmploymentObservation(
                person_id=p.id,
                company_id=company.id,
                start_month=date(2023, 1, 1),
                end_month=None,
                is_current_role=True,
            )
        )
    session.commit()
    estimate_series(
        session,
        start_month=date(2023, 1, 1),
        end_month=date(2023, 12, 1),
        as_of_month=date(2023, 12, 1),
        sample_floor=1,
    )
    session.commit()
    return company


def test_healthz(client: TestClient) -> None:
    resp = client.get("/healthz")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert body["api_version"]


def test_list_companies(client: TestClient, session_factory: sessionmaker[Session]) -> None:
    with session_factory() as session:
        _seed(session)
    resp = client.get("/companies")
    assert resp.status_code == 200
    body = resp.json()
    assert len(body) == 1
    assert body[0]["canonical_name"] == "Acme Inc"


def test_get_company_404(client: TestClient) -> None:
    resp = client.get("/companies/does-not-exist")
    assert resp.status_code == 404


def test_company_series(client: TestClient, session_factory: sessionmaker[Session]) -> None:
    with session_factory() as session:
        company = _seed(session)
    resp = client.get(f"/companies/{company.id}/series")
    assert resp.status_code == 200
    body = resp.json()
    assert body["company"]["id"] == company.id
    assert body["estimate_version_id"] is not None
    assert len(body["months"]) == 12
    # With start filter
    resp2 = client.get(
        f"/companies/{company.id}/series", params={"start": "2023-06", "end": "2023-08"}
    )
    assert resp2.status_code == 200
    assert len(resp2.json()["months"]) == 3


def test_company_evidence(client: TestClient, session_factory: sessionmaker[Session]) -> None:
    with session_factory() as session:
        company = _seed(session)
    resp = client.get(f"/companies/{company.id}/months/2023-06/evidence")
    assert resp.status_code == 200
    tree = resp.json()
    assert tree["company"]["id"] == company.id
    assert tree["estimate"]["month"] == "2023-06-01"
    assert tree["inputs"]["anchors"]
    # Evidence trace must include the 6m/1y/2y growth windows block
    # (populated when enough monthly estimates exist; here we have 12
    # months of history so the 6m window should be present).
    windows = {w["window"] for w in tree["growth"]}
    assert windows == {"6m", "1y", "2y"}


def test_company_growth_endpoint(
    client: TestClient, session_factory: sessionmaker[Session]
) -> None:
    with session_factory() as session:
        company = _seed(session)
    resp = client.get(f"/companies/{company.id}/growth")
    assert resp.status_code == 200
    body = resp.json()
    assert body["company"]["id"] == company.id
    windows = {w["window"]: w for w in body["windows"]}
    assert set(windows) == {"6m", "1y", "2y"}
    # Only the 6m window has a start point in this fixture (end is
    # 2023-12, 6m-ago = 2023-06 which exists; 1y/2y do not).
    assert windows["6m"]["suppressed"] is False
    assert windows["1y"]["suppressed"] is True
    assert windows["2y"]["suppressed"] is True


def test_company_evidence_404(client: TestClient, session_factory: sessionmaker[Session]) -> None:
    with session_factory() as session:
        company = _seed(session)
    resp = client.get(f"/companies/{company.id}/months/2019-01/evidence")
    assert resp.status_code == 404


def test_review_queue(client: TestClient, session_factory: sessionmaker[Session]) -> None:
    with session_factory() as session:
        company = _seed(session)
        session.add(
            ReviewQueueItem(
                company_id=company.id,
                review_reason=ReviewReason.low_confidence,
                priority=80,
                status=ReviewStatus.open,
                detail="sample borderline",
            )
        )
        session.commit()
    resp = client.get("/review-queue")
    assert resp.status_code == 200
    rows = resp.json()
    assert rows
    assert rows[0]["priority"] == 80
    assert rows[0]["canonical_name"] == "Acme Inc"


def test_review_queue_transition_claim_and_resolve(
    client: TestClient, session_factory: sessionmaker[Session]
) -> None:
    with session_factory() as session:
        company = _seed(session)
        item = ReviewQueueItem(
            company_id=company.id,
            review_reason=ReviewReason.low_confidence,
            priority=80,
            status=ReviewStatus.open,
            detail="needs eyes",
        )
        session.add(item)
        session.commit()
        item_id = item.id

    # Claim (open -> assigned)
    resp = client.post(
        f"/review-queue/{item_id}/transition",
        json={"status": "assigned", "assigned_to": "alice", "note": "taking a look"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "assigned"
    assert body["assigned_to"] == "alice"

    # Resolve (assigned -> resolved)
    resp = client.post(
        f"/review-queue/{item_id}/transition",
        json={"status": "resolved", "note": "fixed via override", "actor_id": "alice"},
    )
    assert resp.status_code == 200
    assert resp.json()["status"] == "resolved"

    # Audit log captures both transitions.
    audits = client.get(
        "/audit",
        params={"target_type": "review_queue_item", "target_id": item_id},
    ).json()
    actions = {(a["payload"]["from"], a["payload"]["to"]) for a in audits}
    assert ("open", "assigned") in actions
    assert ("assigned", "resolved") in actions

    # 404 on unknown item.
    missing = client.post(
        "/review-queue/does-not-exist/transition", json={"status": "resolved"}
    )
    assert missing.status_code == 404

    # 400 on bad status.
    bad = client.post(
        f"/review-queue/{item_id}/transition", json={"status": "nonsense"}
    )
    assert bad.status_code == 400

    # 400 on assigned without assigned_to.
    bad_assign = client.post(
        f"/review-queue/{item_id}/transition", json={"status": "assigned"}
    )
    assert bad_assign.status_code == 400


def test_runs_list_and_detail(
    client: TestClient, session_factory: sessionmaker[Session]
) -> None:
    with session_factory() as session:
        _seed(session)
    runs = client.get("/runs").json()
    assert runs
    run_id = runs[0]["id"]
    detail = client.get(f"/runs/{run_id}").json()
    assert detail["run"]["id"] == run_id
    # We should see the estimate_series stage rows.
    assert any(
        s["stage"] == "estimate_series" and sum(s["counts"].values()) >= 1
        for s in detail["stages"]
    )


def test_override_create_and_list(
    client: TestClient, session_factory: sessionmaker[Session]
) -> None:
    with session_factory() as session:
        company = _seed(session)
    body = {
        "company_id": company.id,
        "field_name": OverrideField.current_anchor.value,
        "payload": {
            "anchor_month": "2023-06-01",
            "value_min": 1900,
            "value_point": 2000,
            "value_max": 2100,
            "confidence": 0.9,
        },
        "reason": "manual adjustment",
        "entered_by": "api-test",
    }
    resp = client.post("/overrides", json=body)
    assert resp.status_code == 201, resp.text
    created = resp.json()
    assert created["company_id"] == company.id
    assert created["field_name"] == "current_anchor"

    listed = client.get("/overrides", params={"company_id": company.id}).json()
    assert any(o["id"] == created["id"] for o in listed)

    # Audit entry appears.
    audits = client.get(
        "/audit", params={"target_type": "manual_override", "target_id": created["id"]}
    ).json()
    assert audits


def test_benchmark_comparison_endpoint(
    client: TestClient, session_factory: sessionmaker[Session]
) -> None:
    with session_factory() as session:
        company = _seed(session)
        session.add(
            BenchmarkObservation(
                company_id=company.id,
                source_workbook="t.xlsx",
                source_sheet="S",
                source_row_index=1,
                company_name_raw=company.canonical_name,
                provider=BenchmarkProvider.zeeshan,
                metric=BenchmarkMetric.headcount_current,
                as_of_month=date(2023, 6, 1),
                value_point=200.0,
            )
        )
        session.commit()
    resp = client.get("/benchmarks/comparison", params={"threshold": 0.25})
    assert resp.status_code == 200
    body = resp.json()
    assert body["companies_with_benchmarks"] == 1
    assert body["disagreements_total"] == 1


def test_status_summary(client: TestClient, session_factory: sessionmaker[Session]) -> None:
    with session_factory() as session:
        _seed(session)
    body = client.get("/status/summary").json()
    assert body["companies_total"] == 1
    assert body["latest_run"] is not None
    assert "stage_counts" in body["latest_run"]


def _iso(dt: datetime) -> str:
    return dt.astimezone(UTC).isoformat()
