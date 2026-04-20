"""FastAPI application for the headcount estimator.

Phase 9 brings up the read-heavy surface plus a few write endpoints for
manual overrides and run triggers. This is an internal, localhost-only
tool, so we deliberately do not wire auth here - the deployment model is
"trusted network" and any remote access must go through an ingress that
enforces its own authn/authz.

Routers are grouped by resource, with one sub-module for trivial things
(health, metrics) kept inline.

Endpoint surface:

- ``GET /healthz``, ``GET /metrics`` (infra, from Phase 0).
- ``GET /companies``, ``GET /companies/{id}``.
- ``GET /companies/{id}/series`` (latest monthly estimate series).
- ``GET /companies/{id}/months/{YYYY-MM}/evidence`` (structured trace).
- ``GET /runs``, ``GET /runs/{id}`` (run admin surface).
- ``GET /review-queue`` (priority-ordered).
- ``GET /overrides?company_id=...`` and ``POST /overrides``.
- ``GET /benchmarks/comparison`` (batch disagreement summary).
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, date, datetime
from typing import Annotated, Any

from fastapi import Depends, FastAPI, HTTPException, Query, Response
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select
from sqlalchemy.orm import Session

from headcount import __version__
from headcount.config import get_settings
from headcount.db.engine import get_sessionmaker
from headcount.db.enums import (
    CompanyRunStageStatus,
    OverrideField,
    ReviewStatus,
)
from headcount.models.audit_log import AuditLog
from headcount.models.company import Company
from headcount.models.estimate_version import EstimateVersion
from headcount.models.headcount_estimate_monthly import HeadcountEstimateMonthly
from headcount.models.manual_override import ManualOverride
from headcount.models.review_queue_item import ReviewQueueItem
from headcount.models.run import CompanyRunStatus, Run
from headcount.review.audit import record_audit
from headcount.serving.benchmark_comparison import (
    ComparisonSummary,
    compare_estimates_to_benchmarks,
)
from headcount.serving.evidence import (
    EvidenceNotFoundError,
    build_evidence,
    compute_growth_windows,
)
from headcount.utils.logging import configure_logging, get_logger
from headcount.utils.metrics import REGISTRY

API_VERSION = "api_v1"


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class CompanySummary(_StrictModel):
    id: str
    canonical_name: str
    canonical_domain: str | None = None
    priority_tier: str
    status: str


class MonthlyEstimateRow(_StrictModel):
    month: date
    value_point: float
    value_min: float
    value_max: float
    method: str
    confidence_band: str
    confidence_score: float | None = None
    needs_review: bool
    public_profile_count: int
    suppression_reason: str | None = None


class CompanySeriesResponse(_StrictModel):
    company: CompanySummary
    estimate_version_id: str | None
    months: list[MonthlyEstimateRow]


class GrowthWindowRow(_StrictModel):
    window: str = Field(pattern=r"^(6m|1y|2y)$")
    start_month: date
    end_month: date
    start_value: float | None = None
    end_value: float | None = None
    absolute_delta: float | None = None
    percent_delta: float | None = None
    confidence_band: str
    suppressed: bool = False
    suppression_reason: str | None = None


class CompanyGrowthResponse(_StrictModel):
    company: CompanySummary
    estimate_version_id: str | None
    windows: list[GrowthWindowRow]


class ReviewQueueRow(_StrictModel):
    id: str
    company_id: str
    canonical_name: str
    review_reason: str
    priority: int
    status: str
    detail: str | None = None
    assigned_to: str | None = None
    estimate_version_id: str | None = None
    updated_at: datetime | None = None


class RunRow(_StrictModel):
    id: str
    kind: str
    status: str
    started_at: datetime
    finished_at: datetime | None = None
    cutoff_month: date
    method_version: str
    priority_tier: str | None = None
    note: str | None = None


class RunStageCounts(_StrictModel):
    stage: str
    counts: dict[str, int]


class RunDetailResponse(_StrictModel):
    run: RunRow
    stages: list[RunStageCounts]


class OverrideRow(_StrictModel):
    id: str
    company_id: str
    field_name: str
    payload: dict[str, Any]
    reason: str | None = None
    entered_by: str | None = None
    expires_at: datetime | None = None
    created_at: datetime


class ReviewTransitionRequest(_StrictModel):
    status: str
    assigned_to: str | None = None
    note: str | None = None
    actor_id: str | None = None


class OverrideCreateRequest(_StrictModel):
    company_id: str
    field_name: OverrideField
    payload: dict[str, Any] = Field(default_factory=dict)
    reason: str | None = None
    entered_by: str | None = None
    expires_at: datetime | None = None


def _latest_version_per_company(session: Session, company_ids: list[str]) -> dict[str, str]:
    stmt = (
        select(EstimateVersion)
        .where(EstimateVersion.company_id.in_(company_ids))
        .order_by(EstimateVersion.company_id, EstimateVersion.created_at.desc())
    )
    out: dict[str, str] = {}
    for ev in session.execute(stmt).scalars():
        out.setdefault(ev.company_id, ev.id)
    return out


def _company_summary(c: Company) -> CompanySummary:
    return CompanySummary(
        id=c.id,
        canonical_name=c.canonical_name,
        canonical_domain=c.canonical_domain,
        priority_tier=c.priority_tier.value,
        status=c.status.value,
    )


def _review_row(item: ReviewQueueItem, company: Company) -> ReviewQueueRow:
    return ReviewQueueRow(
        id=item.id,
        company_id=company.id,
        canonical_name=company.canonical_name,
        review_reason=item.review_reason.value,
        priority=int(item.priority),
        status=item.status.value,
        detail=item.detail,
        assigned_to=item.assigned_to,
        estimate_version_id=item.estimate_version_id,
        updated_at=item.updated_at,
    )


def _override_row(o: ManualOverride) -> OverrideRow:
    return OverrideRow(
        id=o.id,
        company_id=o.company_id,
        field_name=o.field_name.value,
        payload=dict(o.override_value_json or {}),
        reason=o.reason,
        entered_by=o.entered_by,
        expires_at=o.expires_at,
        created_at=o.created_at,
    )


def create_app(session_factory: Any | None = None) -> FastAPI:
    """Application factory.

    ``session_factory`` is injectable for tests: pass a sessionmaker bound
    to an in-memory SQLite engine and the app will route DB access through
    it. Default wiring uses :func:`headcount.db.engine.get_sessionmaker`.
    """

    configure_logging()
    settings = get_settings()
    log = get_logger("headcount.api")
    factory = session_factory or get_sessionmaker()

    app = FastAPI(
        title="Headcount Estimator API",
        version=__version__,
        docs_url="/docs",
        redoc_url=None,
    )

    def get_session() -> Iterator[Session]:
        session = factory()
        try:
            yield session
        finally:
            session.close()

    @app.get("/healthz", tags=["infra"])
    def healthz() -> dict[str, Any]:
        return {
            "status": "ok",
            "version": __version__,
            "app_env": settings.app_env,
            "method_version": settings.method_version,
            "api_version": API_VERSION,
        }

    if settings.metrics_enabled:

        @app.get("/metrics", tags=["infra"])
        def metrics() -> Response:
            from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

            payload = generate_latest(REGISTRY)
            return Response(content=payload, media_type=CONTENT_TYPE_LATEST)

    @app.get("/companies", tags=["companies"], response_model=list[CompanySummary])
    def list_companies(
        session: Session = Depends(get_session),
        limit: Annotated[int, Query(ge=1, le=500)] = 100,
        offset: Annotated[int, Query(ge=0)] = 0,
        priority_tier: Annotated[str | None, Query()] = None,
    ) -> list[CompanySummary]:
        stmt = select(Company).order_by(Company.canonical_name)
        if priority_tier is not None:
            stmt = stmt.where(Company.priority_tier == priority_tier)
        stmt = stmt.offset(offset).limit(limit)
        return [_company_summary(c) for c in session.execute(stmt).scalars()]

    @app.get("/companies/{company_id}", tags=["companies"], response_model=CompanySummary)
    def get_company(company_id: str, session: Session = Depends(get_session)) -> CompanySummary:
        company = session.get(Company, company_id)
        if company is None:
            raise HTTPException(status_code=404, detail=f"company not found: {company_id}")
        return _company_summary(company)

    @app.get(
        "/companies/{company_id}/series",
        tags=["companies"],
        response_model=CompanySeriesResponse,
    )
    def get_company_series(
        company_id: str,
        session: Session = Depends(get_session),
        start_month: Annotated[str | None, Query(alias="start")] = None,
        end_month: Annotated[str | None, Query(alias="end")] = None,
    ) -> CompanySeriesResponse:
        company = session.get(Company, company_id)
        if company is None:
            raise HTTPException(status_code=404, detail=f"company not found: {company_id}")
        version_map = _latest_version_per_company(session, [company_id])
        version_id = version_map.get(company_id)
        if version_id is None:
            return CompanySeriesResponse(
                company=_company_summary(company),
                estimate_version_id=None,
                months=[],
            )
        try:
            start_parsed = _parse_month(start_month) if start_month else None
            end_parsed = _parse_month(end_month) if end_month else None
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        stmt = (
            select(HeadcountEstimateMonthly)
            .where(HeadcountEstimateMonthly.estimate_version_id == version_id)
            .order_by(HeadcountEstimateMonthly.month)
        )
        if start_parsed is not None:
            stmt = stmt.where(HeadcountEstimateMonthly.month >= start_parsed)
        if end_parsed is not None:
            stmt = stmt.where(HeadcountEstimateMonthly.month <= end_parsed)
        rows = [
            MonthlyEstimateRow(
                month=r.month,
                value_point=float(r.estimated_headcount),
                value_min=float(r.estimated_headcount_min),
                value_max=float(r.estimated_headcount_max),
                method=r.method.value,
                confidence_band=r.confidence_band.value,
                confidence_score=r.confidence_score,
                needs_review=bool(r.needs_review),
                public_profile_count=int(r.public_profile_count),
                suppression_reason=r.suppression_reason,
            )
            for r in session.execute(stmt).scalars()
        ]
        return CompanySeriesResponse(
            company=_company_summary(company),
            estimate_version_id=version_id,
            months=rows,
        )

    @app.get(
        "/companies/{company_id}/growth",
        tags=["companies"],
        response_model=CompanyGrowthResponse,
    )
    def get_company_growth(
        company_id: str,
        session: Session = Depends(get_session),
    ) -> CompanyGrowthResponse:
        """Product-contract ``6m / 1y / 2y`` growth windows.

        Anchored at the latest available month in the company's most
        recent :class:`EstimateVersion`. Each window carries the same
        suppression semantics as the monthly series so analysts see a
        row (with a clear reason) even when growth is undefined.
        """
        company = session.get(Company, company_id)
        if company is None:
            raise HTTPException(status_code=404, detail=f"company not found: {company_id}")
        version_map = _latest_version_per_company(session, [company_id])
        version_id = version_map.get(company_id)
        if version_id is None:
            return CompanyGrowthResponse(
                company=_company_summary(company),
                estimate_version_id=None,
                windows=[],
            )
        windows = [
            GrowthWindowRow(**w) for w in compute_growth_windows(session, version_id=version_id)
        ]
        return CompanyGrowthResponse(
            company=_company_summary(company),
            estimate_version_id=version_id,
            windows=windows,
        )

    @app.get(
        "/companies/{company_id}/months/{month}/evidence",
        tags=["companies"],
    )
    def get_company_evidence(
        company_id: str,
        month: str,
        session: Session = Depends(get_session),
        version_id: Annotated[str | None, Query()] = None,
    ) -> dict[str, Any]:
        try:
            parsed = _parse_month(month)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        try:
            return build_evidence(
                session, company_id=company_id, month=parsed, version_id=version_id
            )
        except EvidenceNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.get("/runs", tags=["runs"], response_model=list[RunRow])
    def list_runs(
        session: Session = Depends(get_session),
        limit: Annotated[int, Query(ge=1, le=200)] = 50,
        kind: Annotated[str | None, Query()] = None,
    ) -> list[RunRow]:
        stmt = select(Run).order_by(Run.started_at.desc()).limit(limit)
        if kind is not None:
            stmt = stmt.where(Run.kind == kind)
        return [
            RunRow(
                id=r.id,
                kind=r.kind.value,
                status=r.status.value,
                started_at=r.started_at,
                finished_at=r.finished_at,
                cutoff_month=r.cutoff_month,
                method_version=r.method_version,
                priority_tier=r.priority_tier.value if r.priority_tier else None,
                note=r.note,
            )
            for r in session.execute(stmt).scalars()
        ]

    @app.get("/runs/{run_id}", tags=["runs"], response_model=RunDetailResponse)
    def get_run(run_id: str, session: Session = Depends(get_session)) -> RunDetailResponse:
        run = session.get(Run, run_id)
        if run is None:
            raise HTTPException(status_code=404, detail=f"run not found: {run_id}")
        rows = (
            session.execute(select(CompanyRunStatus).where(CompanyRunStatus.run_id == run_id))
            .scalars()
            .all()
        )
        by_stage: dict[str, dict[str, int]] = {}
        for row in rows:
            stage_counts = by_stage.setdefault(row.stage.value, {})
            stage_counts[row.status.value] = stage_counts.get(row.status.value, 0) + 1
        stages = [
            RunStageCounts(stage=stage, counts=counts) for stage, counts in sorted(by_stage.items())
        ]
        run_row = RunRow(
            id=run.id,
            kind=run.kind.value,
            status=run.status.value,
            started_at=run.started_at,
            finished_at=run.finished_at,
            cutoff_month=run.cutoff_month,
            method_version=run.method_version,
            priority_tier=run.priority_tier.value if run.priority_tier else None,
            note=run.note,
        )
        return RunDetailResponse(run=run_row, stages=stages)

    @app.get("/review-queue", tags=["review"], response_model=list[ReviewQueueRow])
    def list_review_queue(
        session: Session = Depends(get_session),
        status: Annotated[str | None, Query()] = "open",
        limit: Annotated[int, Query(ge=1, le=500)] = 50,
    ) -> list[ReviewQueueRow]:
        stmt = (
            select(ReviewQueueItem, Company)
            .join(Company, Company.id == ReviewQueueItem.company_id)
            .order_by(ReviewQueueItem.priority.desc(), ReviewQueueItem.updated_at.desc())
            .limit(limit)
        )
        if status is not None:
            try:
                enum = ReviewStatus(status)
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=f"unknown status: {status}") from exc
            stmt = stmt.where(ReviewQueueItem.status == enum)
        out: list[ReviewQueueRow] = []
        for item, company in session.execute(stmt).all():
            out.append(_review_row(item, company))
        return out

    @app.post(
        "/review-queue/{item_id}/transition",
        tags=["review"],
        response_model=ReviewQueueRow,
    )
    def transition_review_item(
        item_id: str,
        body: ReviewTransitionRequest,
        session: Session = Depends(get_session),
    ) -> ReviewQueueRow:
        """Move a review queue row between states.

        The UI uses this for *claim* (``open -> assigned`` with
        ``assigned_to``) and *resolve/dismiss* (``* -> resolved`` or
        ``* -> dismissed``). Every transition writes an audit entry so we
        have a paper trail of who touched which item and why.
        """

        item = session.get(ReviewQueueItem, item_id)
        if item is None:
            raise HTTPException(status_code=404, detail=f"review item not found: {item_id}")
        try:
            target = ReviewStatus(body.status)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=f"unknown status: {body.status}") from exc
        previous = item.status
        item.status = target
        if target is ReviewStatus.assigned:
            if not body.assigned_to:
                raise HTTPException(
                    status_code=400,
                    detail="assigned_to is required when transitioning to 'assigned'",
                )
            item.assigned_to = body.assigned_to
        elif target in (ReviewStatus.resolved, ReviewStatus.dismissed):
            item.resolved_at = datetime.now(tz=UTC)
        elif target is ReviewStatus.open:
            item.assigned_to = None
            item.resolved_at = None
        record_audit(
            session,
            actor_type="api",
            actor_id=body.actor_id or body.assigned_to,
            action="review_transition",
            target_type="review_queue_item",
            target_id=item.id,
            payload={
                "company_id": item.company_id,
                "from": previous.value,
                "to": target.value,
                "assigned_to": item.assigned_to,
                "note": body.note,
            },
        )
        session.commit()
        session.refresh(item)
        company = session.get(Company, item.company_id)
        assert company is not None  # FK guarantees this
        log.info(
            "review_transition",
            review_id=item.id,
            company_id=item.company_id,
            **{"from": previous.value, "to": target.value},
        )
        return _review_row(item, company)

    @app.get("/overrides", tags=["overrides"], response_model=list[OverrideRow])
    def list_overrides(
        session: Session = Depends(get_session),
        company_id: Annotated[str | None, Query()] = None,
        active_only: Annotated[bool, Query()] = True,
    ) -> list[OverrideRow]:
        stmt = select(ManualOverride).order_by(ManualOverride.created_at.desc())
        if company_id is not None:
            stmt = stmt.where(ManualOverride.company_id == company_id)
        rows = list(session.execute(stmt).scalars().all())
        if active_only:
            now = datetime.now(tz=UTC)
            rows = [r for r in rows if r.expires_at is None or r.expires_at >= now]
        return [_override_row(r) for r in rows]

    @app.post(
        "/overrides",
        tags=["overrides"],
        response_model=OverrideRow,
        status_code=201,
    )
    def create_override(
        body: OverrideCreateRequest, session: Session = Depends(get_session)
    ) -> OverrideRow:
        company = session.get(Company, body.company_id)
        if company is None:
            raise HTTPException(status_code=404, detail=f"company not found: {body.company_id}")
        override = ManualOverride(
            company_id=body.company_id,
            field_name=body.field_name,
            override_value_json=body.payload,
            reason=body.reason,
            entered_by=body.entered_by,
            expires_at=body.expires_at,
        )
        session.add(override)
        session.flush()
        record_audit(
            session,
            actor_type="api",
            actor_id=body.entered_by,
            action="override_created",
            target_type="manual_override",
            target_id=override.id,
            payload={
                "company_id": body.company_id,
                "field": body.field_name.value,
                "reason": body.reason,
                "expires_at": body.expires_at.isoformat() if body.expires_at else None,
                "payload": body.payload,
            },
        )
        session.commit()
        session.refresh(override)
        log.info(
            "override_created_api",
            override_id=override.id,
            company_id=body.company_id,
            field=body.field_name.value,
        )
        return _override_row(override)

    @app.get("/benchmarks/comparison", tags=["benchmarks"])
    def get_benchmark_comparison(
        session: Session = Depends(get_session),
        company_id: Annotated[list[str] | None, Query()] = None,
        threshold: Annotated[float, Query(ge=0.0, le=1.0)] = 0.25,
    ) -> dict[str, Any]:
        summary: ComparisonSummary = compare_estimates_to_benchmarks(
            session, company_ids=company_id, threshold=threshold
        )
        return summary.to_dict()

    @app.get("/audit", tags=["audit"])
    def list_audit(
        session: Session = Depends(get_session),
        target_type: Annotated[str | None, Query()] = None,
        target_id: Annotated[str | None, Query()] = None,
        limit: Annotated[int, Query(ge=1, le=500)] = 100,
    ) -> list[dict[str, Any]]:
        stmt = select(AuditLog).order_by(AuditLog.created_at.desc()).limit(limit)
        if target_type is not None:
            stmt = stmt.where(AuditLog.target_type == target_type)
        if target_id is not None:
            stmt = stmt.where(AuditLog.target_id == target_id)
        return [
            {
                "id": a.id,
                "actor_type": a.actor_type,
                "actor_id": a.actor_id,
                "action": a.action,
                "target_type": a.target_type,
                "target_id": a.target_id,
                "payload": dict(a.payload_json or {}),
                "created_at": a.created_at.isoformat() if a.created_at else None,
            }
            for a in session.execute(stmt).scalars()
        ]

    @app.get("/status/summary", tags=["runs"])
    def status_summary(session: Session = Depends(get_session)) -> dict[str, Any]:
        """Lightweight roll-up for dashboard tiles.

        Returns companies_total, review_queue counts by status, and
        latest run status. Intentionally cheap so dashboards can poll.
        """

        companies_total = session.scalar(select(Company.id).limit(1))
        _ = companies_total  # scalar(...) returns first id or None.
        companies_count = len(session.execute(select(Company.id)).scalars().all())
        queue_counts: dict[str, int] = {}
        for item in session.execute(select(ReviewQueueItem.status)).scalars():
            queue_counts[item.value] = queue_counts.get(item.value, 0) + 1
        latest_run = session.execute(
            select(Run).order_by(Run.started_at.desc()).limit(1)
        ).scalar_one_or_none()
        latest_run_out: dict[str, Any] | None = None
        if latest_run is not None:
            stage_rows = (
                session.execute(
                    select(CompanyRunStatus).where(CompanyRunStatus.run_id == latest_run.id)
                )
                .scalars()
                .all()
            )
            stage_counts: dict[str, int] = {}
            for sr in stage_rows:
                key = sr.status.value
                stage_counts[key] = stage_counts.get(key, 0) + 1
            # Ensure every status appears even if zero so dashboards can
            # treat the keys as stable.
            for stat in CompanyRunStageStatus:
                stage_counts.setdefault(stat.value, 0)
            latest_run_out = {
                "id": latest_run.id,
                "status": latest_run.status.value,
                "started_at": latest_run.started_at.isoformat(),
                "finished_at": latest_run.finished_at.isoformat()
                if latest_run.finished_at
                else None,
                "stage_counts": stage_counts,
            }
        return {
            "api_version": API_VERSION,
            "companies_total": companies_count,
            "review_queue_by_status": queue_counts,
            "latest_run": latest_run_out,
        }

    def _eval_summary(row: Any) -> dict[str, Any]:
        """Promote the headline columns of an ``EvaluationRun`` row to
        a dict suitable for ``/eval/latest`` / ``/eval/history``.

        The full scoreboard JSON is *not* included; callers that need
        the per-provider detail / disagreement table should hit
        ``/eval/{id}``.
        """

        return {
            "id": row.id,
            "created_at": row.created_at.isoformat() if row.created_at else None,
            "as_of_month": row.as_of_month.isoformat(),
            "evaluation_version": row.evaluation_version,
            "primary_provider": row.primary_provider,
            "companies_in_scope": row.companies_in_scope,
            "companies_evaluated": row.companies_evaluated,
            "companies_with_benchmark": row.companies_with_benchmark,
            "harmonic_cohort_size": row.harmonic_cohort_size,
            "harmonic_cohort_evaluated": row.harmonic_cohort_evaluated,
            "coverage_in_scope": row.coverage_in_scope,
            "coverage_with_benchmark": row.coverage_with_benchmark,
            "mape_headcount_current": row.mape_headcount_current,
            "mae_growth_6m_pct": row.mae_growth_6m_pct,
            "mae_growth_1y_pct": row.mae_growth_1y_pct,
            "mae_growth_2y_pct": row.mae_growth_2y_pct,
            "spearman_growth_6m": row.spearman_growth_6m,
            "spearman_growth_1y": row.spearman_growth_1y,
            "mape_headcount_current_zeeshan": row.mape_headcount_current_zeeshan,
            "mape_headcount_current_linkedin": row.mape_headcount_current_linkedin,
            "review_queue_open": row.review_queue_open,
            "high_confidence_disagreements": row.high_confidence_disagreements,
            "supporting_disagreements": row.supporting_disagreements,
            "note": row.note,
        }

    @app.get("/eval/latest", tags=["eval"])
    def eval_latest(session: Session = Depends(get_session)) -> dict[str, Any]:
        """Return the most recent ``evaluation_run`` scoreboard.

        Harmonic-primary regression harness. Emits the full scoreboard
        JSON plus the promoted headline metrics so dashboards can
        render tiles without re-parsing.
        """

        from headcount.models.evaluation_run import EvaluationRun

        row = session.execute(
            select(EvaluationRun).order_by(EvaluationRun.created_at.desc()).limit(1)
        ).scalar_one_or_none()
        if row is None:
            raise HTTPException(status_code=404, detail="no evaluation runs yet")
        payload = _eval_summary(row)
        payload["scoreboard"] = row.scoreboard_json
        return payload

    @app.get("/eval/history", tags=["eval"])
    def eval_history(
        session: Session = Depends(get_session),
        limit: Annotated[int, Query(ge=1, le=200)] = 50,
    ) -> list[dict[str, Any]]:
        """Return up to ``limit`` recent ``evaluation_run`` rows (newest first).

        Only the promoted summary columns are returned; callers that
        need the full scoreboard should hit ``/eval/{id}``.
        """

        from headcount.models.evaluation_run import EvaluationRun

        rows = session.execute(
            select(EvaluationRun).order_by(EvaluationRun.created_at.desc()).limit(limit)
        ).scalars()
        return [_eval_summary(r) for r in rows]

    @app.get("/eval/{evaluation_id}", tags=["eval"])
    def eval_detail(evaluation_id: str, session: Session = Depends(get_session)) -> dict[str, Any]:
        from headcount.models.evaluation_run import EvaluationRun

        row = session.get(EvaluationRun, evaluation_id)
        if row is None:
            raise HTTPException(
                status_code=404, detail=f"evaluation_run not found: {evaluation_id}"
            )
        payload = _eval_summary(row)
        payload["scoreboard"] = row.scoreboard_json
        return payload

    log.info(
        "api_ready",
        version=__version__,
        metrics_enabled=settings.metrics_enabled,
        api_version=API_VERSION,
    )
    return app


def _parse_month(raw: str) -> date:
    s = raw.strip()
    if len(s) == 7:
        s = f"{s}-01"
    try:
        return date.fromisoformat(s).replace(day=1)
    except ValueError as exc:
        raise ValueError(f"expected YYYY-MM or YYYY-MM-DD, got {raw!r}") from exc


__all__ = ["API_VERSION", "create_app"]
