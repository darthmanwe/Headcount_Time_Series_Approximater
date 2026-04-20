"""Estimation pipeline.

Reads anchors, employment history, and canonical events for each target
company and writes one :class:`EstimateVersion` + N
:class:`HeadcountEstimateMonthly` + one :class:`AnchorReconciliation` per
segment. This is the one module in :mod:`headcount.estimate` that talks
to SQLAlchemy - everything else is pure functions.

Flow per company:

1. Load input: anchors (``CompanyAnchorObservation``), employment
   intervals (``PersonEmploymentObservation``), events
   (``CompanyEvent``).
2. Partition the window into segments (hard break on acquisitions /
   mergers / spinouts / layoffs / parent_sub_reassignment; rebrand /
   stealth_to_public are NOT breaks - they don't change the workforce).
3. Compute monthly profile counts once across the full window.
4. For each segment: reconcile anchors -> ``ReconciledAnchor``, call
   :func:`reconcile_series`, run anomaly detection, merge into the
   company's output list.
5. Persist: one ``EstimateVersion`` per company, one
   ``AnchorReconciliation`` per reconciled segment that actually had
   anchors, one ``HeadcountEstimateMonthly`` per month.
6. Drive ``CompanyRunStatus`` through ``running -> succeeded`` /
   ``failed`` / ``gated`` so the orchestration surface stays consistent
   with ``collect-anchors``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, date, datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from headcount.config import get_settings
from headcount.db.enums import (
    CompanyRunStage,
    CompanyRunStageStatus,
    RunKind,
    RunStatus,
)
from headcount.estimate.anchors import (
    ANCHOR_POLICY_VERSION,
    AnchorCandidate,
    reconcile_segment_anchors,
)
from headcount.estimate.anomalies import AnomalyFlags, detect_anomalies
from headcount.estimate.coverage import (
    COVERAGE_CURVE_VERSION,
    CoverageCurve,
    build_default_coverage_curve,
)
from headcount.estimate.employment import (
    EmploymentInterval,
    monthly_public_profile_counts,
)
from headcount.estimate.reconcile import (
    DEFAULT_SAMPLE_FLOOR,
    METHOD_VERSION,
    MonthlyEstimate,
    reconcile_series,
)
from headcount.estimate.segments import Segment, split_into_segments
from headcount.models.anchor_reconciliation import AnchorReconciliation
from headcount.models.company import Company
from headcount.models.company_anchor_observation import CompanyAnchorObservation
from headcount.models.company_event import CompanyEvent
from headcount.models.estimate_version import EstimateVersion
from headcount.models.headcount_estimate_monthly import HeadcountEstimateMonthly
from headcount.models.person_employment_observation import PersonEmploymentObservation
from headcount.models.run import CompanyRunStatus, Run
from headcount.utils.logging import get_logger

log = get_logger("headcount.estimate.pipeline")


@dataclass(slots=True)
class CompanyEstimateReport:
    company_id: str
    segments: int = 0
    months_written: int = 0
    months_flagged: int = 0
    anchor_reconciliations_written: int = 0
    stage_status: CompanyRunStageStatus = CompanyRunStageStatus.pending
    error: str | None = None
    estimate_version_id: str | None = None


@dataclass(slots=True)
class EstimateResult:
    run_id: str
    companies_attempted: int = 0
    companies_succeeded: int = 0
    companies_failed: int = 0
    companies_degraded: int = 0
    months_written: int = 0
    months_flagged: int = 0
    reports: list[CompanyEstimateReport] = field(default_factory=list)

    @property
    def final_status(self) -> RunStatus:
        if self.companies_attempted == 0:
            return RunStatus.succeeded
        if self.companies_failed == self.companies_attempted:
            return RunStatus.failed
        if self.companies_failed > 0 or self.companies_degraded > 0:
            return RunStatus.partial
        return RunStatus.succeeded


def _month_floor(d: date) -> date:
    return d.replace(day=1)


def _load_anchors(session: Session, company_id: str) -> list[AnchorCandidate]:
    rows = (
        session.execute(
            select(CompanyAnchorObservation)
            .where(CompanyAnchorObservation.company_id == company_id)
            .order_by(CompanyAnchorObservation.anchor_month)
        )
        .scalars()
        .all()
    )
    return [
        AnchorCandidate(
            anchor_month=_month_floor(r.anchor_month),
            value_min=float(r.headcount_value_min),
            value_point=float(r.headcount_value_point),
            value_max=float(r.headcount_value_max),
            kind=r.headcount_value_kind,
            anchor_type=r.anchor_type,
            confidence=float(r.confidence or 0.0),
            observation_id=r.id,
        )
        for r in rows
    ]


def _load_employment(session: Session, company_id: str) -> list[EmploymentInterval]:
    rows = (
        session.execute(
            select(PersonEmploymentObservation).where(
                PersonEmploymentObservation.company_id == company_id
            )
        )
        .scalars()
        .all()
    )
    return [
        EmploymentInterval(
            person_id=r.person_id,
            start_month=_month_floor(r.start_month),
            end_month=_month_floor(r.end_month) if r.end_month else None,
            is_current_role=bool(r.is_current_role),
            confidence=float(r.confidence or 0.0),
        )
        for r in rows
    ]


def _load_events(session: Session, company_id: str) -> list[CompanyEvent]:
    return list(
        session.execute(select(CompanyEvent).where(CompanyEvent.company_id == company_id))
        .scalars()
        .all()
    )


def _segment_anchors(segment: Segment, anchors: list[AnchorCandidate]) -> list[AnchorCandidate]:
    return [a for a in anchors if segment.start_month <= a.anchor_month <= segment.end_month]


def _create_run(
    session: Session,
    *,
    cutoff_month: date,
    note: str | None,
) -> Run:
    settings = get_settings()
    now = datetime.now(tz=UTC)
    run = Run(
        kind=RunKind.full,
        status=RunStatus.running,
        started_at=now,
        cutoff_month=cutoff_month,
        method_version=METHOD_VERSION,
        anchor_policy_version=ANCHOR_POLICY_VERSION,
        coverage_curve_version=COVERAGE_CURVE_VERSION,
        config_hash="estimate-series",
        note=note or "estimate_series",
    )
    # The legacy collect-anchors run also writes its own versions from
    # settings. We prefer the module-level constants here so replay is
    # stable even if settings drift.
    _ = settings
    session.add(run)
    session.flush()
    return run


def _ensure_stage_row(session: Session, *, run_id: str, company_id: str) -> CompanyRunStatus:
    stmt = select(CompanyRunStatus).where(
        CompanyRunStatus.run_id == run_id,
        CompanyRunStatus.company_id == company_id,
        CompanyRunStatus.stage == CompanyRunStage.estimate_series,
    )
    row = session.execute(stmt).scalar_one_or_none()
    if row is None:
        row = CompanyRunStatus(
            run_id=run_id,
            company_id=company_id,
            stage=CompanyRunStage.estimate_series,
            status=CompanyRunStageStatus.running,
            attempts=1,
        )
        session.add(row)
        session.flush()
    else:
        row.status = CompanyRunStageStatus.running
        row.attempts += 1
    return row


def _persist_company_estimates(
    session: Session,
    *,
    company_id: str,
    run_id: str,
    cutoff_month: date,
    segments: list[Segment],
    per_segment_anchor: dict[Segment, object],
    per_segment_rows: dict[Segment, list[MonthlyEstimate]],
    per_segment_flags: dict[Segment, list[AnomalyFlags]],
) -> CompanyEstimateReport:
    report = CompanyEstimateReport(company_id=company_id)
    report.segments = len(segments)

    version = EstimateVersion(
        company_id=company_id,
        estimation_run_id=run_id,
        method_version=METHOD_VERSION,
        anchor_policy_version=ANCHOR_POLICY_VERSION,
        coverage_curve_version=COVERAGE_CURVE_VERSION,
        source_snapshot_cutoff=cutoff_month,
    )
    session.add(version)
    session.flush()
    report.estimate_version_id = version.id

    for seg in segments:
        anchor = per_segment_anchor.get(seg)
        rows = per_segment_rows.get(seg, [])
        flags = per_segment_flags.get(seg, [])
        flags_by_month = {f.month: f for f in flags}

        if anchor is not None:
            session.add(
                AnchorReconciliation(
                    estimate_version_id=version.id,
                    chosen_point=float(anchor.value_point),  # type: ignore[attr-defined]
                    chosen_min=float(anchor.value_min),  # type: ignore[attr-defined]
                    chosen_max=float(anchor.value_max),  # type: ignore[attr-defined]
                    inputs_json=[
                        {"observation_id": oid}
                        for oid in anchor.contributing_ids  # type: ignore[attr-defined]
                    ],
                    weights_json=dict(anchor.weights),  # type: ignore[attr-defined]
                    rationale=anchor.rationale,  # type: ignore[attr-defined]
                )
            )
            report.anchor_reconciliations_written += 1

        for est in rows:
            flags_for_month = flags_by_month.get(est.month)
            needs_review = est.needs_review or (
                flags_for_month.needs_review if flags_for_month else False
            )
            suppression = est.suppression_reason
            if flags_for_month and flags_for_month.reasons and suppression is None:
                suppression = "; ".join(flags_for_month.reasons)[:256]

            session.add(
                HeadcountEstimateMonthly(
                    company_id=company_id,
                    estimate_version_id=version.id,
                    month=est.month,
                    estimated_headcount=float(est.value_point),
                    estimated_headcount_min=float(est.value_min),
                    estimated_headcount_max=float(est.value_max),
                    public_profile_count=int(est.public_profile_count),
                    scaled_from_anchor_value=float(est.scaled_from_anchor_value),
                    method=est.method,
                    confidence_band=est.confidence_band,
                    needs_review=needs_review,
                    suppression_reason=suppression,
                )
            )
            report.months_written += 1
            if needs_review:
                report.months_flagged += 1

    return report


def estimate_company(
    session: Session,
    *,
    company: Company,
    run_id: str,
    start_month: date,
    end_month: date,
    as_of_month: date,
    coverage: CoverageCurve,
    sample_floor: int = DEFAULT_SAMPLE_FLOOR,
) -> CompanyEstimateReport:
    """Run estimation for a single company; returns a per-company report.

    The caller owns the session (no commit here). On error we mark the
    ``company_run_status`` row failed and return a populated report with
    the error message; we do **not** re-raise so batch runs continue.
    """

    stage = _ensure_stage_row(session, run_id=run_id, company_id=company.id)

    try:
        anchors = _load_anchors(session, company.id)
        employment = _load_employment(session, company.id)
        events = _load_events(session, company.id)

        segments = split_into_segments(
            events,
            start_month=_month_floor(start_month),
            end_month=_month_floor(end_month),
        )

        monthly_profiles = monthly_public_profile_counts(
            employment,
            start_month=_month_floor(start_month),
            end_month=_month_floor(end_month),
            as_of_month=_month_floor(as_of_month),
        )

        per_segment_anchor: dict[Segment, object] = {}
        per_segment_rows: dict[Segment, list[MonthlyEstimate]] = {}
        per_segment_flags: dict[Segment, list[AnomalyFlags]] = {}

        for seg in segments:
            seg_anchors = _segment_anchors(seg, anchors)
            reconciled = reconcile_segment_anchors(
                seg_anchors,
                segment_start=seg.start_month,
                segment_end=seg.end_month,
            )
            rows = reconcile_series(
                seg,
                anchor=reconciled,
                monthly_profiles=monthly_profiles,
                coverage=coverage,
                as_of_month=_month_floor(as_of_month),
                sample_floor=sample_floor,
            )
            break_months = {s.start_month for s in segments if s is not segments[0]}
            flags = detect_anomalies(rows, segment_break_months=break_months)

            if reconciled is not None:
                per_segment_anchor[seg] = reconciled
            per_segment_rows[seg] = rows
            per_segment_flags[seg] = flags

        report = _persist_company_estimates(
            session,
            company_id=company.id,
            run_id=run_id,
            cutoff_month=_month_floor(as_of_month),
            segments=segments,
            per_segment_anchor=per_segment_anchor,
            per_segment_rows=per_segment_rows,
            per_segment_flags=per_segment_flags,
        )

        any_degraded = any(e.needs_review for rows in per_segment_rows.values() for e in rows)
        if report.months_written == 0:
            stage.status = CompanyRunStageStatus.failed
            stage.last_error = "no_months_written"
            report.stage_status = CompanyRunStageStatus.failed
        elif any_degraded:
            stage.status = CompanyRunStageStatus.succeeded
            report.stage_status = CompanyRunStageStatus.succeeded
        else:
            stage.status = CompanyRunStageStatus.succeeded
            report.stage_status = CompanyRunStageStatus.succeeded
        return report
    except Exception as exc:
        log.exception("estimate_company_failed", company_id=company.id, error=str(exc))
        stage.status = CompanyRunStageStatus.failed
        stage.last_error = str(exc)[:2048]
        return CompanyEstimateReport(
            company_id=company.id,
            stage_status=CompanyRunStageStatus.failed,
            error=str(exc),
        )


def estimate_series(
    session: Session,
    *,
    start_month: date,
    end_month: date | None = None,
    as_of_month: date | None = None,
    company_ids: list[str] | None = None,
    coverage: CoverageCurve | None = None,
    sample_floor: int = DEFAULT_SAMPLE_FLOOR,
    note: str | None = None,
) -> EstimateResult:
    """Run estimation over a batch of companies.

    Parameters
    ----------
    start_month, end_month:
        Output window. ``end_month`` defaults to ``as_of_month``.
    as_of_month:
        The "now" month used for coverage correction and open-ended
        employment intervals. Defaults to the current month floor.
    company_ids:
        Restrict the batch. ``None`` means "every company".
    """

    resolved_as_of = _month_floor(as_of_month or date.today())
    resolved_end = _month_floor(end_month or resolved_as_of)
    resolved_start = _month_floor(start_month)
    coverage_curve = coverage or build_default_coverage_curve()

    run = _create_run(session, cutoff_month=resolved_as_of, note=note)
    result = EstimateResult(run_id=run.id)

    stmt = select(Company)
    if company_ids:
        stmt = stmt.where(Company.id.in_(company_ids))
    stmt = stmt.order_by(Company.canonical_name)
    companies = list(session.execute(stmt).scalars().all())

    for company in companies:
        result.companies_attempted += 1
        report = estimate_company(
            session,
            company=company,
            run_id=run.id,
            start_month=resolved_start,
            end_month=resolved_end,
            as_of_month=resolved_as_of,
            coverage=coverage_curve,
            sample_floor=sample_floor,
        )
        result.reports.append(report)
        result.months_written += report.months_written
        result.months_flagged += report.months_flagged
        if report.stage_status == CompanyRunStageStatus.failed:
            result.companies_failed += 1
        elif report.months_flagged > 0:
            result.companies_degraded += 1
            result.companies_succeeded += 1
        else:
            result.companies_succeeded += 1

    run.status = result.final_status
    run.finished_at = datetime.now(tz=UTC)
    session.flush()

    return result


__all__ = [
    "CompanyEstimateReport",
    "EstimateResult",
    "estimate_company",
    "estimate_series",
]
