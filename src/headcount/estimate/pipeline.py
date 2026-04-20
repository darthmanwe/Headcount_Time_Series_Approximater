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
    ConfidenceBand,
    EstimateMethod,
    ReviewReason,
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
    has_employment_signal,
    interpolate_series_from_anchors,
    reconcile_series,
)
from headcount.estimate.segments import Segment, split_into_segments
from headcount.models.anchor_reconciliation import AnchorReconciliation
from headcount.models.company import Company
from headcount.models.company_anchor_observation import CompanyAnchorObservation
from headcount.models.company_event import CompanyEvent
from headcount.models.confidence_component_score import ConfidenceComponentScore
from headcount.models.estimate_version import EstimateVersion
from headcount.models.headcount_estimate_monthly import HeadcountEstimateMonthly
from headcount.models.person_employment_observation import PersonEmploymentObservation
from headcount.models.run import CompanyRunStatus, Run
from headcount.models.source_observation import SourceObservation
from headcount.review.audit import record_audit
from headcount.review.overrides import ActiveOverrides, load_active_overrides
from headcount.review.queue import QueueCandidate, upsert_review_items
from headcount.review.scoring import (
    SCORING_VERSION,
    ConfidenceBreakdown,
    ConfidenceInputs,
    score_confidence,
)
from headcount.utils.logging import get_logger

log = get_logger("headcount.estimate.pipeline")


@dataclass(slots=True)
class CompanyEstimateReport:
    company_id: str
    segments: int = 0
    months_written: int = 0
    months_flagged: int = 0
    anchor_reconciliations_written: int = 0
    review_items_inserted: int = 0
    review_items_refreshed: int = 0
    overrides_applied: int = 0
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
    review_items_inserted: int = 0
    review_items_refreshed: int = 0
    overrides_applied: int = 0
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
    """Load anchors for a company, tagged with originating source name.

    The ``source_name`` travels with the candidate so the scorer can
    count distinct source classes for
    :func:`~headcount.review.scoring.score_confidence`'s
    multi-source corroboration component.
    """

    rows = session.execute(
        select(CompanyAnchorObservation, SourceObservation.source_name)
        .join(
            SourceObservation,
            SourceObservation.id == CompanyAnchorObservation.source_observation_id,
            isouter=True,
        )
        .where(CompanyAnchorObservation.company_id == company_id)
        .order_by(CompanyAnchorObservation.anchor_month)
    ).all()
    return [
        AnchorCandidate(
            anchor_month=_month_floor(r.anchor_month),
            value_min=float(r.headcount_value_min),
            value_point=float(r.headcount_value_point),
            value_max=float(r.headcount_value_max),
            kind=r.headcount_value_kind,
            anchor_type=r.anchor_type,
            confidence=float(r.confidence or 0.0),
            source_name=(source_name.value if source_name is not None else "manual"),
            observation_id=r.id,
        )
        for (r, source_name) in rows
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


def _apply_suppress_windows(
    rows: list[MonthlyEstimate],
    overrides: ActiveOverrides,
) -> list[MonthlyEstimate]:
    """Force-suppress any month falling inside an analyst suppress window."""

    if not overrides.suppress_windows:
        return rows
    out: list[MonthlyEstimate] = []
    for r in rows:
        window = overrides.is_suppressed(r.month)
        if window is None:
            out.append(r)
            continue
        out.append(
            MonthlyEstimate(
                month=r.month,
                value_min=r.value_min,
                value_point=r.value_point,
                value_max=r.value_max,
                public_profile_count=r.public_profile_count,
                scaled_from_anchor_value=r.scaled_from_anchor_value,
                method=EstimateMethod.suppressed_low_sample,
                confidence_band=ConfidenceBand.manual_review_required,
                needs_review=True,
                suppression_reason=f"manual_suppress({window.reason})"[:256],
                coverage_factor=r.coverage_factor,
                ratio=r.ratio,
                anchor_month=r.anchor_month,
                contributing_anchor_ids=r.contributing_anchor_ids,
            )
        )
    return out


def _distinct_source_classes(anchors: list[AnchorCandidate]) -> int:
    return len({a.source_name for a in anchors if a.source_name})


def _segment_break_months(segments: list[Segment]) -> tuple[date, ...]:
    return tuple(s.start_month for s in segments if s is not segments[0])


def _score_segment(
    *,
    segment: Segment,
    segment_anchors: list[AnchorCandidate],
    rows: list[MonthlyEstimate],
    break_months: tuple[date, ...],
    as_of_month: date,
    coverage: CoverageCurve,
    sample_floor: int,
) -> dict[date, ConfidenceBreakdown]:
    anchors_tuple = tuple(segment_anchors)
    distinct = _distinct_source_classes(segment_anchors)
    out: dict[date, ConfidenceBreakdown] = {}
    for r in rows:
        inputs = ConfidenceInputs(
            estimate=r,
            segment_anchors=anchors_tuple,
            segment_break_months=break_months,
            distinct_source_classes=distinct,
            as_of_month=as_of_month,
            coverage=coverage,
            sample_floor=sample_floor,
        )
        out[r.month] = score_confidence(inputs)
    # Segments with no anchors still need rows in the return dict so
    # the persist step writes a confidence_score of 0 and forces
    # manual_review_required. The scorer already produces exactly that
    # shape when segment_anchors is empty; no special handling here.
    _ = segment  # kept for future per-segment instrumentation.
    return out


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
    per_segment_confidence: dict[Segment, dict[date, ConfidenceBreakdown]],
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

    # Aggregate per-component averages across all months for the
    # version-level ConfidenceComponentScore rows.
    component_totals: dict[str, list[float]] = {}

    for seg in segments:
        anchor = per_segment_anchor.get(seg)
        rows = per_segment_rows.get(seg, [])
        flags = per_segment_flags.get(seg, [])
        confidences = per_segment_confidence.get(seg, {})
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

            breakdown = confidences.get(est.month)
            if breakdown is not None:
                band = breakdown.band
                score_value: float | None = breakdown.score
                components_json: dict[str, object] | None = breakdown.as_json()
                for k, v in breakdown.components.items():
                    component_totals.setdefault(k, []).append(v)
                if band is ConfidenceBand.manual_review_required:
                    needs_review = True
            else:
                band = est.confidence_band
                score_value = None
                components_json = None

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
                    confidence_band=band,
                    needs_review=needs_review,
                    suppression_reason=suppression,
                    confidence_score=score_value,
                    confidence_components_json=components_json,
                )
            )
            report.months_written += 1
            if needs_review:
                report.months_flagged += 1

    # Persist the version-level component averages. This is what
    # dashboards use to compare whole-company confidence across runs
    # without scanning every month.
    for name, values in component_totals.items():
        if not values:
            continue
        session.add(
            ConfidenceComponentScore(
                estimate_version_id=version.id,
                component_name=name,
                component_score=sum(values) / len(values),
                note=f"n_months={len(values)} scoring_version={SCORING_VERSION}",
            )
        )

    return report


def _build_queue_candidates(
    *,
    company_id: str,
    estimate_version_id: str,
    segments: list[Segment],
    per_segment_rows: dict[Segment, list[MonthlyEstimate]],
    per_segment_flags: dict[Segment, list[AnomalyFlags]],
    per_segment_confidence: dict[Segment, dict[date, ConfidenceBreakdown]],
    overrides: ActiveOverrides,
) -> list[QueueCandidate]:
    """Derive queue rows from pipeline output.

    We emit *at most one* queue row per (company, version, reason)
    because the queue dedupes on that key.
    """

    low_confidence_months: list[tuple[date, float]] = []
    anomaly_months: list[tuple[date, tuple[str, ...]]] = []
    suppress_hits: list[tuple[date, str]] = []

    for seg in segments:
        rows = per_segment_rows.get(seg, [])
        flags = per_segment_flags.get(seg, [])
        confidences = per_segment_confidence.get(seg, {})
        flags_by_month = {f.month: f for f in flags}
        for r in rows:
            fl = flags_by_month.get(r.month)
            if fl is not None and fl.needs_review and fl.reasons:
                anomaly_months.append((r.month, fl.reasons))
            breakdown = confidences.get(r.month)
            if breakdown is not None and breakdown.band in (
                ConfidenceBand.low,
                ConfidenceBand.manual_review_required,
            ):
                low_confidence_months.append((r.month, breakdown.score))
            hit = overrides.is_suppressed(r.month)
            if hit is not None:
                suppress_hits.append((r.month, hit.reason))

    candidates: list[QueueCandidate] = []

    if low_confidence_months:
        worst = min(score for _, score in low_confidence_months)
        detail_months = ",".join(m.isoformat() for m, _ in sorted(low_confidence_months)[:6])
        candidates.append(
            QueueCandidate(
                company_id=company_id,
                estimate_version_id=estimate_version_id,
                review_reason=ReviewReason.low_confidence,
                detail=(
                    f"{len(low_confidence_months)} months below medium "
                    f"(min_score={worst:.2f}); months={detail_months}"
                ),
                severity=1.0 - worst,
                confidence_score=worst,
            )
        )

    if anomaly_months:
        reason_counts: dict[str, int] = {}
        for _, reasons in anomaly_months:
            for rsn in reasons:
                label = rsn.split("=")[0] if "=" in rsn else rsn
                reason_counts[label] = reason_counts.get(label, 0) + 1
        breakdown_text = ", ".join(
            f"{k}:{v}" for k, v in sorted(reason_counts.items(), key=lambda kv: -kv[1])
        )
        candidates.append(
            QueueCandidate(
                company_id=company_id,
                estimate_version_id=estimate_version_id,
                review_reason=ReviewReason.anomaly,
                detail=f"{len(anomaly_months)} flagged months; {breakdown_text}",
                severity=min(1.0, len(anomaly_months) / max(1, len(segments) * 6)),
            )
        )

    if suppress_hits:
        detail = "; ".join(f"{m.isoformat()}:{reason}" for m, reason in suppress_hits[:6])
        candidates.append(
            QueueCandidate(
                company_id=company_id,
                estimate_version_id=estimate_version_id,
                review_reason=ReviewReason.manual,
                detail=f"{len(suppress_hits)} suppressed months via override; {detail}",
                severity=0.3,
            )
        )

    return candidates


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
        overrides = load_active_overrides(session, company.id)
        anchors = overrides.merged_into_anchors(_load_anchors(session, company.id))
        employment = _load_employment(session, company.id)
        events = overrides.merged_into_events(_load_events(session, company.id))

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
        per_segment_confidence: dict[Segment, dict[date, ConfidenceBreakdown]] = {}
        per_segment_anchor_inputs: dict[Segment, list[AnchorCandidate]] = {}
        break_months_tuple = _segment_break_months(segments)

        for seg in segments:
            seg_anchors = _segment_anchors(seg, anchors)
            per_segment_anchor_inputs[seg] = seg_anchors
            reconciled = reconcile_segment_anchors(
                seg_anchors,
                segment_start=seg.start_month,
                segment_end=seg.end_month,
            )
            interp_rows: list[MonthlyEstimate] | None = None
            seg_months = seg.months()
            if not has_employment_signal(
                seg_months, monthly_profiles, sample_floor=sample_floor
            ):
                interp_rows = interpolate_series_from_anchors(
                    seg, segment_anchors=seg_anchors
                )
            if interp_rows is not None:
                rows = interp_rows
            else:
                rows = reconcile_series(
                    seg,
                    anchor=reconciled,
                    monthly_profiles=monthly_profiles,
                    coverage=coverage,
                    as_of_month=_month_floor(as_of_month),
                    sample_floor=sample_floor,
                )
            rows = _apply_suppress_windows(rows, overrides)
            flags = detect_anomalies(rows, segment_break_months=set(break_months_tuple))
            confidences = _score_segment(
                segment=seg,
                segment_anchors=seg_anchors,
                rows=rows,
                break_months=break_months_tuple,
                as_of_month=_month_floor(as_of_month),
                coverage=coverage,
                sample_floor=sample_floor,
            )

            if reconciled is not None:
                per_segment_anchor[seg] = reconciled
            per_segment_rows[seg] = rows
            per_segment_flags[seg] = flags
            per_segment_confidence[seg] = confidences

        report = _persist_company_estimates(
            session,
            company_id=company.id,
            run_id=run_id,
            cutoff_month=_month_floor(as_of_month),
            segments=segments,
            per_segment_anchor=per_segment_anchor,
            per_segment_rows=per_segment_rows,
            per_segment_flags=per_segment_flags,
            per_segment_confidence=per_segment_confidence,
        )

        if report.estimate_version_id is not None:
            queue_candidates = _build_queue_candidates(
                company_id=company.id,
                estimate_version_id=report.estimate_version_id,
                segments=segments,
                per_segment_rows=per_segment_rows,
                per_segment_flags=per_segment_flags,
                per_segment_confidence=per_segment_confidence,
                overrides=overrides,
            )
            queue_stats = upsert_review_items(session, queue_candidates)
            report.review_items_inserted = queue_stats["inserted"]
            report.review_items_refreshed = queue_stats["refreshed"]

        if overrides.override_ids:
            report.overrides_applied = len(overrides.override_ids)
            record_audit(
                session,
                actor_type="pipeline",
                action="overrides_applied",
                target_type="estimate_version",
                target_id=report.estimate_version_id,
                payload={
                    "company_id": company.id,
                    "override_ids": list(overrides.override_ids),
                    "n_pins": len(overrides.anchor_pins),
                    "n_windows": len(overrides.suppress_windows),
                    "n_synthetic_events": len(overrides.synthetic_events),
                },
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
        result.review_items_inserted += report.review_items_inserted
        result.review_items_refreshed += report.review_items_refreshed
        result.overrides_applied += report.overrides_applied
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
