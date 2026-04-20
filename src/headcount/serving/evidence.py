"""Structured "why is this number what it is" trace.

The evidence endpoint is the single source of truth for analyst review:
given ``(company_id, month)`` it returns the whole chain of reasoning
the pipeline used, mirroring the mental model of
:mod:`headcount.estimate.pipeline`.

Shape (pure dicts so both the API and the Streamlit UI can consume it):

.. code-block:: python

    {
      "company": {...},
      "month": "YYYY-MM",
      "estimate": {...},              # the HeadcountEstimateMonthly row
      "segment": {...},               # resolved segment + opening events
      "reconciled_anchor": {...},     # AnchorReconciliation for the segment
      "inputs": {
        "anchors": [...],             # CompanyAnchorObservation contributing
        "events": [...],              # CompanyEvent in window
        "employment_snapshot": {...}, # profile count for this month
      },
      "confidence": {...},            # components + band + score
      "overrides_applied": [...],     # ManualOverride rows active at run
      "audit": [...],                 # AuditLog entries for this version
    }

Every field is a plain JSON-safe dict so it round-trips through FastAPI
and the Streamlit UI without additional serialization gymnastics.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any

from sqlalchemy import and_, select
from sqlalchemy.orm import Session

from headcount.db.enums import EstimateMethod
from headcount.models.anchor_reconciliation import AnchorReconciliation
from headcount.models.audit_log import AuditLog
from headcount.models.company import Company
from headcount.models.company_anchor_observation import CompanyAnchorObservation
from headcount.models.company_event import CompanyEvent
from headcount.models.estimate_version import EstimateVersion
from headcount.models.headcount_estimate_monthly import HeadcountEstimateMonthly
from headcount.models.manual_override import ManualOverride
from headcount.models.source_observation import SourceObservation

EVIDENCE_VERSION = "evidence_v2"
"""Bumped when the tree shape changes in a way consumers must adapt to."""


class EvidenceNotFoundError(LookupError):
    """Raised when the caller asks for a company/month that has no estimate."""


def _iso(value: datetime | date | None) -> str | None:
    if value is None:
        return None
    return value.isoformat()


def _month_floor(d: date) -> date:
    return d.replace(day=1)


def _latest_version_for_company(session: Session, company_id: str) -> EstimateVersion | None:
    stmt = (
        select(EstimateVersion)
        .where(EstimateVersion.company_id == company_id)
        .order_by(EstimateVersion.created_at.desc())
        .limit(1)
    )
    return session.execute(stmt).scalar_one_or_none()


def _load_estimate_row(
    session: Session,
    *,
    version_id: str,
    month: date,
) -> HeadcountEstimateMonthly | None:
    stmt = select(HeadcountEstimateMonthly).where(
        and_(
            HeadcountEstimateMonthly.estimate_version_id == version_id,
            HeadcountEstimateMonthly.month == month,
        )
    )
    return session.execute(stmt).scalar_one_or_none()


def _load_segment_bounds(
    session: Session,
    *,
    company_id: str,
    month: date,
) -> tuple[date | None, date | None, list[CompanyEvent]]:
    """Return ``(segment_start, segment_end, opening_events)`` for ``month``.

    We recompute boundaries from ``CompanyEvent`` rather than pulling from
    a persisted segment table because the pipeline does not store
    segments as rows - they're a pure function of the event table. Keeps
    the evidence tree honest: analysts see the same segmentation the
    pipeline used.
    """

    from headcount.estimate.segments import is_break_event

    events = (
        session.execute(
            select(CompanyEvent)
            .where(CompanyEvent.company_id == company_id)
            .order_by(CompanyEvent.event_month)
        )
        .scalars()
        .all()
    )
    break_events = [e for e in events if is_break_event(e.event_type)]
    start: date | None = None
    end: date | None = None
    openers: list[CompanyEvent] = []
    for ev in break_events:
        em = _month_floor(ev.event_month)
        if em <= month:
            if start is None or em > start:
                start = em
                openers = [ev]
            elif em == start:
                openers.append(ev)
        elif em > month and (end is None or em < end):
            end = em
    return start, end, openers


def _load_anchors_in_window(
    session: Session,
    *,
    company_id: str,
    start: date | None,
    end: date | None,
) -> list[tuple[CompanyAnchorObservation, SourceObservation | None]]:
    stmt = (
        select(CompanyAnchorObservation, SourceObservation)
        .join(
            SourceObservation,
            SourceObservation.id == CompanyAnchorObservation.source_observation_id,
            isouter=True,
        )
        .where(CompanyAnchorObservation.company_id == company_id)
        .order_by(CompanyAnchorObservation.anchor_month)
    )
    if start is not None:
        stmt = stmt.where(CompanyAnchorObservation.anchor_month >= start)
    if end is not None:
        stmt = stmt.where(CompanyAnchorObservation.anchor_month < end)
    rows = session.execute(stmt).all()
    return [(r[0], r[1]) for r in rows]


def _load_events_in_window(
    session: Session,
    *,
    company_id: str,
    start: date | None,
    end: date | None,
) -> list[CompanyEvent]:
    stmt = select(CompanyEvent).where(CompanyEvent.company_id == company_id)
    if start is not None:
        stmt = stmt.where(CompanyEvent.event_month >= start)
    if end is not None:
        stmt = stmt.where(CompanyEvent.event_month < end)
    return list(session.execute(stmt.order_by(CompanyEvent.event_month)).scalars().all())


def _load_reconciliation(
    session: Session, *, version_id: str
) -> list[AnchorReconciliation]:
    return list(
        session.execute(
            select(AnchorReconciliation).where(
                AnchorReconciliation.estimate_version_id == version_id
            )
        )
        .scalars()
        .all()
    )


def _load_active_overrides_at(
    session: Session, *, company_id: str, at: datetime
) -> list[ManualOverride]:
    stmt = select(ManualOverride).where(ManualOverride.company_id == company_id)
    rows = list(session.execute(stmt).scalars().all())
    return [r for r in rows if r.expires_at is None or r.expires_at >= at]


def _load_audit_for_version(
    session: Session, *, version_id: str
) -> list[AuditLog]:
    stmt = (
        select(AuditLog)
        .where(AuditLog.target_type == "estimate_version", AuditLog.target_id == version_id)
        .order_by(AuditLog.created_at)
    )
    return list(session.execute(stmt).scalars().all())


def _format_anchor(
    anchor: CompanyAnchorObservation, source: SourceObservation | None
) -> dict[str, Any]:
    return {
        "id": anchor.id,
        "anchor_month": anchor.anchor_month.isoformat(),
        "anchor_type": anchor.anchor_type.value,
        "value_min": float(anchor.headcount_value_min),
        "value_point": float(anchor.headcount_value_point),
        "value_max": float(anchor.headcount_value_max),
        "value_kind": anchor.headcount_value_kind.value,
        "confidence": float(anchor.confidence or 0.0),
        "source_name": source.source_name.value if source is not None else None,
        "source_url": source.source_url if source is not None else None,
        "observed_at": _iso(source.observed_at) if source is not None else None,
    }


def _format_event(ev: CompanyEvent) -> dict[str, Any]:
    return {
        "id": ev.id,
        "event_type": ev.event_type.value,
        "event_month": ev.event_month.isoformat(),
        "source_class": ev.source_class.value,
        "confidence": float(ev.confidence),
        "description": ev.description,
    }


def _format_override(o: ManualOverride) -> dict[str, Any]:
    return {
        "id": o.id,
        "field_name": o.field_name.value,
        "reason": o.reason,
        "entered_by": o.entered_by,
        "expires_at": _iso(o.expires_at),
        "payload": dict(o.override_value_json or {}),
    }


def _format_reconciliation(r: AnchorReconciliation) -> dict[str, Any]:
    return {
        "id": r.id,
        "chosen_point": float(r.chosen_point),
        "chosen_min": float(r.chosen_min),
        "chosen_max": float(r.chosen_max),
        "inputs": list(r.inputs_json or []),
        "weights": dict(r.weights_json or {}),
        "rationale": r.rationale,
    }


_GROWTH_HORIZONS: tuple[tuple[int, str], ...] = ((6, "6m"), (12, "1y"), (24, "2y"))
"""Product-level growth horizons: (months_back, label)."""


def _months_back(month: date, n: int) -> date:
    year = month.year
    m = month.month - n
    while m <= 0:
        m += 12
        year -= 1
    return date(year, m, 1)


def _row_is_usable(row: HeadcountEstimateMonthly) -> bool:
    """A growth endpoint must be a non-suppressed, positive estimate."""
    return (
        row.method
        not in {
            EstimateMethod.suppressed_low_sample,
            EstimateMethod.degraded_current_only,
        }
        and float(row.estimated_headcount) > 0.0
    )


def compute_growth_windows(
    session: Session,
    *,
    version_id: str,
) -> list[dict[str, Any]]:
    """Compute ``6m / 1y / 2y`` growth windows anchored at the latest month.

    Pure read-only: loads every ``HeadcountEstimateMonthly`` row for the
    version and produces ``GrowthWindow``-shaped dicts (matching
    :class:`~headcount.schemas.estimates.GrowthWindow`). Endpoints that
    are suppressed / degraded yield a ``suppressed=True`` window so the
    UI can still show a row with a clear reason.
    """

    rows = list(
        session.execute(
            select(HeadcountEstimateMonthly)
            .where(HeadcountEstimateMonthly.estimate_version_id == version_id)
            .order_by(HeadcountEstimateMonthly.month)
        )
        .scalars()
        .all()
    )
    if not rows:
        return []
    by_month = {r.month: r for r in rows}
    latest = rows[-1]
    out: list[dict[str, Any]] = []
    for months, label in _GROWTH_HORIZONS:
        start_month = _months_back(latest.month, months)
        start = by_month.get(start_month)
        if start is None:
            out.append(
                {
                    "window": label,
                    "start_month": start_month.isoformat(),
                    "end_month": latest.month.isoformat(),
                    "start_value": None,
                    "end_value": None,
                    "absolute_delta": None,
                    "percent_delta": None,
                    "confidence_band": latest.confidence_band.value,
                    "suppressed": True,
                    "suppression_reason": "no_estimate_at_start_month",
                }
            )
            continue
        if not _row_is_usable(start) or not _row_is_usable(latest):
            out.append(
                {
                    "window": label,
                    "start_month": start_month.isoformat(),
                    "end_month": latest.month.isoformat(),
                    "start_value": float(start.estimated_headcount),
                    "end_value": float(latest.estimated_headcount),
                    "absolute_delta": None,
                    "percent_delta": None,
                    "confidence_band": latest.confidence_band.value,
                    "suppressed": True,
                    "suppression_reason": (
                        start.suppression_reason or latest.suppression_reason or "endpoint_suppressed"
                    ),
                }
            )
            continue
        start_point = float(start.estimated_headcount)
        end_point = float(latest.estimated_headcount)
        absolute_delta = end_point - start_point
        percent_delta = (end_point / start_point) - 1.0 if start_point > 0.0 else None
        # Weakest band among the two endpoints propagates to the window.
        band = (
            latest.confidence_band
            if latest.confidence_band.value >= start.confidence_band.value
            else start.confidence_band
        )
        out.append(
            {
                "window": label,
                "start_month": start_month.isoformat(),
                "end_month": latest.month.isoformat(),
                "start_value": start_point,
                "end_value": end_point,
                "absolute_delta": absolute_delta,
                "percent_delta": percent_delta,
                "confidence_band": band.value,
                "suppressed": False,
                "suppression_reason": None,
            }
        )
    return out


def _format_estimate(est: HeadcountEstimateMonthly) -> dict[str, Any]:
    return {
        "month": est.month.isoformat(),
        "value_min": float(est.estimated_headcount_min),
        "value_point": float(est.estimated_headcount),
        "value_max": float(est.estimated_headcount_max),
        "method": est.method.value,
        "confidence_band": est.confidence_band.value,
        "confidence_score": est.confidence_score,
        "confidence_components": dict(est.confidence_components_json or {}),
        "public_profile_count": int(est.public_profile_count),
        "scaled_from_anchor_value": float(est.scaled_from_anchor_value),
        "needs_review": bool(est.needs_review),
        "suppression_reason": est.suppression_reason,
    }


def build_evidence(
    session: Session,
    *,
    company_id: str,
    month: date,
    version_id: str | None = None,
) -> dict[str, Any]:
    """Assemble the structured evidence tree.

    If ``version_id`` is omitted, the latest ``EstimateVersion`` for the
    company is used. Raises :class:`EvidenceNotFoundError` when the
    company has no estimate row for that month under the chosen version.
    """

    month = _month_floor(month)

    company = session.get(Company, company_id)
    if company is None:
        raise EvidenceNotFoundError(f"company not found: {company_id}")

    if version_id is not None:
        version = session.get(EstimateVersion, version_id)
        if version is None or version.company_id != company_id:
            raise EvidenceNotFoundError(
                f"estimate_version {version_id} not found for company {company_id}"
            )
    else:
        version = _latest_version_for_company(session, company_id)
        if version is None:
            raise EvidenceNotFoundError(
                f"no estimate_version for company {company_id}; run `hc estimate-series` first"
            )

    estimate = _load_estimate_row(session, version_id=version.id, month=month)
    if estimate is None:
        raise EvidenceNotFoundError(
            f"no estimate row for company={company_id} month={month.isoformat()} "
            f"under version={version.id}"
        )

    seg_start, seg_end, openers = _load_segment_bounds(
        session, company_id=company_id, month=month
    )
    anchors_rows = _load_anchors_in_window(
        session, company_id=company_id, start=seg_start, end=seg_end
    )
    events_rows = _load_events_in_window(
        session, company_id=company_id, start=seg_start, end=seg_end
    )
    recon_rows = _load_reconciliation(session, version_id=version.id)
    overrides = _load_active_overrides_at(
        session, company_id=company_id, at=datetime.now(tz=UTC)
    )
    audit_rows = _load_audit_for_version(session, version_id=version.id)
    growth_windows = compute_growth_windows(session, version_id=version.id)

    return {
        "evidence_version": EVIDENCE_VERSION,
        "company": {
            "id": company.id,
            "canonical_name": company.canonical_name,
            "canonical_domain": company.canonical_domain,
            "priority_tier": company.priority_tier.value,
            "status": company.status.value,
        },
        "month": month.isoformat(),
        "estimate_version": {
            "id": version.id,
            "status": version.status.value,
            "method_version": version.method_version,
            "anchor_policy_version": version.anchor_policy_version,
            "coverage_curve_version": version.coverage_curve_version,
            "source_snapshot_cutoff": version.source_snapshot_cutoff.isoformat(),
        },
        "estimate": _format_estimate(estimate),
        "segment": {
            "start_month": seg_start.isoformat() if seg_start else None,
            "end_month_exclusive": seg_end.isoformat() if seg_end else None,
            "opening_events": [_format_event(e) for e in openers],
        },
        "inputs": {
            "anchors": [_format_anchor(a, s) for a, s in anchors_rows],
            "events": [_format_event(e) for e in events_rows],
            "employment_snapshot": {
                "public_profile_count": int(estimate.public_profile_count),
                "scaled_from_anchor_value": float(estimate.scaled_from_anchor_value),
            },
        },
        "reconciled_anchors": [_format_reconciliation(r) for r in recon_rows],
        "growth": growth_windows,
        "confidence": {
            "band": estimate.confidence_band.value,
            "score": estimate.confidence_score,
            "components": dict(estimate.confidence_components_json or {}),
        },
        "overrides_applied": [_format_override(o) for o in overrides],
        "audit": [
            {
                "id": a.id,
                "action": a.action,
                "actor_type": a.actor_type,
                "actor_id": a.actor_id,
                "created_at": _iso(a.created_at),
                "payload": dict(a.payload_json or {}),
            }
            for a in audit_rows
        ],
    }


__all__ = [
    "EVIDENCE_VERSION",
    "EvidenceNotFoundError",
    "build_evidence",
    "compute_growth_windows",
]
