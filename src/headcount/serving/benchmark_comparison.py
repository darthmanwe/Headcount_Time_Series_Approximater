"""Estimate-vs-benchmark comparison for acceptance reporting.

This is a thin wrapper over
:func:`headcount.review.benchmark_disagreement.detect_benchmark_disagreement`
that reads ``HeadcountEstimateMonthly`` + ``BenchmarkObservation`` from
the DB for a set of companies and returns a structured comparison summary
suitable for CLI printing, CSV export, or the FastAPI surface.

We deliberately do not persist a new table - the raw benchmark rows are
the source of truth and disagreement is a derived per-run view. Runs
that want to enqueue disagreements into the review queue should call the
review layer directly.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import date

from sqlalchemy import select
from sqlalchemy.orm import Session

from headcount.estimate.reconcile import MonthlyEstimate
from headcount.models.benchmark import BenchmarkObservation
from headcount.models.company import Company
from headcount.models.estimate_version import EstimateVersion
from headcount.models.headcount_estimate_monthly import HeadcountEstimateMonthly
from headcount.review.benchmark_disagreement import (
    BENCHMARK_DISAGREEMENT_VERSION,
    BenchmarkDisagreement,
    detect_benchmark_disagreement,
)

COMPARISON_VERSION = "comparison_v1"


@dataclass(slots=True)
class CompanyComparison:
    """One per-company roll-up of benchmark comparisons."""

    company_id: str
    canonical_name: str
    estimate_version_id: str | None
    benchmarks_total: int = 0
    benchmarks_matched: int = 0
    disagreements: list[BenchmarkDisagreement] = field(default_factory=list)

    @property
    def match_rate(self) -> float:
        if self.benchmarks_matched == 0:
            return 0.0
        return 1.0 - (len(self.disagreements) / self.benchmarks_matched)


@dataclass(slots=True)
class ComparisonSummary:
    """Batch-level comparison summary across many companies."""

    comparison_version: str = COMPARISON_VERSION
    disagreement_version: str = BENCHMARK_DISAGREEMENT_VERSION
    threshold: float = 0.25
    companies_total: int = 0
    companies_with_benchmarks: int = 0
    benchmarks_total: int = 0
    benchmarks_matched: int = 0
    disagreements_total: int = 0
    per_company: list[CompanyComparison] = field(default_factory=list)

    def to_dict(self) -> dict[str, object]:
        return {
            "comparison_version": self.comparison_version,
            "disagreement_version": self.disagreement_version,
            "threshold": self.threshold,
            "companies_total": self.companies_total,
            "companies_with_benchmarks": self.companies_with_benchmarks,
            "benchmarks_total": self.benchmarks_total,
            "benchmarks_matched": self.benchmarks_matched,
            "disagreements_total": self.disagreements_total,
            "per_company": [
                {
                    "company_id": c.company_id,
                    "canonical_name": c.canonical_name,
                    "estimate_version_id": c.estimate_version_id,
                    "benchmarks_total": c.benchmarks_total,
                    "benchmarks_matched": c.benchmarks_matched,
                    "disagreements": [
                        {
                            "benchmark_id": d.benchmark_id,
                            "month": d.month.isoformat(),
                            "benchmark_point": d.benchmark_point,
                            "estimate_point": d.estimate_point,
                            "relative_gap": d.relative_gap,
                            "interval_overlap": d.interval_overlap,
                            "provider": d.provider,
                            "metric": d.metric,
                        }
                        for d in c.disagreements
                    ],
                }
                for c in self.per_company
            ],
        }


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


def _load_estimates_map(
    session: Session, *, version_id: str
) -> dict[date, MonthlyEstimate]:
    rows = (
        session.execute(
            select(HeadcountEstimateMonthly).where(
                HeadcountEstimateMonthly.estimate_version_id == version_id
            )
        )
        .scalars()
        .all()
    )
    out: dict[date, MonthlyEstimate] = {}
    for r in rows:
        out[r.month] = MonthlyEstimate(
            month=r.month,
            value_min=float(r.estimated_headcount_min),
            value_point=float(r.estimated_headcount),
            value_max=float(r.estimated_headcount_max),
            public_profile_count=int(r.public_profile_count),
            scaled_from_anchor_value=float(r.scaled_from_anchor_value),
            method=r.method,
            confidence_band=r.confidence_band,
            needs_review=bool(r.needs_review),
            suppression_reason=r.suppression_reason,
            coverage_factor=1.0,
            ratio=1.0,
            anchor_month=None,
            contributing_anchor_ids=(),
        )
    return out


def _load_benchmarks(
    session: Session, *, company_ids: list[str]
) -> dict[str, list[BenchmarkObservation]]:
    rows = (
        session.execute(
            select(BenchmarkObservation).where(
                BenchmarkObservation.company_id.in_(company_ids)
            )
        )
        .scalars()
        .all()
    )
    out: dict[str, list[BenchmarkObservation]] = {}
    for r in rows:
        if r.company_id is None:
            continue
        out.setdefault(r.company_id, []).append(r)
    return out


def compare_estimates_to_benchmarks(
    session: Session,
    *,
    company_ids: Iterable[str] | None = None,
    threshold: float = 0.25,
) -> ComparisonSummary:
    """Compare the latest estimate per company against its benchmarks.

    Parameters
    ----------
    company_ids:
        Restrict to these companies. ``None`` means every company with
        at least one benchmark row.
    threshold:
        Relative-gap threshold. Estimates whose ``|bench - est| /
        max(1, bench)`` exceeds this value are flagged as disagreements.
    """

    if company_ids is None:
        ids_from_bench = (
            session.execute(
                select(BenchmarkObservation.company_id)
                .where(BenchmarkObservation.company_id.is_not(None))
                .distinct()
            )
            .scalars()
            .all()
        )
        resolved_ids: list[str] = [cid for cid in ids_from_bench if cid]
    else:
        resolved_ids = list(company_ids)

    if not resolved_ids:
        return ComparisonSummary(threshold=threshold)

    companies = {
        c.id: c
        for c in session.execute(
            select(Company).where(Company.id.in_(resolved_ids))
        ).scalars()
    }
    version_map = _latest_version_per_company(session, resolved_ids)
    benchmarks_by_company = _load_benchmarks(session, company_ids=resolved_ids)

    summary = ComparisonSummary(threshold=threshold)
    summary.companies_total = len(resolved_ids)

    for cid in resolved_ids:
        bench_rows = benchmarks_by_company.get(cid, [])
        if not bench_rows:
            continue
        summary.companies_with_benchmarks += 1

        company = companies.get(cid)
        canonical_name = company.canonical_name if company else cid
        version_id = version_map.get(cid)

        per_company = CompanyComparison(
            company_id=cid,
            canonical_name=canonical_name,
            estimate_version_id=version_id,
            benchmarks_total=len(bench_rows),
        )

        if version_id is None:
            summary.benchmarks_total += len(bench_rows)
            summary.per_company.append(per_company)
            continue

        estimates_map = _load_estimates_map(session, version_id=version_id)
        disagreements = detect_benchmark_disagreement(
            bench_rows, estimates_map, threshold=threshold
        )
        # Count "matched" as benchmarks where we had an estimate at the target
        # offset month - i.e. same filter the disagreement detector applies
        # minus the threshold check.
        matched = _count_matched(bench_rows, estimates_map)

        per_company.benchmarks_matched = matched
        per_company.disagreements = disagreements

        summary.benchmarks_total += len(bench_rows)
        summary.benchmarks_matched += matched
        summary.disagreements_total += len(disagreements)
        summary.per_company.append(per_company)

    return summary


def _count_matched(
    benchmarks: Iterable[BenchmarkObservation],
    estimates_by_month: dict[date, MonthlyEstimate],
) -> int:
    from headcount.review.benchmark_disagreement import _METRIC_MONTH_OFFSET, _offset_month

    total = 0
    for b in benchmarks:
        if b.metric not in _METRIC_MONTH_OFFSET:
            continue
        if b.value_point is None or b.as_of_month is None:
            continue
        offset = _METRIC_MONTH_OFFSET[b.metric]
        target = _offset_month(b.as_of_month, offset)
        if target in estimates_by_month:
            total += 1
    return total


__all__ = [
    "COMPARISON_VERSION",
    "CompanyComparison",
    "ComparisonSummary",
    "compare_estimates_to_benchmarks",
]
