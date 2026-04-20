"""Promote ``BenchmarkObservation`` rows into ``CompanyAnchorObservation`` rows.

The analyst workbook (:mod:`headcount.ingest.seeds.benchmark_loader`) lands
provider headcount values in ``benchmark_observation`` as canonical
evidence: ``(provider, company, as_of_month, value_point, ...)``. Until
now those rows were only consumed by the **disagreement** check in
Phase 8 - they never influenced the estimated series itself.

This module promotes them into :class:`CompanyAnchorObservation` rows of
type :class:`AnchorType.historical_statement` (or
:class:`AnchorType.current_headcount_anchor` for the "as of today"
metric), so the estimator can treat them like any other anchor. This is
the mechanism that makes the pipeline operational *without* licensed
per-profile data: benchmark rows alone give us up to four historical
datapoints per company (``T``, ``T-6m``, ``T-12m``, ``T-24m``) - enough
for the multi-anchor interpolation path in
:mod:`headcount.estimate.reconcile` to reconstruct a monthly series.

Invariants
----------

1. **Read-through by company_id.** Only benchmark rows whose
   ``company_id`` is populated are eligible. Rows linked only to a
   ``company_candidate`` are skipped - they need resolution first.
2. **Only headcount metrics.** Growth percentage metrics
   (``growth_6m_pct``, ``growth_1y_pct``, ``growth_2y_pct``) and
   ``web_traffic`` are not promoted - they describe a *change*, not a
   point estimate.
3. **Value is required.** Rows with ``value_point IS NULL`` are skipped
   (the loader only fills in points when the workbook cell parsed).
4. **Idempotent.** We look up any existing promoted anchor by
   ``(company_id, source_observation_id)`` and skip it. Reruns are free.
5. **Provenance preserved.** A dedicated :class:`SourceObservation` row
   is created per benchmark observation with ``source_name=benchmark``
   and a stable ``raw_content_hash`` of the workbook/row/metric/provider
   tuple, so deduping is deterministic across reruns.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from headcount.db.enums import (
    AnchorType,
    BenchmarkMetric,
    BenchmarkProvider,
    HeadcountValueKind,
    ParseStatus,
    SourceEntityType,
    SourceName,
)
from headcount.models.benchmark import BenchmarkObservation
from headcount.models.company_anchor_observation import CompanyAnchorObservation
from headcount.models.source_observation import SourceObservation
from headcount.utils.logging import get_logger

BENCHMARK_ANCHOR_PARSER_VERSION = "benchmark_anchors_v1"

_log = get_logger("headcount.parsers.benchmark_anchors")

_PROMOTABLE_METRICS: frozenset[BenchmarkMetric] = frozenset(
    {
        BenchmarkMetric.headcount_current,
        BenchmarkMetric.headcount_6m_ago,
        BenchmarkMetric.headcount_1y_ago,
        BenchmarkMetric.headcount_2y_ago,
    }
)

_CURRENT_METRICS: frozenset[BenchmarkMetric] = frozenset(
    {BenchmarkMetric.headcount_current}
)

# Baseline confidence per provider.
#
# The benchmark workbook's providers have very different trust profiles:
#
# * ``zeeshan`` is an analyst-verified column produced by human research
#   (ranges and targeted point estimates). When the analyst has taken a
#   position on a historical or current value, it should dominate.
# * ``harmonic`` is an automated third-party data feed. Useful, but
#   known to be wrong on specific slices (we observed cases where its
#   ``headcount_current`` was an order of magnitude off the analyst's
#   verified range - e.g. 65 vs. 350 for a real 201-500 company).
# * ``linkedin`` numbers are scraped from the logged-out LinkedIn company
#   page. The historical "Employee Count (N months ago)" cells are
#   profile-appearance counts, not total headcount, and so should be the
#   lowest-weight source for promotion.
#
# Ordering matters: whenever two providers emit a value for the same
# ``(company, month)``, the highest-confidence anchor wins in
# :func:`headcount.estimate.reconcile.interpolate_series_from_anchors`
# (via the per-month ``best_per_month`` dict). We therefore want the
# analyst column to outrank automated feeds.
_PROVIDER_CONFIDENCE: dict[BenchmarkProvider, float] = {
    BenchmarkProvider.zeeshan: 0.65,
    BenchmarkProvider.harmonic: 0.55,
    BenchmarkProvider.linkedin: 0.45,
}


@dataclass(slots=True)
class PromotionResult:
    """Summary of a single :func:`promote_benchmark_anchors` invocation."""

    scanned: int = 0
    eligible: int = 0
    skipped_no_company: int = 0
    skipped_no_value: int = 0
    skipped_unsupported_metric: int = 0
    inserted_source_rows: int = 0
    inserted_anchor_rows: int = 0
    already_promoted: int = 0
    errors: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, object]:
        return {
            "scanned": self.scanned,
            "eligible": self.eligible,
            "skipped_no_company": self.skipped_no_company,
            "skipped_no_value": self.skipped_no_value,
            "skipped_unsupported_metric": self.skipped_unsupported_metric,
            "inserted_source_rows": self.inserted_source_rows,
            "inserted_anchor_rows": self.inserted_anchor_rows,
            "already_promoted": self.already_promoted,
            "errors": len(self.errors),
        }


def _content_hash(obs: BenchmarkObservation) -> str:
    """Stable digest for the benchmark cell, for dedup across reruns."""
    payload = {
        "workbook": obs.source_workbook,
        "sheet": obs.source_sheet,
        "row": obs.source_row_index,
        "provider": obs.provider.value,
        "metric": obs.metric.value,
        "value_point": obs.value_point,
        "as_of_month": obs.as_of_month.isoformat() if obs.as_of_month else None,
    }
    digest = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return digest


def _anchor_type_for(metric: BenchmarkMetric) -> AnchorType:
    if metric in _CURRENT_METRICS:
        return AnchorType.current_headcount_anchor
    return AnchorType.historical_statement


def _value_kind_for(obs: BenchmarkObservation) -> HeadcountValueKind:
    if obs.value_kind is not None:
        return obs.value_kind
    # Benchmark workbooks carry scalar headcounts, so default to exact.
    return HeadcountValueKind.exact


def _interval(obs: BenchmarkObservation) -> tuple[float, float, float] | None:
    point = obs.value_point
    if point is None:
        return None
    vmin = obs.value_min if obs.value_min is not None else point
    vmax = obs.value_max if obs.value_max is not None else point
    if vmin > point or vmax < point:
        return None
    return float(vmin), float(point), float(vmax)


def _confidence_for(provider: BenchmarkProvider) -> float:
    return _PROVIDER_CONFIDENCE.get(provider, 0.50)


def _existing_source_id_for(
    session: Session, *, content_hash: str
) -> str | None:
    row = session.execute(
        select(SourceObservation.id).where(
            SourceObservation.source_name == SourceName.benchmark,
            SourceObservation.raw_content_hash == content_hash,
        )
    ).scalar_one_or_none()
    return row


def _anchor_already_promoted(
    session: Session, *, company_id: str, source_observation_id: str
) -> bool:
    row = session.execute(
        select(CompanyAnchorObservation.id).where(
            CompanyAnchorObservation.company_id == company_id,
            CompanyAnchorObservation.source_observation_id == source_observation_id,
        )
    ).first()
    return row is not None


def promote_benchmark_anchors(
    session: Session,
    *,
    company_ids: Sequence[str] | None = None,
    observed_at: datetime | None = None,
) -> PromotionResult:
    """Promote every eligible ``BenchmarkObservation`` into an anchor row.

    ``company_ids`` restricts the scan to a subset when supplied; otherwise
    every benchmark observation with a populated ``company_id`` is
    considered. The caller owns the unit of work (no commit here).
    """

    result = PromotionResult()
    observed_at = observed_at or datetime.now(tz=UTC)

    stmt = select(BenchmarkObservation).where(BenchmarkObservation.metric.in_(_PROMOTABLE_METRICS))
    if company_ids is not None:
        ids = [cid for cid in company_ids if cid]
        if not ids:
            return result
        stmt = stmt.where(BenchmarkObservation.company_id.in_(ids))

    rows: Iterable[BenchmarkObservation] = session.execute(stmt).scalars()
    for obs in rows:
        result.scanned += 1

        if obs.company_id is None:
            result.skipped_no_company += 1
            continue
        if obs.metric not in _PROMOTABLE_METRICS:
            result.skipped_unsupported_metric += 1
            continue
        interval = _interval(obs)
        if interval is None or obs.as_of_month is None:
            result.skipped_no_value += 1
            continue
        result.eligible += 1

        vmin, point, vmax = interval
        kind = _value_kind_for(obs)
        anchor_type = _anchor_type_for(obs.metric)
        confidence = _confidence_for(obs.provider)
        content_hash = _content_hash(obs)

        source_id = _existing_source_id_for(session, content_hash=content_hash)
        if source_id is None:
            src = SourceObservation(
                source_name=SourceName.benchmark,
                entity_type=SourceEntityType.benchmark,
                source_url=(
                    f"benchmark://{obs.provider.value}/{obs.source_workbook}"
                    f"#{obs.source_sheet}:{obs.source_row_index}:{obs.metric.value}"
                ),
                observed_at=observed_at,
                raw_text=obs.raw_value_text,
                raw_content_hash=content_hash,
                parser_version=BENCHMARK_ANCHOR_PARSER_VERSION,
                parse_status=ParseStatus.ok,
                normalized_payload_json={
                    "provider": obs.provider.value,
                    "metric": obs.metric.value,
                    "value_point": point,
                    "value_min": vmin,
                    "value_max": vmax,
                    "as_of_month": obs.as_of_month.isoformat(),
                    "source_workbook": obs.source_workbook,
                    "source_sheet": obs.source_sheet,
                    "source_row_index": obs.source_row_index,
                    "source_cell_address": obs.source_cell_address,
                },
            )
            session.add(src)
            session.flush()
            source_id = src.id
            result.inserted_source_rows += 1

        if _anchor_already_promoted(
            session, company_id=obs.company_id, source_observation_id=source_id
        ):
            result.already_promoted += 1
            continue

        anchor = CompanyAnchorObservation(
            company_id=obs.company_id,
            source_observation_id=source_id,
            anchor_type=anchor_type,
            headcount_value_min=vmin,
            headcount_value_point=point,
            headcount_value_max=vmax,
            headcount_value_kind=kind,
            anchor_month=obs.as_of_month,
            confidence=confidence,
            note=(
                f"promoted_benchmark provider={obs.provider.value} "
                f"metric={obs.metric.value}"
            ),
        )
        session.add(anchor)
        result.inserted_anchor_rows += 1

    _log.info("benchmark_anchor_promotion_done", **result.as_dict())
    return result


__all__ = [
    "BENCHMARK_ANCHOR_PARSER_VERSION",
    "PromotionResult",
    "promote_benchmark_anchors",
]
