"""Anomaly detection on a monthly estimate series.

The goal is not to auto-correct outliers - analysts should still be in the
loop for that - but to *surface* them. Every :class:`MonthlyEstimate` gets
a set of boolean flags that tell reviewers where to look.

Detected anomalies (all deterministic):

- ``interval_too_wide``: interval width exceeds
  ``max_relative_interval_width`` of the point estimate.
- ``sample_floor_violation``: profile count below
  ``sample_floor``. Already surfaced by reconcile's suppression_reason;
  mirrored here for a single source of truth downstream.
- ``mom_jump``: absolute month-over-month growth exceeds
  ``mom_jump_threshold`` (default 30%) AND is not at a segment boundary.
  Segment boundaries are legal jumps (event-driven) so the caller passes
  in a set of ``segment_break_months``.
- ``non_monotonic_interval``: ``value_min > value_point`` or
  ``value_point > value_max`` (schema check guarantees this never persists
  but pre-flight catches it before a commit).
- ``coverage_floor_hit``: coverage correction saturated at the minimum
  floor, meaning the curve thinks we essentially don't trust the sample.

The module returns a parallel list of :class:`AnomalyFlags`, one per input
estimate, and a scalar ``needs_review`` derived from the flag set. Callers
can ``or`` this into the monthly row's ``needs_review`` column.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import date

from headcount.estimate.coverage import DEFAULT_MIN_COVERAGE
from headcount.estimate.reconcile import MonthlyEstimate

ANOMALIES_VERSION = "anomalies_v1"
"""Bumped when flag definitions or thresholds change materially."""


@dataclass(frozen=True, slots=True)
class AnomalyFlags:
    """Flags attached to a single :class:`MonthlyEstimate`."""

    month: date
    interval_too_wide: bool = False
    sample_floor_violation: bool = False
    mom_jump: bool = False
    non_monotonic_interval: bool = False
    coverage_floor_hit: bool = False
    reasons: tuple[str, ...] = field(default_factory=tuple)

    @property
    def needs_review(self) -> bool:
        return any(
            (
                self.interval_too_wide,
                self.sample_floor_violation,
                self.mom_jump,
                self.non_monotonic_interval,
                self.coverage_floor_hit,
            )
        )


def detect_anomalies(
    estimates: Iterable[MonthlyEstimate],
    *,
    segment_break_months: Iterable[date] = (),
    max_relative_interval_width: float = 0.75,
    mom_jump_threshold: float = 0.30,
    coverage_floor: float = DEFAULT_MIN_COVERAGE,
) -> list[AnomalyFlags]:
    """Return one :class:`AnomalyFlags` per input estimate, in order."""

    est_list = list(estimates)
    breaks = set(segment_break_months)
    out: list[AnomalyFlags] = []

    prev: MonthlyEstimate | None = None
    for e in est_list:
        reasons: list[str] = []

        interval_too_wide = False
        if e.value_point > 0:
            span = max(0.0, e.value_max - e.value_min)
            rel = span / e.value_point
            if rel > max_relative_interval_width:
                interval_too_wide = True
                reasons.append(f"width={rel:.2f}>{max_relative_interval_width:.2f}")

        sample_floor_violation = False
        if e.suppression_reason and e.suppression_reason.startswith("profiles_below_floor"):
            sample_floor_violation = True
            reasons.append(e.suppression_reason)

        mom_jump = False
        if prev is not None and e.month not in breaks and prev.value_point > 0:
            rel_change = abs(e.value_point - prev.value_point) / prev.value_point
            if rel_change > mom_jump_threshold:
                mom_jump = True
                reasons.append(f"mom_jump={rel_change:.2f}>{mom_jump_threshold:.2f}")

        non_monotonic = not (e.value_min <= e.value_point <= e.value_max)
        if non_monotonic:
            reasons.append("non_monotonic_interval")

        # coverage_factor is 1/coverage_at_age; hitting the floor means
        # coverage_factor = 1 / coverage_floor.
        coverage_floor_hit = False
        if coverage_floor > 0:
            saturation = 1.0 / coverage_floor
            if e.coverage_factor >= saturation - 1e-9:
                coverage_floor_hit = True
                reasons.append("coverage_floor_hit")

        out.append(
            AnomalyFlags(
                month=e.month,
                interval_too_wide=interval_too_wide,
                sample_floor_violation=sample_floor_violation,
                mom_jump=mom_jump,
                non_monotonic_interval=non_monotonic,
                coverage_floor_hit=coverage_floor_hit,
                reasons=tuple(reasons),
            )
        )
        prev = e

    return out


__all__ = [
    "ANOMALIES_VERSION",
    "AnomalyFlags",
    "detect_anomalies",
]
