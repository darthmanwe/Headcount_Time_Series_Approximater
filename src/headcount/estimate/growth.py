"""Month-over-month / quarter-over-quarter / year-over-year growth.

Interval-valued growth: propagating uncertainty from the monthly interval
into the growth ratio yields another interval. For a ratio ``y/x`` with
``x in [x_min, x_max]``, ``y in [y_min, y_max]``, all > 0, the tightest
bounds are ``(y_min/x_max, y_max/x_min)`` and the point estimate is
``y_point/x_point``. That's what we use.

Growth is undefined (``None``) when either endpoint is missing, suppressed,
or has a non-positive value. Segment breaks are not special-cased here -
the caller decides whether to show MoM that crosses an event - but we do
carry through the ``anchor_month`` so the consumer can decide.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date

from headcount.db.enums import EstimateMethod
from headcount.estimate.reconcile import MonthlyEstimate

GROWTH_VERSION = "growth_v1"


@dataclass(frozen=True, slots=True)
class GrowthPoint:
    """Growth measurement from ``prev_month`` to ``month``."""

    month: date
    prev_month: date
    horizon: str  # "1m" | "3m" | "12m"
    value_min: float | None
    value_point: float | None
    value_max: float | None
    delta_headcount: float | None


def _compute(
    target: MonthlyEstimate | None,
    prev: MonthlyEstimate | None,
    *,
    horizon: str,
) -> GrowthPoint | None:
    if target is None or prev is None:
        return None
    if prev.method in {
        EstimateMethod.suppressed_low_sample,
        EstimateMethod.degraded_current_only,
    }:
        return None
    if target.method in {
        EstimateMethod.suppressed_low_sample,
        EstimateMethod.degraded_current_only,
    }:
        return None
    if prev.value_point <= 0.0 or target.value_point <= 0.0:
        return None

    point = target.value_point / prev.value_point - 1.0
    g_min = (target.value_min / max(prev.value_max, 1e-9)) - 1.0
    g_max = (target.value_max / max(prev.value_min, 1e-9)) - 1.0
    delta = target.value_point - prev.value_point

    return GrowthPoint(
        month=target.month,
        prev_month=prev.month,
        horizon=horizon,
        value_min=g_min,
        value_point=point,
        value_max=g_max,
        delta_headcount=delta,
    )


def _months_back(index: dict[date, MonthlyEstimate], month: date, months_back: int) -> date | None:
    year = month.year
    m = month.month - months_back
    while m <= 0:
        m += 12
        year -= 1
    candidate = date(year, m, 1)
    return candidate if candidate in index else None


def compute_growth_series(
    estimates: Iterable[MonthlyEstimate],
    *,
    horizons: tuple[int, ...] = (1, 3, 12),
) -> list[GrowthPoint]:
    """Return a flat list of :class:`GrowthPoint` rows across all horizons."""

    est_list = list(estimates)
    index = {e.month: e for e in est_list}
    label = {1: "1m", 3: "3m", 12: "12m"}
    out: list[GrowthPoint] = []

    for e in est_list:
        for h in horizons:
            prev_month = _months_back(index, e.month, h)
            if prev_month is None:
                continue
            point = _compute(e, index[prev_month], horizon=label.get(h, f"{h}m"))
            if point is not None:
                out.append(point)

    return out


__all__ = [
    "GROWTH_VERSION",
    "GrowthPoint",
    "compute_growth_series",
]
