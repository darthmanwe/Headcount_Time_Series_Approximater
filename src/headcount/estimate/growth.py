"""Growth windows over an estimated monthly series.

Two horizon conventions live in this module:

- The **product contract** (what the API and exports emit) is
  ``6m / 1y / 2y`` - matching the benchmark columns we load from the
  analyst workbook and the ``GrowthWindow.window`` schema regex. These are
  the horizons ``compute_growth_series`` emits by default.
- **Shorter internal horizons** (``1m / 3m / 12m``) are still available
  to callers who need MoM/QoQ/YoY for anomaly detection or charts via an
  explicit ``horizons=`` argument. They are never surfaced to the product.

Interval-valued growth: propagating uncertainty from the monthly interval
into the growth ratio yields another interval. For a ratio ``y/x`` with
``x in [x_min, x_max]``, ``y in [y_min, y_max]``, all > 0, the tightest
bounds are ``(y_min/x_max, y_max/x_min)`` and the point estimate is
``y_point/x_point``. That's what we use.

Growth is undefined (``None``) when either endpoint is missing, suppressed,
or has a non-positive value. Segment breaks are not special-cased here -
the caller decides whether to show MoM that crosses an event.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date

from headcount.db.enums import EstimateMethod
from headcount.estimate.reconcile import MonthlyEstimate

GROWTH_VERSION = "growth_v2"

PRODUCT_HORIZONS: tuple[int, ...] = (6, 12, 24)
"""Default product-level horizons (6m / 1y / 2y), in months."""

HORIZON_LABELS: dict[int, str] = {
    1: "1m",
    3: "3m",
    6: "6m",
    12: "1y",
    24: "2y",
}
"""Canonical label for each supported horizon in months.

Product-contract horizons use short labels ``6m / 1y / 2y`` to match the
:class:`~headcount.schemas.estimates.GrowthWindow` regex and the benchmark
columns. Internal callers that pass ``horizons=(1, 3, 12)`` receive
``1m / 3m / 12m`` labels instead.
"""


@dataclass(frozen=True, slots=True)
class GrowthPoint:
    """Growth measurement from ``prev_month`` to ``month``."""

    month: date
    prev_month: date
    horizon: str  # e.g. "6m" | "1y" | "2y" (product) or "1m"/"3m"/"12m"
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
    horizons: tuple[int, ...] = PRODUCT_HORIZONS,
) -> list[GrowthPoint]:
    """Return a flat list of :class:`GrowthPoint` rows across all horizons.

    The default ``horizons=(6, 12, 24)`` matches the product contract
    (``GrowthWindow.window`` pattern ``^(6m|1y|2y)$``) and the benchmark
    workbook columns. Callers that need MoM/QoQ/YoY for anomaly
    detection or charts can pass ``horizons=(1, 3, 12)``.
    """

    est_list = list(estimates)
    index = {e.month: e for e in est_list}
    out: list[GrowthPoint] = []

    for e in est_list:
        for h in horizons:
            prev_month = _months_back(index, e.month, h)
            if prev_month is None:
                continue
            label = HORIZON_LABELS.get(h, f"{h}m")
            point = _compute(e, index[prev_month], horizon=label)
            if point is not None:
                out.append(point)

    return out


def latest_growth_windows(
    estimates: Iterable[MonthlyEstimate],
    *,
    horizons: tuple[int, ...] = PRODUCT_HORIZONS,
) -> list[GrowthPoint]:
    """Return one :class:`GrowthPoint` per horizon anchored at the latest month.

    Convenience wrapper used by the API / evidence / exports so a
    company's "6m / 1y / 2y growth" is a single number per horizon
    ending at the most recent available month.
    """

    est_list = sorted(estimates, key=lambda e: e.month)
    if not est_list:
        return []
    latest_month = est_list[-1].month
    all_points = compute_growth_series(est_list, horizons=horizons)
    return [p for p in all_points if p.month == latest_month]


__all__ = [
    "GROWTH_VERSION",
    "HORIZON_LABELS",
    "PRODUCT_HORIZONS",
    "GrowthPoint",
    "compute_growth_series",
    "latest_growth_windows",
]
