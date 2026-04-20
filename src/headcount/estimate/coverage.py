"""Time-varying coverage correction.

Public employment profiles (LinkedIn, etc.) don't cover 100% of real
employees, and coverage is itself a function of time - older roles are
under-reported (people drop old jobs from profiles), very recent roles are
under-reported (profiles haven't been updated yet). Any ratio-scaling
method that doesn't correct for this bakes a systematic bias into every
historical month.

``CoverageCurve`` stores per-age-in-months coverage multipliers so a
reconcile step can scale the monthly profile count *up* by the reciprocal
before dividing into the anchor ratio. The curve is deliberately simple
and monotone: a floor value for very old history, a plateau for "healthy"
middle ages (typically 12-60 months), and a dip for the most recent few
months.

The default curve below is a plausible starting point drawn from
internal analyst intuition; it is **not** calibrated against benchmark
data. Phase 11 goldens will drive fitting a real curve. Until then we
treat the curve as a tunable parameter and stamp
``coverage_curve_version`` on the estimate_version row so we can replay.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

COVERAGE_CURVE_VERSION = "coverage_default_v1"
"""Stamped onto :class:`EstimateVersion.coverage_curve_version`."""

DEFAULT_MIN_COVERAGE = 0.15
"""Coverage of a single month can never drop below this (safety floor)."""


@dataclass(frozen=True, slots=True)
class CoverageCurve:
    """Coverage multiplier as a function of age (months before ``as_of``).

    The curve is parametrised by four piecewise-linear anchors:

    - ``coverage_at_recent`` for age 0 (the reference month).
    - ``coverage_plateau`` for ages in ``[plateau_start, plateau_end]``.
    - ``coverage_at_old`` for ages >= ``old_age``.

    Values outside these ranges are linearly interpolated. All numbers live
    in ``(0, 1]``.
    """

    coverage_at_recent: float = 0.55
    plateau_start: int = 6
    coverage_plateau: float = 0.85
    plateau_end: int = 36
    old_age: int = 96
    coverage_at_old: float = 0.35

    def at_age(self, age_months: int) -> float:
        age = max(0, int(age_months))
        c0 = self.coverage_at_recent
        cp = self.coverage_plateau
        co = self.coverage_at_old
        a0 = 0
        a1 = max(a0 + 1, self.plateau_start)
        a2 = max(a1 + 1, self.plateau_end)
        a3 = max(a2 + 1, self.old_age)

        if age <= a0:
            value = c0
        elif age < a1:
            t = (age - a0) / (a1 - a0)
            value = c0 + (cp - c0) * t
        elif age <= a2:
            value = cp
        elif age < a3:
            t = (age - a2) / (a3 - a2)
            value = cp + (co - cp) * t
        else:
            value = co

        return max(DEFAULT_MIN_COVERAGE, min(1.0, float(value)))

    def correction_factor(self, age_months: int) -> float:
        """Multiplier to apply to a raw profile count to undo coverage bias."""

        return 1.0 / self.at_age(age_months)


def build_default_coverage_curve() -> CoverageCurve:
    """Return the default curve. Centralised so overrides land in one place."""

    return CoverageCurve()


def months_between(month: date, as_of_month: date) -> int:
    """Number of whole months from ``month`` back to ``as_of_month`` (>=0)."""

    delta = (as_of_month.year - month.year) * 12 + (as_of_month.month - month.month)
    return max(0, int(delta))


__all__ = [
    "COVERAGE_CURVE_VERSION",
    "DEFAULT_MIN_COVERAGE",
    "CoverageCurve",
    "build_default_coverage_curve",
    "months_between",
]
