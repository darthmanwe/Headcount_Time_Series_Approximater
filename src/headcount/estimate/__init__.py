"""Estimation core.

Pure, deterministic logic that transforms interval anchors, public employment
history, and canonical events into monthly headcount estimates with
propagated uncertainty. No I/O, no DB writes inside the math modules -
:mod:`headcount.estimate.pipeline` is the only module that talks to a
SQLAlchemy ``Session`` and it delegates the numerical work to the helpers in
this package.

Design invariants
-----------------

1. **Hard-break segmentation.** We never interpolate or smooth across an
   event month (acquisition, merger, etc.). Each inter-event segment is
   reconciled on its own; post-event jumps are legal.
2. **Interval-valued everything.** Every monthly estimate carries
   ``(min, point, max)`` so downstream consumers can see uncertainty
   instead of a false-precision scalar.
3. **Fail-closed on weak evidence.** If coverage is below the configured
   floor or all anchors in a segment disagree materially, we emit
   ``EstimateMethod.suppressed_low_sample`` or
   ``EstimateMethod.degraded_current_only`` with a reason rather than
   guessing.
4. **Versioned and replayable.** Each run stamps ``method_version``,
   ``anchor_policy_version``, ``coverage_curve_version`` on the
   :class:`~headcount.models.estimate_version.EstimateVersion` so
   replay / A-B compare is trivial.

Modules
-------

- :mod:`headcount.estimate.segments` - event-aware hard-break partitioning.
- :mod:`headcount.estimate.anchors` - interval anchor rollup within a
  segment.
- :mod:`headcount.estimate.employment` - expand
  :class:`PersonEmploymentObservation` rows into monthly public-profile
  counts.
- :mod:`headcount.estimate.coverage` - time-varying coverage correction
  factor.
- :mod:`headcount.estimate.reconcile` - ratio-scale employment series and
  reconcile with anchor series into the final monthly interval.
- :mod:`headcount.estimate.anomalies` - contradiction / width / jump /
  sample-floor flag detection.
- :mod:`headcount.estimate.growth` - MoM / QoQ / YoY growth with interval
  propagation.
- :mod:`headcount.estimate.pipeline` - orchestrator that runs the above in
  order against a SQLAlchemy session.
"""

from headcount.estimate.anchors import (
    ANCHOR_POLICY_VERSION,
    AnchorCandidate,
    ReconciledAnchor,
    reconcile_segment_anchors,
)
from headcount.estimate.anomalies import (
    AnomalyFlags,
    detect_anomalies,
)
from headcount.estimate.coverage import (
    COVERAGE_CURVE_VERSION,
    CoverageCurve,
    build_default_coverage_curve,
)
from headcount.estimate.employment import (
    EMPLOYMENT_EXPANSION_VERSION,
    monthly_public_profile_counts,
)
from headcount.estimate.growth import (
    GrowthPoint,
    compute_growth_series,
)
from headcount.estimate.reconcile import (
    METHOD_VERSION,
    MonthlyEstimate,
    reconcile_series,
)
from headcount.estimate.segments import (
    Segment,
    split_into_segments,
)

__all__ = [
    "ANCHOR_POLICY_VERSION",
    "COVERAGE_CURVE_VERSION",
    "EMPLOYMENT_EXPANSION_VERSION",
    "METHOD_VERSION",
    "AnchorCandidate",
    "AnomalyFlags",
    "CoverageCurve",
    "GrowthPoint",
    "MonthlyEstimate",
    "ReconciledAnchor",
    "Segment",
    "build_default_coverage_curve",
    "compute_growth_series",
    "detect_anomalies",
    "monthly_public_profile_counts",
    "reconcile_segment_anchors",
    "reconcile_series",
    "split_into_segments",
]
