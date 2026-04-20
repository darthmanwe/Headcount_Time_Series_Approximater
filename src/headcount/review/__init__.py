"""Phase 8: confidence scoring, analyst review queue, overrides, audit.

This package sits between :mod:`headcount.estimate` (which decides
"what do the numbers say") and the Streamlit / FastAPI review surface
(which decides "who should look at this and why").

Modules
-------

- :mod:`headcount.review.scoring`
    Deterministic per-month confidence score that decomposes into
    explainable components. The estimator stamps both the final
    :class:`~headcount.db.enums.ConfidenceBand` and the component
    breakdown onto each :class:`HeadcountEstimateMonthly` row so review
    UI can answer "why is this low-confidence".

- :mod:`headcount.review.benchmark_disagreement`
    Detect month-level disagreement between our estimates and
    :class:`BenchmarkObservation` (the acceptance test suite's "truth"
    snapshot). Emits explicit
    :class:`~headcount.db.enums.ReviewReason.benchmark_disagreement`
    rows instead of silently averaging.

- :mod:`headcount.review.queue`
    Build and upsert :class:`ReviewQueueItem` rows from estimate-time
    signals (low confidence, anomaly flags, benchmark disagreement,
    gated LinkedIn fetches). Dedupe by
    ``(company_id, estimate_version_id, review_reason)`` and keep the
    highest-priority instance.

- :mod:`headcount.review.overrides`
    Apply :class:`ManualOverride` rows to the pipeline: anchor
    pinning, segment-level suppression, canonical-company overrides.
    Expired overrides are ignored automatically.

- :mod:`headcount.review.audit`
    Immutable :class:`AuditLog` writer used by overrides and queue
    transitions so every decision-changing mutation has a trace.

Versioning
----------

Each module exposes a module-level ``*_VERSION`` string. When scoring
weights or thresholds change we bump the version and re-score on the
next run; this keeps the estimate_version diff reviewable.
"""

from __future__ import annotations

from headcount.review.audit import AUDIT_VERSION, record_audit
from headcount.review.benchmark_disagreement import (
    BENCHMARK_DISAGREEMENT_VERSION,
    BenchmarkDisagreement,
    detect_benchmark_disagreement,
)
from headcount.review.evaluation import (
    EVALUATION_VERSION,
    Disagreement,
    EvaluationConfig,
    Scoreboard,
    evaluate_against_benchmarks,
    persist_scoreboard,
)
from headcount.review.golden import (
    GOLDEN_VERSION,
    GoldenFixture,
    GoldenMismatch,
    GoldenReport,
    load_goldens_dir,
    run_goldens,
)
from headcount.review.overrides import (
    OVERRIDES_VERSION,
    ActiveOverrides,
    load_active_overrides,
)
from headcount.review.queue import (
    QUEUE_VERSION,
    QueueCandidate,
    upsert_review_items,
)
from headcount.review.scoring import (
    SCORING_VERSION,
    ConfidenceBreakdown,
    ConfidenceInputs,
    score_confidence,
)

__all__ = [
    "AUDIT_VERSION",
    "BENCHMARK_DISAGREEMENT_VERSION",
    "EVALUATION_VERSION",
    "GOLDEN_VERSION",
    "OVERRIDES_VERSION",
    "QUEUE_VERSION",
    "SCORING_VERSION",
    "ActiveOverrides",
    "BenchmarkDisagreement",
    "ConfidenceBreakdown",
    "ConfidenceInputs",
    "Disagreement",
    "EvaluationConfig",
    "GoldenFixture",
    "GoldenMismatch",
    "GoldenReport",
    "QueueCandidate",
    "Scoreboard",
    "detect_benchmark_disagreement",
    "evaluate_against_benchmarks",
    "load_active_overrides",
    "load_goldens_dir",
    "persist_scoreboard",
    "record_audit",
    "run_goldens",
    "score_confidence",
    "upsert_review_items",
]
