"""Read APIs, exports, benchmark comparison (Phase 9).

Public surface:

- :func:`create_app` builds the FastAPI application.
- :mod:`evidence` exposes :func:`build_evidence` for the structured
  ``/companies/{id}/months/{month}/evidence`` payload. Pure function so
  the Streamlit UI in Phase 10 can call it directly without HTTP.
- :mod:`benchmark_comparison` exposes
  :func:`compare_estimates_to_benchmarks` for the ``hc compare-benchmark``
  command and the ``/benchmarks/comparison`` endpoint.
- :mod:`exports` exposes :func:`export_table` for CSV/JSON/Parquet dumps
  of the analyst-facing tables.
"""

from __future__ import annotations

from headcount.serving.api import API_VERSION, create_app
from headcount.serving.benchmark_comparison import (
    COMPARISON_VERSION,
    CompanyComparison,
    ComparisonSummary,
    compare_estimates_to_benchmarks,
)
from headcount.serving.evidence import (
    EVIDENCE_VERSION,
    EvidenceNotFoundError,
    build_evidence,
)
from headcount.serving.exports import (
    EXPORT_VERSION,
    ExportFormatError,
    ExportResult,
    export_table,
)

__all__ = [
    "API_VERSION",
    "COMPARISON_VERSION",
    "EVIDENCE_VERSION",
    "EXPORT_VERSION",
    "CompanyComparison",
    "ComparisonSummary",
    "EvidenceNotFoundError",
    "ExportFormatError",
    "ExportResult",
    "build_evidence",
    "compare_estimates_to_benchmarks",
    "create_app",
    "export_table",
]
