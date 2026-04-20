"""Offline-only seed loaders for ``test_source/`` spreadsheets.

These never perform network I/O: they read local Excel files, preserve
workbook/sheet/row provenance, and write into ``company_candidate``,
``benchmark_observation`` and ``benchmark_event_candidate``.
"""

from __future__ import annotations

from headcount.ingest.seeds.benchmark_loader import BenchmarkLoadResult, load_benchmarks
from headcount.ingest.seeds.candidate_importer import (
    CandidateImportResult,
    import_candidates,
)

__all__ = [
    "BenchmarkLoadResult",
    "CandidateImportResult",
    "import_candidates",
    "load_benchmarks",
]
