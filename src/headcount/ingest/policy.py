"""Public exports for ingest policy primitives.

Phase 4 supplies the real implementations in
:mod:`headcount.ingest.rate_limit`; this module keeps the stable import
path that earlier phases (and tests) already reference.
"""

from __future__ import annotations

from typing import Literal

from headcount.ingest.rate_limit import (
    BudgetExhaustedError,
    BudgetTrippedError,
    CircuitBreaker,
    SourceBudgetStore,
    TokenBucket,
)

FetchOutcome = Literal["ok", "gated", "cache_hit", "error"]

__all__ = [
    "BudgetExhaustedError",
    "BudgetTrippedError",
    "CircuitBreaker",
    "FetchOutcome",
    "SourceBudgetStore",
    "TokenBucket",
]
