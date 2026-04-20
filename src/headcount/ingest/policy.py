"""Forward-ref stubs for source budgets and circuit breakers.

Phase 1 defines the typed surface so later phases can import and call
``reserve``/``record_outcome`` without reshaping consumers. Phase 4 fills
the implementations and wires them to ``source_budget`` rows.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Protocol

FetchOutcome = Literal["ok", "gated", "cache_hit", "error"]


class SourceBudgetStore(Protocol):
    """Persistence surface for ``source_budget`` rows."""

    def reserve(self, run_id: str, source_name: str, *, allowed: int) -> int:
        """Return the budget row id, creating it if missing."""

    def increment_usage(self, budget_id: str, *, delta: int = 1) -> None: ...

    def trip(self, budget_id: str, *, reason: str) -> None: ...

    def is_tripped(self, budget_id: str) -> bool: ...


@dataclass(slots=True)
class TokenBucket:
    """Token-bucket limiter. Phase-1 stub: no-op refill, no persistence.

    Contract is final so Phase 4 only fills ``acquire``/``refill``:

    - ``capacity``: maximum tokens the bucket can hold.
    - ``refill_per_second``: tokens added per real-time second.
    - ``min_delay_ms``: minimum spacing between successful acquires.
    """

    capacity: int
    refill_per_second: float
    min_delay_ms: int = 0

    def acquire(self, tokens: int = 1) -> bool:  # pragma: no cover - Phase 4
        raise NotImplementedError("TokenBucket.acquire is implemented in Phase 4")

    def refill(self) -> None:  # pragma: no cover - Phase 4
        raise NotImplementedError("TokenBucket.refill is implemented in Phase 4")


@dataclass(slots=True)
class CircuitBreaker:
    """N-consecutive-failures breaker keyed by ``source_name``.

    Phase-1 stub; Phase 4 wires the counter to ``source_budget`` so state
    survives process restarts and is visible in the review UI.
    """

    trip_after_n: int
    consecutive_failures: int = 0
    is_open: bool = False

    def record(self, outcome: FetchOutcome) -> None:  # pragma: no cover - Phase 4
        raise NotImplementedError("CircuitBreaker.record is implemented in Phase 4")

    def should_short_circuit(self) -> bool:  # pragma: no cover - Phase 4
        raise NotImplementedError(
            "CircuitBreaker.should_short_circuit is implemented in Phase 4"
        )
