"""Async token-bucket rate limiter and DB-backed source budget store.

Two complementary concerns share this module:

- :class:`TokenBucket` bounds the *instantaneous* rate of outbound
  requests. It is purely in-memory and survives only for the lifetime
  of a single run. Each source gets its own bucket so one slow adapter
  can't starve another.
- :class:`SourceBudgetStore` enforces the *per-run quota* persisted in
  ``source_budget``. That quota is configurable per source (e.g. a
  conservative LinkedIn budget) and survives process restarts so a
  crashed run doesn't silently exceed the cap on resume.

Together they give us the guarantee the plan requires: "fail closed"
behavior when a source is exhausted or has tripped its circuit breaker,
never a silent retry loop.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.orm import Session

from headcount.db.enums import SourceBudgetStatus, SourceName
from headcount.models.source_budget import SourceBudget


class BudgetExhaustedError(RuntimeError):
    """Raised when a source budget has been exhausted for this run."""


class BudgetTrippedError(RuntimeError):
    """Raised when a source's circuit breaker has tripped this run."""


@dataclass(slots=True)
class TokenBucket:
    """Classic leaky-bucket limiter: async safe, monotonic-clock based.

    ``refill_per_second`` controls the steady-state rate (e.g. 0.1 ->
    one token every 10s). ``capacity`` controls burst size. Tokens are
    floating-point so sub-1 RPS limits are expressed cleanly.
    """

    capacity: float
    refill_per_second: float
    _tokens: float = 0.0
    _last_refill: float = 0.0
    _lock: asyncio.Lock | None = None

    def __post_init__(self) -> None:
        if self.capacity <= 0:
            raise ValueError("capacity must be positive")
        if self.refill_per_second < 0:
            raise ValueError("refill_per_second must be non-negative")
        self._tokens = self.capacity
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_refill
        if elapsed <= 0:
            return
        self._tokens = min(self.capacity, self._tokens + elapsed * self.refill_per_second)
        self._last_refill = now

    async def acquire(self, tokens: float = 1.0) -> None:
        """Block until ``tokens`` are available, then consume them."""
        if tokens > self.capacity:
            raise ValueError(f"requested {tokens} tokens exceeds capacity {self.capacity}")
        assert self._lock is not None  # set in __post_init__
        while True:
            async with self._lock:
                self._refill()
                if self._tokens >= tokens:
                    self._tokens -= tokens
                    return
                if self.refill_per_second <= 0:
                    raise BudgetExhaustedError("token bucket empty and refill rate is zero")
                deficit = tokens - self._tokens
                wait = deficit / self.refill_per_second
            await asyncio.sleep(wait)


class SourceBudgetStore:
    """Persistent budget / circuit-breaker state for a run.

    Every mutation is committed immediately so a crashing process never
    loses accounting. Consumers should hold a short-lived reference and
    call ``reserve`` + ``record_outcome`` around each adapter call.
    """

    def __init__(
        self,
        session: Session,
        *,
        run_id: str,
        default_allowed: int = 1_000_000,
        trip_after_n_failures: int = 5,
    ) -> None:
        self._session = session
        self._run_id = run_id
        self._default_allowed = default_allowed
        self._trip_after = trip_after_n_failures
        self._cache: dict[SourceName, SourceBudget] = {}

    def _get_or_create(self, source: SourceName, *, allowed: int | None = None) -> SourceBudget:
        if source in self._cache:
            return self._cache[source]
        stmt = select(SourceBudget).where(
            SourceBudget.run_id == self._run_id,
            SourceBudget.source_name == source,
        )
        row = self._session.execute(stmt).scalar_one_or_none()
        if row is None:
            row = SourceBudget(
                run_id=self._run_id,
                source_name=source,
                requests_allowed=allowed if allowed is not None else self._default_allowed,
            )
            self._session.add(row)
            self._session.flush()
        self._cache[source] = row
        return row

    def reserve(self, source: SourceName, *, allowed: int | None = None) -> SourceBudget:
        """Ensure a row exists; raise if exhausted / tripped.

        Call this before performing any outbound work so the check is
        cheap and consistent across adapters.
        """
        row = self._get_or_create(source, allowed=allowed)
        if row.status is SourceBudgetStatus.tripped:
            raise BudgetTrippedError(
                f"source {source.value} circuit breaker tripped: {row.trip_reason}"
            )
        if row.status is SourceBudgetStatus.exhausted or row.requests_used >= row.requests_allowed:
            row.status = SourceBudgetStatus.exhausted
            self._session.flush()
            raise BudgetExhaustedError(
                f"source {source.value} budget exhausted: "
                f"{row.requests_used}/{row.requests_allowed}"
            )
        return row

    def record_outcome(self, source: SourceName, *, outcome: str) -> None:
        """Apply the outcome to budget / breaker state.

        ``outcome`` is one of "ok", "cache_hit", "gated", "error". Only
        real network calls (not cache hits) consume budget; any non-ok
        outcome increments the breaker's consecutive-failure counter and
        may trip it.
        """
        row = self._get_or_create(source)
        if outcome in {"ok", "gated", "error"}:
            row.requests_used += 1
        if outcome in {"ok", "cache_hit"}:
            row.consecutive_failures = 0
        else:
            row.consecutive_failures += 1
            if row.consecutive_failures >= self._trip_after:
                row.status = SourceBudgetStatus.tripped
                row.trip_reason = f"{row.consecutive_failures} consecutive {outcome}"
        if row.requests_used >= row.requests_allowed and row.status is SourceBudgetStatus.open:
            row.status = SourceBudgetStatus.exhausted
        self._session.flush()

    def remaining(self, source: SourceName) -> int:
        row = self._get_or_create(source)
        return max(0, row.requests_allowed - row.requests_used)

    def status(self, source: SourceName) -> SourceBudgetStatus:
        return self._get_or_create(source).status


@dataclass(slots=True)
class CircuitBreaker:
    """Thin read/write wrapper over a single :class:`SourceBudget` row.

    Kept separate from the store so adapters can hold a reference to a
    cheap value object without retaining a session handle.
    """

    store: SourceBudgetStore
    source: SourceName

    def is_open(self) -> bool:
        return self.store.status(self.source) is SourceBudgetStatus.tripped

    def should_short_circuit(self) -> bool:
        status = self.store.status(self.source)
        return status in {SourceBudgetStatus.tripped, SourceBudgetStatus.exhausted}

    def record(self, outcome: str) -> None:
        self.store.record_outcome(self.source, outcome=outcome)
