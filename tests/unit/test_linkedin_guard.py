"""Unit tests for :class:`headcount.ingest.linkedin_guard.LinkedInRateGuard`.

Focus areas:

- jitter only fires when the previous request was a network hit
- ``daily_request_budget`` exhausts on network calls only (cache hits free)
- breaker trips after N consecutive gates and short-circuits future probes
- breaker re-arms after the cooldown elapses (key for multi-day runs)
- deferred queue is one-shot and dedupes
"""

from __future__ import annotations

import asyncio
import random
from datetime import UTC, datetime, timedelta

from headcount.ingest.linkedin_guard import LinkedInRateGuard


def _make_guard(
    *,
    threshold: int = 3,
    budget: int = 10,
    jitter: tuple[int, int] = (0, 0),
    cooldown: float = 60.0,
    now: datetime | None = None,
) -> LinkedInRateGuard:
    base_now = now or datetime(2026, 1, 1, tzinfo=UTC)
    clock = {"now": base_now}

    return LinkedInRateGuard(
        circuit_threshold=threshold,
        daily_request_budget=budget,
        jitter_ms=jitter,
        cooldown_seconds=cooldown,
        rng=random.Random(0),
        now=lambda: clock["now"],
    ), clock


class TestBudget:
    def test_cache_hits_do_not_consume_budget(self) -> None:
        guard, _ = _make_guard(budget=2)
        for _ in range(5):
            guard.record_response(from_cache=True)
        assert guard.requests_made == 0
        assert not guard.is_budget_exhausted()

    def test_network_hits_consume_budget(self) -> None:
        guard, _ = _make_guard(budget=2)
        guard.record_response(from_cache=False)
        guard.record_response(from_cache=False)
        assert guard.requests_made == 2
        assert guard.is_budget_exhausted()

    def test_budget_zero_means_disabled(self) -> None:
        guard, _ = _make_guard(budget=0)
        for _ in range(100):
            guard.record_response(from_cache=False)
        assert not guard.is_budget_exhausted()


class TestJitter:
    def test_no_sleep_when_previous_was_cache(self) -> None:
        guard, _ = _make_guard(jitter=(50, 50))
        sleeps: list[float] = []

        async def _sleep(s: float) -> None:
            sleeps.append(s)

        guard.sleep = _sleep
        guard.record_response(from_cache=True)
        asyncio.run(guard.before_request())
        assert sleeps == []

    def test_sleeps_when_previous_was_network(self) -> None:
        guard, _ = _make_guard(jitter=(100, 100))
        sleeps: list[float] = []

        async def _sleep(s: float) -> None:
            sleeps.append(s)

        guard.sleep = _sleep
        guard.record_response(from_cache=False)
        asyncio.run(guard.before_request())
        assert sleeps == [0.1]


class TestBreaker:
    def test_trips_at_threshold(self) -> None:
        guard, _ = _make_guard(threshold=3)
        assert not guard.note_gate()
        assert not guard.note_gate()
        tripped = guard.note_gate()
        assert tripped is True
        assert guard.is_circuit_open()
        assert guard.trip_count == 1

    def test_success_resets_streak(self) -> None:
        guard, _ = _make_guard(threshold=3)
        guard.note_gate()
        guard.note_gate()
        guard.note_success()
        # Next gate is the start of a fresh streak; should NOT trip.
        assert not guard.note_gate()
        assert not guard.is_circuit_open()

    def test_rearm_after_cooldown(self) -> None:
        guard, clock = _make_guard(threshold=2, cooldown=300.0)
        guard.note_gate()
        guard.note_gate()
        assert guard.is_circuit_open()
        # Half-way through the cooldown the breaker is still open.
        clock["now"] = clock["now"] + timedelta(seconds=150)
        assert guard.is_circuit_open()
        # Past the cooldown the breaker re-arms and the streak resets.
        clock["now"] = clock["now"] + timedelta(seconds=200)
        assert not guard.is_circuit_open()
        assert guard.consecutive_gates == 0
        # And a single new gate after re-arming does NOT trip a 2-threshold
        # breaker (proves the streak counter genuinely went back to 0).
        assert not guard.note_gate()
        assert not guard.is_circuit_open()


class TestDeferredQueue:
    def test_dedupes_company_ids(self) -> None:
        guard, _ = _make_guard()
        guard.defer_company("c1")
        guard.defer_company("c1")
        guard.defer_company("c2")
        assert guard.deferred_companies == ["c1", "c2"]

    def test_drain_is_one_shot(self) -> None:
        guard, _ = _make_guard()
        guard.defer_company("c1")
        guard.defer_company("c2")
        assert guard.drain_deferred() == ["c1", "c2"]
        assert guard.deferred_companies == []
        # And the dedupe set is also cleared so a fresh defer works.
        guard.defer_company("c1")
        assert guard.deferred_companies == ["c1"]

    def test_skips_empty_company_id(self) -> None:
        guard, _ = _make_guard()
        guard.defer_company("")
        assert guard.deferred_companies == []
