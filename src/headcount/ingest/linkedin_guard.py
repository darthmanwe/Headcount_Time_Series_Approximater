"""Shared rate / breaker / budget guard for every LinkedIn caller.

Two distinct surfaces hit ``linkedin.com`` from this codebase:

1. :mod:`headcount.resolution.linkedin_resolver` - probes
   ``/company/<candidate>/`` to discover unknown slugs.
2. :mod:`headcount.ingest.observers.linkedin_public` - probes the
   resolved ``/company/<slug>/`` (+ ``/about/``, ``/people/``) to
   extract a headcount anchor.

Without coordination they shoot independently. The resolver burns the
daily request budget *before* the observer ever runs, no jitter spaces
the resolver's probes, and a 999 streak across one of them does not
trip the other's breaker. In a cohort of 250 - 2,000 companies that
loses every observer attempt to the bot wall.

:class:`LinkedInRateGuard` centralises the bot-defence state so the
cohort runner can construct one and inject it into both. Behaviour is
unchanged when callers omit the guard - they instantiate a private
default - so existing tests keep working.

State shape
-----------
- ``circuit_threshold``        -- consecutive gates allowed before trip
- ``daily_request_budget``     -- total network requests per process
- ``jitter_ms``                -- (lo, hi) inter-request sleep window
- ``cooldown_seconds``         -- how long the breaker stays open after
                                  a trip; once elapsed the streak
                                  resets and a fresh probe is allowed
- ``deferred_companies``       -- company ids that hit a circuit-open
                                  short-circuit and should be retried
                                  by the orchestrator after cooldown

Time is captured via a constructor-injected ``now`` callable so tests
can advance the clock without sleeping.
"""

from __future__ import annotations

import asyncio
import random
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime

from headcount.config.settings import get_settings
from headcount.utils.logging import get_logger
from headcount.utils.metrics import linkedin_gate_total

_log = get_logger("headcount.ingest.linkedin_guard")


def _utcnow() -> datetime:
    return datetime.now(UTC)


@dataclass
class LinkedInRateGuard:
    """Coordinates jitter, daily budget and circuit-breaker for LinkedIn."""

    circuit_threshold: int
    daily_request_budget: int
    jitter_ms: tuple[int, int]
    cooldown_seconds: float
    rng: random.Random = field(default_factory=random.Random)
    now: Callable[[], datetime] = field(default=_utcnow)
    # Optional sleep override (tests). When ``None`` the guard calls
    # ``asyncio.sleep`` resolved at call time so monkey-patches against
    # this module's ``asyncio.sleep`` work without re-constructing the
    # guard.
    sleep: Callable[[float], "asyncio.Future[None] | None"] | None = field(default=None)

    _consecutive_gates: int = field(default=0, init=False)
    _circuit_opened_at: datetime | None = field(default=None, init=False)
    _trip_count: int = field(default=0, init=False)
    _requests_made: int = field(default=0, init=False)
    _last_was_network: bool = field(default=False, init=False)
    _deferred_companies: list[str] = field(default_factory=list, init=False)
    _deferred_seen: set[str] = field(default_factory=set, init=False)

    @classmethod
    def from_settings(
        cls,
        *,
        circuit_threshold: int | None = None,
        daily_request_budget: int | None = None,
        jitter_ms: tuple[int, int] | None = None,
        cooldown_seconds: float | None = None,
        rng: random.Random | None = None,
    ) -> "LinkedInRateGuard":
        """Build a guard with overrides falling back to env-driven settings."""

        settings = get_settings()
        if circuit_threshold is None:
            circuit_threshold = settings.linkedin_public_circuit_breaker_n
        if daily_request_budget is None:
            daily_request_budget = settings.linkedin_public_max_requests_per_run
        if jitter_ms is None:
            jitter_ms = (
                settings.linkedin_public_request_jitter_ms_min,
                settings.linkedin_public_request_jitter_ms_max,
            )
        if cooldown_seconds is None:
            cooldown_seconds = settings.linkedin_public_circuit_cooldown_seconds
        lo, hi = jitter_ms
        if hi < lo:
            lo, hi = hi, lo
        return cls(
            circuit_threshold=max(1, int(circuit_threshold)),
            daily_request_budget=max(0, int(daily_request_budget)),
            jitter_ms=(max(0, int(lo)), max(0, int(hi))),
            cooldown_seconds=max(0.0, float(cooldown_seconds)),
            rng=rng if rng is not None else random.Random(),
        )

    # ------------------------------------------------------------------ state

    @property
    def consecutive_gates(self) -> int:
        return self._consecutive_gates

    @property
    def requests_made(self) -> int:
        return self._requests_made

    @property
    def trip_count(self) -> int:
        """Total times the breaker has tripped this process."""

        return self._trip_count

    @property
    def deferred_companies(self) -> list[str]:
        """Snapshot of company ids parked while the breaker was open."""

        return list(self._deferred_companies)

    def cooldown_remaining(self) -> float:
        """Seconds left on the active cooldown, or 0 when armed."""

        if self._circuit_opened_at is None:
            return 0.0
        elapsed = (self.now() - self._circuit_opened_at).total_seconds()
        remaining = self.cooldown_seconds - elapsed
        return max(0.0, remaining)

    def is_circuit_open(self) -> bool:
        """True iff we are still inside the cooldown window after a trip.

        Once cooldown elapses the streak is reset and ``False`` is
        returned. The next call to :meth:`note_gate` starts a fresh
        streak. This is what lets multi-day cohort runs survive a
        transient ban: pass 1 trips and parks N companies; pass 2 (run
        after the cooldown) finds the breaker armed again.
        """

        if self._circuit_opened_at is None:
            return False
        if self.cooldown_remaining() > 0:
            return True
        # Cooldown elapsed -> rearm and clear streak so the next caller
        # gets a fresh chance.
        _log.info(
            "linkedin_guard_breaker_rearmed",
            cooldown_seconds=self.cooldown_seconds,
            previous_trips=self._trip_count,
        )
        self._circuit_opened_at = None
        self._consecutive_gates = 0
        self._last_was_network = False
        return False

    def is_budget_exhausted(self) -> bool:
        if self.daily_request_budget <= 0:
            return False
        return self._requests_made >= self.daily_request_budget

    # ------------------------------------------------------------------ timing

    async def before_request(self) -> None:
        """Apply jitter only when the previous call hit the network.

        Cache-only runs stay fast. Live runs space adjacent live calls
        by a uniform random delay inside ``jitter_ms``.
        """

        if not self._last_was_network:
            return
        lo, hi = self.jitter_ms
        if hi <= 0:
            return
        delay_ms = self.rng.randint(lo, hi) if hi > lo else lo
        if delay_ms <= 0:
            return
        sleep_fn = self.sleep if self.sleep is not None else asyncio.sleep
        await sleep_fn(delay_ms / 1000.0)

    def record_response(self, *, from_cache: bool) -> None:
        """Update budget + jitter state after a successful HTTP round-trip."""

        is_network = not from_cache
        self._last_was_network = is_network
        if is_network:
            self._requests_made += 1

    # ------------------------------------------------------------------ breaker

    def note_gate(self) -> bool:
        """Record a primary-URL gate. Returns True if the breaker just tripped."""

        self._consecutive_gates += 1
        if (
            self._consecutive_gates >= self.circuit_threshold
            and self._circuit_opened_at is None
        ):
            self._circuit_opened_at = self.now()
            self._trip_count += 1
            linkedin_gate_total.labels(reason="circuit_tripped").inc()
            _log.error(
                "linkedin_guard_circuit_tripped",
                threshold=self.circuit_threshold,
                streak=self._consecutive_gates,
                cooldown_seconds=self.cooldown_seconds,
                trip_count=self._trip_count,
            )
            return True
        return False

    def note_success(self) -> None:
        """Reset the streak after any parsed signal."""

        if self._consecutive_gates:
            _log.info(
                "linkedin_guard_streak_reset",
                previous_streak=self._consecutive_gates,
            )
        self._consecutive_gates = 0

    def defer_company(self, company_id: str) -> None:
        """Park a company id for the orchestrator to retry post-cooldown."""

        if not company_id or company_id in self._deferred_seen:
            return
        self._deferred_companies.append(company_id)
        self._deferred_seen.add(company_id)

    def drain_deferred(self) -> list[str]:
        """Return + clear the list of parked companies (one-shot)."""

        out = list(self._deferred_companies)
        self._deferred_companies.clear()
        self._deferred_seen.clear()
        return out
