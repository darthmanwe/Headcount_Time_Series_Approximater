"""Source adapter contract.

Every Phase 4+ observer (manual, company-web, SEC, Wikidata, LinkedIn...)
implements :class:`AnchorSourceAdapter`. The orchestrator calls
``fetch_current_anchor`` once per company and gets back zero or more
:class:`RawAnchorSignal` rows, each carrying enough provenance that the
database layer can persist a ``source_observation`` + ``company_anchor_observation``
pair without any ambiguity about what was seen, where, and when.

The interface is deliberately narrow:

- Input: a ``CompanyTarget`` with canonical name/domain/LinkedIn so
  adapters can build whatever query they need without reaching back into
  the ORM.
- Output: ``RawAnchorSignal`` objects with an interval-valued headcount
  (``min/point/max`` + ``HeadcountValueKind``) plus the raw text that was
  parsed, a content hash, and the URL. That lets Phase 7 reconcile
  multiple sources without re-fetching anything.
- Errors: adapters raise ``AdapterFetchError`` on transient failures so
  the orchestrator can apply circuit-breaker / retry logic uniformly.
  Returning an empty list means "adapter succeeded but found nothing";
  that distinction matters for the budget store.

All adapters are async so the orchestrator can fan out with bounded
concurrency. Adapters that don't do I/O (e.g. the manual YAML observer)
still declare ``async def`` for interface uniformity.
"""

from __future__ import annotations

import abc
import hashlib
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from typing import Any

from headcount.db.enums import (
    AnchorType,
    HeadcountValueKind,
    ParseStatus,
    SourceEntityType,
    SourceName,
)


class AdapterFetchError(Exception):
    """Transient failure; orchestrator decides whether to retry."""


class AdapterGatedError(AdapterFetchError):
    """Adapter was blocked by the remote (e.g. login wall, 429).

    Distinguished from generic errors so the circuit breaker can trip
    fast: one gated response is more signal than one flaky 500.
    """


@dataclass(frozen=True, slots=True)
class CompanyTarget:
    """Lightweight projection of :class:`headcount.models.Company`.

    Adapters get a stable, side-effect-free handle rather than a
    SQLAlchemy instance so they can be unit-tested without a DB.
    """

    company_id: str
    canonical_name: str
    canonical_domain: str | None
    linkedin_company_url: str | None
    country: str | None = None
    aliases: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class RawAnchorSignal:
    """One anchor observation emitted by an adapter.

    Fields mirror ``company_anchor_observation`` + ``source_observation``
    so the orchestrator can persist both atomically. ``raw_text`` is the
    substring the adapter parsed (e.g. "1,250 employees") so analysts can
    audit what the parser actually saw.
    """

    source_name: SourceName
    entity_type: SourceEntityType
    source_url: str | None
    anchor_month: date
    anchor_type: AnchorType
    headcount_value_min: float
    headcount_value_point: float
    headcount_value_max: float
    headcount_value_kind: HeadcountValueKind
    confidence: float
    raw_text: str
    parser_version: str
    parse_status: ParseStatus = ParseStatus.ok
    note: str | None = None
    normalized_payload: dict[str, Any] = field(default_factory=dict)
    observed_at: datetime = field(default_factory=lambda: datetime.now(tz=UTC))

    def __post_init__(self) -> None:
        if not (self.headcount_value_min <= self.headcount_value_point <= self.headcount_value_max):
            raise ValueError(
                f"headcount interval not monotonic: "
                f"{self.headcount_value_min}<={self.headcount_value_point}<={self.headcount_value_max}"
            )
        if not 0.0 <= self.confidence <= 1.0:
            raise ValueError(f"confidence must be in [0,1], got {self.confidence!r}")

    @property
    def raw_content_hash(self) -> str:
        """Stable SHA-256 over the raw text + URL for dedup / cache keying."""
        payload = f"{self.source_url or ''}\n{self.raw_text}".encode()
        return hashlib.sha256(payload).hexdigest()


@dataclass(slots=True)
class FetchReport:
    """Per-adapter run summary. The orchestrator rolls these up per run."""

    source_name: SourceName
    companies_attempted: int = 0
    signals_emitted: int = 0
    gated: int = 0
    errors: int = 0
    cache_hits: int = 0


class AnchorSourceAdapter(abc.ABC):
    """Async adapter that yields anchor signals for a company.

    Concrete subclasses configure ``SourceName`` plus any parser version.
    The orchestrator owns rate limiting / circuit breaking so adapters
    don't each implement their own.
    """

    source_name: SourceName
    parser_version: str

    def __init__(self, *, parser_version: str | None = None) -> None:
        if not hasattr(self, "source_name"):  # pragma: no cover - developer error
            raise TypeError(f"{type(self).__name__} must set source_name")
        if parser_version is not None:
            self.parser_version = parser_version
        elif not hasattr(self, "parser_version"):  # pragma: no cover
            raise TypeError(f"{type(self).__name__} must set parser_version")

    @abc.abstractmethod
    async def fetch_current_anchor(
        self,
        target: CompanyTarget,
        *,
        context: FetchContext,
    ) -> list[RawAnchorSignal]:
        """Return zero-or-more anchor signals for ``target``.

        An empty list is a valid "nothing found" signal. Raise
        :class:`AdapterFetchError` / :class:`AdapterGatedError` for
        transient issues that should be fed to the circuit breaker.
        """


@dataclass(slots=True)
class FetchContext:
    """Runtime handles passed to every adapter call.

    The orchestrator creates one :class:`FetchContext` per run and reuses
    it across adapters so cache / rate-limit / circuit-breaker state is
    shared. Phase 4b wires in the real ``HttpClient`` and cache; Phase 4c
    fills in ``rate_limiter`` and ``circuit_breaker``. For Phase 4a we
    keep the shape abstract via :class:`typing.Any` so adapters and
    tests can use their own fakes.
    """

    run_id: str
    http: Any
    rate_limiter: Any | None = None
    circuit_breaker: Any | None = None
    budget_store: Any | None = None
    live: bool = False
    method_version: str = "hc-v1"
