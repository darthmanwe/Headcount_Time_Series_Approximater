"""Deterministic canonical resolver (Phase 3).

Promotes ``company_candidate`` rows into canonical ``company`` rows, using
name/domain/LinkedIn keys computed by :mod:`headcount.resolution.normalize`.
Idempotent: replaying the resolver on the same inputs must not create
duplicate companies, aliases, source links, or relations.

Data flow
---------
1. Build in-memory indices from existing ``company`` / ``company_alias`` /
   ``company_source_link`` so we resolve without O(N) per-candidate queries.
2. Walk candidates ordered by (workbook, sheet, row) so re-runs produce a
   stable insertion order and therefore stable IDs in test fixtures.
3. For each candidate:
   - Gather benchmark domain/LinkedIn hints so we can match by the richest
     identifiers available, even if the seed workbook only has a name.
   - Try matching by domain -> LinkedIn -> name key. First match wins; any
     subsequent candidate in the same batch that hits the same keys is
     treated as the same canonical company.
   - If unmatched, create a new ``company`` row with a ``legal`` alias for
     the raw name.
   - Backfill the candidate's ``company_id`` and flip its status.
   - Write ``company_source_link`` for any LinkedIn URL we observed.
4. After all candidates resolve, walk ``benchmark_event_candidate`` rows:
   parse acquirer / new-name references and emit ``company_relation`` when
   both sides exist in the canonical store. Unresolved acquirers are
   counted in the result so Phase 8 can enqueue them for analyst review.

The resolver never fetches anything over the network; Phase 4+ observers
are expected to feed ``company_anchor_observation`` / ``source_observation``
which this module leaves untouched.
"""

from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date

from sqlalchemy import select
from sqlalchemy.orm import Session

from headcount.db.enums import (
    AliasType,
    BenchmarkEventHintType,
    CandidateStatus,
    CompanyStatus,
    PriorityTier,
    RelationKind,
    SourceName,
)
from headcount.models.benchmark import (
    BenchmarkEventCandidate,
    BenchmarkObservation,
)
from headcount.models.company import Company
from headcount.models.company_alias import CompanyAlias
from headcount.models.company_candidate import CompanyCandidate
from headcount.models.company_relation import CompanyRelation
from headcount.models.company_source_link import CompanySourceLink
from headcount.resolution.normalize import (
    NormalizedCompany,
    normalize_company,
    normalize_linkedin_slug,
    normalize_name_key,
)
from headcount.utils.logging import get_logger

_log = get_logger("headcount.resolution.resolver")

_ACQUIRER_RE = re.compile(
    r"acquired\s+by\s+([A-Za-z0-9][A-Za-z0-9 &\-.,'/()]+?)(?:\s+(?:in|on|during)\b|[.,;]|$)",
    flags=re.IGNORECASE,
)
_RENAMED_RE = re.compile(
    r"(?:rebranded|renamed|now)\s+(?:to|as)\s+([A-Za-z0-9][A-Za-z0-9 &\-.,'/()]+?)(?:\s+(?:in|on|during)\b|[.,;]|$)",
    flags=re.IGNORECASE,
)


@dataclass(slots=True)
class ResolveResult:
    candidates_scanned: int = 0
    candidates_resolved: int = 0
    candidates_already_resolved: int = 0
    candidates_failed: int = 0
    companies_created: int = 0
    aliases_created: int = 0
    source_links_created: int = 0
    relations_created: int = 0
    unresolved_acquirers: list[str] = field(default_factory=list)
    unresolved_renames: list[str] = field(default_factory=list)


@dataclass(slots=True)
class _Index:
    by_domain: dict[str, Company] = field(default_factory=dict)
    by_linkedin: dict[str, Company] = field(default_factory=dict)
    by_name_key: dict[str, Company] = field(default_factory=dict)
    alias_keys: set[tuple[str, str, str]] = field(default_factory=set)
    linkedin_urls: dict[tuple[str, SourceName], str] = field(default_factory=dict)

    def register(self, company: Company) -> None:
        if company.canonical_domain:
            self.by_domain.setdefault(company.canonical_domain, company)
        slug = normalize_linkedin_slug(company.linkedin_company_url)
        if slug:
            self.by_linkedin.setdefault(slug, company)
        name_key = normalize_name_key(company.canonical_name)
        if name_key:
            self.by_name_key.setdefault(name_key, company)


def _build_index(session: Session) -> _Index:
    idx = _Index()
    for company in session.execute(select(Company)).scalars():
        idx.register(company)
    for alias in session.execute(select(CompanyAlias)).scalars():
        key = normalize_name_key(alias.alias_name)
        if key:
            idx.alias_keys.add((alias.company_id, key, alias.alias_type.value))
            idx.by_name_key.setdefault(key, _lookup_by_id(session, alias.company_id))
    for link in session.execute(select(CompanySourceLink)).scalars():
        idx.linkedin_urls[(link.company_id, link.source_name)] = link.source_url
    return idx


def _lookup_by_id(session: Session, company_id: str) -> Company:
    return session.get(Company, company_id)  # type: ignore[return-value]


def _candidate_hints(
    candidate: CompanyCandidate,
    obs_index: dict[str, list[BenchmarkObservation]],
) -> tuple[str | None, str | None]:
    """Return the richest (domain, linkedin) pair observed for a candidate."""
    domain = candidate.domain
    linkedin: str | None = None
    for obs in obs_index.get(candidate.id, []):
        if domain is None and obs.company_domain_raw:
            domain = obs.company_domain_raw
        if linkedin is None and obs.linkedin_url_raw:
            linkedin = obs.linkedin_url_raw
        if domain and linkedin:
            break
    return domain, linkedin


def _match_company(idx: _Index, normalized: NormalizedCompany) -> Company | None:
    if normalized.domain_key and (hit := idx.by_domain.get(normalized.domain_key)):
        return hit
    if normalized.linkedin_slug and (hit := idx.by_linkedin.get(normalized.linkedin_slug)):
        return hit
    if normalized.name_key and (hit := idx.by_name_key.get(normalized.name_key)):
        return hit
    return None


def _ensure_alias(
    session: Session,
    idx: _Index,
    *,
    company: Company,
    alias_name: str,
    alias_type: AliasType,
    source: str,
    confidence: float,
    result: ResolveResult,
) -> None:
    key = normalize_name_key(alias_name)
    if not key:
        return
    dedup_key = (company.id, key, alias_type.value)
    if dedup_key in idx.alias_keys:
        return
    session.add(
        CompanyAlias(
            company_id=company.id,
            alias_name=alias_name.strip(),
            alias_type=alias_type,
            confidence=confidence,
            source=source,
        )
    )
    idx.alias_keys.add(dedup_key)
    idx.by_name_key.setdefault(key, company)
    result.aliases_created += 1


def _ensure_source_link(
    session: Session,
    idx: _Index,
    *,
    company: Company,
    source_name: SourceName,
    url: str,
    confidence: float,
    result: ResolveResult,
) -> None:
    existing = idx.linkedin_urls.get((company.id, source_name))
    if existing == url:
        return
    if existing is not None:
        return
    session.add(
        CompanySourceLink(
            company_id=company.id,
            source_name=source_name,
            source_url=url,
            is_primary=True,
            confidence=confidence,
        )
    )
    idx.linkedin_urls[(company.id, source_name)] = url
    result.source_links_created += 1


def _promote_candidate(
    session: Session,
    idx: _Index,
    *,
    candidate: CompanyCandidate,
    normalized: NormalizedCompany,
    linkedin_raw: str | None,
    default_tier: PriorityTier,
    result: ResolveResult,
) -> Company:
    company = _match_company(idx, normalized)
    raw_name = candidate.company_name.strip()
    if company is None:
        company = Company(
            canonical_name=normalized.display_name or raw_name,
            canonical_domain=normalized.domain,
            linkedin_company_url=linkedin_raw.strip() if linkedin_raw else None,
            status=CompanyStatus.active,
            priority_tier=default_tier,
        )
        session.add(company)
        session.flush()
        idx.register(company)
        result.companies_created += 1
    else:
        if normalized.domain and not company.canonical_domain:
            company.canonical_domain = normalized.domain
            idx.by_domain.setdefault(normalized.domain, company)
        if linkedin_raw and not company.linkedin_company_url:
            company.linkedin_company_url = linkedin_raw.strip()
            slug = normalize_linkedin_slug(linkedin_raw)
            if slug:
                idx.by_linkedin.setdefault(slug, company)

    alias_type = AliasType.legal if normalized.legal_suffix else AliasType.dba
    _ensure_alias(
        session,
        idx,
        company=company,
        alias_name=raw_name,
        alias_type=alias_type,
        source=f"candidate:{candidate.source_workbook}:{candidate.source_sheet}",
        confidence=0.9,
        result=result,
    )

    if linkedin_raw:
        _ensure_source_link(
            session,
            idx,
            company=company,
            source_name=SourceName.linkedin_public,
            url=linkedin_raw.strip(),
            confidence=0.6,
            result=result,
        )

    candidate.company_id = company.id
    candidate.status = CandidateStatus.resolved
    return company


def _extract_relation_target(description: str, pattern: re.Pattern[str]) -> str | None:
    match = pattern.search(description)
    if match is None:
        return None
    target = match.group(1).strip(" .,;\"'()")
    return target or None


def _emit_relations(
    session: Session,
    idx: _Index,
    *,
    result: ResolveResult,
) -> None:
    existing_pairs: set[tuple[str, str, str]] = set()
    for rel in session.execute(select(CompanyRelation)).scalars():
        existing_pairs.add((rel.parent_id, rel.child_id, rel.kind.value))

    events = session.execute(
        select(BenchmarkEventCandidate).order_by(
            BenchmarkEventCandidate.source_workbook,
            BenchmarkEventCandidate.source_sheet,
            BenchmarkEventCandidate.source_row_index,
        )
    ).scalars()

    for event in events:
        child = _child_company_for_event(session, idx, event)
        if child is None:
            continue

        parent_name, relation_kind = _relation_pattern_for(event)
        if parent_name is None or relation_kind is None:
            continue

        parent_key = normalize_name_key(parent_name)
        parent = idx.by_name_key.get(parent_key) if parent_key else None
        if parent is None:
            (
                result.unresolved_acquirers
                if relation_kind is RelationKind.acquired
                else result.unresolved_renames
            ).append(parent_name)
            continue
        if parent.id == child.id:
            continue

        pair_key = (parent.id, child.id, relation_kind.value)
        if pair_key in existing_pairs:
            continue

        session.add(
            CompanyRelation(
                parent_id=parent.id,
                child_id=child.id,
                kind=relation_kind,
                effective_month=_coerce_month(event.event_month_hint),
                confidence=0.6,
                note=event.description,
            )
        )
        existing_pairs.add(pair_key)
        result.relations_created += 1


def _child_company_for_event(
    session: Session,
    idx: _Index,
    event: BenchmarkEventCandidate,
) -> Company | None:
    if event.company_id is not None:
        return session.get(Company, event.company_id)
    if event.company_candidate_id is None:
        return None
    candidate = session.get(CompanyCandidate, event.company_candidate_id)
    if candidate is None or candidate.company_id is None:
        return None
    return session.get(Company, candidate.company_id)


def _relation_pattern_for(
    event: BenchmarkEventCandidate,
) -> tuple[str | None, RelationKind | None]:
    if event.hint_type is BenchmarkEventHintType.acquisition:
        return _extract_relation_target(event.description, _ACQUIRER_RE), RelationKind.acquired
    if event.hint_type is BenchmarkEventHintType.rebrand:
        return _extract_relation_target(event.description, _RENAMED_RE), RelationKind.renamed
    return None, None


def _coerce_month(value: date | None) -> date | None:
    if value is None:
        return None
    return value.replace(day=1)


def _backfill_benchmark_links(session: Session) -> None:
    """Set benchmark.company_id once candidates are canonicalized."""
    for obs in session.execute(
        select(BenchmarkObservation).where(
            BenchmarkObservation.company_candidate_id.is_not(None),
            BenchmarkObservation.company_id.is_(None),
        )
    ).scalars():
        candidate = session.get(CompanyCandidate, obs.company_candidate_id)
        if candidate is not None and candidate.company_id is not None:
            obs.company_id = candidate.company_id
    for ev in session.execute(
        select(BenchmarkEventCandidate).where(
            BenchmarkEventCandidate.company_candidate_id.is_not(None),
            BenchmarkEventCandidate.company_id.is_(None),
        )
    ).scalars():
        candidate = session.get(CompanyCandidate, ev.company_candidate_id)
        if candidate is not None and candidate.company_id is not None:
            ev.company_id = candidate.company_id


def resolve_candidates(
    session: Session,
    *,
    default_priority_tier: PriorityTier = PriorityTier.P1,
    only_pending: bool = True,
) -> ResolveResult:
    """Run the deterministic canonical resolver over ``company_candidate``.

    Parameters
    ----------
    default_priority_tier:
        Priority assigned to newly created ``Company`` rows. Analysts can
        promote / demote via ``manual_override`` in Phase 8.
    only_pending:
        When ``True`` (default) we skip candidates already marked resolved,
        which makes incremental re-runs cheap. Set to ``False`` to re-run
        resolution across every candidate (useful after changing the
        normalization rules).
    """
    result = ResolveResult()
    idx = _build_index(session)

    stmt = select(CompanyCandidate).order_by(
        CompanyCandidate.source_workbook,
        CompanyCandidate.source_sheet,
        CompanyCandidate.source_row_index,
    )
    if only_pending:
        stmt = stmt.where(CompanyCandidate.status == CandidateStatus.pending_resolution)
    candidates = list(session.execute(stmt).scalars())

    obs_index: dict[str, list[BenchmarkObservation]] = defaultdict(list)
    for obs in session.execute(
        select(BenchmarkObservation).where(
            BenchmarkObservation.company_candidate_id.is_not(None),
        )
    ).scalars():
        if obs.company_candidate_id is not None:
            obs_index[obs.company_candidate_id].append(obs)

    for candidate in candidates:
        result.candidates_scanned += 1
        if candidate.status == CandidateStatus.resolved and candidate.company_id is not None:
            result.candidates_already_resolved += 1
            continue
        try:
            domain, linkedin = _candidate_hints(candidate, obs_index)
            normalized = normalize_company(
                candidate.company_name,
                raw_domain=domain,
                raw_linkedin=linkedin,
            )
            if not normalized.name_key and not normalized.domain_key:
                candidate.status = CandidateStatus.failed
                candidate.note = "unresolvable: blank name and domain after normalization"
                result.candidates_failed += 1
                continue
            _promote_candidate(
                session,
                idx,
                candidate=candidate,
                normalized=normalized,
                linkedin_raw=linkedin,
                default_tier=default_priority_tier,
                result=result,
            )
            result.candidates_resolved += 1
        except Exception as exc:  # pragma: no cover - defensive guard
            candidate.status = CandidateStatus.failed
            candidate.note = f"resolver error: {exc!r}"
            result.candidates_failed += 1
            _log.error(
                "resolver_candidate_failed",
                candidate_id=candidate.id,
                error=repr(exc),
            )

    session.flush()
    _backfill_benchmark_links(session)
    _emit_relations(session, idx, result=result)

    _log.info(
        "resolver_summary",
        scanned=result.candidates_scanned,
        resolved=result.candidates_resolved,
        already_resolved=result.candidates_already_resolved,
        failed=result.candidates_failed,
        companies_created=result.companies_created,
        aliases_created=result.aliases_created,
        source_links_created=result.source_links_created,
        relations_created=result.relations_created,
        unresolved_acquirers=len(result.unresolved_acquirers),
        unresolved_renames=len(result.unresolved_renames),
    )
    return result
