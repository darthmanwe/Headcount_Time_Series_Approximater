"""Stable, serialized string enums used across ORM + Pydantic layers.

Keeping them in one module means migrations, schemas, and estimation code
all reference the same canonical values.
"""

from __future__ import annotations

import enum


class CompanyStatus(enum.StrEnum):
    active = "active"
    merged = "merged"
    defunct = "defunct"
    unknown = "unknown"


class PriorityTier(enum.StrEnum):
    P0 = "P0"
    P1 = "P1"
    P2 = "P2"


class AliasType(enum.StrEnum):
    legal = "legal"
    dba = "dba"
    old_name = "old_name"
    acquired_by = "acquired_by"
    subsidiary = "subsidiary"
    brand = "brand"


class SourceName(enum.StrEnum):
    manual = "manual"
    company_web = "company_web"
    linkedin_public = "linkedin_public"
    sec = "sec"
    wikidata = "wikidata"
    free_api = "free_api"
    benchmark = "benchmark"
    wayback = "wayback"


class SourceEntityType(enum.StrEnum):
    company = "company"
    person_profile = "person_profile"
    press_release = "press_release"
    manual = "manual"
    benchmark = "benchmark"


class ParseStatus(enum.StrEnum):
    ok = "ok"
    failed = "failed"
    partial = "partial"
    gated = "gated"


class AnchorType(enum.StrEnum):
    current_headcount_anchor = "current_headcount_anchor"
    historical_statement = "historical_statement"
    manual_anchor = "manual_anchor"
    reconciled_anchor = "reconciled_anchor"


class HeadcountValueKind(enum.StrEnum):
    exact = "exact"
    range = "range"
    bucket = "bucket"


class EventType(enum.StrEnum):
    acquisition = "acquisition"
    merger = "merger"
    rebrand = "rebrand"
    spinout = "spinout"
    layoff = "layoff"
    parent_sub_reassignment = "parent_sub_reassignment"
    stealth_to_public = "stealth_to_public"


class EventSourceClass(enum.StrEnum):
    first_party = "first_party"
    benchmark = "benchmark"
    press = "press"
    manual_hint = "manual_hint"
    manual = "manual"


class RelationKind(enum.StrEnum):
    acquired = "acquired"
    renamed = "renamed"
    spinout = "spinout"
    subsidiary = "subsidiary"
    brand = "brand"


class ConfidenceBand(enum.StrEnum):
    high = "high"
    medium = "medium"
    low = "low"
    manual_review_required = "manual_review_required"


class EstimateMethod(enum.StrEnum):
    scaled_ratio = "scaled_ratio"
    scaled_ratio_coverage_corrected = "scaled_ratio_coverage_corrected"
    degraded_current_only = "degraded_current_only"
    suppressed_low_sample = "suppressed_low_sample"
    interpolated_multi_anchor = "interpolated_multi_anchor"


class EstimateVersionStatus(enum.StrEnum):
    draft = "draft"
    published = "published"
    superseded = "superseded"


class RunKind(enum.StrEnum):
    full = "full"
    refresh = "refresh"
    company = "company"


class RunStatus(enum.StrEnum):
    started = "started"
    running = "running"
    succeeded = "succeeded"
    failed = "failed"
    partial = "partial"
    cancelled = "cancelled"


class CompanyRunStage(enum.StrEnum):
    canonicalize = "canonicalize"
    collect_anchors = "collect_anchors"
    collect_employment = "collect_employment"
    estimate_series = "estimate_series"
    score_confidence = "score_confidence"
    export = "export"


class CompanyRunStageStatus(enum.StrEnum):
    pending = "pending"
    running = "running"
    succeeded = "succeeded"
    failed = "failed"
    gated = "gated"
    review = "review"


class ReviewStatus(enum.StrEnum):
    open = "open"
    assigned = "assigned"
    resolved = "resolved"
    dismissed = "dismissed"


class ReviewReason(enum.StrEnum):
    low_confidence = "low_confidence"
    resolution_ambiguity = "resolution_ambiguity"
    linkedin_gated = "linkedin_gated"
    benchmark_disagreement = "benchmark_disagreement"
    degraded_run = "degraded_run"
    anomaly = "anomaly"
    anchor_disagreement = "anchor_disagreement"
    manual = "manual"


class OverrideField(enum.StrEnum):
    canonical_company = "canonical_company"
    current_anchor = "current_anchor"
    event_segment = "event_segment"
    estimate_suppress_window = "estimate_suppress_window"
    company_relation = "company_relation"
    person_identity_merge = "person_identity_merge"


class CandidateStatus(enum.StrEnum):
    pending_resolution = "pending_resolution"
    resolved = "resolved"
    failed = "failed"


class SourceBudgetStatus(enum.StrEnum):
    open = "open"
    tripped = "tripped"
    exhausted = "exhausted"


class BenchmarkProvider(enum.StrEnum):
    zeeshan = "zeeshan"
    harmonic = "harmonic"
    linkedin = "linkedin"


class BenchmarkMetric(enum.StrEnum):
    headcount_current = "headcount_current"
    headcount_6m_ago = "headcount_6m_ago"
    headcount_1y_ago = "headcount_1y_ago"
    headcount_2y_ago = "headcount_2y_ago"
    growth_6m_pct = "growth_6m_pct"
    growth_1y_pct = "growth_1y_pct"
    growth_2y_pct = "growth_2y_pct"
    web_traffic = "web_traffic"


class BenchmarkEventHintType(enum.StrEnum):
    acquisition = "acquisition"
    rebrand = "rebrand"
    merger = "merger"
    unknown = "unknown"


class BenchmarkEventCandidateStatus(enum.StrEnum):
    pending_merge = "pending_merge"
    merged = "merged"
    rejected = "rejected"
