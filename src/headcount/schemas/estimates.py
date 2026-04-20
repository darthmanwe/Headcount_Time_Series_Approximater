"""Pydantic contracts for estimation outputs and responses."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator

from headcount.db.enums import (
    CompanyRunStageStatus,
    ConfidenceBand,
    EstimateMethod,
    ReviewReason,
)


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)


class HeadcountSeriesPoint(StrictModel):
    month: date
    estimated_headcount: float = Field(ge=0)
    estimated_headcount_min: float = Field(ge=0)
    estimated_headcount_max: float = Field(ge=0)
    public_profile_count: int = Field(ge=0)
    scaled_from_anchor_value: float = Field(ge=0)
    method: EstimateMethod
    confidence_band: ConfidenceBand
    needs_review: bool = False
    suppression_reason: str | None = None

    @model_validator(mode="after")
    def _check_interval(self) -> HeadcountSeriesPoint:
        if not (
            self.estimated_headcount_min <= self.estimated_headcount <= self.estimated_headcount_max
        ):
            raise ValueError("estimate interval must satisfy min <= point <= max")
        return self


class GrowthWindow(StrictModel):
    window: str = Field(pattern=r"^(6m|1y|2y)$")
    start_month: date
    end_month: date
    start_value: float | None = Field(default=None, ge=0)
    end_value: float | None = Field(default=None, ge=0)
    absolute_delta: float | None = None
    percent_delta: float | None = None
    confidence_band: ConfidenceBand
    suppressed: bool = False
    suppression_reason: str | None = None


class ConfidenceBreakdown(StrictModel):
    band: ConfidenceBand
    components: dict[str, float]
    notes: dict[str, str] = Field(default_factory=dict)


class AnchorReconciliationInput(StrictModel):
    source_name: str
    point: float
    minimum: float
    maximum: float
    weight: float
    confidence: float


class AnchorReconciliationPayload(StrictModel):
    chosen_point: float
    chosen_min: float
    chosen_max: float
    inputs: list[AnchorReconciliationInput]
    weights: dict[str, float]
    rationale: str | None = None


class EstimationRunRequest(StrictModel):
    company_batch: str | None = None
    company_ids: list[str] = Field(default_factory=list)
    cutoff_month: date | None = None
    resume: bool = False
    dry_run: bool = False


class EvidenceTraceResponse(StrictModel):
    company_id: str
    canonical_name: str
    anchors: list[dict[str, Any]]
    employment_sample: list[dict[str, Any]]
    events: list[dict[str, Any]]
    reconciliation: AnchorReconciliationPayload | None = None
    confidence: ConfidenceBreakdown
    override_history: list[dict[str, Any]]
    series: list[HeadcountSeriesPoint]
    growth: list[GrowthWindow]


class RunStageSummary(StrictModel):
    stage: str
    counts: dict[CompanyRunStageStatus, int]


class RunStatusResponse(StrictModel):
    run_id: str
    status: str
    kind: str
    started_at: datetime
    finished_at: datetime | None = None
    cutoff_month: date
    method_version: str
    stages: list[RunStageSummary]


class ReviewEnqueueRequest(StrictModel):
    company_id: str
    estimate_version_id: str | None = None
    review_reason: ReviewReason
    priority: int = Field(default=50, ge=0, le=100)
    detail: str | None = None
