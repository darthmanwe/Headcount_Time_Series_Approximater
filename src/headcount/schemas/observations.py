"""Pydantic contracts for source-layer observations.

These models are the wire/storage shape produced by adapters and consumed
by parsers/estimation. They deliberately mirror ORM column names so
round-tripping is mechanical. Interval fields on anchor observations keep
uncertainty first-class end-to-end.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator

from headcount.db.enums import (
    AnchorType,
    EventSourceClass,
    EventType,
    HeadcountValueKind,
    ParseStatus,
    RelationKind,
    SourceEntityType,
    SourceName,
)


class StrictModel(BaseModel):
    """Shared base: strict types, frozen-by-default is not used because
    downstream code mutates freely during construction."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)


class SourceObservation(StrictModel):
    source_name: SourceName
    entity_type: SourceEntityType
    source_url: str | None = None
    observed_at: datetime
    raw_text: str | None = None
    raw_html_path: str | None = None
    raw_content_hash: str = Field(min_length=16)
    parser_version: str = Field(min_length=1)
    parse_status: ParseStatus = ParseStatus.ok
    normalized_payload: dict[str, Any] | None = None


class CompanyAnchorObservation(StrictModel):
    company_id: str
    source_observation_id: str | None = None
    anchor_type: AnchorType
    headcount_value_min: float = Field(ge=0)
    headcount_value_point: float = Field(ge=0)
    headcount_value_max: float = Field(ge=0)
    headcount_value_kind: HeadcountValueKind
    anchor_month: date
    confidence: float = Field(ge=0.0, le=1.0)
    note: str | None = None

    @model_validator(mode="after")
    def _check_interval(self) -> CompanyAnchorObservation:
        if not (
            self.headcount_value_min
            <= self.headcount_value_point
            <= self.headcount_value_max
        ):
            raise ValueError(
                "anchor interval must satisfy min <= point <= max; "
                f"got ({self.headcount_value_min}, {self.headcount_value_point}, "
                f"{self.headcount_value_max})"
            )
        if self.headcount_value_kind is HeadcountValueKind.exact and (
            self.headcount_value_min != self.headcount_value_point
            or self.headcount_value_point != self.headcount_value_max
        ):
            raise ValueError("kind='exact' requires min == point == max")
        return self


class EmploymentObservation(StrictModel):
    person_id: str
    company_id: str
    source_observation_id: str | None = None
    observed_company_name: str | None = None
    job_title: str | None = None
    start_month: date
    end_month: date | None = None
    is_current_role: bool = False
    confidence: float = Field(ge=0.0, le=1.0)

    @model_validator(mode="after")
    def _check_range(self) -> EmploymentObservation:
        if self.end_month is not None and self.end_month < self.start_month:
            raise ValueError("end_month must not precede start_month")
        if self.is_current_role and self.end_month is not None:
            raise ValueError("is_current_role=True requires end_month is None")
        return self


class CompanyEventContract(StrictModel):
    company_id: str
    event_type: EventType
    event_month: date
    source_observation_id: str | None = None
    source_class: EventSourceClass
    confidence: float = Field(ge=0.0, le=1.0)
    description: str | None = None


class CompanyRelationContract(StrictModel):
    parent_id: str
    child_id: str
    kind: RelationKind
    effective_month: date | None = None
    confidence: float = Field(ge=0.0, le=1.0)
    source_observation_id: str | None = None
    note: str | None = None
