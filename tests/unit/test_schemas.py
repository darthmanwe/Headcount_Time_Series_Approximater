"""Pydantic contract validation tests."""

from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from headcount.db.enums import (
    AnchorType,
    ConfidenceBand,
    EstimateMethod,
    EventSourceClass,
    EventType,
    HeadcountValueKind,
    ParseStatus,
    RelationKind,
    SourceEntityType,
    SourceName,
)
from headcount.schemas import (
    AnchorReconciliationInput,
    AnchorReconciliationPayload,
    CompanyAnchorObservation,
    CompanyEventContract,
    CompanyRelationContract,
    ConfidenceBreakdown,
    EmploymentObservation,
    EstimationRunRequest,
    EvidenceTraceResponse,
    GrowthWindow,
    HeadcountSeriesPoint,
    SourceObservation,
)


def test_source_observation_ok() -> None:
    obs = SourceObservation(
        source_name=SourceName.company_web,
        entity_type=SourceEntityType.company,
        observed_at=datetime.now(tz=UTC),
        raw_content_hash="a" * 64,
        parser_version="v1",
        parse_status=ParseStatus.ok,
    )
    assert obs.raw_content_hash.startswith("a")


def test_anchor_observation_interval_ok_range_and_exact() -> None:
    interval = CompanyAnchorObservation(
        company_id="c1",
        anchor_type=AnchorType.current_headcount_anchor,
        headcount_value_min=200,
        headcount_value_point=350,
        headcount_value_max=500,
        headcount_value_kind=HeadcountValueKind.range,
        anchor_month=date(2024, 3, 1),
        confidence=0.7,
    )
    assert interval.headcount_value_point == 350

    exact = CompanyAnchorObservation(
        company_id="c1",
        anchor_type=AnchorType.current_headcount_anchor,
        headcount_value_min=500,
        headcount_value_point=500,
        headcount_value_max=500,
        headcount_value_kind=HeadcountValueKind.exact,
        anchor_month=date(2024, 3, 1),
        confidence=0.9,
    )
    assert exact.headcount_value_kind is HeadcountValueKind.exact


def test_anchor_observation_rejects_inverted() -> None:
    with pytest.raises(ValueError):
        CompanyAnchorObservation(
            company_id="c1",
            anchor_type=AnchorType.current_headcount_anchor,
            headcount_value_min=500,
            headcount_value_point=200,
            headcount_value_max=100,
            headcount_value_kind=HeadcountValueKind.range,
            anchor_month=date(2024, 3, 1),
            confidence=0.7,
        )


def test_anchor_exact_requires_collapsed_interval() -> None:
    with pytest.raises(ValueError):
        CompanyAnchorObservation(
            company_id="c1",
            anchor_type=AnchorType.current_headcount_anchor,
            headcount_value_min=200,
            headcount_value_point=500,
            headcount_value_max=700,
            headcount_value_kind=HeadcountValueKind.exact,
            anchor_month=date(2024, 3, 1),
            confidence=0.9,
        )


def test_employment_observation_validations() -> None:
    EmploymentObservation(
        person_id="p1",
        company_id="c1",
        start_month=date(2023, 1, 1),
        is_current_role=True,
        confidence=0.5,
    )
    with pytest.raises(ValueError):
        EmploymentObservation(
            person_id="p1",
            company_id="c1",
            start_month=date(2023, 6, 1),
            end_month=date(2023, 1, 1),
            confidence=0.5,
        )
    with pytest.raises(ValueError):
        EmploymentObservation(
            person_id="p1",
            company_id="c1",
            start_month=date(2023, 1, 1),
            end_month=date(2023, 3, 1),
            is_current_role=True,
            confidence=0.5,
        )


def test_company_event_and_relation_contracts() -> None:
    CompanyEventContract(
        company_id="c1",
        event_type=EventType.acquisition,
        event_month=date(2023, 6, 1),
        source_class=EventSourceClass.press,
        confidence=0.7,
    )
    CompanyRelationContract(
        parent_id="p",
        child_id="c",
        kind=RelationKind.acquired,
        effective_month=date(2023, 6, 1),
        confidence=0.8,
    )


def test_series_point_interval_enforced() -> None:
    HeadcountSeriesPoint(
        month=date(2024, 3, 1),
        estimated_headcount=300,
        estimated_headcount_min=250,
        estimated_headcount_max=350,
        public_profile_count=100,
        scaled_from_anchor_value=300,
        method=EstimateMethod.scaled_ratio,
        confidence_band=ConfidenceBand.medium,
    )
    with pytest.raises(ValueError):
        HeadcountSeriesPoint(
            month=date(2024, 3, 1),
            estimated_headcount=300,
            estimated_headcount_min=350,
            estimated_headcount_max=250,
            public_profile_count=100,
            scaled_from_anchor_value=300,
            method=EstimateMethod.scaled_ratio,
            confidence_band=ConfidenceBand.medium,
        )


def test_growth_window_pattern() -> None:
    GrowthWindow(
        window="1y",
        start_month=date(2023, 3, 1),
        end_month=date(2024, 3, 1),
        confidence_band=ConfidenceBand.medium,
    )
    with pytest.raises(ValueError):
        GrowthWindow(
            window="3y",
            start_month=date(2022, 3, 1),
            end_month=date(2024, 3, 1),
            confidence_band=ConfidenceBand.low,
        )


def test_reconciliation_and_evidence_trace_shape() -> None:
    payload = AnchorReconciliationPayload(
        chosen_point=320,
        chosen_min=250,
        chosen_max=400,
        inputs=[
            AnchorReconciliationInput(
                source_name="linkedin_public",
                point=350,
                minimum=201,
                maximum=500,
                weight=0.6,
                confidence=0.7,
            )
        ],
        weights={"linkedin_public": 0.6},
    )
    conf = ConfidenceBreakdown(band=ConfidenceBand.medium, components={"coverage": 0.7})
    resp = EvidenceTraceResponse(
        company_id="c1",
        canonical_name="Acme",
        anchors=[],
        employment_sample=[],
        events=[],
        reconciliation=payload,
        confidence=conf,
        override_history=[],
        series=[],
        growth=[],
    )
    assert resp.reconciliation is payload


def test_estimation_run_request_defaults() -> None:
    req = EstimationRunRequest()
    assert req.company_ids == []
    assert req.resume is False
    assert req.dry_run is False
