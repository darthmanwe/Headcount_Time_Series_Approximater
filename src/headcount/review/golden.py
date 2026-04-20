"""Golden fixture harness for hand-validated per-company expectations.

Phase 11 introduces *golden* YAML files under ``tests/golden/goldens/``
with hand-validated expected outputs for a small set of companies
drawn from ``test_source/`` spreadsheets. Each golden pins

* the canonical company name and the source row it was seeded from
* the accepted current anchor (value, month, provider, workbook row)
* 3-5 monthly-series sample points with per-field tolerances
* expected 6m / 1y / 2y growth windows (with tolerances in %-points)
* expected confidence band at the latest month

The loader here is pure: it reads YAML, validates the schema, and
returns :class:`GoldenFixture` dataclasses. The differ takes a
fixture plus the corresponding :class:`MonthlyEstimate` rows and
returns a list of :class:`GoldenMismatch` entries. Empty list means
the golden passes.

Hybrid expected-value source
----------------------------

Per Phase 11 decision, expected values are **hybrid**:

* analyst-verified (``zeeshan``) is the primary source; each field
  records ``provider: zeeshan``
* when the analyst column is silent, ``provider: harmonic`` is used
* LinkedIn is never used as a golden source (profile-appearance
  counts, not full headcount)

The per-field ``provider`` key is informational and shows up in
mismatch messages so analysts can see which reference they were
measured against.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any

import yaml

from headcount.db.enums import ConfidenceBand
from headcount.estimate.growth import latest_growth_windows
from headcount.estimate.reconcile import MonthlyEstimate

GOLDEN_VERSION = "golden_v1"


# ---------------------------------------------------------------------------
# Schema dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class SourceRef:
    workbook: str
    sheet: str
    row_index: int

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> SourceRef:
        return cls(
            workbook=str(d["workbook"]),
            sheet=str(d["sheet"]),
            row_index=int(d["row_index"]),
        )


@dataclass(frozen=True, slots=True)
class AnchorExpectation:
    month: date
    value_point: float
    value_min: float | None
    value_max: float | None
    provider: str
    source: SourceRef

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> AnchorExpectation:
        return cls(
            month=_parse_month(d["month"]),
            value_point=float(d["value_point"]),
            value_min=None if d.get("value_min") is None else float(d["value_min"]),
            value_max=None if d.get("value_max") is None else float(d["value_max"]),
            provider=str(d["provider"]),
            source=SourceRef.from_dict(d["source"]),
        )


@dataclass(frozen=True, slots=True)
class MonthlySample:
    month: date
    value_point: float
    tolerance_pct: float
    provider: str

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> MonthlySample:
        return cls(
            month=_parse_month(d["month"]),
            value_point=float(d["value_point"]),
            tolerance_pct=float(d.get("tolerance_pct", 5.0)),
            provider=str(d["provider"]),
        )


@dataclass(frozen=True, slots=True)
class GrowthExpectation:
    horizon: str  # "6m" | "1y" | "2y"
    pct: float
    tolerance_pct_points: float
    provider: str

    @classmethod
    def from_dict(cls, horizon: str, d: dict[str, Any]) -> GrowthExpectation:
        return cls(
            horizon=horizon,
            pct=float(d["pct"]),
            tolerance_pct_points=float(d.get("tolerance", 2.0)),
            provider=str(d["provider"]),
        )


@dataclass(frozen=True, slots=True)
class GoldenFixture:
    canonical_name: str
    source_company_row: SourceRef
    accepted_anchor: AnchorExpectation
    monthly_samples: tuple[MonthlySample, ...]
    growth_windows: tuple[GrowthExpectation, ...]
    expected_confidence_band_latest: ConfidenceBand
    notes: str | None = None


@dataclass(frozen=True, slots=True)
class GoldenMismatch:
    kind: str
    detail: str
    expected: Any = None
    actual: Any = None

    def as_line(self) -> str:
        return f"[{self.kind}] {self.detail} (expected={self.expected!r}, actual={self.actual!r})"


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------


def _parse_month(raw: Any) -> date:
    if isinstance(raw, date):
        return raw.replace(day=1)
    text = str(raw).strip()
    # Accept "2024-04" or "2024-04-01".
    if len(text) == 7:
        text = text + "-01"
    return date.fromisoformat(text)


def load_golden_from_dict(payload: dict[str, Any]) -> GoldenFixture:
    company = payload["company"]
    anchor = payload["accepted_anchor"]
    samples = [MonthlySample.from_dict(s) for s in payload.get("monthly_samples", [])]
    growth_raw = payload.get("growth_windows", {}) or {}
    growth = tuple(
        GrowthExpectation.from_dict(horizon, data)
        for horizon, data in growth_raw.items()
    )
    band_raw = str(payload.get("expected_confidence_band_latest", "medium"))
    try:
        band = ConfidenceBand(band_raw)
    except ValueError as exc:
        raise ValueError(
            f"unknown confidence_band in golden: {band_raw!r}"
        ) from exc

    return GoldenFixture(
        canonical_name=str(company["canonical_name"]),
        source_company_row=SourceRef.from_dict(company["source_row"]),
        accepted_anchor=AnchorExpectation.from_dict(anchor),
        monthly_samples=tuple(samples),
        growth_windows=growth,
        expected_confidence_band_latest=band,
        notes=payload.get("notes"),
    )


def load_goldens_dir(directory: Path) -> list[GoldenFixture]:
    """Load every ``*.yaml`` golden file in ``directory``, sorted by name."""

    out: list[GoldenFixture] = []
    for path in sorted(directory.glob("*.yaml")):
        with path.open("r", encoding="utf-8") as fh:
            payload = yaml.safe_load(fh)
        if payload is None:
            continue
        out.append(load_golden_from_dict(payload))
    return out


# ---------------------------------------------------------------------------
# Differ
# ---------------------------------------------------------------------------


def diff_monthly_samples(
    fixture: GoldenFixture,
    monthly: dict[date, MonthlyEstimate],
) -> list[GoldenMismatch]:
    mismatches: list[GoldenMismatch] = []
    for sample in fixture.monthly_samples:
        est = monthly.get(sample.month)
        if est is None:
            mismatches.append(
                GoldenMismatch(
                    kind="missing_monthly_estimate",
                    detail=(
                        f"{fixture.canonical_name} @ {sample.month} "
                        f"(expected {sample.value_point} vs. {sample.provider})"
                    ),
                    expected=sample.value_point,
                    actual=None,
                )
            )
            continue

        tol = abs(sample.value_point) * sample.tolerance_pct / 100.0
        tol = max(tol, 1.0)  # floor at 1 person so tiny companies aren't over-strict
        if abs(est.value_point - sample.value_point) > tol:
            mismatches.append(
                GoldenMismatch(
                    kind="monthly_estimate_out_of_tolerance",
                    detail=(
                        f"{fixture.canonical_name} @ {sample.month} "
                        f"(±{sample.tolerance_pct}% vs {sample.provider})"
                    ),
                    expected=sample.value_point,
                    actual=est.value_point,
                )
            )
    return mismatches


def diff_growth_windows(
    fixture: GoldenFixture,
    monthly: dict[date, MonthlyEstimate],
) -> list[GoldenMismatch]:
    if not fixture.growth_windows:
        return []
    latest = latest_growth_windows(monthly.values())
    actual_by_horizon = {p.horizon: p for p in latest}
    out: list[GoldenMismatch] = []
    for expectation in fixture.growth_windows:
        actual = actual_by_horizon.get(expectation.horizon)
        if actual is None or actual.value_point is None:
            out.append(
                GoldenMismatch(
                    kind="missing_growth_window",
                    detail=(
                        f"{fixture.canonical_name} horizon={expectation.horizon} "
                        f"(expected {expectation.pct}% vs {expectation.provider})"
                    ),
                    expected=expectation.pct,
                    actual=None,
                )
            )
            continue
        actual_pct = float(actual.value_point) * 100.0
        if abs(actual_pct - expectation.pct) > expectation.tolerance_pct_points:
            out.append(
                GoldenMismatch(
                    kind="growth_out_of_tolerance",
                    detail=(
                        f"{fixture.canonical_name} horizon={expectation.horizon} "
                        f"(±{expectation.tolerance_pct_points}pp vs {expectation.provider})"
                    ),
                    expected=expectation.pct,
                    actual=round(actual_pct, 4),
                )
            )
    return out


def diff_confidence_band(
    fixture: GoldenFixture,
    monthly: dict[date, MonthlyEstimate],
) -> list[GoldenMismatch]:
    if not monthly:
        return [
            GoldenMismatch(
                kind="no_estimates",
                detail=f"{fixture.canonical_name} has no estimates at all",
                expected=fixture.expected_confidence_band_latest.value,
                actual=None,
            )
        ]
    latest_month = max(monthly)
    actual_band = monthly[latest_month].confidence_band
    if actual_band is not fixture.expected_confidence_band_latest:
        return [
            GoldenMismatch(
                kind="confidence_band_drift",
                detail=(
                    f"{fixture.canonical_name} @ latest month {latest_month}"
                ),
                expected=fixture.expected_confidence_band_latest.value,
                actual=actual_band.value,
            )
        ]
    return []


def diff_fixture(
    fixture: GoldenFixture,
    monthly: dict[date, MonthlyEstimate],
) -> list[GoldenMismatch]:
    """Combine all per-fixture diffs into a single list."""

    mismatches: list[GoldenMismatch] = []
    mismatches.extend(diff_monthly_samples(fixture, monthly))
    mismatches.extend(diff_growth_windows(fixture, monthly))
    mismatches.extend(diff_confidence_band(fixture, monthly))
    return mismatches


@dataclass(slots=True)
class GoldenReport:
    fixture: GoldenFixture
    mismatches: list[GoldenMismatch] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return not self.mismatches

    def format_report(self) -> str:
        if self.passed:
            return f"PASS {self.fixture.canonical_name}"
        lines = [f"FAIL {self.fixture.canonical_name}"]
        for m in self.mismatches:
            lines.append("  " + m.as_line())
        return "\n".join(lines)


def run_goldens(
    fixtures: Iterable[GoldenFixture],
    monthly_by_company: dict[str, dict[date, MonthlyEstimate]],
) -> list[GoldenReport]:
    """Return one :class:`GoldenReport` per fixture."""

    out: list[GoldenReport] = []
    for fx in fixtures:
        monthly = monthly_by_company.get(fx.canonical_name, {})
        out.append(GoldenReport(fixture=fx, mismatches=diff_fixture(fx, monthly)))
    return out


__all__ = [
    "GOLDEN_VERSION",
    "AnchorExpectation",
    "GoldenFixture",
    "GoldenMismatch",
    "GoldenReport",
    "GrowthExpectation",
    "MonthlySample",
    "SourceRef",
    "diff_confidence_band",
    "diff_fixture",
    "diff_growth_windows",
    "diff_monthly_samples",
    "load_golden_from_dict",
    "load_goldens_dir",
    "run_goldens",
]
