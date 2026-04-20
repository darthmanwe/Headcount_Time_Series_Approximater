"""Integration tests for :class:`ManualAnchorObserver`."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from headcount.db.enums import AnchorType, HeadcountValueKind, SourceName
from headcount.ingest.base import CompanyTarget, FetchContext
from headcount.ingest.observers.manual import ManualAnchorObserver, load_manual_entries


def _write_yaml(path: Path, body: str) -> Path:
    path.write_text(body, encoding="utf-8")
    return path


@pytest.mark.asyncio
async def test_manual_observer_matches_by_domain(tmp_path: Path) -> None:
    yaml_path = _write_yaml(
        tmp_path / "manual.yaml",
        """
- canonical_name: Acme Corp
  domain: acme.com
  anchor_month: 2026-04-01
  headcount:
    min: 1200
    point: 1250
    max: 1300
    kind: range
  confidence: 0.95
  note: 2026 Q1 earnings call
  source_url: https://example.com/press/acme-q1-2026
""",
    )
    observer = ManualAnchorObserver(path=yaml_path)
    target = CompanyTarget(
        company_id="c-1",
        canonical_name="Acme Corporation",
        canonical_domain="https://www.acme.com/",
        linkedin_company_url=None,
    )
    signals = await observer.fetch_current_anchor(
        target,
        context=FetchContext(run_id="r", http=None, live=False),
    )
    assert len(signals) == 1
    sig = signals[0]
    assert sig.source_name is SourceName.manual
    assert sig.anchor_month == date(2026, 4, 1)
    assert sig.anchor_type is AnchorType.manual_anchor
    assert sig.headcount_value_kind is HeadcountValueKind.range
    assert sig.headcount_value_min == 1200
    assert sig.headcount_value_point == 1250
    assert sig.headcount_value_max == 1300
    assert sig.confidence == pytest.approx(0.95)
    assert sig.source_url == "https://example.com/press/acme-q1-2026"


@pytest.mark.asyncio
async def test_manual_observer_matches_by_name_key(tmp_path: Path) -> None:
    yaml_path = _write_yaml(
        tmp_path / "manual.yaml",
        """
- canonical_name: Rocket Industries
  anchor_month: 2026-03-01
  headcount:
    min: 480
    point: 500
    max: 520
    kind: exact
  note: internal memo
""",
    )
    observer = ManualAnchorObserver(path=yaml_path)
    target = CompanyTarget(
        company_id="c-2",
        canonical_name="Rocket Industries LLC",
        canonical_domain=None,
        linkedin_company_url=None,
    )
    signals = await observer.fetch_current_anchor(
        target,
        context=FetchContext(run_id="r", http=None, live=False),
    )
    assert len(signals) == 1
    assert signals[0].headcount_value_point == 500


@pytest.mark.asyncio
async def test_manual_observer_returns_empty_on_miss(tmp_path: Path) -> None:
    yaml_path = _write_yaml(
        tmp_path / "manual.yaml",
        """
- canonical_name: Globex
  anchor_month: 2026-01-01
  headcount:
    min: 10
    point: 20
    max: 30
    kind: range
  note: test
""",
    )
    observer = ManualAnchorObserver(path=yaml_path)
    target = CompanyTarget(
        company_id="c-3",
        canonical_name="Initech",
        canonical_domain=None,
        linkedin_company_url=None,
    )
    assert (
        await observer.fetch_current_anchor(
            target,
            context=FetchContext(run_id="r", http=None, live=False),
        )
        == []
    )


def test_manual_loader_requires_note(tmp_path: Path) -> None:
    yaml_path = _write_yaml(
        tmp_path / "manual.yaml",
        """
- canonical_name: Acme
  anchor_month: 2026-01-01
  headcount:
    min: 10
    point: 20
    max: 30
    kind: range
""",
    )
    with pytest.raises(ValueError):
        load_manual_entries(yaml_path)


def test_manual_loader_requires_headcount_block(tmp_path: Path) -> None:
    yaml_path = _write_yaml(
        tmp_path / "manual.yaml",
        """
- canonical_name: Acme
  anchor_month: 2026-01-01
  note: missing headcount
""",
    )
    with pytest.raises(ValueError):
        load_manual_entries(yaml_path)
