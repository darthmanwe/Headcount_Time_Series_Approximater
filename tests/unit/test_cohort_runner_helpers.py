"""Unit tests for cohort runner helpers used by the long-running pipeline.

We pull the small, side-effect-free helpers out of
``scripts/run_harmonic_cohort_live.py`` so they can be exercised
without the full pipeline harness:

- ``_parse_cohort_slice``: validates ``--cohort-slice N/M``.
- ``_apply_cohort_slice``: deterministically slices the cohort.

The full breaker-recovery loop is exercised end-to-end by the cohort
live runner; here we only assert the building blocks.
"""

from __future__ import annotations

import importlib.util
import sys
from dataclasses import dataclass
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
RUNNER_PATH = REPO_ROOT / "scripts" / "run_harmonic_cohort_live.py"


def _import_runner_module():
    """Import the cohort runner script as a module without executing main."""

    spec = importlib.util.spec_from_file_location(
        "_cohort_runner_under_test", RUNNER_PATH
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@dataclass
class _StubCompany:
    """Minimal duck-type for ``_apply_cohort_slice``: needs name + id."""

    canonical_name: str
    id: str


@pytest.fixture(scope="module")
def runner():
    return _import_runner_module()


class TestParseCohortSlice:
    def test_valid(self, runner) -> None:
        assert runner._parse_cohort_slice("1/4") == (1, 4)
        assert runner._parse_cohort_slice("4/4") == (4, 4)
        assert runner._parse_cohort_slice("2/3") == (2, 3)

    def test_zero_or_negative(self, runner) -> None:
        with pytest.raises(ValueError):
            runner._parse_cohort_slice("0/4")
        with pytest.raises(ValueError):
            runner._parse_cohort_slice("-1/4")
        with pytest.raises(ValueError):
            runner._parse_cohort_slice("1/0")

    def test_out_of_range(self, runner) -> None:
        with pytest.raises(ValueError):
            runner._parse_cohort_slice("5/4")

    def test_garbage(self, runner) -> None:
        with pytest.raises(ValueError):
            runner._parse_cohort_slice("foo")
        with pytest.raises(ValueError):
            runner._parse_cohort_slice("")
        with pytest.raises(ValueError):
            runner._parse_cohort_slice("1")


class TestApplyCohortSlice:
    def _companies(self, n: int) -> list[_StubCompany]:
        return [_StubCompany(canonical_name=f"company-{i:02d}", id=f"id-{i}") for i in range(n)]

    def test_partition_is_complete_and_disjoint(self, runner) -> None:
        companies = self._companies(20)
        seen_ids: set[str] = set()
        for shard in (1, 2, 3, 4):
            sliced, meta = runner._apply_cohort_slice(
                companies, f"{shard}/4"
            )
            ids = {c.id for c in sliced}
            assert ids.isdisjoint(seen_ids), "shards must not overlap"
            seen_ids |= ids
            assert meta["shard"] == shard
            assert meta["total"] == 4
            assert meta["in_shard"] == len(sliced)
            assert meta["skipped"] == 20 - len(sliced)
        # Union of all shards covers the full cohort.
        assert seen_ids == {c.id for c in companies}

    def test_deterministic_order(self, runner) -> None:
        # Same N/M against the same cohort must produce the same slice
        # regardless of input order.
        companies = self._companies(10)
        a, _ = runner._apply_cohort_slice(companies, "2/3")
        shuffled = list(reversed(companies))
        b, _ = runner._apply_cohort_slice(shuffled, "2/3")
        assert [c.id for c in a] == [c.id for c in b]

    def test_single_shard_keeps_all(self, runner) -> None:
        companies = self._companies(7)
        sliced, meta = runner._apply_cohort_slice(companies, "1/1")
        assert len(sliced) == 7
        assert meta["skipped"] == 0


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
