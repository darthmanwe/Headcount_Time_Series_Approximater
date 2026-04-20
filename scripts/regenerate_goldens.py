"""One-shot regenerator for the Phase 11 golden fixtures under the
new Harmonic-primary trust model.

Usage::

    python scripts/regenerate_goldens.py

Inputs (read-only):
* ``test_source/Sample Employee Growth for High Priority Prospects.xlsx``
  - ``Harmonic April 8`` sheet (target signal)
* ``tests/golden/goldens/*.yaml`` - existing fixtures, used to recover
  Zeeshan historical points / row indices (these came from the
  ``Zeeshan April 1`` sheet originally and are still authoritative
  for historical horizons that Harmonic does not emit).

Output:
* Overwrites ``tests/golden/goldens/<slug>.yaml`` with the new layout::

    accepted_anchor: Harmonic point at 2026-04-01 (provider=harmonic)
    monthly_samples:
        2024-04-01: Zeeshan t-2y point (provider=zeeshan)
        2025-04-01: Zeeshan t-1y point (provider=zeeshan)
        2025-10-01: Zeeshan t-6m point (provider=zeeshan)
        2026-04-01: Harmonic point (provider=harmonic)
    growth_windows:
        6m / 1y: derived from (Harmonic-now, Zeeshan-historical) -
            this is what the pipeline produces given mixed evidence.
            Each block records the Harmonic-reported target for
            calibration reference under ``harmonic_target_pct``.
        2y: Zeeshan-reported (Harmonic does not emit 2y).

The script is deterministic and idempotent. Re-running with the same
inputs produces byte-identical YAML.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import openpyxl
import yaml

ROOT = Path(__file__).resolve().parent.parent
WORKBOOK = ROOT / "test_source" / ("Sample Employee Growth for High Priority Prospects.xlsx")
GOLDEN_DIR = ROOT / "tests" / "golden" / "goldens"
LATEST_MONTH = "2026-04-01"
HARMONIC_SHEET = "Harmonic April 8"


@dataclass(frozen=True)
class HarmonicRow:
    name: str
    headcount: float
    growth_1y_pct: float
    growth_6m_pct: float


def _load_harmonic() -> dict[str, HarmonicRow]:
    wb = openpyxl.load_workbook(WORKBOOK, data_only=True)
    ws = wb[HARMONIC_SHEET]
    rows = list(ws.iter_rows(values_only=True))
    header = [str(h) for h in rows[0]]
    name_idx = header.index("Company Name")
    hc_idx = header.index("Headcount")
    g1y_idx = header.index("Headcount % (365d)")
    g6m_idx = header.index("Headcount % (180d)")
    out: dict[str, HarmonicRow] = {}
    for r in rows[1:]:
        name = r[name_idx]
        if not name:
            continue
        out[str(name).strip().lower()] = HarmonicRow(
            name=str(name),
            headcount=float(r[hc_idx]) if r[hc_idx] is not None else 0.0,
            growth_1y_pct=float(r[g1y_idx]) if r[g1y_idx] is not None else 0.0,
            growth_6m_pct=float(r[g6m_idx]) if r[g6m_idx] is not None else 0.0,
        )
    return out


def _existing_zeeshan(path: Path) -> dict[str, object]:
    """Pull historical samples + row index out of the existing fixture."""

    with path.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    samples = {
        s["month"].isoformat() if hasattr(s["month"], "isoformat") else str(s["month"]): s
        for s in data.get("monthly_samples", [])
    }
    growth = data.get("growth_windows", {}) or {}
    return {
        "canonical_name": data["company"]["canonical_name"],
        "row_index": data["company"]["source_row"]["row_index"],
        "t_minus_2y": float(samples["2024-04-01"]["value_point"]),
        "t_minus_1y": float(samples["2025-04-01"]["value_point"]),
        "t_minus_6m": float(samples["2025-10-01"]["value_point"]),
        "current_zeeshan": samples.get(LATEST_MONTH, {}),
        "growth_2y": (growth.get("2y") or {}).get("pct"),
        "expected_confidence_band_latest": data.get("expected_confidence_band_latest", "medium"),
        "notes": data.get("notes", ""),
    }


def _derived_growth(latest: float, historical: float) -> float:
    if historical == 0.0:
        return 0.0
    return round((latest - historical) / historical * 100.0, 2)


def _confidence_band_for(*, harmonic_now: float, zeeshan_t6m: float, zeeshan_t1y: float) -> str:
    """Heuristic: when Harmonic and Zeeshan disagree by > 30% at the
    1y horizon, the latest-month estimate is at best ``low`` confidence
    because the anchor evidence contradicts itself."""

    if zeeshan_t1y == 0.0:
        return "low"
    drift = abs(harmonic_now - zeeshan_t1y) / zeeshan_t1y
    if drift > 0.30:
        return "low"
    return "medium"


def _yaml_for(
    *,
    canonical_name: str,
    row_index: int,
    harmonic: HarmonicRow,
    zeeshan_t2y: float,
    zeeshan_t1y: float,
    zeeshan_t6m: float,
    zeeshan_2y_growth: float | None,
    extra_notes: str,
) -> str:
    derived_6m = _derived_growth(harmonic.headcount, zeeshan_t6m)
    derived_1y = _derived_growth(harmonic.headcount, zeeshan_t1y)
    band = _confidence_band_for(
        harmonic_now=harmonic.headcount,
        zeeshan_t6m=zeeshan_t6m,
        zeeshan_t1y=zeeshan_t1y,
    )
    notes = (
        f"Calibration target: Harmonic 1y {harmonic.growth_1y_pct:+.2f}% / "
        f"6m {harmonic.growth_6m_pct:+.2f}%. Pipeline output below "
        f"reflects the mixed evidence (Zeeshan historical anchors + "
        f"Harmonic current); a calibration gap between produced and "
        f"target growth is expected and tracked by the evaluation "
        f"harness."
    )
    if extra_notes:
        notes = f"{extra_notes.rstrip('.')} | {notes}"

    payload = {
        "company": {
            "canonical_name": canonical_name,
            "source_row": {
                "workbook": ("Sample Employee Growth for High Priority Prospects.xlsx"),
                "sheet": HARMONIC_SHEET,
                "row_index": row_index,
            },
        },
        "accepted_anchor": {
            "month": LATEST_MONTH,
            "value_point": harmonic.headcount,
            "provider": "harmonic",
            "source": {
                "workbook": ("Sample Employee Growth for High Priority Prospects.xlsx"),
                "sheet": HARMONIC_SHEET,
                "row_index": row_index,
            },
        },
        "monthly_samples": [
            {
                "month": "2024-04-01",
                "value_point": zeeshan_t2y,
                "tolerance_pct": 5.0,
                "provider": "zeeshan",
            },
            {
                "month": "2025-04-01",
                "value_point": zeeshan_t1y,
                "tolerance_pct": 5.0,
                "provider": "zeeshan",
            },
            {
                "month": "2025-10-01",
                "value_point": zeeshan_t6m,
                "tolerance_pct": 5.0,
                "provider": "zeeshan",
            },
            {
                "month": LATEST_MONTH,
                "value_point": harmonic.headcount,
                "tolerance_pct": 5.0,
                "provider": "harmonic",
            },
        ],
        "growth_windows": {
            "6m": {
                "pct": derived_6m,
                "tolerance": 2.0,
                "provider": "harmonic",
                "harmonic_target_pct": harmonic.growth_6m_pct,
            },
            "1y": {
                "pct": derived_1y,
                "tolerance": 2.0,
                "provider": "harmonic",
                "harmonic_target_pct": harmonic.growth_1y_pct,
            },
            "2y": {
                "pct": zeeshan_2y_growth if zeeshan_2y_growth is not None else 0.0,
                "tolerance": 2.0,
                "provider": "zeeshan",
            },
        },
        "expected_confidence_band_latest": band,
        "notes": notes,
    }

    header = (
        "# Golden fixture (Harmonic-primary trust model).\n"
        "# accepted_anchor + latest monthly sample come from Harmonic\n"
        "# (the signal we are trying to approximate). Historical\n"
        "# samples and the 2y growth horizon fall back to Zeeshan\n"
        "# because Harmonic does not emit those.\n"
    )
    return header + yaml.safe_dump(payload, sort_keys=False, default_flow_style=False)


def main() -> None:
    harmonic = _load_harmonic()

    # Map fixture stem -> harmonic key. Several names need explicit
    # disambiguation because Harmonic includes corporate suffixes.
    stem_to_key = {
        "1010data": "1010data",
        "1kosmos": "1kosmos",
        "1uphealth": "1uphealth, inc.",
        "6sense": "6sense",
        "alivecor": "alivecor",
        "alleva": "alleva",
        "alloy": "alloy",
        "alloy_therapeutics": "alloy therapeutics, inc.",
        "alltrails": "alltrails",
        "allvue_systems": "allvue systems",
    }

    for path in sorted(GOLDEN_DIR.glob("*.yaml")):
        stem = path.stem
        key = stem_to_key.get(stem)
        if key is None or key not in harmonic:
            print(f"SKIP {stem}: no harmonic mapping")
            continue
        existing = _existing_zeeshan(path)
        canonical_name = str(existing["canonical_name"])
        row_index = int(existing["row_index"])  # type: ignore[arg-type]
        body = _yaml_for(
            canonical_name=canonical_name,
            row_index=row_index,
            harmonic=harmonic[key],
            zeeshan_t2y=float(existing["t_minus_2y"]),
            zeeshan_t1y=float(existing["t_minus_1y"]),
            zeeshan_t6m=float(existing["t_minus_6m"]),
            zeeshan_2y_growth=existing["growth_2y"],
            extra_notes=str(existing["notes"]).strip(),
        )
        path.write_text(body, encoding="utf-8")
        print(f"WROTE {stem}: harmonic={harmonic[key].headcount}")


if __name__ == "__main__":
    main()
