"""Quick per-company summary of a cohort run's per_company.json."""

from __future__ import annotations

import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]


def _latest_run() -> Path:
    return sorted((REPO / "data" / "runs" / "harmonic_live").glob("*"))[-1]


def main(argv: list[str]) -> int:
    run_dir = Path(argv[1]).resolve() if len(argv) > 1 else _latest_run()
    data = json.loads((run_dir / "per_company.json").read_text(encoding="utf-8"))

    print(f"{'company':<28} {'harm':>6} {'est':>6} {'conf':>5} {'err%':>7}")
    print("-" * 60)
    scored: list[dict] = []
    declined: list[dict] = []
    unmatched: list[str] = []
    for r in data:
        if not r.get("company_id"):
            unmatched.append(r["harmonic_name"])
            continue
        harm = r.get("harmonic_headcount")
        est = r.get("estimate_current")
        if est is None:
            continue
        if est <= 0:
            declined.append(r)
            continue
        err = (est - harm) / harm * 100 if harm else None
        scored.append({**r, "err_pct": err})
        print(
            f"{r['canonical_name']:<28} {harm:>6} {est:>6} {r.get('confidence_current') or 0:>5.3f} {err:>7.1f}"
        )

    print("-" * 60)
    mape = (
        sum(abs(r["err_pct"]) for r in scored) / len(scored) if scored else float("nan")
    )
    print(
        f"Scored: {len(scored)}, declined (0.0 fallback): {len(declined)}, "
        f"unmatched (no canonical): {len(unmatched)}"
    )
    print(f"Mean absolute pct error vs Harmonic: {mape:.2f}%")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
