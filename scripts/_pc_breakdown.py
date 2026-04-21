"""Quick per-company summary for cohort runs."""

from __future__ import annotations

import json
import sys
from pathlib import Path


def main(run_dir: str) -> None:
    data = json.loads((Path(run_dir) / "per_company.json").read_text(encoding="utf-8"))
    scored = [c for c in data if c.get("estimate_current") not in (None, 0.0)]
    declined = [c for c in data if c.get("estimate_current") in (None, 0.0)]
    print(f"Total: {len(data)}, Scored: {len(scored)}, Declined: {len(declined)}")
    print("--- SCORED ---")
    for c in scored:
        h = c.get("harmonic_headcount")
        e = c.get("estimate_current")
        if h:
            err = abs(e - h) / h * 100.0
            print(f"  {c['canonical_name']}: est={e}, harm={h}, err={err:.1f}%")
        else:
            print(f"  {c['canonical_name']}: est={e}, harm={h}")
    print("--- DECLINED ---")
    for c in declined:
        print(f"  {c['canonical_name']}: harm={c.get('harmonic_headcount')}")


if __name__ == "__main__":
    main(sys.argv[1])
