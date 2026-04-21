"""Dump every JSON-LD numberOfEmployees occurrence from a cohort's LI cache."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]


def _latest_run() -> Path:
    return sorted((REPO / "data" / "runs" / "harmonic_live").glob("*"))[-1]


def main(argv: list[str]) -> int:
    run_dir = Path(argv[1]).resolve() if len(argv) > 1 else _latest_run()
    cache_dir = run_dir / "http_cache" / "linkedin_public"
    if not cache_dir.is_dir():
        return 1

    sys.path.insert(0, str(REPO / "src"))
    from headcount.parsers.anchors import extract_linkedin_jsonld_employees  # noqa: E402

    for f in sorted(cache_dir.glob("*.json")):
        doc = json.loads(f.read_text(encoding="utf-8"))
        body = doc.get("body", "") or doc.get("text", "") or ""
        url = doc.get("url") or ""
        result = extract_linkedin_jsonld_employees(body)
        print(f"\n=== {f.name[:12]} {url[-50:]} ===")
        if result is None:
            # find the raw context to debug
            for snippet in re.findall(r".{0,80}numberOfEmployees.{0,200}", body):
                print(f"  RAW: {snippet[:280]}")
                break
            print("  parser=None")
        else:
            print(f"  parsed: low={result.low} high={result.high} kind={result.kind} phrase={result.phrase}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
