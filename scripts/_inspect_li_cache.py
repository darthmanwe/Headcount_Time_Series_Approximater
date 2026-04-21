"""Quick post-mortem of cached LinkedIn responses from a cohort run.

Usage::

    python scripts/_inspect_li_cache.py [run_dir]

Defaults to the most recent ``data/runs/harmonic_live/`` run. Prints
one line per cached LinkedIn response with whether the body contains
JSON-LD, ``numberOfEmployees``, and the ``sign in to see`` gate marker.
Used to validate the L1+L2 wiring against real bot-walled bodies.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]


def _latest_run() -> Path:
    runs = sorted((REPO / "data" / "runs" / "harmonic_live").glob("*"))
    if not runs:
        raise SystemExit("no runs under data/runs/harmonic_live/")
    return runs[-1]


def main(argv: list[str]) -> int:
    run_dir = Path(argv[1]).resolve() if len(argv) > 1 else _latest_run()
    cache_dir = run_dir / "http_cache" / "linkedin_public"
    if not cache_dir.is_dir():
        print(f"no cache dir at {cache_dir}", file=sys.stderr)
        return 1

    print(f"Inspecting {cache_dir}\n")
    rows: list[dict[str, object]] = []
    for f in sorted(cache_dir.glob("*.json")):
        try:
            doc = json.loads(f.read_text(encoding="utf-8"))
        except Exception as exc:
            print(f"  ! {f.name[:12]}: parse error {exc!r}")
            continue
        body = doc.get("body", "") or doc.get("text", "") or ""
        url = doc.get("url") or doc.get("request", {}).get("url") or ""
        status = doc.get("status_code") or doc.get("status")
        ld_blocks = re.findall(
            r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
            body,
            flags=re.I | re.S,
        )
        has_num_emp = "numberOfEmployees" in body
        has_signin = "sign in to see" in body.lower()
        has_authwall = "authwall" in body.lower()
        rows.append(
            {
                "file": f.name[:12],
                "url": url[-60:],
                "status": status,
                "size": len(body),
                "ld_blocks": len(ld_blocks),
                "numberOfEmployees": has_num_emp,
                "signin_marker": has_signin,
                "authwall_marker": has_authwall,
            }
        )

    for r in rows:
        print(
            f"  {r['file']} url={r['url']!s:<62} status={r['status']!s:<4} "
            f"size={r['size']!s:<8} ld={r['ld_blocks']} "
            f"numEmp={r['numberOfEmployees']!s:<5} signin={r['signin_marker']} authwall={r['authwall_marker']}"
        )

    # Sample one body that has numberOfEmployees and dump the matching
    # JSON-LD block(s) so we can see the schema shape.
    sample = next((r for r in rows if r["numberOfEmployees"]), None)
    if sample is None:
        print("\nNo body contains numberOfEmployees - JSON-LD parser cannot help here.")
        return 0
    sample_path = cache_dir / f"{sample['file']}".replace("...", "")
    matches = [f for f in cache_dir.glob("*.json") if f.name.startswith(sample["file"])]
    if matches:
        doc = json.loads(matches[0].read_text(encoding="utf-8"))
        body = doc.get("body", "") or doc.get("text", "") or ""
        for i, block in enumerate(
            re.findall(
                r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
                body,
                flags=re.I | re.S,
            )
        ):
            if "numberOfEmployees" in block:
                print(f"\n--- JSON-LD block {i} from {matches[0].name[:12]} ---")
                snippet = block.strip()
                if len(snippet) > 2000:
                    snippet = snippet[:2000] + "...<truncated>..."
                print(snippet)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
