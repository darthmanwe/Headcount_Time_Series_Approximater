"""Dump raw Bing SERP body for one query."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path


def main() -> None:
    run_dir = Path(sys.argv[1])
    target = sys.argv[2].lower() if len(sys.argv) > 2 else None
    cache_dir = run_dir / "http_cache" / "company_web"
    for path in cache_dir.glob("*.json"):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        url = data.get("url", "")
        if "bing.com/search" not in url:
            continue
        if target and target not in url.lower():
            continue
        body = data.get("text") or ""
        print("url:", url)
        print("status:", data.get("status_code"))
        print("len:", len(body))
        for kw in ["linkedin", "company", "captcha", "verify", "no results", "robot", "1Kosmos"]:
            count = body.lower().count(kw.lower())
            print(f"kw={kw}: count={count}")
        # Dump first 5k of body
        print("--- body head (4k) ---")
        print(body[:4000])


if __name__ == "__main__":
    main()
