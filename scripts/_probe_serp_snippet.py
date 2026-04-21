"""One-off: fetch a DDG/Bing SERP for a company and dump any LinkedIn
snippet with an employee count."""

from __future__ import annotations

import asyncio
import re
import sys
from urllib.parse import quote_plus

import httpx


USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

ENGINES = [
    ("duckduckgo", "https://html.duckduckgo.com/html/?q={q}"),
    ("bing", "https://www.bing.com/search?q={q}&form=QBLH"),
]

EMP_RE = re.compile(
    r"([\d,]+\+?)\s*(?:employees|people|team members)", re.IGNORECASE
)


async def fetch_one(client: httpx.AsyncClient, name: str, domain: str) -> None:
    query = f'"{name}" {domain} site:linkedin.com/company'
    for engine, tmpl in ENGINES:
        url = tmpl.format(q=quote_plus(query))
        print(f"\n[{engine}] {url}")
        try:
            resp = await client.get(url, timeout=20.0)
        except Exception as exc:
            print(f"  fetch error: {exc!r}")
            continue
        print(f"  status={resp.status_code} len={len(resp.text)}")
        text = resp.text
        if resp.status_code != 200:
            continue
        for m in EMP_RE.finditer(text):
            start = max(0, m.start() - 80)
            end = min(len(text), m.end() + 80)
            ctx = text[start:end].replace("\n", " ")
            print(f"  employees-match: ...{ctx}...")
        linkedin_hits = re.findall(
            r"https?://(?:www\.)?linkedin\.com/company/[^\"'<>\s]+", text
        )[:3]
        print(f"  linkedin_hits sample: {linkedin_hits}")


async def main() -> None:
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "en-US,en;q=0.9",
    }
    async with httpx.AsyncClient(headers=headers, follow_redirects=True) as client:
        for name, domain in [
            ("Alleva", "helloalleva.com"),
            ("Alloy", "alloy.com"),
            ("6sense", "6sense.com"),
            ("1010data", "1010data.com"),
        ]:
            print(f"\n=== {name} / {domain} ===")
            await fetch_one(client, name, domain)


if __name__ == "__main__":
    asyncio.run(main())
