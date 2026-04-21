from __future__ import annotations

import asyncio
from urllib.parse import quote_plus

import httpx


USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


async def main() -> None:
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "en-US,en;q=0.9",
    }
    query = quote_plus('"Alleva" helloalleva.com site:linkedin.com/company')
    url = f"https://www.bing.com/search?q={query}&form=QBLH"
    async with httpx.AsyncClient(
        headers=headers, follow_redirects=True, timeout=20.0
    ) as client:
        resp = await client.get(url)
    print(f"status={resp.status_code} len={len(resp.text)}")
    body = resp.text
    for keyword in [
        "captcha",
        "sorry",
        "recaptcha",
        "verify",
        "LinkedIn",
        "linkedin",
        "employees",
        "company/",
    ]:
        count = body.lower().count(keyword.lower())
        print(f"  '{keyword}': {count}")
    print("---sample (first 2000 chars)---")
    print(body[:2000])


asyncio.run(main())
