"""Realistic browser fingerprints for endpoints that gate generic bots.

Part of the Phase 11 response to LinkedIn's logged-out bot wall (see
``docs/LINKEDIN_BOT_WALL_STRATEGY.md``, lever L1). The intent is to
present as a plausible top-level navigation from a modern Chromium or
Firefox build rather than as a default ``httpx`` client.

Design rules
------------
* One UA is chosen per process so a single run looks like one coherent
  session; rotating UA mid-session is itself a strong bot signal.
* The UA pool is deliberately small and curated to currently-shipping
  stable builds so 2026-era fingerprint databases recognise the strings
  as real browsers.
* Headers mimic a top-level document navigation: no ``Referer`` (we are
  arriving direct), no ``Cookie`` (logged-out), and no fetch-metadata
  values that would imply a subresource or iframe load.
* ``Accept-Encoding`` deliberately excludes ``br`` because ``httpx``'s
  default build has no Brotli decoder; advertising support we cannot
  decode would corrupt ``response.text`` and silently break parsers.
"""

from __future__ import annotations

import random
from collections.abc import Mapping
from types import MappingProxyType

BROWSER_USER_AGENTS: tuple[str, ...] = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 Edg/130.0.0.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) "
    "Gecko/20100101 Firefox/128.0",
)

BROWSER_NAV_HEADERS: Mapping[str, str] = MappingProxyType(
    {
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;q=0.9,"
            "image/avif,image/webp,image/apng,*/*;q=0.8"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Cache-Control": "max-age=0",
    }
)


def pick_user_agent(rng: random.Random | None = None) -> str:
    """Return a single User-Agent for the caller's session lifetime.

    Tests pass a seeded ``random.Random`` to pin the choice; production
    callers leave ``rng=None`` so each CLI invocation gets a fresh pick
    from the global RNG.
    """

    chooser = rng if rng is not None else random
    return chooser.choice(BROWSER_USER_AGENTS)
