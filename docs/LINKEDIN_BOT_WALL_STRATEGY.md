# LinkedIn Bot-Wall: Findings & Schedule-Around Plan

Status: research + proposal, no code yet
Author: generated 2026-04-20 as follow-up to BUG-A (slug resolver) and BUG-B
(decline-to-estimate) in `docs/HARMONIC_COHORT_LIVE_RUN.md`.
Constraint: must stay inside our "free/public data only, no paid
scrapers, no accounts, no proxy rental" operating envelope.

## 1. What's actually blocking us

The 21-company blind run on 2026-04-20 resolved 6 valid LinkedIn slugs
via `linkedin_resolver`, and all 6 hit the auth-wall with HTTP `999` or
a `sign_in` HTML marker. Our `linkedin_public` observer correctly
classifies those as `gated` rather than `missing`, so the failure mode
is visible — but the content is still zero. Before buying our way out,
the question is: what's triggering the gate, and is there a cheaper
dial we haven't turned yet?

### 1.1 What LinkedIn scores on (2026 detection stack)

Consistent picture across four independent 2026 guides (Scrapfly,
TorchProxies, Proxies.sx, Scraperly) plus two working open-source
scrapers (Apify `automation-lab/linkedin-company-scraper`,
`sovereigntaylor/linkedin-company-scraper`):

| Signal              | Weight | Our current posture                                  |
|---------------------|--------|------------------------------------------------------|
| IP ASN class        | very high | Local dev = home residential (OK). CI / cloud = AWS/GCP/Hetzner ASNs, **blocked on sight**. |
| Request header shape| high   | **Bad**: only `User-Agent: Headcount-Estimator/0.1 (+internal-use; contact@example.com)`. No `Accept`, no `Accept-Language`, no `Accept-Encoding`, no `Sec-Fetch-*`. |
| Request velocity    | medium | Concurrency is 1, but no explicit inter-request delay. The TokenBucket is the only governor. |
| UA identity         | medium | **Bad**: we advertise as a bot by name. First thing LinkedIn's UA filter keys on. |
| TLS / JA3 fingerprint | medium | `httpx` default ciphers; distinguishable from Chrome but usually not a first-line trigger. |
| Session continuity  | low    | Not applicable to us — one URL per company, no multi-page flow. |
| Behavioral velocity | low    | Our run hits ≤ 21 LinkedIn URLs per day, far below any published threshold. |

### 1.2 Concrete numbers from 2026 community data

- **Daily-view ceiling on a single unauthenticated IP**: ≈ 500 public-profile views / day before the 999/429 gate escalates from
  "transient" to "sticky". Company pages are treated slightly more
  leniently than individual profiles (see PROXIES.SX 2026 table), but
  the same ceiling applies in practice.
- **Safe request spacing**: 3.0 – 6.0 s jittered delay between company
  page requests is the common "public-page" profile in all four guides.
  Fixed-interval requests (e.g. exactly 3.0 s) are themselves a signal
  — randomness is required, not optional.
- **Sticky session window**: only relevant when you traverse multiple
  pages per company (e.g. `/company/<slug>/` → `/company/<slug>/people/`).
  Recommended ≥ 30 minutes on the same IP. We do hit two URLs per
  company (`/` + `/people/` soft-gate), so this is mildly relevant.
- **JSON-LD path is more stable than the HTML badge.** LinkedIn emits
  a `<script type="application/ld+json">` block server-side for SEO
  (Googlebot eats it) *before* the authwall script runs. Apify's
  `automation-lab` scraper gets away with pure HTTP, no proxy, no
  cookies, 1 s delay, because it parses JSON-LD. The visible HTML
  badge regex we use today is inside a DOM fragment that the authwall
  banner can obscure. JSON-LD is present even when the body is mostly
  auth-wall chrome.
- **999 is IP-level, not account-level.** Recovery is about waiting
  out the reputation score on the current IP (hours to days), not
  changing any in-request signal. Once we're banned on an IP the
  cheapest unblock is to wait.

### 1.3 What our current logs tell us

From `data/runs/harmonic_live/20260420T235...Z/issues.json` and
`pipeline.json`:

- `linkedin_public.gated: 6` / `attempted: 21` / `no_slug: 15` — the 6
  we had slugs for all got walled.
- Every gate reason is either `marker:sign_in_to_see` (HTML authwall
  served with status 200) or `login_redirect` (we followed an HTTP 302
  into `/authwall`). We did *not* see HTTP 999 on those 6, which is
  actually informative: LinkedIn was willing to hand us *a* response,
  just an authwalled one. That means the IP was not on its burn-list —
  the headers/UA scored us as "bot, serve the decorated authwall"
  rather than "bot, drop the connection". That's a softer state than a
  hard 999, and it's the state where header tweaks can still flip the
  outcome.

## 2. Levers we can pull for free

Ordered by (expected lift) / (implementation cost). Each lever maps to
a concrete code change.

### L1. Browser-realistic header profile

**Biggest single win on our current posture.** Two stackoverflow
reports (from the 2014-era `curl -I` 999 thread through the 2026
`automation-lab` Apify actor) both confirm: LinkedIn's 999 filter is
keyed heavily on `User-Agent` + `Accept-Encoding`. Adding
`Accept-Encoding: gzip, deflate`, a current Chrome UA, and
`Accept-Language` has flipped 999 → 200 on identical IPs in multiple
reports.

**Code change** (`src/headcount/ingest/collect.py`, `default_http_configs`):

```python
SourceName.linkedin_public: HttpClientConfig(
    user_agent=(
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    max_concurrency=1,
    cache_ttl_seconds=settings.linkedin_public_company_ttl_days * 86400,
    default_headers={
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;q=0.9,"
            "image/avif,image/webp,*/*;q=0.8"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
        "Cache-Control": "max-age=0",
    },
),
```

Notes:
- `default_headers` is already supported by `HttpClientConfig` and
  plumbed through in `http.py:283` (`effective_headers`). No plumbing
  needed.
- Drop the self-identifying UA and contact email only for LinkedIn.
  Other sources (SEC, Wikidata) keep the polite identifying UA because
  those feeds *require* it.
- Rotating through 3-5 modern UAs (Chrome stable-1/2, Firefox ESR,
  Safari) costs < 20 LoC and lifts the "same UA over and over" signal.

**Expected lift**: flips the 6 auth-walled companies from
`marker:sign_in_to_see` to a real `200` with a parseable page on any
IP that isn't already on LinkedIn's reputation blocklist. On a pure
home-residential ASN (current dev state), this is the single change
most likely to get us from 0/6 to 4-6/6 on the Harmonic cohort.

### L2. JSON-LD parse fallback

Even when the visible HTML has the authwall banner, the JSON-LD block
is usually still emitted. Shape (LinkedIn, 2026):

```json
{
  "@context": "https://schema.org",
  "@type": "Organization",
  "name": "Stripe",
  "numberOfEmployees": {
    "@type": "QuantitativeValue",
    "value": 10000
  }
}
```

or, for bucket companies:

```json
"numberOfEmployees": {
  "@type": "QuantitativeValue",
  "minValue": 1001,
  "maxValue": 5000
}
```

**Code change** (`src/headcount/parsers/anchors.py`): add
`extract_linkedin_jsonld_employees(text)` that returns
`(low, high, open_ended, phrase)` or `None`, ordered ahead of the
regex-based `extract_linkedin_badge` in
`observers/linkedin_public.py:fetch_current_anchor`. Falls through to
the existing badge regex when JSON-LD is absent.

**Expected lift**: raises the parse rate on a *successful* fetch from
"depends on badge regex hitting" to "almost always succeeds", and
gives us a single numeric `employeeCount` on large companies that the
badge regex never had access to (badge is always a bucket).

### L3. Explicit per-request jitter + daily budget

Our current setup has concurrency=1 but no inter-request delay — on
cache misses we burst as fast as `httpx` will go. Add:

- `linkedin_public_min_delay_seconds: 3.0`
- `linkedin_public_max_delay_seconds: 6.0`
- `linkedin_public_daily_request_budget: 200`  (per-IP, per 24h,
  persisted to `SourceBudgetStore` we already have)

**Code change** (`src/headcount/ingest/collect.py` +
`src/headcount/config/settings.py`): wrap the `linkedin_public`
observer call in a `TokenBucket`-like rate that sleeps
`random.uniform(min, max)` between cache-miss requests, and a daily
budget that hard-caps total LinkedIn fetches per calendar day.
Exceeding the daily budget produces a `linkedin_budget_exhausted`
review item instead of firing more requests.

**Expected lift**: none at current scale (21 companies), but essential
before we scale to the 250–2000-company sweep. Keeps us beneath the
~500/day/IP threshold.

### L4. 999-aware circuit breaker

Today, if LinkedIn 999s request 3 of 21, we still send requests
4-through-21 and get 18 more 999s, each of which burns our daily
budget and deepens the IP reputation score. A circuit breaker at the
observer-factory level ("if the last N LinkedIn responses were all
gated, short-circuit the remaining LinkedIn fetches for this run")
stops bleeding once we've already been flagged.

Suggested threshold: 3 consecutive gates within a 60s window ⇒ trip
the breaker for the rest of the run.

**Code change** (new
`src/headcount/ingest/observers/linkedin_public.py`): track
`_gate_streak` and `_gate_streak_first_at` on the observer, raise
`AdapterGatedError` early when the breaker is tripped so the adapter
layer emits the same `linkedin_gated` review item but skips the HTTP
round-trip.

**Expected lift**: burns less of the daily budget on a bad day, and
preserves IP reputation for the next run.

### L5. Long-TTL cache bump

Company-size buckets move glacially — a 501–1,000 company becomes
1,001–5,000 maybe once over 6+ months. The
`linkedin_public_company_ttl_days` setting already defaults to 30 days
(`config/settings.py:66`), which is actually fine. Worth keeping in
mind while we iterate: within a TTL window, re-runs cost ≈ 0 HTTP
requests against LinkedIn, so measuring L1/L2 is cheap once you've
primed the cache.

**Code change**: none required today; flagged so we do not accidentally
shorten the TTL while tuning other knobs.

### L6. Bing / Google SERP sidecar for the hard cases

Not all companies will fall. For the tail of the cohort that keeps
999'ing our IP, the lightest-weight free alternative is to issue
`site:linkedin.com/company/<slug>` on Bing and parse the result
snippet. Bing's free programmatic endpoint (Bing Web Search v7, free
tier: 1000 qps/mo) returns the `/company/<slug>/` preview, which
LinkedIn specifically serves to search crawlers *including the
company-size badge text*. Example snippet:

```
SerpApi is a real time API to access Google search results. …
Software Development · 34 employees (+66.7% YoY) · Austin, United States
```

That "34 employees" / "10,001+ employees" / "201-500 employees"
substring is the same badge phrase our regex already recognises — we
can feed it straight into `extract_linkedin_badge` unchanged.

**Code change** (new
`src/headcount/ingest/observers/linkedin_bing_snippet.py`): fetch
`https://www.bing.com/search?q=site%3Alinkedin.com%2Fcompany%2F<slug>
%20<canonical_name>`, grep the badge phrase out of the SERP HTML,
emit it as a low-confidence anchor (confidence ≈ 0.35; lower than
`linkedin_public` because we're one level removed from the source).
Only fires when `linkedin_public` returned gated / nothing.

**Expected lift**: converts most of the gated-on-LinkedIn cases into
one-month-old cached badges at the cost of one extra HTTP round-trip
per company, on an IP (bing.com) that doesn't care who's knocking.

### L7. Offline weekly Harmonic-cohort schedule

Once L1–L6 are in place, spreading the 25-company Harmonic cohort
over a week (≈4 companies/day) effectively zeroes our risk of ever
tripping the daily budget *and* lets the cache warm up naturally. The
full 250-company sweep takes ~10 days, the 2000-company sweep ~50
days. That's acceptable given the product contract is monthly
estimates — we don't need daily freshness on company-size buckets
that change every 6-12 months.

**Code change**: `scripts/run_harmonic_cohort_live.py` already accepts
a cohort-slice argument in spirit (`--mode`); add
`--cohort-slice N/M` to split the company list into M shards and run
only shard N, cron'd once per day.

## 3. What we will NOT do

- **Paid proxy rental** (residential / mobile / ISP). Out of scope
  under the "free data only" constraint. Cost is $12-33 per account
  per month at scale and would break our reproducibility.
- **Logged-in account automation.** Direct ToS violation; LinkedIn
  permabans accounts caught scraping and the hiQ precedent doesn't
  protect authenticated access.
- **Anti-detect browsers** (Camoufox, GoLogin, Playwright + stealth).
  Overkill for the logged-out company page, breaks our offline-
  reproducible invariant, and wouldn't even help us with the 999 IP
  reputation problem — that's purely IP-shaped.
- **Custom TLS fingerprint spoofing** (curl-cffi, Chrome JA3 replay).
  The 2026 guides put this behind headers and IP in the detection
  stack. Returns are diminishing until L1+L2+L6 are all in place.

## 4. Recommended implementation order

1. **L5** (cache TTL bump to 14 d) — 3 line change, eliminates HTTP
   re-fetch cost while iterating.
2. **L1** (browser-realistic headers + UA rotation) — highest expected
   gain per LoC. Ship alone first and re-run the cohort to measure.
3. **L2** (JSON-LD parser) — raises parse rate on the 200s that L1
   unlocks.
4. **L4** (999-aware circuit breaker) — cheap insurance while L1/L2
   land.
5. **L3** (per-request jitter + daily budget) — required before we
   scale past the 25-company cohort.
6. **L6** (Bing snippet sidecar) — tail fix for the ~20 % that stays
   gated after L1+L2.
7. **L7** (cron'd cohort shard) — scale-out, only after L3+L6 prove
   durable on the 25-company cohort.

## 5. Measurement plan

After L1+L2 ship, rerun `scripts/run_harmonic_cohort_live.py
--mode=live-safe --cohort=harmonic25` and diff
`pipeline.json.stages.linkedin_public.*` against the current baseline:

| Metric                                | Baseline (2026-04-20) | Target (post-L1+L2) |
|---------------------------------------|-----------------------|---------------------|
| `linkedin_public.attempted`           | 21 (all, post-BUG-A)  | 21                  |
| `linkedin_public.gated`               | 6                     | 0 – 2               |
| `linkedin_public.parsed_badge`        | 0                     | 4 – 6               |
| `companies_declined_to_estimate`      | 21                    | 15 – 19             |
| `accuracy.harmonic.headcount_current.n` | 0                   | 4 – 6               |
| `accuracy.harmonic.headcount_current.mape` | null            | 0.20 – 0.40         |

If we clear the `n ≥ 4` bar, BUG-A + BUG-B + L1 + L2 together have
given us a real, Harmonic-scoreable signal for ~25% of the cohort on
purely free, public-page HTTP — which is the product-contract
minimum we need before tackling the SEC / employment-collection work
(BUG-F / BUG-H in the original report) and scaling past 25 companies.

## 6. Sources

- "How to Scrape LinkedIn in 2026", Scrapfly (2026-03)
- "LinkedIn Proxy & Automation Guide 2026", PROXIES.SX (2026-02)
- "LinkedIn Scraper Proxy Setup Guide", TorchProxies (2026-03)
- "How to Scrape LinkedIn in 2026: Tools, Proxies & Anti-Bot Guide",
  Scraperly (2026-03)
- "A Deep Dive into the HTTP 999 Status Code", UptimeRobot
  (2026-02-02)
- Apify actor `automation-lab/linkedin-company-scraper` v1.x — source
  of the "no login, no proxy, 1s delay" pure-HTTP baseline.
- Stackoverflow Q&A on HTTP 999 from 2014-2025 — cumulative evidence
  that UA + `Accept-Encoding` are the first-line filters.
