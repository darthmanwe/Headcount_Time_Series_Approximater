# Harmonic Cohort — Blind Live Run (Production-Shape Test)

> Status: reports a full end-to-end live run where **Harmonic and Zeeshan data
> are evaluation-only**. The pipeline is fed solely by our free-data stack
> (SEC, Wikidata, company-web, LinkedIn public, LinkedIn OCR, manual anchors).
> This is the honest production snapshot.
>
> Baseline run dir: `data/runs/harmonic_live/20260420T231635Z/`
> Post-L1..L4+bugfix run dir: `data/runs/harmonic_live/20260421T003808Z/`
> Mode: `live-full` | Cohort: 21 of 24 Harmonic-sampled companies

## Change log (this document)

| Revision | Change | Delta |
| --- | --- | --- |
| Baseline (20260420T231635Z) | Initial blind run, 10 bugs logged | Harmonic MAPE `n=0` (no evaluable rows) |
| After BUG-A (slug resolver) | LinkedIn attempts visible, still all gated | unchanged |
| After BUG-B (estimator 0.0 fallback) + L1+L2+L4+L3 | 999 rate dropped to ~30%, bodies arrive as 200s | signals_written 1 → 1 (gate check still blocked JSON-LD) |
| After **L2 gate-order fix** | JSON-LD parsed on 4/6 resolved slugs | signals_written **1 → 5**, companies_with_signals **1 → 5** |
| After **BUG-G (eval)** | `degraded_current_only` counts as real for current | Harmonic `n=0 → 6`, **MAPE 27.57%**, 3 companies within 3% |
| After A+C+B slice (shared `LinkedInRateGuard`, DDG→Bing SERP fallback, `--cohort-slice N/M`, broader `company_web` paths, single-token disambig tightening) | More companies reach LinkedIn; disambig rejects false positives (Alleva/Alloy) | Scored 6 → 6, MAPE 28.7% → 14.5% |
| After **fast-fail slug + defer-on-gate + recovery floor** (commit `52ee9c3`) | First 999 per company ends the heuristic loop; all slug-gated companies get parked for the recovery pass; recovery floor defaults to 120s, configurable via `--breaker-recovery-floor-seconds` | Scored ~5, MAPE ~17% under active LinkedIn rate limiting |
| After **BUG-leak** (commit `3bdf4ff`): stop promoting Harmonic/Zeeshan/LinkedIn workbook rows into anchor observations | Closes an evaluation leak where a DB with pre-loaded benchmarks (eg a resumed cohort SQLite) would let the estimator read the ground-truth back out | Scored **8** (of 21 harmonic-matched), **MAPE 13.25%**, AllTrails / 6sense / AlphaSense / AppZen / Arbor all inside ±7% |
| After **targeted DDG/Bing cache purge + slug retry loop** (uncommitted scripts `_retry_declined_slugs.py`, `_collect_and_estimate_specific.py`, `_reestimate_cohort.py`) | DDG/Bing cache entries that had 202/CAPTCHA during the main run were removed so opportunistic retries could hit fresh SERP responses; LinkedIn 999-gating for domain-label slugs eased enough to admit 1010data / AliveCor / Appvance / aPriori / Arbol | Scored **13** (of 21 harmonic-matched), **MAPE 10.24%**, median abs err 10 employees |

See [LINKEDIN_BOT_WALL_STRATEGY.md](LINKEDIN_BOT_WALL_STRATEGY.md) for L1..L7 lever catalogue.

## Post-fix headline (latest run)

**Run dir**: `data/runs/harmonic_live/20260420T205059Z_postleak/`
(live-full, `--retry-breaker-skips --breaker-recovery-floor-seconds=900`,
http_cache copied from `20260421T003808Z`, fresh cohort DB so there is
no benchmark-anchor leak, followed by several targeted slug-retry
passes driven by `scripts/_retry_declined_slugs.py` once the stale
DDG-202/Bing-CAPTCHA envelopes had been purged from the cache.)

| Provider | n | MAPE | MAE | Median abs err |
| --- | ---: | ---: | ---: | ---: |
| Harmonic (primary) | **13** | **10.24%** | 44.8 | **10.0** |
| Zeeshan (supporting) | 13 | 6.27% | 22.0 | 0.0 |
| LinkedIn (supporting) | 13 | 27.41% | 48.5 | 0.0 |

- `companies_declined_to_estimate`: **8 of 21** Harmonic-matched
  companies. Every scored company's anchor came from the
  `linkedin_public` JSON-LD path; the remaining 8 declines are all
  companies whose domain_label probe either 999-gated against
  LinkedIn while DDG was simultaneously throttling and Bing was
  serving CAPTCHAs, or whose heuristic slug returned a 404/disambig
  rejection and the SERP fallback could not supply an alternative.
- Median absolute error is **10 employees**, and 10 of the 13 scored
  companies sit inside ±13% of Harmonic (9 of 13 inside ±10%, 6 of 13
  inside ±3%).

### Per-company (Harmonic vs our current estimate)

Scored (ordered by err%):

| Company | Harmonic | Our estimate | Err % | Source |
| --- | ---: | ---: | ---: | --- |
| 1010data | 65 | 65 | **0.0** | LinkedIn JSON-LD |
| AllTrails | 376 | 376 | **0.0** | LinkedIn JSON-LD |
| 6sense | 1,565 | 1,590 | **+1.6** | LinkedIn JSON-LD |
| AliveCor | 206 | 210 | **+1.9** | LinkedIn JSON-LD |
| AlphaSense | 2,969 | 3,044 | **+2.5** | LinkedIn JSON-LD |
| AppZen | 376 | 386 | **+2.7** | LinkedIn JSON-LD |
| aPriori Technologies | 268 | 278 | **+3.7** | LinkedIn JSON-LD |
| Arbor Biotechnologies | 99 | 93 | **-6.1** | LinkedIn JSON-LD |
| Arbol | 96 | 105 | **+9.4** | LinkedIn JSON-LD (Bing SERP slug) |
| Appvance | 50 | 56 | **+12.0** | LinkedIn JSON-LD |
| 1Kosmos | 118 | 133 | **+12.7** | LinkedIn JSON-LD |
| Alpaca | 390 | 275 | **-29.5** | LinkedIn JSON-LD (Harmonic counts contractors?) |
| 15Five | 602 | 295 | **-51.0** | LinkedIn JSON-LD (stale / restructured?) |

Declined (all 8 caused by the same external ceiling — LinkedIn 999 +
DDG 202 + Bing CAPTCHA combined to prevent either a heuristic or a
SERP-resolved slug from verifying):

| Company | Harmonic | Cause |
| --- | ---: | --- |
| Alleva | 78 | LinkedIn 999 on `helloalleva`; SERP blocked |
| Alloy | 400 | LinkedIn 999 on `alloy`; SERP blocked |
| Alloy Therapeutics, Inc. | 128 | LinkedIn 404 on `alloytx`, 999 on `alloy-therapeutics-inc`; SERP blocked |
| Allvue Systems | 575 | LinkedIn 999 on `allvuesystems`; SERP blocked |
| AlphaPoint | 91 | LinkedIn 999 on `alphapoint`; SERP blocked |
| Apptega | 54 | LinkedIn 999 on `apptega`; SERP blocked |
| Aprimo | 311 | LinkedIn 404 on `aprimo` (real slug is elsewhere); SERP blocked |
| Arable | 84 | disambig correctly rejects "Arable Consulting" on `arable`; SERP blocked so no alternative |

### Stop condition

Per owner instruction, iteration stopped at the user-specified
exception: "You've exhausted LinkedIn daily limit and other sources
from online have similar problems." Every remaining decline is a
direct consequence of that three-way lockout from this IP; the
shared `LinkedInRateGuard`, the defer-on-gate queue, the 900s
breaker-recovery floor, and the cohort-slice sharding are already
wired up to drain the deferred companies on a cold IP or a later
day with no code changes.

### Change-log of the retry loop inside this run

| Pass | Action | Scored | MAPE |
| --- | --- | ---: | ---: |
| Initial `_postleak` anchor collect | BUG-leak closed, cold cache | 8 | 13.25% |
| Retry 1 (DDG/Bing cache purge + slug backfill) | AliveCor resolved (rolled back by estimate_series bug) | 8 | 13.25% |
| Retry 2 (same, after fixing `estimate_series` signature) | +Arbol via Bing SERP | 9 | 12.4% |
| Targeted recollect for AliveCor | +AliveCor (LinkedIn 200 this window) | 10 | 11.74% |
| Targeted recollect for 1010data + Appvance | +2 both LinkedIn 200 | 12 | 10.78% |
| Retry 3 (opportunistic, DDG still 202) | +aPriori Technologies via 200 | 13 | **10.24%** |
| Retry 4 | 0 new slugs (LinkedIn 999 / DDG 202 / Bing CAPTCHA) | 13 | 10.24% |

---

## Historical baseline snapshot (kept for reference)

## Methodology

- Isolated SQLite DB (`cohort.sqlite`) provisioned from scratch, Alembic
  `upgrade head`.
- Seeded the full `High Priority Companies_01.04.2026.xlsx` (1,754 candidates
  → 1,745 canonical companies + 1,754 aliases).
- Resolved the 24 Harmonic target names against `Company`/`CompanyAlias`
  (no benchmark rows consulted). 21 matched; 3 blocked by canonical-resolver
  legal-suffix handling.
- Ran `collect_anchors` (live HTTP) scoped to those 21 companies, with
  all five observers: `manual`, `sec`, `wikidata`, `company_web`,
  `linkedin_public`.
- Ran `collect_employment` with `linkedin_ocr` requested (no profiles CSV).
- Ran `estimate_series` scoped to those 21 companies.
- **Then** loaded Harmonic / Zeeshan / LinkedIn benchmark observations
  from the workbook as **evaluation-only** data. They were never
  promoted into `company_anchor_observation` and never touched the
  estimator.
- Backfilled `benchmark_observation.company_id` FKs via the existing
  resolver linkage so the evaluator could join.
- Scored with `evaluate_against_benchmarks` (eval_v2).

`scripts/run_harmonic_cohort_live.py` is the full reproducible runbook.
`scripts/_harmonic_amend_eval.py` re-runs the FK backfill + evaluation
on an existing run dir.

## Headline Scoreboard

- `primary_provider`: `harmonic`; `supporting`: `zeeshan`, `linkedin`.
- Companies in scope: **21**; with Harmonic benchmark: **21**.
- **`mape_headcount_current` vs Harmonic: 0.9664** (~97% error).
- `mape_headcount_current` vs Zeeshan: 0.9524.
- `mape_headcount_current` vs LinkedIn (loader bug): 0.9524.
- `spearman_growth_6m`, `spearman_growth_1y`: **null** — our estimator
  produced zero growth signal for 20 of 21 companies.
- Confidence bands across the 1,596 monthly estimate rows:
  `high: 0`, `medium: 0`, `low: 76`, `manual_review_required: 1,520`.

## Production coverage — where our free-data pipeline actually produced signal

Across all 21 companies we created **exactly one anchor observation**.

| Source | attempted | signals | gated | errors | anchors produced |
|---|---|---|---|---|---|
| `manual`          | 21 | 0 | 0 | 0 | 0 |
| `sec`             | 21 | 0 | 0 | 1 | 0 |
| `wikidata`        | 21 | 0 | 0 | 0 | 0 |
| `company_web`     | 21 | 1 | 9 | 0 | **1** (Alpaca) |
| `linkedin_public` | 21 | 0 | 0 | 0 | 0 |
| `linkedin_ocr`    | 21 | 0 | 0 | 0 | 0 |

The lone Alpaca anchor came from the company-website observer parsing
a `250-500` range on `alpaca.markets`, yielding `point=275.0, min=250,
max=312.5, confidence=0.55`.

## Per-company snapshot (all 21 resolved)

| Harmonic name | Harmonic HC | Our estimate | Conf | Signal? |
|---|---:|---:|---:|---|
| 1010data | 65 | 0.0 | 0.30 | — |
| 15Five | 602 | 0.0 | 0.30 | — |
| 1Kosmos | 118 | 0.0 | 0.30 | — |
| 6sense | 1565 | 0.0 | 0.30 | — |
| AliveCor | 206 | 0.0 | 0.30 | — |
| Alleva | 78 | 0.0 | 0.30 | — |
| Alloy | 400 | 0.0 | 0.30 | — |
| Alloy Therapeutics | 128 | 0.0 | 0.30 | — |
| AllTrails | 376 | 0.0 | 0.30 | — |
| Allvue Systems | 575 | 0.0 | 0.30 | — |
| **Alpaca** | **390** | **275.0** | **0.62** | **company_web** |
| AlphaPoint | 91 | 0.0 | 0.30 | — |
| AlphaSense | 2969 | 0.0 | 0.30 | — |
| Apptega | 54 | 0.0 | 0.30 | — |
| Appvance | 50 | 0.0 | 0.30 | — |
| AppZen | 376 | 0.0 | 0.30 | — |
| Aprimo | 311 | 0.0 | 0.30 | — |
| aPriori Technologies | 268 | 0.0 | 0.30 | — |
| Arable | 84 | 0.0 | 0.30 | — |
| Arbol | 96 | 0.0 | 0.30 | — |
| Arbor Biotechnologies | 99 | 0.0 | 0.30 | — |

Unresolved (canonical matching dropped them): `1upHealth, Inc.`,
`Apptopia Inc.`, `Aptos Labs`.

## Bugs and fix plan

### P0 — the pipeline is effectively empty; fix these to get real coverage

#### BUG-A — LinkedIn public observer silently returns 0 when `linkedin_company_url` is missing

- Where: `src/headcount/ingest/observers/linkedin_public.py:94-97`.
- Root cause: observer bails out if `target.linkedin_company_url` is
  empty, and the `import_candidates` seed workbook has no LinkedIn URL
  column. Every one of the 21 companies short-circuits before any HTTP
  fetch, which explains `attempted=21, errors=0, gated=0, signals=0`.
- Effect: the single highest-signal public source contributes **zero**
  anchors for the full cohort. In production, ~0 companies will have a
  LinkedIn slug seeded manually.
- Fix plan:
  1. Add a **LinkedIn slug resolver** that runs during
     `canonicalize`. Order of attempts:
     - Domain-slug heuristic: try
       `linkedin.com/company/<first label of domain>` and
       `linkedin.com/company/<normalized company name>`; accept the
       slug if the page returns 200 and the `og:title` / meta tag
       matches the canonical name (fuzzy).
     - Wikidata: `P6634` ("LinkedIn company ID") when the company
       resolves to a Wikidata entity.
     - Fallback: DuckDuckGo / Bing search
       `site:linkedin.com/company <name>` (first hit). Cache the
       resolver result in a new `CompanyAlias(alias_type=linkedin)` row
       or a dedicated `linkedin_company_url` column on `Company`.
  2. Make the observer log `reason=no_linkedin_slug` so this failure
     mode is never silent again.

#### BUG-B — degraded-only estimator emits `0.0`, not `NULL` / `no_data`

- Where: `src/headcount/estimate/pipeline.py` (the branch that produces
  the fallback monthly series with `confidence_score=0.3`).
- Root cause: when `reconcile_series` runs with zero anchors, the
  estimator still writes a flat series of `0.0`. That is a wrong
  positive — consumers cannot distinguish "we estimated 0 people" from
  "we have no idea".
- Effect: every MAPE row shows `estimate_point=0.0` and
  `abs_ratio=1.0`, inflating error metrics and poisoning any
  downstream aggregations (percentile ranking, reference comparisons).
- Fix plan:
  1. Replace the `0.0` fallback with either (a) no rows written and a
     `CompanyRunStatus(stage=estimate_series, status=skipped,
     note="no_anchors")`, or (b) rows written with a new
     `HeadcountEstimateMethod.none` enum and `estimated_headcount=NULL`.
     The evaluator should skip such rows instead of scoring them as 0.
  2. Update `Scoreboard` to track `companies_no_data` separately from
     `companies_evaluated`.

#### BUG-C — `company_web` parser misses headcount claims on 11 of 21 200-OK pages

- Where: `src/headcount/ingest/observers/company_web.py` anchor
  extractor (`company_web_anchor_hits` logs `matched=0` for all
  successful fetches except Alpaca).
- Root cause: the regex / DOM rules cover "250-500 employees" style
  strings well, but miss common patterns we saw in the cohort:
  `"team of N"`, `"N+ people"`, `"over N professionals"`, JSON-LD
  `@type=Organization, numberOfEmployees=N`, and Schema.org hidden
  meta.
- Effect: even when a private-SMB site is reachable and contains a
  headcount claim, we cannot parse it.
- Fix plan:
  1. Add JSON-LD `Organization.numberOfEmployees` parser (covers a
     large fraction of Careers / About pages).
  2. Add regex variants: `team of \d+`, `\d+\+?\s+(employees|people|
     professionals|engineers)`, `over \d+`, `more than \d+`.
  3. Add Archive.org fallback: when a live URL returns 403/429/5xx,
     retry once via
     `https://web.archive.org/web/YYYY/<url>` (same parser).
  4. Pick the **most recent** claim when a page contains multiple
     (typical for "grew from 50 to 300" marketing copy).

### P1 — coverage improvements

#### BUG-D — canonical resolver drops legal suffix handling for 3 of 24 Harmonic targets

- Where: same as prior report. `1upHealth, Inc.`, `Apptopia Inc.`,
  `Aptos Labs` do not match any seeded company row even though the
  workbook contains them.
- Root cause: the normalizer strips `", Inc."` but the corresponding
  seed record has `Inc.` kept, so lookup keys differ. `Aptos Labs`
  isn't in the seed workbook at all (data gap, not normalization).
- Effect: ~12% of the Harmonic cohort lost before any pipeline stage.
- Fix plan: **same fix as in the previous draft of this report**:
  tighten normalize_legal_suffix to canonicalize both sides to a
  suffix-stripped key, and treat `"Inc.", ",Inc", " Inc"` as
  equivalent. Then re-import Aptos Labs manually (data gap).

#### BUG-E — OCR observer is silently inert

- Where: `LinkedInGrowthTrendObserver` builds fine but produces zero
  signals because (a) `tesseract` binary is not on this workstation
  and (b) there is no image-acquisition driver, so even with
  tesseract installed the queue is empty.
- Effect: LinkedIn OCR growth-trend, explicitly listed as a high-value
  source in the original design, contributes zero in practice.
- Fix plan:
  1. Add a startup probe: call `pytesseract.get_tesseract_version()`
     at observer init; if it raises, log `tesseract_unavailable` and
     refuse to register unless `--enable-ocr` is passed explicitly.
  2. Build an image-acquisition step that, for companies with a
     resolved LinkedIn slug and a `playwright`-capable environment,
     screenshots the growth-trend chart on `/insights/` and stashes
     the PNG in `RUN_ARTIFACT_DIR` to be consumed by the OCR pass.
  3. Document the install path
     (`winget install UB-Mannheim.TesseractOCR` on Windows; `brew
     install tesseract` on macOS).

#### BUG-F — SEC observer is structurally useless for the Harmonic cohort

- Where: `src/headcount/ingest/observers/sec.py`.
- Root cause: the Harmonic sample is entirely private SMBs. `sec` is
  only useful for filers. This isn't a bug per se, but every
  `attempted=21` is wasted load.
- Fix plan: add a pre-filter — skip SEC if the company is not in
  `company_tickers.json` (already fetched). Saves roundtrips and makes
  the `errors=1` noise disappear.

### P2 — polish

#### BUG-G — bot-block handling on company_web

- `company_web` hit 9 gates (403) out of 21. Cloudflare / Akamai
  shields are not going to let us through with a plain httpx client.
- Fix plan: add a fallback request through
  `https://web.archive.org/web/2y/<url>` when a live fetch returns
  403/429/503 — historical snapshots are plentiful for marketing
  pages and uncensored.

#### BUG-H — Spearman / growth metrics null because we have no series

- Not a code bug — a consequence of BUG-A through BUG-C. Once we have
  ≥2 historical anchors per company (via LinkedIn public, archived
  company pages, or OCR), `mae_growth_6m_pct` and Spearman will start
  populating naturally.

## What works (validated by this run)

- Isolated run harness (Alembic upgrade, scoped live observers, clean
  FK backfill, evaluation-only benchmark loading).
- Canonical resolution at full workbook scale (1,754 candidates in ~0.9s).
- `benchmark_anchor_promotion` correctly **does not fire** when no
  benchmark rows are present at `collect_employment` time — verified
  by `scanned=0, inserted_anchor_rows=0`.
- Evaluator joins through `company_candidate_id → company_id` once the
  resolver backfill runs, exposing Harmonic/Zeeshan/LinkedIn side by
  side in `top_disagreements`.
- HTTP cache and rate-limited observer dispatch ran to completion in
  ~100s for 21 companies × 5 adapters. This is a reasonable per-company
  cost budget for the 250–2,000 company production batches.

## What this means for next steps

The run confirms that the **evaluation harness is sound**, but the
**production pipeline has almost no yield** against private SMBs
without paid data. The ranked fix sequence is:

1. Land BUG-A (LinkedIn slug inference). This is the single highest-
   leverage change — LinkedIn public is the only free source with
   broad SMB coverage.
2. Land BUG-B (no more fake `0.0` fallback) so evaluation numbers stop
   being poisoned by silent no-data rows.
3. Land BUG-C (company_web parser widening + Archive.org fallback).
4. Rerun this harness; expect `mape_headcount_current` to drop to the
   0.25–0.45 range on the companies that gain LinkedIn coverage.

Ticket-ready bugs: A, B, C, D, E, F, G. Bug H resolves by transitivity
once A–C ship.
