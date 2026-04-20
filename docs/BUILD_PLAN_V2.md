# Internal Headcount Time Series System - Build Plan V2

## 0. What this system is and is not

This system is an **internal-use evidence-driven estimation tool** for reconstructing company employee headcount time series and growth metrics over 6-month, 1-year, and 2-year windows.

It is not a general-purpose commercial enrichment platform.
It is not a crawler framework meant to push through access controls.
It is not a data broker replacement.

The narrow slice is:

- take a priority company list
- resolve canonical company identity
- gather lawful public evidence and free-tier evidence
- include a bounded **logged-out public LinkedIn observation path**
- reconstruct monthly headcount estimates
- compute 6m / 1y / 2y growth
- surface confidence and evidence trace
- route weak cases to manual review

The first version should optimize for:

- accuracy on a narrow set of priority companies
- reproducibility
- evidence traceability
- low operational complexity
- agent-friendly implementation
- local-first execution

---

## 1. Executive technical decision summary

### 1.1 Recommended path for least resistance

Use:

- Python 3.12
- Postgres 16
- DuckDB
- FastAPI
- Typer CLI
- SQLAlchemy 2.x
- Alembic
- Pydantic v2
- httpx
- BeautifulSoup4 + lxml
- Playwright only when a public page requires rendered DOM access
- pandas / polars for transforms
- Streamlit for the first review UI
- Redis only if queueing becomes necessary

### 1.2 Why this path

This path is the lowest-friction option because it is:

- easy to run locally
- easy to reason about in Codex and Cursor
- modular without heavy platform work
- deterministic for tests
- cheap to iterate
- straightforward for hybrid batch + manual workflows

### 1.3 Source acquisition modes

This system supports four source modes.

#### Mode A: First-party public web
Examples:
- company about page
- team page
- careers page
- investor page
- newsroom
- press releases

Purpose:
- company identity
- public headcount statements
- event evidence
- hiring/growth clues

#### Mode B: Free-tier APIs and public datasets
Examples:
- free credits from company/person APIs
- public SEC / issuer data
- public registries
- startup databases with free access tiers

Purpose:
- secondary anchors
- canonicalization support
- event validation
- spot checks

#### Mode C: Logged-out public LinkedIn observation path
This path is included because the target approximation is closest to LinkedIn-style employee-count time series.

Purpose:
- current employee count anchor when visible on a public company page
- public profile evidence
- public employment-history interval evidence
- validation against observed public company data

Hard boundaries:
- public pages only
- no authentication
- no CAPTCHA solving
- no rotating proxies
- no session/account farming
- no stealth browser fingerprint evasion
- no retry loops designed to force access after blocks
- fail closed on friction or gating

#### Mode D: Manual analyst validation
Purpose:
- top-priority account verification
- low-confidence override
- acquisition / alias / rebrand corrections
- disputed current anchors
- stored anchor observations

### 1.4 Core estimation method

The best approximation for a LinkedIn-like historical series is:

1. resolve canonical company
2. capture current headcount anchor
3. collect public employment-history evidence tied to that company
4. convert each profile experience block to month-level activity windows
5. compute monthly public active-profile counts
6. form a ratio series against current public sample
7. scale by current headcount anchor
8. segment around major company events
9. smooth or reject anomalies
10. assign confidence and evidence trace

This yields a monthly series from which:
- 6-month growth
- 1-year growth
- 2-year growth

are directly computed.

This is the right narrow slice.

---

## 2. What likely drives the target metric quality

The likely differentiators are not fancy math.
They are:

- entity resolution
- alias and parent/sub handling
- stored snapshot continuity
- event segmentation
- quality review
- confidence gating

The biggest failures will come from identity errors, not arithmetic errors.

The architecture must reflect that.

---

## 3. System goals and non-goals

## 3.1 Goals

- produce an evidence-backed monthly headcount estimate for each company
- produce 6m / 1y / 2y growth metrics
- support confidence scoring and manual review
- retain raw evidence snapshots and parsing outputs
- make every final metric traceable to evidence
- support repeated refreshes and versioned estimation runs
- make the repo easy for agentic builders to extend

## 3.2 Non-goals for V1 narrow slice

- no fully automated planet-scale crawling
- no distributed crawler fleet
- no real-time event streaming
- no commercial multi-tenant productization
- no irreversible data pipelines
- no opaque ML model as the primary estimator
- no heavy vector/RAG stack for first implementation

---

## 4. Architecture overview

## 4.1 High-level components

1. `ingest`
   - source adapters
   - HTTP fetchers
   - public page renderers
   - raw snapshot persistence

2. `normalize`
   - parsers
   - date normalization
   - canonical field extraction
   - parser versioning

3. `resolve`
   - company canonicalization
   - alias mapping
   - parent/sub disambiguation
   - source-link resolution

4. `estimate`
   - employment interval expansion
   - monthly active-public-profile counts
   - scaling to current anchor
   - anomaly detection
   - event-aware segmentation
   - confidence scoring

5. `review`
   - analyst work queue
   - manual overrides
   - evidence comparison UI
   - audit log

6. `serve`
   - API
   - export endpoints
   - CSV / parquet output
   - admin read APIs

---

## 5. Repository structure

```text
headcount-system/
  AGENTS.md
  README.md
  .codex/
    config.toml
  .cursor/
    rules/
      00-project.md
      01-architecture.md
      02-sources.md
      03-estimation.md
      04-testing.md
      05-review.md
  plans/
    PLANS.md
  prompts/
    codex_plan_mode_bootstrap.md
    codex_execplan_prompt.md
    cursor_plan_mode_bootstrap.md
    cursor_exec_prompt.md
  docs/
    BUILD_PLAN_V2.md
    METHODOLOGY_AND_ASSUMPTIONS_V2.md
    SOURCE_MATRIX_V2.md
    ACCEPTANCE_CRITERIA_V2.md
  test_source/
    High Priority Companies_01.04.2026.xlsx
    Sample Employee Growth for High Priority Prospects.xlsx
  apps/
    api/
    review_ui/
  src/
    headcount/
      config/
      db/
      models/
      schemas/
      clients/
      ingest/
      parsers/
      resolution/
      estimation/
      review/
      serving/
      utils/
  tests/
    unit/
    integration/
    golden/
    fixtures/
  scripts/
  migrations/
  data/
    seeds/
    fixtures/
    outputs/
```

---

## 6. Tech stack options and recommendation

## 6.1 Application language

### Option A: Python
Pros:
- fastest path
- strongest parsing/data ecosystem
- easiest for agents
- strong web, ETL, and API tooling

Cons:
- careful type discipline required
- performance tuning may matter later

Recommendation:
**Use Python.**

### Option B: TypeScript
Pros:
- strong app/server ergonomics
- good if frontend-heavy
- one language for more of the stack

Cons:
- weaker for heavy local data workflows
- more awkward for batch estimation math

Recommendation:
Not primary for this slice.

## 6.2 Database

### Option A: Postgres
Pros:
- durable source of truth
- strong relational constraints
- good JSONB support
- easy review/audit modeling

Recommendation:
**Use Postgres as the system of record.**

### Option B: SQLite only
Pros:
- dead simple

Cons:
- too limiting once concurrency and review start

Recommendation:
Good for throwaway prototype only, not recommended.

## 6.3 Analytical compute

### Option A: DuckDB
Pros:
- ideal for local analytical transforms
- great for parquet
- excellent batch rebuild support

Recommendation:
**Use DuckDB in addition to Postgres.**

### Option B: pandas only
Pros:
- simple

Cons:
- less ergonomic for persistent local analytical stores

Recommendation:
Use pandas or polars inside stages, but keep DuckDB.

## 6.4 API layer

### Option A: FastAPI
Recommendation:
**Use FastAPI.**

## 6.5 Review UI

### Option A: Streamlit
Pros:
- fastest internal UI
- enough for review workflows

Recommendation:
**Use Streamlit first.**

### Option B: Retool
Pros:
- fast if already available

Cons:
- external dependency
- less repo-native

Recommendation:
Optional. Use only if already convenient.

## 6.6 Task orchestration

### Option A: Typer + cron/manual job runner
Pros:
- simplest
- enough for V1

Recommendation:
**Use Typer commands first.**

### Option B: Prefect
Pros:
- better workflows and retries
- easier scaling later

Cons:
- more setup

Recommendation:
Use only if job graph grows.

### Option C: Airflow
Not recommended for the narrow slice.

---

## 6.7 Offline benchmark inputs

The build pack includes offline spreadsheet inputs in `test_source/`:

- `High Priority Companies_01.04.2026.xlsx`
- `Sample Employee Growth for High Priority Prospects.xlsx`

These files are used to:

- benchmark approximation quality against Harmonic.ai-style outputs captured in the spreadsheets
- provide company-detail examples that act as ground-truth references for narrow-slice validation
- seed golden tests, comparison exports, and analyst-review scenarios

They are not live acquisition sources.
They are offline validation artifacts and must preserve workbook, sheet, row, and column provenance in any derived fixture or expected-output record.

---

## 7. Source acquisition architecture

## 7.1 Core source adapters

Implement every source behind a typed adapter interface.

### Interface contract

Each adapter must support:

- `fetch_company_evidence(company_candidate) -> list[SourceObservation]`
- `fetch_anchor_observations(canonical_company) -> list[CompanyAnchorObservation]`
- `fetch_employment_observations(canonical_company) -> list[EmploymentObservation]`
- `healthcheck() -> SourceHealth`
- `capabilities() -> SourceCapabilities`

Never let estimation logic depend directly on raw HTML.

## 7.2 Adapters to build first

1. `company_web_observer`
2. `linkedin_public_observer`
3. `sec_observer`
4. `manual_observer`
5. `free_api_observer` placeholders for optional free-tier sources

## 7.3 Logged-out public LinkedIn path

This is included by design.

### Allowed usage
- public company pages
- public profile pages
- public experience blocks
- public page fields visible without auth

### Disallowed implementation
- login or account automation
- anti-bot evasion tooling
- CAPTCHA solving
- rotating proxies
- stealth browser stacks
- headers or retry logic designed to mimic or persist around blocks
- scraping against blocks after gating is detected

### Fail-closed behavior
If the path encounters:
- auth wall
- hard gate
- challenge page
- unstable DOM with insufficient evidence

then:
- stop
- persist source health note
- mark observation unavailable
- route company to alternate evidence or manual review

### Fetch policy
- use low request volume
- cache aggressively
- persist normalized and raw evidence
- version parsers
- hash page content
- only re-fetch based on staleness policy or explicit analyst request

---

## 8. Data model

## 8.1 Core entities

### `company`
Canonical company record.

Fields:
- `id`
- `canonical_name`
- `canonical_domain`
- `linkedin_company_url`
- `country`
- `state_or_region`
- `status`
- `created_at`
- `updated_at`

### `company_alias`
Fields:
- `id`
- `company_id`
- `alias_name`
- `alias_type`
- `confidence`
- `source`

### `company_source_link`
Fields:
- `id`
- `company_id`
- `source_name`
- `source_url`
- `source_external_id`
- `is_primary`
- `confidence`
- `last_verified_at`

### `source_observation`
Raw or normalized observation from any source.

Fields:
- `id`
- `source_name`
- `entity_type`
- `source_url`
- `observed_at`
- `raw_text`
- `raw_html_path`
- `raw_content_hash`
- `parser_version`
- `parse_status`
- `normalized_payload_json`

### `company_anchor_observation`
Fields:
- `id`
- `company_id`
- `source_observation_id`
- `anchor_type`
- `headcount_value`
- `anchor_month`
- `confidence`
- `note`

### `person`
Fields:
- `id`
- `source_person_key`
- `display_name`
- `source_name`
- `profile_url`
- `created_at`
- `updated_at`

### `person_employment_observation`
Fields:
- `id`
- `person_id`
- `company_id`
- `source_observation_id`
- `observed_company_name`
- `job_title`
- `start_month`
- `end_month`
- `is_current_role`
- `confidence`

### `company_event`
Fields:
- `id`
- `company_id`
- `event_type`
- `event_month`
- `source_observation_id`
- `confidence`
- `description`

### `headcount_estimate_monthly`
Fields:
- `id`
- `company_id`
- `estimate_version_id`
- `month`
- `estimated_headcount`
- `public_profile_count`
- `scaled_from_anchor_value`
- `method`
- `confidence_band`
- `needs_review`

### `estimate_version`
Fields:
- `id`
- `company_id`
- `estimation_run_id`
- `method_version`
- `source_snapshot_cutoff`
- `status`
- `created_at`

### `confidence_component_score`
Fields:
- `id`
- `estimate_version_id`
- `component_name`
- `component_score`
- `note`

### `manual_override`
Fields:
- `id`
- `company_id`
- `field_name`
- `override_value_json`
- `reason`
- `entered_by`
- `created_at`
- `expires_at`

### `review_queue_item`
Fields:
- `id`
- `company_id`
- `estimate_version_id`
- `review_reason`
- `priority`
- `status`
- `assigned_to`
- `created_at`
- `resolved_at`

### `audit_log`
Fields:
- `id`
- `actor_type`
- `actor_id`
- `action`
- `target_type`
- `target_id`
- `payload_json`
- `created_at`

---

## 9. Evidence schema contracts

## 9.1 Source observation schema

```json
{
  "source_name": "linkedin_public",
  "entity_type": "company|person_profile|press_release|manual",
  "source_url": "https://example.com",
  "observed_at": "2026-04-19T12:34:56Z",
  "raw_content_hash": "sha256...",
  "parser_version": "linkedin_company_v1",
  "normalized_payload": {}
}
```

## 9.2 Company anchor observation schema

```json
{
  "company_id": "uuid",
  "anchor_type": "current_headcount_anchor|historical_statement|manual_anchor",
  "headcount_value": 1691,
  "anchor_month": "2026-04-01",
  "confidence": 0.89,
  "source_name": "linkedin_public"
}
```

## 9.3 Employment observation schema

```json
{
  "person_id": "uuid",
  "company_id": "uuid",
  "observed_company_name": "Acme",
  "job_title": "Software Engineer",
  "start_month": "2023-08-01",
  "end_month": null,
  "is_current_role": true,
  "confidence": 0.81,
  "source_name": "linkedin_public"
}
```

---

## 10. Estimation methodology

## 10.1 Canonical company resolution

### Inputs
- company list row
- company name
- website/domain
- aliases
- known old names
- candidate source links

### Resolution logic
Use a deterministic scoring model.

Signals:
- exact domain match
- normalized name match
- alias match
- title similarity
- source URL structure
- prior manual override
- parent/sub conflict flags

Output:
- canonical company
- best source links
- resolution confidence
- ambiguity flags

Never skip this.
This is the highest leverage layer in the system.

## 10.2 Current anchor capture

Preferred order:
1. visible public company page employee count anchor
2. first-party company statement
3. manual anchor
4. free-tier API anchor

Rules:
- store all anchors, not just the chosen one
- choose the best anchor by confidence policy
- time-stamp each anchor
- anchor selection must be explainable

## 10.3 Public employment-history reconstruction

For each employment observation:
- parse start month
- parse end month
- infer current role if explicitly indicated
- normalize missing month granularity conservatively
- store source text and parse version

Then expand to month-level activity intervals.

Example:
- start `2023-08`
- end `2024-11`

expands to:
- 2023-08 through 2024-11 inclusive

If current:
- expand through anchor month or run cutoff month

## 10.4 Monthly public profile counts

For each company and month:
- count distinct people active in that month

This produces:
- `public_profile_count_by_month`

Also compute:
- `public_profile_count_current`

## 10.5 Scaled historical headcount estimate

For each historical month:

`estimated_headcount_month = current_anchor * (public_profile_count_month / public_profile_count_current)`

Guardrails:
- if current public count is too small, lower confidence
- if denominator is zero, reject estimate
- if current anchor is missing, do not emit final estimate
- if there is strong evidence of discontinuity, segment before scaling

## 10.6 Event-aware segmentation

Events that can break continuity:
- acquisition
- merger
- rebrand
- spinout
- major layoff
- parent/sub reassignment
- stealth-to-public transition

When an event is detected:
- split the series into pre/post segments
- lower cross-segment continuity confidence
- consider manual review before publishing 2-year growth

## 10.7 Anomaly detection

Flag:
- sudden jumps far outside recent trajectory
- anchor/profile mismatch
- large divergence from prior estimate versions
- implausibly flat series during intense hiring evidence
- sample collapse
- too many profiles with ambiguous company mapping

Anomalies do not automatically overwrite numbers.
They route to review or downgrade confidence.

## 10.8 Confidence scoring

### Components
- resolution confidence
- anchor confidence
- public profile sample size
- date completeness score
- event contamination risk
- historical depth reliability
- cross-source agreement
- prior-version continuity
- manual override presence

### Output bands
- `high`
- `medium`
- `low`
- `manual_review_required`

### Policy
- publish 6m growth more aggressively
- be stricter on 1y growth
- be strictest on 2y growth
- suppress 2y if low confidence or event contamination is high

---

## 11. Narrow-slice acceptance criteria

The first slice is acceptable if it can:

1. ingest a priority company list
2. resolve canonical entities for the first target set
3. capture at least one current anchor for most high-priority companies
4. reconstruct a monthly estimated headcount series
5. compute 6m / 1y / 2y growth where confidence policy permits
6. produce an evidence view for each estimate
7. route weak estimates to review
8. support manual overrides and reruns
9. export a comparison table against benchmark data
10. import or derive benchmark fixtures from the offline spreadsheets in `test_source/`
11. retain row-level provenance for benchmark comparisons and ground-truth examples

---

## 12. Pipeline stages

## 12.1 Stage 0: seed companies
Input:
- spreadsheet or CSV of target companies
- offline benchmark spreadsheets in `test_source/` when preparing comparison fixtures or ground-truth examples

Output:
- seeded `company_candidate` rows

## 12.2 Stage 1: canonicalize
Input:
- candidate rows

Output:
- canonical companies
- aliases
- source-link candidates
- ambiguity flags

## 12.3 Stage 2: collect anchors
Output:
- current anchor observations
- alternative anchor observations

## 12.4 Stage 3: collect employment observations
Output:
- public profile evidence
- parsed employment intervals

## 12.5 Stage 4: build monthly counts
Output:
- monthly public profile counts

## 12.6 Stage 5: estimate monthly headcount
Output:
- monthly headcount estimates
- growth windows

## 12.7 Stage 6: score confidence
Output:
- component scores
- banded confidence
- review flags

## 12.8 Stage 7: review and override
Output:
- analyst decisions
- manual overrides
- rerun triggers

## 12.9 Stage 8: export
Output:
- CSV
- parquet
- API payloads
- benchmark comparison outputs against `test_source/`

---

## 13. API design

## 13.1 Internal API endpoints

### Company
- `GET /companies`
- `GET /companies/{company_id}`
- `GET /companies/{company_id}/evidence`
- `GET /companies/{company_id}/estimates`
- `GET /companies/{company_id}/review`

### Estimation
- `POST /runs/canonicalize`
- `POST /runs/collect-anchors`
- `POST /runs/collect-employment`
- `POST /runs/estimate`
- `POST /runs/full`

### Review
- `GET /review-queue`
- `POST /review-queue/{item_id}/assign`
- `POST /review-queue/{item_id}/resolve`
- `POST /manual-overrides`

### Export
- `GET /exports/headcount-growth.csv`
- `GET /exports/headcount-estimates.parquet`

---

## 14. CLI design

Commands:

```bash
hc seed-companies --input data/priority_companies.csv
hc canonicalize --company-batch priority
hc collect-anchors --company-batch priority
hc collect-employment --company-batch priority
hc estimate-series --company-batch priority
hc score-confidence --company-batch priority
hc export-growth --company-batch priority --format csv
hc rerun-company --company-id <id>
```

Add:
- `--dry-run`
- `--limit`
- `--since`
- `--source`
- `--needs-review-only`

---

## 15. Testing strategy

## 15.1 Unit tests
Test:
- date parsing
- month interval expansion
- anchor selection
- confidence component scoring
- event segmentation
- anomaly rules
- company normalization

## 15.2 Integration tests
Test:
- end-to-end run on a tiny fixture set
- source adapters with stored HTML fixtures
- parser stability
- review queue population

## 15.3 Golden tests
Create a benchmark set of companies with hand-validated outputs.

Seed this benchmark set from the offline spreadsheet inputs in `test_source/` where possible.

For each:
- canonical company record
- accepted anchor
- validated monthly series sample
- expected 6m/1y/2y outputs
- expected confidence band
- source workbook/sheet/row references when expectations come from `test_source/`

Golden tests must fail when parser or estimator regress.

## 15.4 Snapshot fixtures
Persist representative raw public pages as fixtures.
Never make tests depend on live web responses.

## 15.5 Benchmark comparison tests

Add an offline benchmark comparison layer that:

- reads or normalizes the spreadsheet examples in `test_source/`
- compares produced monthly series and growth outputs against Harmonic.ai-style reference values where available
- uses company-detail examples as ground-truth checks for canonicalization, anchor selection, and review outcomes
- reports mismatches with preserved workbook/sheet/row provenance

---

## 16. Review UI requirements

The first UI only needs:

- company search
- canonical company detail
- candidate source links
- anchor evidence list
- employment evidence list
- monthly estimate chart
- growth metrics table
- confidence components
- anomaly flags
- override form
- rerun action

Do not overbuild the UI.

---

## 17. Operational policy

## 17.1 Caching
All source fetches must be cached.
Cache key should include:
- source
- URL
- fetch mode
- parser version where relevant

## 17.2 Parser versioning
Every normalized observation stores:
- parser version
- raw hash
- parse timestamp

## 17.3 Idempotency
Every stage must be rerunnable.
Reruns must not duplicate observations unless explicitly versioning a new snapshot.

## 17.4 Auditability
Every final estimate must explain:
- chosen current anchor
- public profile sample size
- method version
- main confidence deductions
- whether manual override was applied

---

## 18. Principal risks and mitigations

## 18.1 Entity resolution risk
This is the top risk.

Mitigation:
- deterministic scoring
- alias table
- parent/sub flags
- manual resolution override
- confidence penalties for ambiguity

## 18.2 Sample bias risk
Public profiles are incomplete and uneven.

Mitigation:
- scale from current anchor
- confidence penalties for small samples
- stronger thresholds for long historical windows

## 18.3 Anchor noise risk
Current anchor may be stale or wrong.

Mitigation:
- store multiple anchors
- rank anchors
- allow manual anchor override
- track anchor age

## 18.4 Event contamination risk
Acquisitions and rebrands break naive continuity.

Mitigation:
- event table
- event-aware segmentation
- suppress or review contaminated windows

## 18.5 Source volatility risk
DOMs change.

Mitigation:
- adapter boundaries
- parser fixtures
- parser versioning
- fail closed, do not silently guess

---

## 19. Build order for agentic builders

## 19.1 Foundation
- repo skeleton
- config system
- database models
- migrations
- shared schemas
- logging
- CLI shell
- benchmark-import contracts for `test_source/`

## 19.2 Company resolution
- company seed import
- normalization functions
- alias store
- deterministic resolver
- manual resolution override

## 19.3 Source adapters
- manual observer
- company web observer
- LinkedIn public observer
- SEC observer
- free API stub adapter interface

## 19.4 Parsing and normalization
- company-page parsers
- employment text parser
- date parser
- source observation persistence

## 19.5 Estimation
- interval expansion
- monthly counts
- scaled series estimator
- event segmentation
- anomaly engine
- confidence scorer

## 19.6 Review
- review queue
- overrides
- Streamlit UI
- rerun flow

## 19.7 Exports
- growth table
- evidence export
- comparison export
- benchmark comparison export keyed back to `test_source/`

---

## 20. What Codex and Cursor should optimize for

They should optimize for:
- deterministic code
- explicit interfaces
- small, testable modules
- evidence traceability
- least-resistance architecture
- no hidden magic
- fixture-based parser tests
- confidence-first outputs

They should not optimize for:
- glamorous infra
- speculative ML
- crawler scale tricks
- premature parallelism
- broad feature spread

---

## 21. Final recommendation

The narrow slice should be built as an evidence-driven monthly headcount estimator with a bounded logged-out public LinkedIn observation path included as one source adapter, not as the whole system.

The most accurate approximation will come from:
- strong company resolution
- current anchor selection
- public employment-history interval reconstruction
- event-aware estimation
- review-driven correction

That is the path that gets closest to the target metric shape without turning the project into a brittle scrape-first system.
