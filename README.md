# Headcount Time Series Approximater

Evidence-driven, deterministic system for estimating company headcount time series and growth windows (6-month, 1-year, 2-year) from public signals.

This repository is built as an internal-use data product: reproducible runs, explicit confidence gating, and first-class manual review.

## Why this repo exists

The goal is to approximate headcount trends for target companies when direct ground-truth is incomplete, while preserving auditability and operational discipline.

Primary goals:

- Produce monthly headcount estimates that are explainable and repeatable.
- Compute growth metrics (6m/1y/2y) with confidence-aware output gating.
- Preserve evidence provenance for every estimate and downstream decision.
- Route weak-confidence outputs to review instead of presenting false certainty.
- Keep the stack local-first and practical (no heavyweight infra unless justified).

Non-goals:

- Opaque ML-first estimation logic without traceable evidence.
- Aggressive scraping tactics or gated-access bypass strategies.
- Premature distributed scale or multi-tenant platform complexity.

## For hiring managers: what this demonstrates

This codebase is intentionally designed to signal production-minded data engineering and backend architecture decisions:

- **Domain modeling and boundaries**: clear separation across ingestion, parsing, resolution, estimation, review, and serving layers.
- **Deterministic pipelines**: repeatable CLI-driven workflows with auditable reruns and typed contracts.
- **Data quality discipline**: parser/versioning expectations, fixture-backed tests, confidence gating, and benchmark comparison loops.
- **Operational reliability**: explicit failure modes, evidence trace preservation, and low-confidence review queues.
- **Practical API and tooling**: FastAPI service surface, Typer CLI orchestration, SQLAlchemy/Alembic-backed persistence.
- **Responsible acquisition constraints**: public-only source policy and fail-closed behavior under unstable access.

If you evaluate candidates on engineering judgment rather than only algorithmic complexity, this repo highlights design for trust, maintainability, and evidence-backed outputs.

## Technology stack and how each piece is used

- **Python**: core language for pipeline logic, adapters, parsers, estimation, and APIs.
- **Postgres**: primary transactional store for companies, evidence, normalized observations, and run metadata.
- **DuckDB**: fast analytical computations and export-friendly local analytics workflows.
- **FastAPI**: read API layer for health/status/results/evidence access.
- **Typer CLI**: reproducible orchestration interface for each pipeline stage.
- **SQLAlchemy + Alembic**: typed ORM/data access and versioned schema migrations.
- **Pydantic**: typed schemas and validation for interfaces crossing module boundaries.
- **httpx + BeautifulSoup/lxml**: structured retrieval and parsing of public-web evidence.
- **Playwright (limited use)**: DOM-rendered access only when a public page cannot be parsed reliably statically.
- **Streamlit**: initial manual review UI for low-confidence queue triage and overrides.

## Architecture overview

High-level flow:

1. Source adapters collect raw public evidence.
2. Parsers normalize observations into typed structures.
3. Resolution maps observations to canonical company identities.
4. Estimation builds month-level profile activity and scales from selected anchors.
5. Confidence scoring gates weak outputs.
6. Review tooling exposes queue/evidence/override/rerun functionality.
7. Serving layer publishes read-oriented APIs and exports.

Core principle: raw HTML never leaks directly into estimation logic.

## Repository layout

```text
src/headcount/{config,db,models,schemas,clients,ingest,parsers,resolution,estimation,review,serving,utils}
apps/{api,review_ui}
tests/{unit,integration,golden,fixtures}
migrations/  scripts/  data/{seeds,fixtures,outputs,cache}
prompts/  plans/  docs/  test_source/
```

## Usage guide

### 1) Local setup

```bash
python -m pip install -e ".[dev]"
cp .env.example .env
python -m headcount.cli --help
```

Optional Make targets:

```bash
make install-dev
make test
make run-api
```

### 2) Run the API locally

```bash
python -m uvicorn apps.api.main:app --host 127.0.0.1 --port 8000
```

Typical endpoints:

- `http://127.0.0.1:8000/healthz`
- `http://127.0.0.1:8000/metrics`

### 3) Run a full CLI pipeline

```bash
hc seed-companies --input "test_source/High Priority Companies_01.04.2026.xlsx"
hc load-benchmarks --input "test_source/Sample Employee Growth for High Priority Prospects.xlsx"
hc canonicalize --company-batch priority
hc collect-anchors --company-batch priority
hc collect-employment --company-batch priority
hc estimate-series --company-batch priority
hc score-confidence --company-batch priority
hc export-growth --company-batch priority --format csv
hc compare-benchmark --company-batch priority
```

Operational commands:

```bash
hc rerun-company --company-id <id>
hc status --run-id <run_id>
```

Global options (where supported): `--run-id`, `--resume`, `--limit`, `--priority-tier`, `--dry-run`.

### 4) Validate changes with tests

Run all tests:

```bash
make test
```

Project testing expectations:

- Unit tests for parsers and interval logic.
- Integration tests for adapter -> parser -> storage flow.
- Golden/snapshot coverage for estimator outputs.
- Fixture-based tests only (avoid live web dependency in CI).

## Data and benchmark inputs

The `test_source/` spreadsheets are offline benchmark and validation inputs, not live source adapters.

Current benchmark files:

- `test_source/High Priority Companies_01.04.2026.xlsx`
- `test_source/Sample Employee Growth for High Priority Prospects.xlsx`

When deriving fixtures/expectations, preserve workbook/sheet/row/column provenance.

## Guardrails and acquisition policy

Logged-out LinkedIn observation path is restricted to public pages only.

Hard constraints:

- No login automation.
- No CAPTCHA solving.
- No rotating proxies.
- No stealth/fingerprint evasion tooling.
- No gate-bypass retry loops.
- Fail closed when access is gated, blocked, or unstable.

Always cache responses, persist raw evidence snapshots, and version normalized parser outputs.

## Documentation map

Detailed project docs:

- [AGENTS.md](AGENTS.md)
- [docs/BUILD_PLAN_V2.md](docs/BUILD_PLAN_V2.md)
- [docs/METHODOLOGY_AND_ASSUMPTIONS_V2.md](docs/METHODOLOGY_AND_ASSUMPTIONS_V2.md)
- [docs/SOURCE_MATRIX_V2.md](docs/SOURCE_MATRIX_V2.md)
- [docs/ACCEPTANCE_CRITERIA_V2.md](docs/ACCEPTANCE_CRITERIA_V2.md)

## Current maturity

The command surface and architecture are intentionally staged to support phased implementation and deterministic expansion:

repo skeleton -> schema -> benchmark loader -> resolver -> adapters -> estimation -> confidence/review -> exports -> UI -> golden/stress hardening.
