# Headcount Time Series Approximater

Internal-use, evidence-driven estimator for monthly company headcount and
6m / 1y / 2y growth. Narrow-slice v1: deterministic, reproducible, reviewable,
local-first.

See the build-pack docs for the full plan:

- [AGENTS.md](AGENTS.md)
- [docs/BUILD_PLAN_V2.md](docs/BUILD_PLAN_V2.md)
- [docs/METHODOLOGY_AND_ASSUMPTIONS_V2.md](docs/METHODOLOGY_AND_ASSUMPTIONS_V2.md)
- [docs/SOURCE_MATRIX_V2.md](docs/SOURCE_MATRIX_V2.md)
- [docs/ACCEPTANCE_CRITERIA_V2.md](docs/ACCEPTANCE_CRITERIA_V2.md)

## Quick start

```bash
python -m pip install -e ".[dev]"
cp .env.example .env
python -m headcount.cli --help
python -m uvicorn apps.api.main:app --host 127.0.0.1 --port 8000
# http://127.0.0.1:8000/healthz
# http://127.0.0.1:8000/metrics
```

Or via Make:

```bash
make install-dev
make test
make run-api
```

## Layout

```text
src/headcount/{config,db,models,schemas,clients,ingest,parsers,resolution,estimation,review,serving,utils}
apps/{api,review_ui}
tests/{unit,integration,golden,fixtures}
migrations/  scripts/  data/{seeds,fixtures,outputs,cache}
prompts/  plans/  docs/  test_source/
```

## CLI surface

All pipeline stages are exposed as `hc` subcommands:

```bash
hc seed-companies --input test_source/High\ Priority\ Companies_01.04.2026.xlsx
hc load-benchmarks --input "test_source/Sample Employee Growth for High Priority Prospects.xlsx"
hc canonicalize --company-batch priority
hc collect-anchors --company-batch priority
hc collect-employment --company-batch priority
hc estimate-series --company-batch priority
hc score-confidence --company-batch priority
hc export-growth --company-batch priority --format csv
hc compare-benchmark --company-batch priority
hc rerun-company --company-id <id>
hc status --run-id <run_id>
```

Global options: `--run-id`, `--resume`, `--limit`, `--priority-tier`, `--dry-run`.

Phase 0 ships the command surface and stubs. Subsequent phases fill in the
implementations (see plan: repo skeleton -> schema -> benchmark loader ->
resolver -> adapters -> estimation -> confidence/review -> exports -> UI ->
goldens & stress test).

## Guardrails

- Public pages only for the logged-out LinkedIn observation path. Fail closed
  on any gate signal. No login automation, CAPTCHA solving, rotating proxies,
  stealth evasion, or gate-bypass retry logic.
- `test_source/` is an offline validation input: preserve workbook, sheet,
  row, column provenance; never treat as a live data source.
- Every estimate must carry its evidence trace and confidence band.
