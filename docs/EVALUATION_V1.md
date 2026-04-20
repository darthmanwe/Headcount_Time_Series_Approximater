# Evaluation V1

**Status:** shipped in Phase 11. Owner: estimation pipeline.

This document defines how the Headcount Time-Series Approximater is validated,
what the acceptance gate enforces before code can ship, and how to regenerate
the golden fixtures when the pipeline intentionally changes.

The harness has two layers:

1. **Golden fixtures** (`tests/golden/`) — ten hand-verified company
   snapshots with per-field provider provenance. These are unit-scale
   regression tests. They answer *"did the pipeline still produce the
   numbers we signed off on?"*
2. **Benchmark-comparison scoreboard** (`src/headcount/review/evaluation.py`)
   — a full pass that scores every estimate against every benchmark row in
   the DB and writes an immutable `EvaluationRun` snapshot. This is the
   operational dashboard signal.

## 1. Golden fixtures

### 1.1 Layout

```
tests/golden/
├── goldens/
│   ├── 1010data.yaml
│   ├── 1kosmos.yaml
│   ├── 6sense.yaml
│   ├── alivecor.yaml
│   ├── alleva.yaml
│   ├── alloy_therapeutics.yaml
│   ├── applied_medical.yaml
│   ├── axios.yaml
│   ├── backmarket.yaml
│   └── beyondtrust.yaml
└── test_goldens.py
```

Each YAML encodes one "signed off" outcome for one company:

- `company.canonical_name` + `company.source_row` — provenance back into
  the benchmark workbook under `test_source/`.
- `accepted_anchor` — the anchor we expect the pipeline to lock onto
  (month, point, min/max, provider, source row).
- `monthly_samples` — one or more `(month, value_point, tolerance_pct,
  provider)` rows. Tolerance is a percentage band, not an absolute.
- `growth_windows` — expected `6m` / `1y` / `2y` percent growth with
  per-window tolerance in percentage points.
- `expected_confidence_band_latest` — the band the latest monthly estimate
  must land in (exact string match; no drift allowed).
- `notes` — freeform explanation, especially for companies with M&A
  events, layoffs, or hard breaks that suppress months.

### 1.2 Sourcing policy ("hybrid")

Golden values are hybrid by design:

- **Zeeshan first.** When the analyst-verified column has a value for a
  field, that value (and its row) is the expected value, and the
  fixture's `provider` for that field is `zeeshan`.
- **Harmonic fallback.** When Zeeshan is silent, Harmonic.ai's automated
  value is used as the expected value and the `provider` is recorded as
  `harmonic`. This preserves the signal that the expected value is only
  as trustworthy as the feed it came from.
- **No LinkedIn-only goldens.** LinkedIn scrape cells are never the sole
  source of truth for a golden field.

The provider is recorded **per field** — the same fixture may have a
Zeeshan anchor and a Harmonic-sourced historical sample.

### 1.3 What the differ asserts

`headcount.review.golden.diff_fixture` returns a list of
`GoldenMismatch` items. A fixture passes when the list is empty. It
checks:

1. Every `monthly_samples[*]` is present in the estimate output, and the
   value falls within `tolerance_pct` of `value_point`.
2. Every `growth_windows[*]` is within `tolerance` percentage points of
   the expected growth percent.
3. The final monthly estimate's confidence band is exactly
   `expected_confidence_band_latest`.

Mismatches include a stable `kind` (`monthly_out_of_tolerance`,
`growth_out_of_tolerance`, `band_drift`, `monthly_missing`, …) so CI
failures are easy to search.

### 1.4 Regenerating a golden

Run the pipeline end-to-end for the company in question, inspect the
output, then update the YAML directly. Do **not** regenerate them in
bulk — the whole point is that a human signed off on the values.

Typical workflow:

1. Reproduce the change locally and run `hc evaluate --company-id <id>`.
2. Compare the scoreboard's per-company numbers to the existing YAML.
3. If the change is *intentional*, edit the YAML, widen the tolerance
   only if the new value is genuinely more correct, and add a note
   explaining why.
4. Always update `notes` when an M&A event, layoff, or segmentation
   change causes months to appear or disappear.

## 2. Benchmark-comparison scoreboard

### 2.1 What it scores

For every company that (a) has a `MonthlyEstimate` row for the
`as_of_month`, and (b) has at least one `BenchmarkObservation`, the
harness computes:

- **Point metrics** per provider (`zeeshan`, `harmonic`, `linkedin`):
  - `headcount_current` MAPE and MAE at the latest month.
  - `headcount_6m_ago` / `headcount_1y_ago` / `headcount_2y_ago`
    historical accuracy.
- **Growth windows** per provider: MAE on the percent growth at 6m /
  1y / 2y.
- **Interval-overlap credit.** When the benchmark carries
  `(value_min, value_max)` and the estimate's interval overlaps, the
  error contribution for that row is zeroed. We reward the pipeline
  for agreeing with analyst ranges, not for threading a point inside
  them.
- **Disagreements.** A `Disagreement` row is emitted only when the
  relative error exceeds 10% — this keeps the top-N list signal-rich.
  If the estimate *and* benchmark both have non-trivial point values
  and the ratio exceeds `high_confidence_disagreement_ratio` (default
  `1.0` — i.e. off by more than 2×) *and* the estimate's band is
  `high` or `medium`, the disagreement is also counted in
  `high_confidence_disagreements`. That counter is the acceptance-gate
  tripwire.
- **Review queue** open count and latest confidence-band distribution.

### 2.2 Persistence

`persist_scoreboard` writes an `EvaluationRun` row. These rows are
**immutable**. Every call produces a new UUID. Historical trend plots in
the Streamlit Evaluation page are driven straight off this table, so
the shape of the JSON blob is part of the public contract.

### 2.3 Surfaces

- CLI: `hc evaluate [--as-of-month YYYY-MM-DD] [--company-id …]
  [--persist] [--output path.json] [--top-disagreements N]
  [--high-confidence-ratio 1.0] [--note "release v1.3"]`.
- API: `/eval/latest`, `/eval/history?limit=50`, `/eval/{id}`.
- UI: *Evaluation* page under the Streamlit status dashboard.

### 2.4 Versioning

The module exports `EVALUATION_VERSION`. Bump it when the metric
definitions change in any way that would make historical scoreboards
non-comparable. The string is embedded in every `EvaluationRun` row so
UI code can filter or warn.

## 3. Acceptance gate

`tests/integration/test_acceptance_gate.py` is the "can we ship?" test.
It seeds a 5-company benchmark fixture (1010data, 1Kosmos, 6sense,
AliveCor, Alleva), promotes the analyst rows into anchors, runs the
estimation pipeline, and scores the result. Passing thresholds:

| Metric | Threshold | Rationale |
| --- | --- | --- |
| `companies_in_scope` | `== 5` | Every seeded company must be scorable. |
| `companies_with_benchmark` | `== 5` | Benchmark rows must not be silently dropped. |
| `coverage_in_scope` | `>= 0.80` | The pipeline must produce estimates for at least 4/5. |
| Zeeshan `headcount_current` MAPE | `<= 0.05` | With analyst anchors promoted, the interval overlap should drive error to ~zero. |
| `high_confidence_disagreements` | `== 0` | A high/medium-band estimate being > 2× off analyst is treated as a regression. |

A second test (`test_acceptance_gate_coverage_holds_under_scope_restriction`)
confirms that scope restriction is honored — `company_ids` filters the
scoreboard correctly and does not over-count.

When this file fails, read the attached `payload` (the full scoreboard
dict is printed on assert) before touching the thresholds. Thresholds
tighten when we are confident; they do not loosen silently.

## 4. When to add a golden

Add a new golden when:

- A real company reveals an edge case the pipeline now handles
  correctly (M&A, hard layoff, data gap > 12 months, etc.).
- A fix lands that closes a previously-documented disagreement; freeze
  the new expected values so the fix doesn't silently regress.

Do **not** add goldens to cover synthetic or test-only companies.
Goldens are exclusively real companies from `test_source/` with
traceable provenance.

## 5. Escalation

If the acceptance gate fails and the cause is not obvious:

1. Look at the top-N `disagreements` in the failing scoreboard.
2. Run `hc evaluate --company-id <offender> --output /tmp/eval.json`
   for full per-row detail.
3. Cross-check against the golden fixture for that company, if one
   exists. A mismatch at that layer usually points to a specific
   anchor-policy or reconcile change.
4. Never weaken the acceptance thresholds to turn the build green.
   The thresholds are the contract.
