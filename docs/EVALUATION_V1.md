# Evaluation V2

**Status:** shipped in Phase 11; trust model inverted to Harmonic-primary
in the post-Phase-11 follow-up. Owner: estimation pipeline.

This document defines how the Headcount Time-Series Approximater is
validated, what the acceptance gate enforces before code can ship, and
how to regenerate the golden fixtures when the pipeline intentionally
changes.

The harness has two layers:

1. **Golden fixtures** (`tests/golden/`) — ten hand-verified company
   snapshots with per-field provider provenance. Unit-scale regression
   tests. They answer *"did the pipeline still produce the numbers we
   signed off on?"*
2. **Benchmark-comparison scoreboard** (`src/headcount/review/evaluation.py`)
   — a full pass that scores every estimate against every benchmark
   row in the DB and writes an immutable `EvaluationRun` snapshot.
   This is the operational dashboard signal.

## 0. Trust model

The product goal drives every threshold and provider weight in this
document:

> Approximate **Harmonic.ai's** numbers as closely as possible for
> the small (~25-company) cohort where Harmonic is available, then
> rely on our own pipeline for the ~99% of companies Harmonic never
> sees.

That goal pins the trust hierarchy:

| Provider | Role | Anchor confidence | Headline KPI source? |
| --- | --- | --- | --- |
| `harmonic` | **Target signal** — what we are trying to approximate | `0.70` | Yes (current headcount, 6m / 1y growth) |
| `zeeshan` | Supporting evidence — automated service, useful for ranges, historical points, 2y growth | `0.55` | Only for `2y` growth (Harmonic does not emit it) |
| `linkedin` | Trend / tie-break only — profile-appearance counts, noisy at the level | `0.45` | No (diagnostic only) |

Anchor confidence is enforced in
`src/headcount/parsers/benchmark_anchors.py::_PROVIDER_CONFIDENCE`
and is the deciding tie-breaker inside
`headcount.estimate.reconcile.interpolate_series_from_anchors`.

The Harmonic cohort being small is **load-bearing**. The scoreboard
splits accuracy stats into:

* **Harmonic-cohort calibration accuracy** — the small-N lens that
  drives every headline KPI and the acceptance gate. This is where
  we measure *"how close are we to Harmonic?"*.
* **Full-population coverage / band distribution** — the large-N
  production view. This is where we measure *"is the pipeline
  emitting plausible numbers for the companies Harmonic never
  scored?"*.

Mixing the two would dilute the calibration signal we paid for the
Harmonic cohort to give us.

## 1. Golden fixtures

### 1.1 Layout

```
tests/golden/
├── goldens/
│   ├── 1010data.yaml
│   ├── 1kosmos.yaml
│   ├── 1uphealth.yaml
│   ├── 6sense.yaml
│   ├── alivecor.yaml
│   ├── alleva.yaml
│   ├── alloy.yaml
│   ├── alloy_therapeutics.yaml
│   ├── alltrails.yaml
│   └── allvue_systems.yaml
└── test_goldens.py
```

Each YAML encodes one "signed off" outcome for one company:

- `company.canonical_name` + `company.source_row` — provenance back into
  the benchmark workbook under `test_source/`.
- `accepted_anchor` — the anchor we expect the pipeline to lock onto
  at the latest month (Harmonic point + provenance).
- `monthly_samples` — `(month, value_point, tolerance_pct, provider)`
  rows. Tolerance is a percentage band, not an absolute.
- `growth_windows` — expected `6m` / `1y` / `2y` percent growth with
  per-window tolerance in percentage points. The `6m`/`1y` blocks
  also carry a `harmonic_target_pct` field that records the value
  we are trying to converge toward, separate from the value the
  pipeline currently produces.
- `expected_confidence_band_latest` — the band the latest monthly
  estimate must land in (exact string match; no drift allowed).
- `notes` — freeform explanation, especially for companies with M&A
  events, layoffs, or hard breaks that suppress months. Must
  document the calibration gap when produced ≠ Harmonic target.

### 1.2 Sourcing policy ("hybrid, Harmonic-primary")

Golden values are hybrid by design and follow the trust hierarchy
above:

- **Harmonic primary** for fields it emits — `accepted_anchor`,
  the latest-month `monthly_samples` row, and the `growth_windows.6m`
  / `growth_windows.1y` blocks. Each such field records
  `provider: harmonic`.
- **Zeeshan fallback** for fields Harmonic does not emit — historical
  `monthly_samples` rows (t-2y / t-1y / t-6m) and
  `growth_windows.2y`. Those fields record `provider: zeeshan`.
- **Never LinkedIn-only.** LinkedIn scrape cells are never the sole
  source of truth for a golden field.

The provider is recorded **per field** so the differ can produce
provider-aware mismatch messages, and so we can see at a glance which
golden mismatches are about the target signal itself versus the
supporting evidence.

#### 1.2.1 The calibration gap

When Harmonic and Zeeshan disagree (example: 1010data — Harmonic
65 headcount vs Zeeshan range 201-500), the pipeline's interpolated
growth rate will diverge sharply from the rate Harmonic reports
directly. The golden's `growth_windows[*].pct` records what the
pipeline *currently* produces given the mixed evidence; the
`harmonic_target_pct` field records the value we are trying to
converge toward. The gap between them is the calibration debt and
is tracked in the evaluation scoreboard's `growth_accuracy[harmonic]`
MAE and rank-correlation metrics, *not* in the golden tolerances.

Closing the gap is a separate workstream from keeping the goldens
green. Goldens are a regression baseline; the scoreboard is the
calibration dashboard.

### 1.3 What the differ asserts

`headcount.review.golden.diff_fixture` returns a list of
`GoldenMismatch` items. A fixture passes when the list is empty.
It checks:

1. Every `monthly_samples[*]` is present in the estimate output, and
   the value falls within `tolerance_pct` of `value_point`.
2. Every `growth_windows[*]` is within `tolerance` percentage points
   of the expected growth percent. Mismatch messages on `6m`/`1y`
   horizons include the `harmonic_target_pct` so the reviewer can
   see the calibration target alongside the produced value.
3. The final monthly estimate's confidence band is exactly
   `expected_confidence_band_latest`.

Mismatches include a stable `kind` (`monthly_estimate_out_of_tolerance`,
`growth_out_of_tolerance`, `confidence_band_drift`,
`missing_monthly_estimate`, …) so CI failures are easy to search.

### 1.4 Regenerating goldens

The `scripts/regenerate_goldens.py` script reads the Harmonic sheet
from `test_source/` and rewrites every golden YAML deterministically.
Run it whenever:

- The Harmonic sheet is refreshed.
- A pipeline change causes a *justified* shift in produced growth
  rates and a reviewer has signed off on the new values.

Do not run it to silence CI — investigate the underlying behaviour
change first.

## 2. Benchmark-comparison scoreboard

### 2.1 What it scores

For every company that (a) has a `MonthlyEstimate` row for the
`as_of_month`, and (b) has at least one `BenchmarkObservation`, the
harness computes:

- **Point metrics** per provider (`harmonic`, `zeeshan`, `linkedin`):
  - `headcount_current` MAPE and MAE at the latest month.
  - `headcount_6m_ago` / `1y_ago` / `2y_ago` historical accuracy.
- **Growth windows** per provider: MAE on the percent growth at 6m /
  1y / 2y.
- **Rank correlation** (Spearman ρ) per provider per horizon. With
  N≥3 companies the harness records how closely our growth ordering
  matches the provider's. This is the "are we sorting prospects
  the same way?" signal.
- **Interval-overlap credit.** When the benchmark carries
  `(value_min, value_max)` and the estimate's interval overlaps,
  the error contribution for that row is zeroed. We reward the
  pipeline for agreeing with provider ranges, not for threading a
  point inside them. This applies to every provider but matters
  most for Zeeshan (whose rows are typically range buckets).
- **Disagreements.** A `Disagreement` is emitted only when the
  relative error exceeds 10% — keeps the top-N list signal-rich.
  When the estimate's band is `high` or `medium` *and* the ratio
  exceeds `high_confidence_disagreement_ratio` (default `1.0` —
  i.e. off by more than 2×), the disagreement is also counted in:
  - `high_confidence_disagreements` if the row's provider is
    Harmonic. **This is the acceptance-gate tripwire.**
  - `supporting_disagreements` if the row's provider is Zeeshan or
    LinkedIn. Diagnostic only; never blocks the gate.
- **Review queue** open count and latest confidence-band
  distribution.
- **Harmonic cohort** size (companies in scope with at least one
  Harmonic benchmark row) and cohort-evaluated count.

### 2.2 Headline KPIs

The scoreboard's `headline` block surfaces the small set of metrics
that drive the acceptance gate and the Streamlit Evaluation page's
top row:

| Field | Source | Notes |
| --- | --- | --- |
| `mape_headcount_current` | Harmonic accuracy on `headcount_current` | Primary calibration KPI |
| `mae_growth_6m_pct` | Harmonic 180d growth MAE | Secondary calibration KPI |
| `mae_growth_1y_pct` | Harmonic 365d growth MAE | Secondary calibration KPI |
| `mae_growth_2y_pct` | Zeeshan 2y growth MAE | Harmonic does not emit 2y; Zeeshan is the only available signal |
| `spearman_growth_6m` | Spearman ρ vs Harmonic 6m | Ordering signal |
| `spearman_growth_1y` | Spearman ρ vs Harmonic 1y | Ordering signal |

Supporting-provider MAPEs (`mape_headcount_current_zeeshan`,
`mape_headcount_current_linkedin`) live in their own promoted columns
on `EvaluationRun` and in `accuracy[provider]` in the JSON. They are
diagnostic — interesting on the dashboard, not a gate signal.

### 2.3 Persistence

`persist_scoreboard` writes an `EvaluationRun` row. These rows are
**immutable**. Every call produces a new UUID. Historical trend plots
in the Streamlit Evaluation page are driven straight off this table,
so the shape of the JSON blob is part of the public contract.

The row records `primary_provider` so historical scoreboards remain
legible if the target signal ever changes again. Phase 11 rows are
backfilled to `'zeeshan'`; everything written under V2 is `'harmonic'`.

### 2.4 Surfaces

- CLI: `hc evaluate [--as-of-month YYYY-MM-DD] [--company-id …]
  [--persist] [--output path.json] [--top-disagreements N]
  [--high-confidence-ratio 1.0] [--note "release v1.3"]`.
- API: `/eval/latest`, `/eval/history?limit=50`, `/eval/{id}`.
- UI: *Evaluation* page under the Streamlit status dashboard.

### 2.5 Versioning

The module exports `EVALUATION_VERSION` (currently `"eval_v2"`). Bump
it when metric definitions change in any way that would make
historical scoreboards non-comparable. The string is embedded in
every `EvaluationRun` row so UI code can filter or warn.

## 3. Acceptance gate

`tests/integration/test_acceptance_gate.py` is the "can we ship?"
test. It seeds a 5-company benchmark fixture (1010data, 1Kosmos,
6sense, AliveCor, Alleva) where each company carries both a Harmonic
row (point + growth percentages) and a Zeeshan row (range bucket +
historical points + 2y growth), promotes the rows into anchors,
runs the estimation pipeline, and scores the result.

Passing thresholds:

| Metric | Threshold | Rationale |
| --- | --- | --- |
| `companies_in_scope` | `== 5` | Every seeded company must be scorable. |
| `companies_with_benchmark` | `== 5` | Benchmark rows must not be silently dropped. |
| `coverage_in_scope` | `>= 0.80` | At least 4/5 companies must produce an estimate. |
| `harmonic_cohort_size` | `== 5` | Every seeded company has a Harmonic row. |
| `harmonic_cohort_evaluated` | `== 5` | The pipeline must score every Harmonic-cohort company. |
| Harmonic `headcount_current` MAPE | `<= 0.05` | With Harmonic anchors promoted, the latest-month estimate must land on the Harmonic point (or inside the Zeeshan range, which the interval-overlap credit also collapses to zero error). |
| Harmonic `growth_1y_pct` MAE | `<= 0.05` (5 percentage points) | The growth window we emit must track Harmonic's 365-day rate. |
| Harmonic `growth_1y` Spearman ρ | `>= 0.70` | Our growth ordering must match Harmonic's. **Implementation note:** kept loose because N=5 is tiny; revisit upward as the Harmonic cohort grows toward its full ~25-company size. |
| `high_confidence_disagreements` | `== 0` | Any high/medium-band Harmonic disagreement > 2× is a regression. |

A second test (`test_acceptance_gate_coverage_holds_under_scope_restriction`)
confirms that scope restriction is honored — `company_ids` filters
the scoreboard correctly and does not over-count.

When this file fails, read the attached `payload` (the full
scoreboard dict is printed on assert) before touching the thresholds.
Thresholds tighten when we are confident; they do not loosen silently.

## 4. When to add a golden

Add a new golden when:

- A real company reveals an edge case the pipeline now handles
  correctly (M&A, hard layoff, data gap > 12 months, etc.).
- A fix lands that closes a previously-documented Harmonic
  disagreement; freeze the new expected values so the fix doesn't
  silently regress.

Do **not** add goldens to cover synthetic or test-only companies.
Goldens are exclusively real companies from `test_source/` with
traceable Harmonic + Zeeshan provenance.

## 5. Escalation

If the acceptance gate fails and the cause is not obvious:

1. Look at the top-N `disagreements` in the failing scoreboard.
   Sort by `provider == 'harmonic'` first.
2. Run `hc evaluate --company-id <offender> --output /tmp/eval.json`
   for full per-row detail.
3. Cross-check against the golden fixture for that company, if one
   exists. A mismatch at that layer usually points to a specific
   anchor-policy or reconcile change.
4. Distinguish *calibration debt* (Harmonic and pipeline output drift
   apart due to inconsistent historical evidence) from *regressions*
   (we used to be close, now we're not). The first is a tracked
   workstream; the second blocks the merge.
5. Never weaken the acceptance thresholds to turn the build green.
   The thresholds are the contract.
