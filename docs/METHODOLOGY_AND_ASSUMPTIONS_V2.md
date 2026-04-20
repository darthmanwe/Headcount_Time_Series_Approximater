# Methodology and Assumptions V2

## 1. Core assumption set

This system aims to approximate LinkedIn-style headcount time series using evidence that is available for internal use without building a commercial data platform.

The core modeling assumption is:

A company's historical public-profile activity series, when properly resolved and scaled by a current headcount anchor, can approximate the shape of its historical employee count over time.

This is not assumed to be exact truth.
It is an estimation method.

## 2. Why this is the right approximation class

The target output is:
- current headcount
- 6m growth
- 1y growth
- 2y growth

The best narrow-slice approximation for this shape is:

- choose a current anchor
- reconstruct historical public-profile counts by month
- use the historical/current ratio of public profiles
- scale by the current anchor
- correct for known discontinuities

This is the closest practical approximation because:
- current anchors are much easier to observe than historical monthly totals
- person employment histories encode time intervals
- ratios are more robust than naive absolute public-profile counts

## 3. Central methodological warning

The hard problem is not headcount arithmetic.
The hard problem is deciding which evidence belongs to the same company over time.

If canonical company resolution is weak, the series will be wrong even if every parser works perfectly.

## 4. Current anchor assumptions

A current anchor is a best-available estimate of present headcount.
Possible anchor sources:
- public company page employee count
- public company self-statement
- manual analyst anchor
- free-tier API anchor
- third-party benchmark feed (Harmonic.ai)

Assumptions:
- the current anchor is more trustworthy than historical inferred values
- multiple anchors may exist and disagree
- anchor selection should be explicit and explainable

### 4.1 Provider trust ordering

The benchmark workbook ships with three providers and they are not
equally trustworthy *for our purposes*. The product goal is "match
Harmonic for the calibration cohort, then run our own pipeline for
the long tail Harmonic does not cover", which dictates the trust
hierarchy:

| Provider | Trust floor | Role |
| --- | --- | --- |
| `harmonic` | `0.70` | Target signal — what we are explicitly trying to approximate. Wins promotion at the current month. |
| `zeeshan` | `0.55` | Supporting evidence — automated service, useful for ranges, historical points (Harmonic does not emit them), and 2y growth (Harmonic does not emit it). |
| `linkedin` | `0.45` | Trend / tie-break only — profile-appearance counts, noisy at the level. |

These floors live in
`src/headcount/parsers/benchmark_anchors.py::_PROVIDER_CONFIDENCE`
and are the deciding tie-breaker inside
`headcount.estimate.reconcile.interpolate_series_from_anchors`
when two providers report a value for the same `(company, month)`.

The Harmonic cohort is small (~25 companies in the reference
workbook). It is used as a *calibration lens* — every headline KPI
in the evaluation scoreboard is computed against this cohort.
For companies outside the cohort we fall back on the (Zeeshan +
LinkedIn + pipeline) stack and report coverage / confidence-band
distribution rather than absolute accuracy. See `docs/EVALUATION_V1.md`
for the full evaluation policy.

## 5. Employment interval assumptions

Each employment observation is modeled as:
- company
- person
- start month
- end month or current flag

Assumptions:
- month granularity is enough for the target metrics
- exact day precision is not needed
- ambiguous intervals should be normalized conservatively
- missing month granularity should reduce confidence

## 6. Ratio-scaling assumption

The core formula is:

estimated_headcount_month =
    current_anchor * (public_profile_count_month / public_profile_count_current)

Assumptions:
- public profile saturation is imperfect but somewhat stable enough locally within a company to preserve directional shape
- scaling from current anchor helps compensate for undercoverage
- smaller profile samples weaken this assumption and must reduce confidence

## 7. Event segmentation assumption

Acquisitions, mergers, rebrands, and major layoffs can invalidate a single continuous ratio-scaled series.

Assumptions:
- pre-event and post-event segments may need separate treatment
- long-window metrics are more fragile around these events
- event contamination is one of the main reasons to suppress 2-year outputs

## 8. Confidence assumptions

A number should only be surfaced if it is paired with a confidence judgment.

High confidence generally requires:
- strong company resolution
- a credible current anchor
- sufficient public employment observations
- low event contamination
- internally coherent monthly series

Low confidence can still be stored, but should not be presented as if equally trustworthy.

## 9. Manual review assumptions

Some companies will always need review.
Examples:
- acquisitions
- holding-company confusion
- contractor-heavy firms
- stealth-to-public transitions
- duplicate or ambiguous source pages

Manual review is not a failure of the system.
It is part of the architecture.

## 10. Logged-out public LinkedIn observation assumptions

This source path is included because it is often the closest public surface to the target metric shape.

Assumptions:
- only public pages visible without authentication are in scope
- source access can become gated or unstable
- the system should fail closed when access is blocked
- this source is one evidence adapter, not the sole backbone

## 11. What the system should never assume

Never assume:
- one company name equals one company entity
- a visible anchor is automatically correct
- all public profiles are current
- all current employees use LinkedIn at the same rate
- two-year estimates are as stable as six-month estimates
- acquisitions are organic growth
- missing data can be silently guessed

## 12. Practical interpretation of output

Interpret outputs as:

- a best-effort evidence-backed headcount estimate series
- strongest at recent windows
- weakest when entity resolution is poor or event contamination is high
- good enough for prioritization, internal pipeline analysis, and directional prospect assessment when paired with confidence and review workflows

## 13. Benchmarking assumptions

The offline spreadsheets in `test_source/` are used as validation references for this build pack.

Assumptions:
- Harmonic.ai-style values captured in those spreadsheets are benchmark targets for approximation quality, not unquestionable truth
- company-detail examples in those spreadsheets can serve as ground-truth references for narrow, explicit behaviors such as canonical mapping, anchor choice, expected review state, and growth-window validation
- benchmark-derived expectations must preserve workbook, sheet, row, and column provenance
- disagreements between benchmark references and weak public evidence should trigger review, not silent coercion of the estimator
