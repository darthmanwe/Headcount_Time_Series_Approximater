# Acceptance Criteria V2

## Narrow-slice completion criteria

The narrow slice is complete when all of the following are true:

1. Companies can be imported from spreadsheet or CSV.
2. Canonical company resolution works with aliases and manual overrides.
3. At least one source adapter can store raw evidence snapshots.
4. The logged-out public LinkedIn adapter exists with bounded fail-closed behavior.
5. Employment observations can be normalized into month intervals.
6. Monthly public-profile counts can be generated.
7. A current anchor can be selected by policy.
8. Monthly headcount estimates can be produced by method version.
9. 6m / 1y / 2y growth can be computed where allowed by confidence policy.
10. Confidence components and final confidence bands are stored.
11. Manual review queue items are created for low-confidence companies.
12. Manual overrides can be applied and preserved.
13. Golden tests validate estimation on a benchmark fixture set.
14. Export endpoints or CLI exports exist for final output tables.
15. Every final estimate can show its evidence trace.

## Failure criteria

The narrow slice is not acceptable if:
- the estimator emits numbers without evidence trace
- canonical resolution is implicit or hidden
- source adapters return raw HTML directly into estimation code
- 2-year metrics are emitted without confidence gating
- blocked/gated source states are treated as successful empty results
- manual overrides are not auditable
