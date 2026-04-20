Implement the approved ExecPlan exactly.

Before coding:
- restate the plan briefly
- list the files you will modify
- call out any assumption that must be fixed before coding
- state whether `test_source/High Priority Companies_01.04.2026.xlsx` and `test_source/Sample Employee Growth for High Priority Prospects.xlsx` are affected

During implementation:
- keep modules small
- write types
- add tests with fixtures
- preserve evidence traceability
- do not skip confidence or review logic if touched by the change
- preserve workbook, sheet, row, and column provenance for any benchmark-derived fixtures or expectations
- if estimation behavior changes, update the benchmark or golden-test path that compares outputs to the offline Harmonic.ai-style spreadsheets

After implementation:
- summarize what changed
- summarize tests added
- summarize benchmark coverage added or updated from `test_source/`
- list remaining risks or TODOs
