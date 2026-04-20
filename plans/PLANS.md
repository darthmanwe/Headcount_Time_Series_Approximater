# PLANS.md

## ExecPlan template

Use this template for any substantial implementation task.

### Title
Short imperative title.

### Goal
What is being built or changed.

### Scope
What is included.
What is explicitly excluded.

### Assumptions
List assumptions clearly.

### Files and modules
List expected files or modules to create or modify.

### Data model changes
List tables, fields, migrations, indexes.

### Source and parsing changes
List adapters, parsers, and fixture updates.

### Benchmark and validation inputs
List which `test_source/` files are used, how workbook/sheet/row provenance is preserved, and what benchmark fixtures or expected outputs are added or updated.

### Estimation changes
List estimator or confidence logic changes.

### Test plan
Unit, integration, golden tests, and benchmark comparisons against the offline `test_source/` spreadsheets.

### Risks
Top risks and how to mitigate them.

### Execution steps
Numbered implementation steps.

### Definition of done
Concrete completion criteria.
