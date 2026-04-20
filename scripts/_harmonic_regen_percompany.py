"""One-shot helper: regenerate per_company.json from an existing run DB."""

from __future__ import annotations

import json
import os
import sys
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

RUN_DIR = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else None
if RUN_DIR is None:
    raise SystemExit("usage: python scripts/_harmonic_regen_percompany.py <run_dir>")

db_path = (RUN_DIR / "cohort.sqlite").resolve()
os.environ["DB_URL"] = f"sqlite:///{db_path.as_posix()}"
os.environ["CACHE_DIR"] = str((RUN_DIR / "http_cache").resolve())
os.environ["RUN_ARTIFACT_DIR"] = str((RUN_DIR / "run_artifacts").resolve())
os.environ["DUCKDB_PATH"] = str((RUN_DIR / "outputs" / "cohort.duckdb").resolve())

sys.path.insert(0, str(REPO_ROOT / "scripts"))
from run_harmonic_cohort_live import _load_harmonic_targets, _per_company_report  # noqa: E402

from headcount.db.engine import session_scope  # noqa: E402

targets = _load_harmonic_targets()
with session_scope() as session:
    out = _per_company_report(session, harmonic_targets=targets, as_of=date(2026, 4, 1))

(RUN_DIR / "per_company.json").write_text(
    json.dumps(out, indent=2, default=str), encoding="utf-8"
)
print(f"wrote {RUN_DIR / 'per_company.json'}; rows={len(out)}")
