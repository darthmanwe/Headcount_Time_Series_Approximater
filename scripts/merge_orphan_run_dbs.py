"""Merge orphan per-run SQLite DBs into the canonical ``data/headcount.sqlite``.

Motivation
==========

Before the canonical-DB refactor, every invocation of
``run_harmonic_cohort_live.py`` and its retry siblings created a brand
new ``<run_dir>/cohort.sqlite`` and threw it away when the run ended.
That cost us real network work: every 200 LinkedIn JSON-LD hit, every
Wayback snapshot, every SEC filing. This script walks those orphan
DBs and replays the *legitimate observations* they captured into the
canonical long-lived DB so the next run starts with that evidence
already on hand.

Strategy
========

Phase A - **Seed**: if the canonical DB doesn't exist yet, pick the
best orphan (largest, explicitly labelled as post-benchmark-leak-fix)
and copy it wholesale. Alembic version + new migrations are then
applied on top.

Phase B - **Merge**: for every remaining orphan, build an identity map
from canonical ``(canonical_name, canonical_domain)`` to the canonical
``company.id`` and remap the orphan's ``company_anchor_observation``
rows onto canonical IDs, inserting rows whose
``(source_name, raw_content_hash)`` are not already present.

What is **not** merged:
- ``alembic_version`` (canonical owns its own migration chain).
- ``run`` / ``company_run_status`` / ``review_queue_item`` / ``source_budget``
  / ``benchmark_event_candidate`` (run-scoped ephemera - cheap to
  regenerate, and merging them would flood the canonical DB with
  duplicate operational rows).
- ``benchmark_observation`` (these were the leaked-anchor source and
  are rebuilt from the workbook on demand; re-copying risks the
  per-cohort double-count bug).
- Pre-benchmark-leak-fix orphans are explicitly excluded from Phase B
  because some of their anchors were promoted from leaked benchmark
  rows. They are archived untouched instead.

What IS merged per orphan in Phase B:
- ``source_observation`` + ``company_anchor_observation`` (dedup by
  (source_name, raw_content_hash) which is the logical identity of an
  observation).

Flags
-----

- ``--dry-run`` - print per-orphan counts without writing anything.
- ``--archive`` - after a successful merge, rename the orphan directory
  into ``data/runs/_archive/`` so git status stays clean but nothing
  is deleted.
- ``--seed-from <basename>`` - override the post-leak-fix default seed
  source. Use if you want to start fresh from a different run.
"""

from __future__ import annotations

import argparse
import shutil
import sqlite3
import sys
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
CANONICAL_DB = (REPO_ROOT / "data" / "headcount.sqlite").resolve()
ORPHANS_ROOT = (REPO_ROOT / "data" / "runs" / "harmonic_live").resolve()
ARCHIVE_ROOT = (REPO_ROOT / "data" / "runs" / "_archive").resolve()

# Explicitly-safe seed: the post-benchmark-leak-fix cohort run. Every
# run that pre-dates this label has anchors promoted from leaked
# benchmark rows so we exclude them from the merge entirely.
DEFAULT_SEED_BASENAME = "20260420T205059Z_postleak"

# Orphan directories whose contents pre-date the benchmark-leak fix.
# Anchors in these DBs can contain leaked Harmonic/Zeeshan/LinkedIn
# benchmark values. Mergeable companies / aliases are still safe but
# observation rows are excluded.
LEAKED_ORPHAN_BASENAMES: frozenset[str] = frozenset(
    {
        "20260420T225859Z",
        "20260420T231635Z",
        "20260420T233455Z",
        "20260421T002143Z",
        "20260421T002915Z",
        "20260421T003808Z",
        "20260420T191939Z_postguard",
        "20260420T192456Z_retry2",
        "20260420T193330Z_ddg",
        "20260420T194121Z_disambig2",
        "20260420T200309Z_fastfail",
        "20260420T202337Z_merged",
    }
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@dataclass
class MergeStats:
    source_observations_copied: int = 0
    anchors_copied: int = 0
    companies_mapped: int = 0
    companies_unmapped: int = 0
    orphans_seeded: int = 0
    orphans_merged: int = 0
    orphans_skipped_leaked: int = 0
    orphans_archived: int = 0
    errors: list[str] = field(default_factory=list)

    def summary(self) -> str:
        return (
            f"seeded={self.orphans_seeded}"
            f" merged={self.orphans_merged}"
            f" skipped_leaked={self.orphans_skipped_leaked}"
            f" archived={self.orphans_archived}"
            f" source_obs+={self.source_observations_copied}"
            f" anchors+={self.anchors_copied}"
            f" mapped={self.companies_mapped}"
            f" unmapped={self.companies_unmapped}"
            f" errors={len(self.errors)}"
        )


def _open(path: Path) -> sqlite3.Connection:
    con = sqlite3.connect(str(path))
    con.row_factory = sqlite3.Row
    # foreign_keys OFF deliberately: we're doing raw table-copy
    # operations that would otherwise trip cascades at inconvenient
    # moments. We re-enable it in the application engine.
    con.execute("PRAGMA foreign_keys=OFF")
    return con


def _discover_orphans() -> list[Path]:
    if not ORPHANS_ROOT.exists():
        return []
    out: list[Path] = []
    for p in sorted(ORPHANS_ROOT.iterdir()):
        if not p.is_dir():
            continue
        db = p / "cohort.sqlite"
        if db.exists():
            out.append(p)
    return out


def _seed_from_orphan(orphan_dir: Path, *, dry_run: bool) -> None:
    orphan_db = orphan_dir / "cohort.sqlite"
    if not orphan_db.exists():
        raise FileNotFoundError(orphan_db)
    CANONICAL_DB.parent.mkdir(parents=True, exist_ok=True)
    if CANONICAL_DB.exists():
        raise RuntimeError(
            f"Refusing to seed: {CANONICAL_DB} already exists. Delete it"
            " first or skip --seed-from."
        )
    if dry_run:
        print(f"[seed] would copy {orphan_db} -> {CANONICAL_DB}")
        return
    shutil.copy2(orphan_db, CANONICAL_DB)
    # The copied DB already has schema and alembic_version from the
    # seed run. We keep alembic_version exactly as it is so the
    # subsequent ``alembic upgrade head`` runs only the migrations
    # that landed AFTER the seed (eg the new ``run.label`` column),
    # not the whole history which would collide on existing tables.
    print(f"[seed] copied {orphan_db} -> {CANONICAL_DB}")


def _run_alembic_upgrade() -> None:
    """Apply any post-seed migrations (eg the new run.label column)."""
    import os

    os.environ["DB_URL"] = f"sqlite:///{CANONICAL_DB.as_posix()}"
    from alembic import command
    from alembic.config import Config

    cfg = Config(str(REPO_ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(REPO_ROOT / "migrations"))
    cfg.set_main_option("sqlalchemy.url", os.environ["DB_URL"])
    command.upgrade(cfg, "head")
    print("[seed] alembic upgrade head applied")


def _build_company_index(canonical: sqlite3.Connection) -> dict[tuple[str, str], str]:
    """Return ``{(name_norm, domain_norm): canonical_company_id}``.

    Names and domains are lower-cased and trimmed so small formatting
    differences across orphan DBs don't prevent a match. Either field
    may be empty; missing-domain entries key on name alone (``(name,
    '')``) and take priority in ambiguous lookups only if no
    domain-keyed match was found.
    """

    idx: dict[tuple[str, str], str] = {}
    rows = canonical.execute(
        "SELECT id, canonical_name, canonical_domain FROM company"
    ).fetchall()
    for r in rows:
        name = (r["canonical_name"] or "").strip().lower()
        domain = (r["canonical_domain"] or "").strip().lower()
        if name:
            idx[(name, domain)] = r["id"]
            if domain:
                idx[(name, "")] = idx.get((name, ""), r["id"])
    # Also index aliases.
    alias_rows = canonical.execute(
        "SELECT alias_name, company_id FROM company_alias"
    ).fetchall()
    for r in alias_rows:
        alias = (r["alias_name"] or "").strip().lower()
        if alias:
            idx.setdefault((alias, ""), r["company_id"])
    return idx


def _company_id_lookup(
    idx: dict[tuple[str, str], str], name: str | None, domain: str | None
) -> str | None:
    n = (name or "").strip().lower()
    d = (domain or "").strip().lower()
    if not n:
        return None
    return idx.get((n, d)) or idx.get((n, ""))


def _merge_orphan(
    orphan_dir: Path,
    *,
    dry_run: bool,
    stats: MergeStats,
) -> None:
    orphan_db = orphan_dir / "cohort.sqlite"
    if not orphan_db.exists():
        return
    if orphan_dir.resolve() == CANONICAL_DB.parent.resolve():
        return  # shouldn't happen but belt and braces
    canonical = _open(CANONICAL_DB)
    orphan = _open(orphan_db)
    try:
        idx = _build_company_index(canonical)

        # Pull orphan companies once so we can remap their IDs.
        orphan_companies = {
            r["id"]: r
            for r in orphan.execute(
                "SELECT id, canonical_name, canonical_domain FROM company"
            ).fetchall()
        }
        remap: dict[str, str] = {}
        for oid, row in orphan_companies.items():
            hit = _company_id_lookup(idx, row["canonical_name"], row["canonical_domain"])
            if hit is not None:
                remap[oid] = hit
                stats.companies_mapped += 1
            else:
                stats.companies_unmapped += 1

        # Pull existing canonical observation dedup keys.
        existing: set[tuple[str, str]] = {
            (r["source_name"], r["raw_content_hash"])
            for r in canonical.execute(
                "SELECT source_name, raw_content_hash FROM source_observation"
            ).fetchall()
        }

        # Walk orphan source_observation + company_anchor_observation.
        obs_rows = orphan.execute(
            "SELECT * FROM source_observation"
        ).fetchall()
        anchor_rows = orphan.execute(
            "SELECT * FROM company_anchor_observation"
        ).fetchall()
        anchors_by_obs = {r["source_observation_id"]: r for r in anchor_rows}

        inserted_obs_ids: dict[str, str] = {}  # orphan_obs_id -> canonical_obs_id
        for obs in obs_rows:
            key = (obs["source_name"], obs["raw_content_hash"])
            if key in existing:
                continue
            anchor = anchors_by_obs.get(obs["id"])
            if anchor is None:
                continue  # observation without an anchor is not actionable here
            orphan_company = anchor["company_id"]
            canonical_company = remap.get(orphan_company)
            if canonical_company is None:
                continue  # no mapping - safer to skip than invent a company

            if not dry_run:
                canonical.execute(
                    """
                    INSERT INTO source_observation
                        (id, source_name, entity_type, source_url,
                         observed_at, raw_text, raw_content_hash,
                         parser_version, parse_status,
                         normalized_payload_json, created_at, updated_at)
                    VALUES
                        (:id, :source_name, :entity_type, :source_url,
                         :observed_at, :raw_text, :raw_content_hash,
                         :parser_version, :parse_status,
                         :normalized_payload_json, :created_at, :updated_at)
                    """,
                    dict(obs),
                )
            stats.source_observations_copied += 1
            inserted_obs_ids[obs["id"]] = obs["id"]
            existing.add(key)

            if not dry_run:
                payload = dict(anchor)
                payload["company_id"] = canonical_company
                canonical.execute(
                    """
                    INSERT INTO company_anchor_observation
                        (id, company_id, source_observation_id,
                         anchor_type, headcount_value_min,
                         headcount_value_point, headcount_value_max,
                         headcount_value_kind, anchor_month,
                         confidence, note, created_at, updated_at)
                    VALUES
                        (:id, :company_id, :source_observation_id,
                         :anchor_type, :headcount_value_min,
                         :headcount_value_point, :headcount_value_max,
                         :headcount_value_kind, :anchor_month,
                         :confidence, :note, :created_at, :updated_at)
                    """,
                    payload,
                )
            stats.anchors_copied += 1

        if not dry_run:
            canonical.commit()
    finally:
        orphan.close()
        canonical.close()


def _archive_orphan(orphan_dir: Path, *, dry_run: bool, stats: MergeStats) -> None:
    ARCHIVE_ROOT.mkdir(parents=True, exist_ok=True)
    target = ARCHIVE_ROOT / orphan_dir.name
    if target.exists():
        target = ARCHIVE_ROOT / f"{orphan_dir.name}_{datetime.now(tz=UTC):%H%M%S}"
    if dry_run:
        print(f"[archive] would move {orphan_dir} -> {target}")
    else:
        shutil.move(str(orphan_dir), str(target))
        print(f"[archive] moved {orphan_dir} -> {target}")
    stats.orphans_archived += 1


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--archive",
        action="store_true",
        help=(
            "After a successful merge, move each orphan directory into"
            " data/runs/_archive/. Off by default so you can re-run the"
            " merger with different flags."
        ),
    )
    parser.add_argument(
        "--seed-from",
        default=DEFAULT_SEED_BASENAME,
        help=(
            "Basename of the orphan directory to seed the canonical DB"
            " from when it doesn't already exist. Defaults to the"
            " explicit post-benchmark-leak-fix cohort run."
        ),
    )
    parser.add_argument(
        "--skip-seed",
        action="store_true",
        help=(
            "Skip the seed step entirely. Use when you've prepared"
            " canonical by other means (eg alembic upgrade from empty)."
        ),
    )
    args = parser.parse_args()

    orphans = _discover_orphans()
    if not orphans:
        print(f"[merge] no orphan cohort.sqlite files under {ORPHANS_ROOT}")
        return 0

    stats = MergeStats()

    # Phase A: seed canonical.
    if not CANONICAL_DB.exists() and not args.skip_seed:
        seed = ORPHANS_ROOT / args.seed_from
        if not (seed / "cohort.sqlite").exists():
            print(
                f"[seed] seed source missing: {seed}/cohort.sqlite",
                file=sys.stderr,
            )
            return 2
        _seed_from_orphan(seed, dry_run=args.dry_run)
        if not args.dry_run:
            _run_alembic_upgrade()
        stats.orphans_seeded += 1
        # Seed source has been merged; don't double-count it in phase B.
        orphans = [o for o in orphans if o.name != args.seed_from]

    # Phase B: merge remaining orphans.
    for orphan in orphans:
        if orphan.name in LEAKED_ORPHAN_BASENAMES:
            print(
                f"[merge] skipping {orphan.name}"
                " (pre-benchmark-leak-fix; observations excluded by design)"
            )
            stats.orphans_skipped_leaked += 1
            if args.archive:
                _archive_orphan(orphan, dry_run=args.dry_run, stats=stats)
            continue
        try:
            _merge_orphan(orphan, dry_run=args.dry_run, stats=stats)
            stats.orphans_merged += 1
            print(f"[merge] merged {orphan.name}")
            if args.archive:
                _archive_orphan(orphan, dry_run=args.dry_run, stats=stats)
        except Exception as exc:
            stats.errors.append(f"{orphan.name}: {exc!r}")
            print(f"[merge] ERROR on {orphan.name}: {exc!r}", file=sys.stderr)

    print("---")
    print(f"[merge] summary: {stats.summary()}")
    for err in stats.errors:
        print(f"[merge] error: {err}", file=sys.stderr)
    return 0 if not stats.errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
