"""Consolidate legitimate public-scraper observations into canonical DB.

Context
=======
Plan B1 seeded ``data/headcount.sqlite`` from the ``_postleak`` orphan
and moved the other 12 orphans into ``data/runs/_archive/`` untouched
(on the conservative assumption they might carry benchmark-leak
residue). Poll of the archived DBs shows the reality:

- 213 ``source_name=benchmark`` rows in canonical are the leaked
  promotion ("promoted_benchmark provider=zeeshan/linkedin/harmonic")
  and MUST be purged - they are exactly the rows the benchmark-leak
  fix was meant to prevent.
- Every orphan also carries 1-12 *legitimate* public-scraper rows
  (company_web / linkedin_public) that we paid real LinkedIn /
  Wayback requests for and should not lose.

This script does two things, both idempotent:

1. **Purge** every ``source_observation`` row whose ``source_name``
   is ``benchmark`` plus the ``company_anchor_observation`` rows that
   hang off them. ``benchmark`` is an injection channel, not a
   scraper, so nothing in production should ever emit through it; if
   the leak comes back the purge will catch it on the next run.

2. **Merge** every archived orphan's public-scraper observations
   (``source_name`` in an explicit allow-list) into canonical using
   the same ``(source_name, raw_content_hash)`` dedup key
   ``collect_anchors`` uses. Orphan company IDs are remapped onto
   canonical ``company.id`` via the same name/domain index the
   existing merger uses.

Dry-run first: ``python scripts/consolidate_legitimate_observations.py --dry-run``.

No run / company_run_status / benchmark_observation / estimate rows
are touched. The benchmark_observation table is the ground-truth
store for Harmonic/Zeeshan/LinkedIn sample values and is safe to
keep (it is not the leaked path; the leak was the *promotion* into
source_observation).
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CANONICAL_DB = (ROOT / "data" / "headcount.sqlite").resolve()
ARCHIVE_ROOT = (ROOT / "data" / "runs" / "_archive").resolve()

# Only these source_name values are copied out of the orphans. The
# list deliberately excludes ``benchmark`` (the injection path that
# caused the leak) and ``manual`` (ops-only channel; anything worth
# keeping there is already in the canonical manual_anchors file).
LEGITIMATE_SOURCES: frozenset[str] = frozenset(
    {
        "company_web",
        "linkedin_public",
        "linkedin_ocr",
        "sec",
        "wikidata",
        "wayback",
    }
)

# Guard against reintroducing the leak.
LEAKED_SOURCES: frozenset[str] = frozenset({"benchmark"})


@dataclass
class Stats:
    purged_obs: int = 0
    purged_anchors: int = 0
    orphans_scanned: int = 0
    orphans_merged: int = 0
    merged_obs: int = 0
    merged_anchors: int = 0
    skipped_leaked_rows: int = 0
    skipped_unmapped: int = 0
    skipped_dup: int = 0
    errors: list[str] = field(default_factory=list)

    def summary(self) -> str:
        return (
            f"purged_obs={self.purged_obs}"
            f" purged_anchors={self.purged_anchors}"
            f" orphans_merged={self.orphans_merged}/{self.orphans_scanned}"
            f" merged_obs={self.merged_obs}"
            f" merged_anchors={self.merged_anchors}"
            f" skipped_leaked_rows={self.skipped_leaked_rows}"
            f" skipped_unmapped={self.skipped_unmapped}"
            f" skipped_dup={self.skipped_dup}"
            f" errors={len(self.errors)}"
        )


def _open(p: Path) -> sqlite3.Connection:
    con = sqlite3.connect(str(p))
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys=OFF")
    return con


def _purge_leaked(canonical: sqlite3.Connection, *, dry_run: bool, stats: Stats) -> None:
    """Remove every ``source_observation`` whose source_name is in the
    leaked set, plus the anchors that reference them.

    Order matters: delete anchors first because ``source_observation``
    is the parent side of the FK (even with foreign_keys=OFF we want
    the final row count to be coherent).
    """

    leaked_placeholders = ",".join("?" for _ in LEAKED_SOURCES)
    leaked_params = tuple(LEAKED_SOURCES)
    leaked_obs_ids = [
        r["id"]
        for r in canonical.execute(
            f"SELECT id FROM source_observation WHERE source_name IN ({leaked_placeholders})",
            leaked_params,
        ).fetchall()
    ]
    if not leaked_obs_ids:
        return

    ids_placeholder = ",".join("?" for _ in leaked_obs_ids)
    anchor_count = canonical.execute(
        f"SELECT COUNT(*) AS n FROM company_anchor_observation "
        f"WHERE source_observation_id IN ({ids_placeholder})",
        leaked_obs_ids,
    ).fetchone()["n"]
    stats.purged_anchors = anchor_count
    stats.purged_obs = len(leaked_obs_ids)

    if dry_run:
        print(
            f"[purge] would remove {anchor_count} anchor rows + "
            f"{len(leaked_obs_ids)} source_observation rows "
            f"(source_name in {sorted(LEAKED_SOURCES)})"
        )
        return

    canonical.execute(
        f"DELETE FROM company_anchor_observation "
        f"WHERE source_observation_id IN ({ids_placeholder})",
        leaked_obs_ids,
    )
    canonical.execute(
        f"DELETE FROM source_observation WHERE id IN ({ids_placeholder})",
        leaked_obs_ids,
    )
    canonical.commit()
    print(
        f"[purge] removed {anchor_count} anchors + {len(leaked_obs_ids)} "
        f"source_observation rows"
    )


def _build_company_index(canonical: sqlite3.Connection) -> dict[tuple[str, str], str]:
    idx: dict[tuple[str, str], str] = {}
    for r in canonical.execute(
        "SELECT id, canonical_name, canonical_domain FROM company"
    ).fetchall():
        name = (r["canonical_name"] or "").strip().lower()
        domain = (r["canonical_domain"] or "").strip().lower()
        if name:
            idx[(name, domain)] = r["id"]
            idx.setdefault((name, ""), r["id"])
    for r in canonical.execute(
        "SELECT alias_name, company_id FROM company_alias"
    ).fetchall():
        alias = (r["alias_name"] or "").strip().lower()
        if alias:
            idx.setdefault((alias, ""), r["company_id"])
    return idx


def _merge_one(
    orphan_db: Path,
    *,
    canonical: sqlite3.Connection,
    idx: dict[tuple[str, str], str],
    existing: set[tuple[str, str]],
    dry_run: bool,
    stats: Stats,
) -> None:
    orphan = _open(orphan_db)
    try:
        orphan_companies = {
            r["id"]: r
            for r in orphan.execute(
                "SELECT id, canonical_name, canonical_domain FROM company"
            ).fetchall()
        }
        remap: dict[str, str] = {}
        for oid, row in orphan_companies.items():
            n = (row["canonical_name"] or "").strip().lower()
            d = (row["canonical_domain"] or "").strip().lower()
            hit = idx.get((n, d)) or idx.get((n, "")) if n else None
            if hit is not None:
                remap[oid] = hit

        anchors_by_obs = {
            r["source_observation_id"]: r
            for r in orphan.execute(
                "SELECT * FROM company_anchor_observation"
            ).fetchall()
        }

        for obs in orphan.execute("SELECT * FROM source_observation").fetchall():
            src = obs["source_name"]
            if src in LEAKED_SOURCES:
                stats.skipped_leaked_rows += 1
                continue
            if src not in LEGITIMATE_SOURCES:
                # Unknown source - skip rather than import blindly.
                stats.skipped_leaked_rows += 1
                continue
            key = (src, obs["raw_content_hash"])
            if key in existing:
                stats.skipped_dup += 1
                continue
            anchor = anchors_by_obs.get(obs["id"])
            if anchor is None:
                # observation-without-anchor isn't useful here
                continue
            canonical_company = remap.get(anchor["company_id"])
            if canonical_company is None:
                stats.skipped_unmapped += 1
                continue

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
                anchor_row = dict(anchor)
                anchor_row["company_id"] = canonical_company
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
                    anchor_row,
                )
            stats.merged_obs += 1
            stats.merged_anchors += 1
            existing.add(key)

        if not dry_run:
            canonical.commit()
    finally:
        orphan.close()


def _discover_orphans() -> list[Path]:
    if not ARCHIVE_ROOT.exists():
        return []
    return [
        p for p in sorted(ARCHIVE_ROOT.iterdir())
        if p.is_dir() and (p / "cohort.sqlite").exists()
    ]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if not CANONICAL_DB.exists():
        print(f"canonical missing: {CANONICAL_DB}", file=sys.stderr)
        return 2

    stats = Stats()
    canonical = _open(CANONICAL_DB)
    try:
        _purge_leaked(canonical, dry_run=args.dry_run, stats=stats)
        idx = _build_company_index(canonical)
        existing: set[tuple[str, str]] = {
            (r["source_name"], r["raw_content_hash"])
            for r in canonical.execute(
                "SELECT source_name, raw_content_hash FROM source_observation"
            ).fetchall()
        }

        orphans = _discover_orphans()
        for orphan_dir in orphans:
            stats.orphans_scanned += 1
            try:
                _merge_one(
                    orphan_dir / "cohort.sqlite",
                    canonical=canonical,
                    idx=idx,
                    existing=existing,
                    dry_run=args.dry_run,
                    stats=stats,
                )
                stats.orphans_merged += 1
                print(f"[merge] processed {orphan_dir.name}")
            except Exception as exc:
                stats.errors.append(f"{orphan_dir.name}: {exc!r}")
                print(f"[merge] ERROR {orphan_dir.name}: {exc!r}", file=sys.stderr)
    finally:
        canonical.close()

    print("---")
    print(f"[consolidate] {stats.summary()}")
    for err in stats.errors:
        print(f"[consolidate] error: {err}", file=sys.stderr)
    return 0 if not stats.errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
