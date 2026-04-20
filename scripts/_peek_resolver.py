"""Ad-hoc inspection script for smoke runs. Not part of the package."""

from __future__ import annotations

import sqlite3
import sys


def main(db_path: str) -> None:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    for label, query in [
        ("companies", "select count(*) c from company"),
        ("aliases", "select count(*) c from company_alias"),
        ("source_links", "select count(*) c from company_source_link"),
        ("relations", "select count(*) c from company_relation"),
        (
            "resolved_candidates",
            "select count(*) c from company_candidate where status='resolved'",
        ),
        (
            "benchmark_obs_linked",
            "select count(*) c from benchmark_observation where company_id is not null",
        ),
        ("event_candidates", "select count(*) c from benchmark_event_candidate"),
    ]:
        row = conn.execute(query).fetchone()
        print(f"{label}: {row['c']}")

    print("\nevent candidates:")
    for row in conn.execute(
        "select hint_type, description, event_month_hint from benchmark_event_candidate order by hint_type"
    ):
        print(
            f"  [{row['hint_type']}] ({row['event_month_hint']}) {row['description'][:120]}"
        )

    print("\nrelations:")
    for row in conn.execute(
        """
        select p.canonical_name as parent, ch.canonical_name as child, r.kind, r.effective_month, r.note
        from company_relation r
        join company p on p.id = r.parent_id
        join company ch on ch.id = r.child_id
        order by r.kind, parent
        """
    ):
        print(
            f"  {row['parent']} -[{row['kind']}]-> {row['child']} "
            f"({row['effective_month']}): {row['note'][:100] if row['note'] else ''}"
        )

    print("\nsymphony / 1010 lookup:")
    for row in conn.execute(
        "select canonical_name, canonical_domain from company where canonical_name like '%1010%' or canonical_name like '%Symphony%' or canonical_name like '%symphony%'"
    ):
        print(f"  {row['canonical_name']}  [{row['canonical_domain']}]")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "data/headcount_smoke.sqlite")
