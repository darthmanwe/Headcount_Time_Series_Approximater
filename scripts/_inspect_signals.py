"""Dump anchor observations + estimates for companies matching a LIKE pattern."""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path


def main() -> None:
    run_dir = Path(sys.argv[1])
    pattern = sys.argv[2] if len(sys.argv) > 2 else "%"
    con = sqlite3.connect(run_dir / "cohort.sqlite")
    cur = con.cursor()
    cur.execute(
        "select id, canonical_name, canonical_domain from company "
        "where canonical_name like ?",
        (pattern,),
    )
    companies = cur.fetchall()
    for cid, name, domain in companies:
        print(f"=== {name} ({domain}) id={cid}")
        # Company anchor observations + source
        cur.execute(
            """
            select cao.headcount_value_point, cao.headcount_value_min,
                   cao.headcount_value_max, cao.headcount_value_kind,
                   cao.confidence, cao.note,
                   so.source_name, so.source_url, so.raw_text, so.normalized_payload_json
            from company_anchor_observation cao
            left join source_observation so
              on so.id = cao.source_observation_id
            where cao.company_id=?
            """,
            (cid,),
        )
        for row in cur.fetchall():
            p, lo, hi, kind, conf, note, src, url, raw, payload = row
            raw_short = (raw or "")[:180].replace("\n", " ")
            print(
                f"  [{src}] p={p} range={lo}..{hi} kind={kind} conf={conf}"
            )
            print(f"     url={url}")
            print(f"     note={note}")
            print(f"     raw={raw_short}")
            if payload:
                print(f"     payload={(payload or '')[:240]}")
        cur.execute(
            """
            select month, estimated_headcount, estimated_headcount_min,
                   estimated_headcount_max, method,
                   confidence_band, confidence_score
            from headcount_estimate_monthly
            where company_id=? order by month desc limit 3
            """,
            (cid,),
        )
        for row in cur.fetchall():
            month, p, lo, hi, meth, band, conf = row
            print(
                f"  EST [{month}] p={p} range={lo}..{hi} "
                f"method={meth} band={band} conf={conf}"
            )
        print()


if __name__ == "__main__":
    main()
