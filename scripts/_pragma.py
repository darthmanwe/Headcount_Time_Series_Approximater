import sqlite3
import sys

con = sqlite3.connect(sys.argv[1])
cur = con.cursor()
for t in sys.argv[2:]:
    print(f"== {t} ==")
    cur.execute(f"pragma table_info({t})")
    for r in cur.fetchall():
        print(r)
