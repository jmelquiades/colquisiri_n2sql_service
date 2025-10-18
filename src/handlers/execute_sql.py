import os, psycopg
from time import perf_counter

DSN = os.getenv("PG_DEST_DSN")

def execute_sql(sql, args=(), timeout_ms=5000):
    t0 = perf_counter()
    with psycopg.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute(f"SET statement_timeout TO {timeout_ms}")
        cur.execute(sql, args)
        rows = cur.fetchall()
        cols = [d.name for d in cur.description]
    return cols, rows, {"elapsed_ms": int((perf_counter()-t0)*1000)}
