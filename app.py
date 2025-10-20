import os, logging
from typing import Any, List, Optional
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import psycopg
from psycopg.rows import dict_row

log = logging.getLogger("n2sql")
app = FastAPI(title="CRITERIA DataTalk - N2SQL", version="0.1.1")

# ---------- Models ----------
class QueryIn(BaseModel):
    dataset: str
    intent: str
    params: dict = {}

class SqlIn(BaseModel):
    sql: str
    params: Optional[List[Any]] = None

# ---------- DB helpers ----------
def _dsn_from_env() -> str:
    dsn = os.getenv("DATABASE_URL")
    if dsn:
        return dsn
    # fallback a variables PG estándar si no hay DATABASE_URL
    host = os.getenv("PGHOST")
    db   = os.getenv("PGDATABASE")
    user = os.getenv("PGUSER")
    pwd  = os.getenv("PGPASSWORD")
    port = os.getenv("PGPORT", "5432")
    if host and db and user and pwd:
        return f"postgresql://{user}:{pwd}@{host}:{port}/{db}"
    raise RuntimeError("DATABASE_URL/PG* no configuradas")

def _execute_sql(sql: str, params: Optional[List[Any]] = None):
    dsn = _dsn_from_env()
    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql, params or [])
            if cur.description is None:
                return {"rowcount": cur.rowcount}
            rows = cur.fetchall()
            return {"rows": rows}

# ---------- Very-small NLU → SQL (reglas) ----------
def intent_to_sql(intent: str):
    q = (intent or "").strip().lower()

    # tolera "ultimas" o "últimas"
    if ("ultimas" in q or "últimas" in q) and "facturas" in q:
        return "select id, total, fecha from facturas order by fecha desc limit 5;", []

    if (("cuantas" in q or "cuántas" in q) and "facturas" in q) or \
       ("conteo" in q and "facturas" in q):
        return "select count(*) as total from facturas;", []

    # agrega aquí más reglas rápidas según vayas necesitando
    return None, None

# ---------- Endpoints ----------
@app.get("/health")
def health():
    return {"ok": True}

@app.get("/diag/env")
def diag_env():
    return {
        "has_DATABASE_URL": bool(os.getenv("DATABASE_URL")),
        "PGHOST": os.getenv("PGHOST"),
        "PGDATABASE": os.getenv("PGDATABASE"),
        "PGUSER": os.getenv("PGUSER"),
        "PGPORT": os.getenv("PGPORT"),
        "app_version": "0.1.1",
    }

@app.get("/diag/db")
def diag_db():
    try:
        ping = _execute_sql("select 1 as ok;")
        return {"ok": True, "ping": ping}
    except Exception as e:
        return {"ok": False, "error": str(e)}

@app.post("/v1/sql/execute")
def sql_execute(inp: SqlIn):
    try:
        return _execute_sql(inp.sql, inp.params)
    except Exception as e:
        log.exception("sql_execute error")
        raise HTTPException(500, str(e))

@app.post("/v1/query")
def query(body: QueryIn):
    # Por ahora no bloqueamos por dataset; lo pasamos directo al NLU/SQL
    sql, params = intent_to_sql(body.intent)
    if not sql:
        raise HTTPException(400, "Intent no soportado todavía")
    try:
        result = _execute_sql(sql, params)
        return {"dataset": body.dataset, "intent": body.intent, "sql": sql, "result": result}
    except Exception as e:
        log.exception("query error")
        raise HTTPException(500, str(e))
