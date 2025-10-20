# src/app.py
import os, re, time
from typing import Any, Dict, List, Tuple

import psycopg
from psycopg.rows import dict_row

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# --- OpenAI ---
from openai import OpenAI

APP_TITLE = "CRITERIA DataTalk - N2SQL"
APP_VERSION = "0.2.0"

app = FastAPI(title=APP_TITLE, version=APP_VERSION)

# ---------- Config ----------
DB_URL = os.getenv("DATABASE_URL")
if not DB_URL:
    raise RuntimeError("DATABASE_URL no está configurado.")

SAFE_SCHEMA = os.getenv("SAFE_SCHEMA", "odoo_replica")

OPENAI_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_KEY:
    raise RuntimeError("OPENAI_API_KEY no está configurado.")

# OpenAI client (estándar). Si usas Azure, ajusta base_url y api_version.
if os.getenv("OPENAI_API_BASE"):  # Azure u otro endpoint compatible
    client = OpenAI(
        api_key=OPENAI_KEY,
        base_url=os.environ["OPENAI_API_BASE"],
    )
else:
    client = OpenAI(api_key=OPENAI_KEY)

MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

# ---------- DB helpers ----------
def _conn():
    return psycopg.connect(DB_URL, autocommit=True, row_factory=dict_row)

def _execute_sql(sql: str, params: List[Any] | None = None) -> Dict[str, Any]:
    with _conn() as cn, cn.cursor() as cur:
        cur.execute(sql, params or [])
        try:
            rows = cur.fetchall()
        except psycopg.ProgrammingError:
            rows = []
    return {"rows": rows}

# ---------- Schema summary cache ----------
_schema_cache_text = ""
_schema_cache_ts = 0.0

def _schema_summary() -> str:
    """Construye un resumen de columnas por tabla del esquema permitido."""
    global _schema_cache_text, _schema_cache_ts
    now = time.time()
    if _schema_cache_text and (now - _schema_cache_ts) < 3600:
        return _schema_cache_text

    with _conn() as cn, cn.cursor() as cur:
        cur.execute(
            """
            select table_name,
                   string_agg(column_name || ':' || data_type, ', ' order by ordinal_position) as cols
            from information_schema.columns
            where table_schema = %s
            group by table_name
            order by table_name
            limit 200;
            """,
            [SAFE_SCHEMA],
        )
        rows = cur.fetchall()

    _schema_cache_text = "\n".join(
        f"{SAFE_SCHEMA}.{r['table_name']}({r['cols']})" for r in rows
    )
    _schema_cache_ts = now
    return _schema_cache_text

# ---------- LLM → SQL ----------
SELECT_ONLY = re.compile(r"^\s*select\b", re.I)

def llm_to_sql(intent: str) -> Tuple[str, List[Any]]:
    schema = _schema_summary()
    prompt = f"""
Convierte la solicitud del usuario en UNA consulta **PostgreSQL** segura.

Esquema permitido: {SAFE_SCHEMA}
Tablas y columnas:
{schema}

Reglas ESTRICTAS:
- SOLO una consulta **SELECT** (nada de INSERT/UPDATE/DELETE/DDL).
- Usa nombres **totalmente calificados**: {SAFE_SCHEMA}.tabla
- No uses information_schema ni tablas pg_*.
- Si no hay LIMIT explícito, añade **LIMIT 100**.
- Devuelve **solo** el SQL, sin explicaciones ni ```bloques```.

Usuario: {intent}
SQL:
""".strip()

    resp = client.chat.completions.create(
        model=MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=0,
    )
    sql = (resp.choices[0].message.content or "").strip()

    # Quitar fences si vinieran
    m = re.search(r"```(?:sql)?\s*(.*?)\s*```", sql, re.S | re.I)
    if m:
        sql = m.group(1).strip()

    # Guardas
    low = sql.lower()
    if not SELECT_ONLY.match(sql):
        raise HTTPException(400, "El modelo no devolvió un SELECT. Bloqueado.")
    if "information_schema" in low or "pg_" in low:
        raise HTTPException(400, "Acceso a metadatos bloqueado.")
    if f"{SAFE_SCHEMA.lower()}." not in low:
        raise HTTPException(400, "El SQL no usa el esquema permitido.")
    # Asegurar LIMIT
    if " limit " not in low:
        sql = sql.rstrip(";") + " LIMIT 100;"
    else:
        # normalizar punto y coma
        sql = sql.rstrip(";") + ";"

    return sql, []

# ---------- Models ----------
class QueryIn(BaseModel):
    dataset: str
    intent: str
    params: Dict[str, Any] = {}

class QueryOut(BaseModel):
    sql: str
    rows: List[Dict[str, Any]]

# ---------- Endpoints ----------
@app.get("/health")
def health():
    return {"ok": True}

@app.post("/v1/query", response_model=QueryOut)
def query(body: QueryIn):
    # Ignoramos "dataset" a propósito; el LLM trabaja con SAFE_SCHEMA
    sql, params = llm_to_sql(body.intent)
    data = _execute_sql(sql, params)
    return {"sql": sql, "rows": data["rows"]}

# (Opcional) ejecutar SQL directamente – útil para depurar
class ExecIn(BaseModel):
    sql: str
    params: List[Any] | None = None

@app.post("/v1/sql/execute")
def exec_sql(body: ExecIn):
    return _execute_sql(body.sql, body.params or [])
