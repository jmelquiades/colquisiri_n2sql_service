import os
import re
import time
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Request, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import psycopg
from psycopg.rows import dict_row

# Azure OpenAI (v1.*)
from openai import AzureOpenAI

APP_NAME = "n2sql-service"
app = FastAPI(title=APP_NAME)

# === Env / Config ===
DB_URL = (
    os.getenv("DATABASE_URL")
    or os.getenv("PG_URL")
    or os.getenv("PG_DSN")
    or ""
)
if not DB_URL:
    raise RuntimeError("DATABASE_URL no está configurado")

AZURE_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT", "")
AZURE_KEY = os.getenv("AZURE_OPENAI_API_KEY", "")
AZURE_API_VERSION = os.getenv("AZURE_OPENAI_API_VERSION", "2024-06-01")
AZURE_DEPLOYMENT = os.getenv("AZURE_OPENAI_DEPLOYMENT", "")  # p.ej. nlu-4imini

if not (AZURE_ENDPOINT and AZURE_KEY and AZURE_DEPLOYMENT):
    raise RuntimeError("Faltan variables de Azure OpenAI (ENDPOINT/API_KEY/DEPLOYMENT)")

client = AzureOpenAI(
    api_key=AZURE_KEY,
    api_version=AZURE_API_VERSION,
    azure_endpoint=AZURE_ENDPOINT,
)

ODBC_DEFAULT_SCHEMA = "odoo_replica"  # lo usamos en el prompt y en search_path

# ======== Utilidades DB =========
def db_connect():
    return psycopg.connect(DB_URL, autocommit=True)

def fetch_all_dict(conn, sql: str, params: Optional[List[Any]] = None) -> List[Dict[str, Any]]:
    with conn.cursor(row_factory=dict_row) as cur:
        if params:
            cur.execute(sql, params)
        else:
            cur.execute(sql)
        return list(cur.fetchall())

def exec_sql_one(conn, sql: str, params: Optional[List[Any]] = None) -> Optional[Dict[str, Any]]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, params or [])
        row = cur.fetchone()
        return dict(row) if row else None

# ======== Auditoría dinámica =========
# Intentamos escribir en bot_audit.query_logs.
# Si no existe o el esquema de columnas no encaja, caemos a bot_audit.n2sql_logs.
_audit_columns_cache: Optional[set] = None
_audit_mode: Optional[str] = None  # "query_logs", "fallback"

def _discover_audit_columns(conn) -> set:
    sql = """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema='bot_audit' AND table_name='query_logs'
    """
    rows = fetch_all_dict(conn, sql)
    return {r["column_name"] for r in rows}

def _ensure_fallback_table(conn) -> None:
    ddl = """
    CREATE SCHEMA IF NOT EXISTS bot_audit;
    CREATE TABLE IF NOT EXISTS bot_audit.n2sql_logs (
        id BIGSERIAL PRIMARY KEY,
        dataset TEXT,
        intent TEXT,
        sql_text TEXT,
        row_count INT,
        duration_ms INT,
        status TEXT,
        error TEXT,
        request_ip TEXT,
        model TEXT,
        prompt_tokens INT,
        completion_tokens INT,
        total_tokens INT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    """
    with conn.cursor() as cur:
        cur.execute(ddl)

def _audit_try_insert(conn, table: str, payload: Dict[str, Any], cols_available: Optional[set]) -> None:
    # Mapeo flexible de nombres posibles
    aliases = {
        "sql_text": ["sql_text", "sql"],
        "row_count": ["row_count", "rows", "rowcount"],
        "duration_ms": ["duration_ms", "elapsed_ms", "time_ms"],
        "request_ip": ["request_ip", "ip"],
    }
    to_insert = dict(payload)  # copia

    # normalizamos alias si faltan
    for canonical, candidates in aliases.items():
        if canonical not in to_insert:
            for c in candidates:
                if c in to_insert:
                    to_insert[canonical] = to_insert[c]
                    break

    # Si tenemos lista de columnas válidas, filtramos
    if cols_available is not None:
        keys = [k for k in to_insert.keys() if k in cols_available]
    else:
        keys = list(to_insert.keys())

    if not keys:
        # Nada compatible; evitamos error
        return

    cols_sql = ", ".join([f"{k}" for k in keys])
    placeholders = ", ".join(["%s"] * len(keys))
    values = [to_insert[k] for k in keys]

    with conn.cursor() as cur:
        cur.execute(f"INSERT INTO {table} ({cols_sql}) VALUES ({placeholders})", values)

def write_audit(dataset: str,
                intent: str,
                sql_text: str,
                row_count: int,
                duration_ms: int,
                status: str,
                request_ip: Optional[str],
                model: Optional[str],
                usage: Optional[Dict[str, Any]],
                error: Optional[str]) -> None:
    global _audit_columns_cache, _audit_mode
    try:
        with db_connect() as conn:
            if _audit_mode is None:
                # Primer uso: intentamos query_logs
                cols = _discover_audit_columns(conn)
                if cols:
                    _audit_columns_cache = cols
                    _audit_mode = "query_logs"
                else:
                    # No existe tabla => fallback
                    _ensure_fallback_table(conn)
                    _audit_columns_cache = None
                    _audit_mode = "fallback"

            payload = {
                "dataset": dataset,
                "intent": intent,
                "sql_text": sql_text,
                "row_count": row_count,
                "duration_ms": duration_ms,
                "status": status,
                "error": error,
                "request_ip": request_ip,
                "model": model,
            }
            if usage:
                payload["prompt_tokens"] = usage.get("prompt_tokens")
                payload["completion_tokens"] = usage.get("completion_tokens")
                payload["total_tokens"] = usage.get("total_tokens")

            if _audit_mode == "query_logs":
                try:
                    _audit_try_insert(conn, "bot_audit.query_logs", payload, _audit_columns_cache)
                except Exception:
                    # Si falla por esquema, pasamos a fallback definitivo
                    _ensure_fallback_table(conn)
                    _audit_mode = "fallback"
                    _audit_try_insert(conn, "bot_audit.n2sql_logs", payload, None)
            else:
                _audit_try_insert(conn, "bot_audit.n2sql_logs", payload, None)
    except Exception:
        # Nunca romper el request por fallo de auditoría
        pass

# ======== Modelos API =========
class QueryIn(BaseModel):
    dataset: str
    intent: str
    params: Dict[str, Any] = {}

class QueryOut(BaseModel):
    ok: bool
    dataset: str
    schema: str
    sql: str
    rows: List[Dict[str, Any]]
    rowcount: int

# ======== Helpers N2SQL =========
_SELECT_ONLY_RE = re.compile(r"^\s*select\b", re.IGNORECASE | re.DOTALL)
_FORBIDDEN_RE = re.compile(
    r"\b(insert|update|delete|drop|alter|grant|revoke|truncate|create|comment|merge|call|execute)\b",
    re.IGNORECASE,
)

def _parse_sql_from_llm(text: str) -> str:
    # preferimos bloque ```sql ... ```
    m = re.search(r"```sql\s*(.*?)```", text, flags=re.IGNORECASE | re.DOTALL)
    if m:
        return m.group(1).strip().rstrip(";")
    # sino, primer statement que empiece con SELECT
    m2 = re.search(r"(?is)(select\b.*)", text)
    if m2:
        return m2.group(1).strip().rstrip(";")
    return text.strip().rstrip(";")

def _build_schema_hint(conn) -> str:
    # armamos un hint ligero con tablas/columnas más relevantes
    sql = """
    SELECT table_name, string_agg(column_name || ':' || data_type, ', ' ORDER BY ordinal_position) AS cols
    FROM information_schema.columns
    WHERE table_schema = %s
      AND table_name IN ('stg_account_move','stg_res_partner','stg_res_company','stg_res_currency')
    GROUP BY table_name
    ORDER BY table_name
    """
    rows = fetch_all_dict(conn, sql, [ODBC_DEFAULT_SCHEMA])
    lines = []
    for r in rows:
        lines.append(f"{ODBC_DEFAULT_SCHEMA}.{r['table_name']}({r['cols']})")
    return "\n".join(lines)

def _prompt_messages(schema_hint: str, intent: str) -> List[Dict[str, str]]:
    sys = (
        "Eres un generador de SQL para PostgreSQL. Devuelves SOLO una consulta SQL válida y segura.\n"
        f"Usa el esquema '{ODBC_DEFAULT_SCHEMA}'. Si no se especifica esquema, asume search_path='{ODBC_DEFAULT_SCHEMA}, public'.\n"
        "Responde usando un bloque de código con la consulta SQL. No expliques, no agregues texto extra.\n"
        "Prohibido: DML/DDL (INSERT/UPDATE/DELETE/DROP/ALTER/CREATE/etc), múltiples sentencias o CTEs peligrosos.\n"
        "Prefiere nombres exactos de columnas y tablas según el hint de esquema.\n"
        "Siempre incluye LIMIT razonable cuando aplique.\n\n"
        f"Esquema disponible:\n{schema_hint}\n"
    )
    user = f"Intento en lenguaje natural: {intent}\nGenera UNA SOLA consulta SELECT en SQL estándar para Postgres."
    return [
        {"role": "system", "content": sys},
        {"role": "user", "content": user},
    ]

# ======== Endpoints =========
@app.get("/health")
def health():
    return {"ok": True}

@app.get("/")
def root():
    return {"ok": True, "service": APP_NAME}

@app.get("/diag/audit")
def diag_audit(limit: int = Query(20, ge=1, le=200)):
    try:
        with db_connect() as conn:
            # preferimos la tabla solicitada
            exists = exec_sql_one(conn, """
                SELECT 1 AS ok
                FROM information_schema.tables
                WHERE table_schema='bot_audit' AND table_name='query_logs'
            """)
            if exists:
                rows = fetch_all_dict(conn, f"SELECT * FROM bot_audit.query_logs ORDER BY 1 DESC LIMIT {limit}")
                return {"ok": True, "table": "bot_audit.query_logs", "rows": rows}
            # fallback
            exists2 = exec_sql_one(conn, """
                SELECT 1 AS ok
                FROM information_schema.tables
                WHERE table_schema='bot_audit' AND table_name='n2sql_logs'
            """)
            if exists2:
                rows = fetch_all_dict(conn, f"SELECT * FROM bot_audit.n2sql_logs ORDER BY id DESC LIMIT {limit}")
                return {"ok": True, "table": "bot_audit.n2sql_logs", "rows": rows}
            return {"ok": True, "table": None, "rows": []}
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})

@app.post("/v1/query", response_model=QueryOut)
def v1_query(req: QueryIn, request: Request):
    t0 = time.perf_counter()
    status = "ok"
    error: Optional[str] = None
    llm_usage: Optional[Dict[str, Any]] = None
    model_used: Optional[str] = AZURE_DEPLOYMENT
    rows: List[Dict[str, Any]] = []
    sql_text = ""

    try:
        with db_connect() as conn:
            # hint de esquema
            hint = _build_schema_hint(conn)

            # llamada a Azure OpenAI (chat completions)
            messages = _prompt_messages(hint, req.intent)
            resp = client.chat.completions.create(
                model=AZURE_DEPLOYMENT,
                messages=messages,
                temperature=0.0,
                max_tokens=400,
            )
            llm_usage = getattr(resp, "usage", None)
            content = resp.choices[0].message.content or ""
            sql_text = _parse_sql_from_llm(content)

            # validaciones básicas
            if not _SELECT_ONLY_RE.match(sql_text):
                raise HTTPException(status_code=400, detail="Solo se permiten consultas SELECT")
            if _FORBIDDEN_RE.search(sql_text):
                raise HTTPException(status_code=400, detail="Se detectó palabra reservada no permitida")

            # Ejecutamos en modo lectura y search_path controlado
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(f"SET LOCAL search_path TO {ODBC_DEFAULT_SCHEMA}, public")
                cur.execute(sql_text)
                rows = list(cur.fetchall())

            rowcount = len(rows)
            out = {
                "ok": True,
                "dataset": req.dataset,
                "schema": ODBC_DEFAULT_SCHEMA,
                "sql": sql_text,
                "rows": rows,
                "rowcount": rowcount,
            }
            return out

    except HTTPException as he:
        status = "bad_request"
        error = he.detail
        raise
    except Exception as e:
        status = "error"
        error = str(e)
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        duration_ms = int((time.perf_counter() - t0) * 1000)
        try:
            ip = request.client.host if request and request.client else None
        except Exception:
            ip = None
        # auditoría (no rompe el flujo si falla)
        write_audit(
            dataset=req.dataset,
            intent=req.intent,
            sql_text=sql_text,
            row_count=len(rows),
            duration_ms=duration_ms,
            status=status,
            request_ip=ip,
            model=model_used,
            usage=(llm_usage.dict() if hasattr(llm_usage, "dict") else (llm_usage or None)),
            error=error,
        )
