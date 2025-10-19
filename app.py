import os
import re
import json
import logging
from typing import Any, Dict, List, Optional, Tuple

import asyncio
import asyncpg
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

# ---- Logging ----
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger("n2sql")

# ---- OpenAI / Azure OpenAI (v1 client) ----
# Compatible con:
#   - OpenAI: OPENAI_API_KEY, OPENAI_MODEL (p.ej. gpt-4o-mini)
#   - Azure OpenAI: AZURE_OPENAI_API_KEY, AZURE_OPENAI_ENDPOINT, AZURE_OPENAI_DEPLOYMENT, AZURE_OPENAI_API_VERSION
OPENAI_PROVIDER = os.getenv("OPENAI_PROVIDER", "openai").lower().strip()

use_azure = OPENAI_PROVIDER == "azure" or bool(os.getenv("AZURE_OPENAI_ENDPOINT"))
if use_azure:
    # Azure OpenAI
    from openai import OpenAI
    AOAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")  # https://<resource>.openai.azure.com
    AOAI_KEY = os.getenv("AZURE_OPENAI_API_KEY")
    AOAI_DEPLOYMENT = os.getenv("AZURE_OPENAI_DEPLOYMENT")
    AOAI_API_VERSION = os.getenv("AZURE_OPENAI_API_VERSION", "2024-08-01-preview")
    if not (AOAI_ENDPOINT and AOAI_KEY and AOAI_DEPLOYMENT):
        logger.warning("Azure OpenAI: faltan variables (AZURE_OPENAI_ENDPOINT / KEY / DEPLOYMENT).")
    aoai_base_url = f"{AOAI_ENDPOINT}/openai/deployments/{AOAI_DEPLOYMENT}"
    client = OpenAI(
        api_key=AOAI_KEY,
        base_url=aoai_base_url,
        default_query={"api-version": AOAI_API_VERSION},
    )
    DEFAULT_MODEL = AOAI_DEPLOYMENT
else:
    # OpenAI
    from openai import OpenAI
    OAI_KEY = os.getenv("OPENAI_API_KEY")
    client = OpenAI(api_key=OAI_KEY)  # usa api.openai.com por defecto
    DEFAULT_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")


# ---- DB (Postgres) ----
# Espera: DATABASE_URL estilo:
#   postgres://user:pass@host:port/dbname
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    logger.warning("DATABASE_URL no establecido. /api/exec fallará hasta que lo configures.")

POOL_MIN = int(os.getenv("POOL_MIN", "1"))
POOL_MAX = int(os.getenv("POOL_MAX", "5"))

# ---- FastAPI ----
app = FastAPI(title="Colquisiri N2SQL Service", version="1.0.0")

_pool: Optional[asyncpg.Pool] = None


# ---------- Modelos ----------
class N2SQLRequest(BaseModel):
    question: str = Field(..., description="Pregunta en lenguaje natural")
    tables: Optional[List[str]] = Field(None, description="Lista de tablas a considerar (opcional)")
    execute: bool = Field(False, description="Si true, ejecuta el SQL. Si false, sólo lo genera.")
    limit: Optional[int] = Field(50, description="Límite de filas al ejecutar")
    allow_write: bool = Field(False, description="Permitir DML (INSERT/UPDATE/DELETE). Por defecto NO.")

class N2SQLResponse(BaseModel):
    sql: str
    rows: Optional[List[Dict[str, Any]]] = None
    rowcount: Optional[int] = None
    error: Optional[str] = None
    schema_snippet: Optional[str] = None

class ExecRequest(BaseModel):
    sql: str
    limit: Optional[int] = 100
    allow_write: bool = False


# ---------- Utilitarios ----------
SAFE_SELECT_ONLY = re.compile(r"^\s*select\b", re.IGNORECASE | re.DOTALL)
BLOCK_DANGEROUS = re.compile(r"\b(drop|truncate|alter)\b", re.IGNORECASE)

def sanitize_env_dump() -> Dict[str, str]:
    keys = [
        "DATABASE_URL",
        "OPENAI_PROVIDER",
        "OPENAI_MODEL",
        "AZURE_OPENAI_ENDPOINT",
        "AZURE_OPENAI_DEPLOYMENT",
        "AZURE_OPENAI_API_VERSION",
    ]
    out: Dict[str, str] = {}
    for k in keys:
        v = os.getenv(k)
        if not v:
            out[k] = "MISSING"
        else:
            if "KEY" in k or "PASSWORD" in k or "SECRET" in k:
                out[k] = "SET(***masked***)"
            elif k == "DATABASE_URL":
                out[k] = "SET(***masked***)"
            else:
                out[k] = v
    return out


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        if not DATABASE_URL:
            raise RuntimeError("DATABASE_URL no configurado.")
        _pool = await asyncpg.create_pool(
            dsn=DATABASE_URL,
            min_size=POOL_MIN,
            max_size=POOL_MAX,
            command_timeout=60,
        )
    return _pool


async def fetch_schema_snippet(tables: Optional[List[str]] = None) -> str:
    """Lee esquema de information_schema y crea un snippet simple para el prompt."""
    if not DATABASE_URL:
        return "/* DATABASE_URL MISSING */"
    pool = await get_pool()
    # Filtra por public para simplificar (ajusta si usas otros esquemas)
    base_sql = """
        SELECT table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema='public'
        ORDER BY table_name, ordinal_position
    """
    rows = []
    async with pool.acquire() as con:
        if tables:
            # Filtro por tablas solicitadas
            sql = base_sql.replace("ORDER BY", "AND table_name = ANY($1) ORDER BY")
            rows = await con.fetch(sql, tables)
        else:
            rows = await con.fetch(base_sql)

    # Arma un bloque tipo:
    # table: facturas
    #   - id (integer)
    #   - fecha (date)
    #   ...
    lines: List[str] = []
    current_tbl: Optional[str] = None
    for r in rows:
        t = r["table_name"]
        c = r["column_name"]
        d = r["data_type"]
        if t != current_tbl:
            lines.append(f"table: {t}")
            current_tbl = t
        lines.append(f"  - {c} ({d})")
    return "\n".join(lines) if lines else "/* no columns found */"


def build_prompt(question: str, schema_snippet: str, dialect: str = "postgresql", limit: Optional[int] = 50, allow_write: bool = False) -> str:
    limit_clause = f" LIMIT {limit}" if limit and limit > 0 else ""
    guard_text = (
        "Solo SELECT. Prohibe INSERT/UPDATE/DELETE/DROP/TRUNCATE/ALTER."
        if not allow_write else
        "Puedes generar DML si la pregunta lo requiere."
    )
    return f"""Eres un asistente que traduce español → SQL para {dialect}.
{guard_text}
Regresa SOLO el SQL, sin explicaciones. No uses markdown.

Esquema (public):
{schema_snippet}

Pregunta del usuario:
{question}

Si corresponde, agrega ORDER BY y un límite razonable.
Si el usuario pide contar, usa COUNT(*).
Si pide totales por periodo, usa GROUP BY.

Responde solo SQL válido y ejecutable. Aplica este límite por defecto si no hay uno específico: {limit_clause}
"""


async def call_llm_for_sql(prompt: str, model: Optional[str] = None) -> str:
    m = model or DEFAULT_MODEL
    logger.info("LLM model=%s | provider=%s", m, "azure" if use_azure else "openai")
    # Usamos Responses API para máxima compatibilidad en v1
    resp = client.responses.create(
        model=m,
        input=[
            {
                "role": "user",
                "content": prompt
            }
        ],
        temperature=0.1,
    )
    txt = resp.output_text  # concat de bloques
    return txt.strip()


def _is_select_only(sql: str) -> bool:
    return bool(SAFE_SELECT_ONLY.match(sql)) and not BLOCK_DANGEROUS.search(sql)


async def exec_sql(sql: str, limit: Optional[int]) -> Tuple[List[Dict[str, Any]], int]:
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL no configurado.")
    if limit and limit > 0 and re.search(r"\blimit\s+\d+\b", sql, re.IGNORECASE) is None:
        sql = f"{sql.rstrip().rstrip(';')} LIMIT {limit}"
    pool = await get_pool()
    async with pool.acquire() as con:
        rows = await con.fetch(sql)
    # Convierte Record → dict
    py_rows = [dict(r) for r in rows]
    return py_rows, len(py_rows)


# ---------- Rutas ----------
@app.get("/health")
async def health():
    logger.info("Health check solicitado.")
    return {"ok": True, "service": "n2sql", "status": "up"}

@app.get("/diag/env")
async def diag_env():
    return sanitize_env_dump()

@app.post("/api/n2sql", response_model=N2SQLResponse)
async def n2sql(req: N2SQLRequest):
    try:
        schema = await fetch_schema_snippet(req.tables)
        prompt = build_prompt(req.question, schema, "postgresql", req.limit, req.allow_write)
        sql = await call_llm_for_sql(prompt)

        if not req.allow_write and not _is_select_only(sql):
            return N2SQLResponse(sql=sql, error="Bloqueado: solo SELECT permitido.", schema_snippet=schema)

        if not req.execute:
            return N2SQLResponse(sql=sql, rows=None, rowcount=None, schema_snippet=schema)

        # Ejecuta
        rows, rc = await exec_sql(sql, req.limit)
        return N2SQLResponse(sql=sql, rows=rows, rowcount=rc, schema_snippet=schema)

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("n2sql error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/exec")
async def run_sql(req: ExecRequest):
    try:
        sql = req.sql.strip().rstrip(";")
        if not req.allow_write and not _is_select_only(sql):
            raise HTTPException(status_code=400, detail="Solo SELECT permitido. (Set allow_write=true para DML bajo tu riesgo).")
        rows, rc = await exec_sql(sql, req.limit)
        return {"rows": rows, "rowcount": rc}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("exec error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


# ---------- Lifespan ----------
@app.on_event("startup")
async def _startup():
    if DATABASE_URL:
        try:
            await get_pool()
            logger.info("DB pool inicializado.")
        except Exception as e:
            logger.warning("No se pudo crear pool aún: %s", e)

@app.on_event("shutdown")
async def _shutdown():
    global _pool
    if _pool:
        await _pool.close()
        _pool = None
        logger.info("DB pool cerrado.")
