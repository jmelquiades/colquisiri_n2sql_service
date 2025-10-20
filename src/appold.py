import os
import re
import time
import json
import glob
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Request, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel

# Azure OpenAI (SDK v1.x)
from openai import AzureOpenAI

APP_NAME = "n2sql-service"
app = FastAPI(title=APP_NAME)

# ========= Config =========
# Modo sin BD (no tocar odoo_replica)
DISABLE_DB = os.getenv("DISABLE_DB", "true").lower() in ("1", "true", "yes")

# Si quisieras ejecutar SQL algún día, define DATABASE_URL y pon DISABLE_DB=false
DATABASE_URL = os.getenv("DATABASE_URL", "")

# Azure OpenAI
AZURE_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT", "")
AZURE_KEY = os.getenv("AZURE_OPENAI_API_KEY", "")
AZURE_API_VERSION = os.getenv("AZURE_OPENAI_API_VERSION", "2024-06-01")
AZURE_DEPLOYMENT = os.getenv("AZURE_OPENAI_DEPLOYMENT", "")  # p.ej. "gpt-4o-mini"

# Auditoría a archivos
LOG_DIR = os.getenv("LOG_DIR", "logs")
LOG_BASENAME = os.getenv("LOG_BASENAME", "audit.log")
LOG_MAX_MB = int(os.getenv("LOG_MAX_MB", "1"))                   # rota cada ~1MB
LOG_RETENTION_DAYS = int(os.getenv("LOG_RETENTION_DAYS", "60"))  # elimina > N días
os.makedirs(LOG_DIR, exist_ok=True)

_audit_logger = logging.getLogger("n2sql.audit")
_audit_logger.setLevel(logging.INFO)
_handler = RotatingFileHandler(
    filename=os.path.join(LOG_DIR, LOG_BASENAME),
    maxBytes=LOG_MAX_MB * 1024 * 1024,
    backupCount=10000,  # dejamos muchos; limpieza por antigüedad se encarga.
    encoding="utf-8",
)
_handler.setFormatter(logging.Formatter("%(message)s"))
_audit_logger.addHandler(_handler)
_last_cleanup_ts = 0.0

def _cleanup_old_logs() -> None:
    """Elimina archivos de log más antiguos que LOG_RETENTION_DAYS."""
    global _last_cleanup_ts
    now = time.time()
    # no más de 1 vez por hora
    if now - _last_cleanup_ts < 3600:
        return
    _last_cleanup_ts = now

    cutoff = now - LOG_RETENTION_DAYS * 86400
    pattern = os.path.join(LOG_DIR, f"{LOG_BASENAME}*")
    for path in glob.glob(pattern):
        try:
            if os.path.getmtime(path) < cutoff:
                os.remove(path)
        except Exception:
            pass  # nunca romper por limpieza

def _write_audit(payload: Dict[str, Any]) -> None:
    """Escribe una línea JSON al archivo y ejecuta limpieza periódica."""
    try:
        payload = dict(payload)
        payload["ts"] = datetime.utcnow().isoformat() + "Z"
        _audit_logger.info(json.dumps(payload, ensure_ascii=False))
        _cleanup_old_logs()
    except Exception:
        pass

# Cliente Azure OpenAI (se permite que falten credenciales para levantar /health)
client: Optional[AzureOpenAI] = None
if AZURE_ENDPOINT and AZURE_KEY and AZURE_DEPLOYMENT:
    client = AzureOpenAI(
        api_key=AZURE_KEY,
        api_version=AZURE_API_VERSION,
        azure_endpoint=AZURE_ENDPOINT,
    )

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

# SOLO SELECT; prohibimos DDL/DML
_SELECT_ONLY_RE = re.compile(r"^\s*select\b", re.IGNORECASE | re.DOTALL)
_FORBIDDEN_RE = re.compile(
    r"\b(insert|update|delete|drop|alter|grant|revoke|truncate|create|comment|merge|call|execute)\b",
    re.IGNORECASE,
)

def _parse_sql_from_llm(text: str) -> str:
    """Extrae una consulta SQL del contenido devuelto por el LLM."""
    m = re.search(r"```sql\s*(.*?)```", text, flags=re.IGNORECASE | re.DOTALL)
    if m:
        return m.group(1).strip().rstrip(";")
    m2 = re.search(r"(?is)(select\b.*)", text)
    if m2:
        return m2.group(1).strip().rstrip(";")
    return text.strip().rstrip(";")

# Hint de esquema (NO toca la BD; solo guía al modelo)
ODBC_DEFAULT_SCHEMA = "odoo_replica"
SCHEMA_HINT = "\n".join([
    f"{ODBC_DEFAULT_SCHEMA}.stg_account_move(id:bigint, name:text, move_type:text, state:text, payment_state:text, partner_id:bigint, invoice_date:date, invoice_date_due:date, amount_total:numeric, amount_residual:numeric, currency_id:bigint, company_id:bigint)",
    f"{ODBC_DEFAULT_SCHEMA}.stg_res_partner(id:bigint, display_name:text, vat:text, email:text, company_id:bigint)",
    f"{ODBC_DEFAULT_SCHEMA}.stg_res_company(id:bigint, name:text)",
    f"{ODBC_DEFAULT_SCHEMA}.stg_res_currency(id:bigint, name:text, symbol:text)",
])

def _prompt_messages(schema_hint: str, intent: str) -> List[Dict[str, str]]:
    sys = (
        "Eres un generador de SQL para PostgreSQL. Devuelves SOLO una consulta SQL válida y segura.\n"
        f"Usa el esquema '{ODBC_DEFAULT_SCHEMA}'. No des explicación; responde en un bloque ```sql ... ```.\n"
        "Prohibido: DML/DDL (INSERT/UPDATE/DELETE/DROP/ALTER/CREATE/etc), múltiples sentencias.\n"
        "Incluye LIMIT cuando aplique.\n\n"
        f"Esquema disponible (sólo guía, no ejecutes nada):\n{schema_hint}\n"
    )
    user = f"Intento en lenguaje natural: {intent}\nGenera UNA SOLA consulta SELECT en SQL estándar para Postgres."
    return [
        {"role": "system", "content": sys},
        {"role": "user", "content": user},
    ]

# ======== util lectura de logs (tail) ========
def _read_last_audit_lines(limit: int) -> List[Dict[str, Any]]:
    """Lee hasta 'limit' eventos JSON empezando por los archivos más recientes."""
    files = glob.glob(os.path.join(LOG_DIR, f"{LOG_BASENAME}*"))
    files.sort(key=lambda p: os.path.getmtime(p), reverse=True)  # más nuevos primero
    out: List[Dict[str, Any]] = []
    for path in files:
        try:
            # Máx 1MB por archivo => es seguro leer completo
            with open(path, "r", encoding="utf-8") as f:
                lines = f.readlines()
            # Recorremos desde el final
            for line in reversed(lines):
                line = line.strip()
                if not line:
                    continue
                try:
                    evt = json.loads(line)
                    out.append(evt)
                    if len(out) >= limit:
                        return out
                except Exception:
                    continue
        except Exception:
            continue
    return out

# ======== Endpoints =========
@app.get("/health")
def health():
    return {
        "ok": True,
        "service": APP_NAME,
        "db_mode": "disabled" if DISABLE_DB or not DATABASE_URL else "enabled",
        "azure_model_ready": bool(client),
        "log_dir": LOG_DIR,
        "log_max_mb": LOG_MAX_MB,
        "log_retention_days": LOG_RETENTION_DAYS,
    }

@app.get("/")
def root():
    return {"ok": True, "service": APP_NAME}

@app.get("/diag/audit")
def diag_audit(limit: int = Query(100, ge=1, le=2000)):
    try:
        rows = _read_last_audit_lines(limit)
        return {"ok": True, "count": len(rows), "rows": rows}
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})

@app.post("/v1/query", response_model=QueryOut)
def v1_query(req: QueryIn, request: Request):
    if not client:
        raise HTTPException(status_code=500, detail="Azure OpenAI no configurado (endpoint/key/deployment)")

    t0 = time.perf_counter()
    status = "ok"
    error: Optional[str] = None
    usage: Optional[Dict[str, Any]] = None
    rows: List[Dict[str, Any]] = []
    sql_text = ""

    try:
        # 1) Pedimos al LLM
        messages = _prompt_messages(SCHEMA_HINT, req.intent)
        resp = client.chat.completions.create(
            model=AZURE_DEPLOYMENT,
            messages=messages,
            temperature=0.0,
            max_tokens=400,
        )
        usage_obj = getattr(resp, "usage", None)
        if usage_obj:
            # resp.usage es un objeto con attrs; lo hacemos dict simple
            usage = {
                "prompt_tokens": getattr(usage_obj, "prompt_tokens", None),
                "completion_tokens": getattr(usage_obj, "completion_tokens", None),
                "total_tokens": getattr(usage_obj, "total_tokens", None),
            }
        content = resp.choices[0].message.content or ""
        sql_text = _parse_sql_from_llm(content)

        # 2) Validaciones básicas
        if not _SELECT_ONLY_RE.match(sql_text):
            raise HTTPException(status_code=400, detail="Solo se permiten consultas SELECT")
        if _FORBIDDEN_RE.search(sql_text):
            raise HTTPException(status_code=400, detail="Se detectó palabra reservada no permitida")

        # 3) En modo sin BD devolvemos solo el SQL (sin ejecutar)
        if DISABLE_DB or not DATABASE_URL:
            out = {
                "ok": True,
                "dataset": req.dataset,
                "schema": ODBC_DEFAULT_SCHEMA,
                "sql": sql_text,
                "rows": rows,      # vacío en modo sin BD
                "rowcount": 0,
            }
            return out

        # (Opcional) Si habilitas BD en el futuro, aquí iría la ejecución segura del SELECT.
        # left intentionally blank (no tocar BD por pedido del usuario)

        out = {
            "ok": True,
            "dataset": req.dataset,
            "schema": ODBC_DEFAULT_SCHEMA,
            "sql": sql_text,
            "rows": rows,
            "rowcount": len(rows),
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
        elapsed_ms = int((time.perf_counter() - t0) * 1000)
        # auditoría a archivo
        ip = None
        try:
            ip = request.client.host if request and request.client else None
        except Exception:
            pass
        _write_audit({
            "dataset": req.dataset,
            "intent": req.intent,
            "sql_text": sql_text,
            "row_count": len(rows),
            "duration_ms": elapsed_ms,
            "status": status,
            "error": error,
            "request_ip": ip,
            "model": AZURE_DEPLOYMENT,
            **(usage or {}),
        })
