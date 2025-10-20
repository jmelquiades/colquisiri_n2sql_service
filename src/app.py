import logging, time
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from .config import DATASET_TO_SCHEMA
from .db import run_query, schema_signature
from .models import QueryIn, QueryOut
from .n2sql import generate_sql

log = logging.getLogger("n2sql")
logging.basicConfig(level=logging.INFO)

app = FastAPI(title="CRITERIA DataTalk - N2SQL", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=False,
    allow_methods=["*"], allow_headers=["*"],
)

@app.get("/health")
def health():
    log.info("Health check solicitado.")
    return {"ok": True}

@app.post("/v1/query", response_model=QueryOut)
def query(q: QueryIn):
    # dataset → schema
    schema = DATASET_TO_SCHEMA.get(q.dataset.lower())
    if not schema:
        raise HTTPException(400, detail="Dataset no soportado. Usa: 'odoo'.")

    # introspección del esquema para guiar al modelo
    spec = schema_signature(schema)

    try:
        t0 = time.perf_counter()
        sql = generate_sql(q.intent, spec)
        rows, count = run_query(sql)
        took = round((time.perf_counter() - t0) * 1000)
        log.info("SQL OK (%sms): %s", took, sql)
        return QueryOut(
            ok=True, dataset=q.dataset, schema=schema, sql=sql, rows=rows, rowcount=count
        )
    except HTTPException:
        raise
    except Exception as e:
        log.exception("NLU/SQL error: %s", e)
        raise HTTPException(500, detail=str(e))
