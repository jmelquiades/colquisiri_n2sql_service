from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from handlers.validate_sql import validate_sql
from handlers.execute_sql import execute_sql
from handlers.sql_gen import partners_search, moves_expiring

app = FastAPI(title="CRITERIA DataTalk - N2SQL")

class QueryIn(BaseModel):
    dataset: str          # "partners" | "moves"
    intent: str           # "search"   | "expiring"
    params: dict = {}

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/v1/query")
def query(q: QueryIn):
    if q.dataset == "partners" and q.intent == "search":
        sql, args = partners_search(q.params)
    elif q.dataset == "moves" and q.intent == "expiring":
        sql, args = moves_expiring(q.params)
    else:
        raise HTTPException(400, "Dataset/intent no soportado")
    try:
        validate_sql(sql)
        cols, rows, meta = execute_sql(sql, args)
        return {"columns": cols, "rows": rows, "meta": meta}
    except Exception as e:
        raise HTTPException(400, f"Error: {e}")
