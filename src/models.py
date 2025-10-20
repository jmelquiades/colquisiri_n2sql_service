from pydantic import BaseModel, Field
from typing import Any, Dict, Optional

class QueryIn(BaseModel):
    dataset: str = Field(..., description="Nombre lógico de dataset; p.ej. 'odoo'")
    intent: str = Field(..., description="Instrucción en lenguaje natural")
    params: Optional[Dict[str, Any]] = Field(default_factory=dict)

class QueryOut(BaseModel):
    ok: bool
    dataset: str
    schema: str
    sql: str
    rows: list[dict]
    rowcount: int

