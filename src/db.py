from typing import Any, List, Dict, Tuple
import psycopg
from psycopg.rows import dict_row
from .config import PG_DEST_DSN, STATEMENT_TIMEOUT_MS

def run_query(sql: str) -> Tuple[List[Dict[str, Any]], int]:
    # Conexión por llamada; usuario de solo lectura recomendado
    # Forzamos statement_timeout para evitar consultas largas
    with psycopg.connect(PG_DEST_DSN, row_factory=dict_row, options=f"-c statement_timeout={STATEMENT_TIMEOUT_MS}") as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            # Solo SELECT: fetch
            try:
                rows = cur.fetchall()
                return rows, len(rows)
            except psycopg.ProgrammingError:
                # Si no hay result set (no debería ocurrir si validamos SELECT)
                return [], cur.rowcount

def schema_signature(schema: str) -> str:
    """Devuelve un resumen tipo:
       stg_account_move(id bigint, name text, amount_total numeric, ...)
    """
    q = """
    SELECT c.table_name,
           string_agg(c.column_name || ' ' || c.data_type, ', ' ORDER BY c.ordinal_position) AS cols
    FROM information_schema.columns c
    JOIN information_schema.tables t
      ON t.table_schema = c.table_schema AND t.table_name = c.table_name
    WHERE c.table_schema = %s AND t.table_type='BASE TABLE'
    GROUP BY c.table_name
    ORDER BY c.table_name;
    """
    out = []
    with psycopg.connect(PG_DEST_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(q, (schema,))
            for tbl, cols in cur.fetchall():
                out.append(f"{schema}.{tbl}({cols})")
    return "\n".join(out)

