import re, yaml, pathlib

CAT_PATH = pathlib.Path(__file__).parent.parent / "schema" / "catalog.yaml"
CAT = yaml.safe_load(open(CAT_PATH, "r", encoding="utf-8"))
DATASETS = {k: v for k, v in CAT["datasets"].items()}
DENY = re.compile(r"\b(insert|update|delete|drop|truncate|alter|grant|revoke|create)\b", re.I)

def validate_sql(sql: str):
    s = sql.strip().lower()
    if not s.startswith("select"):
        raise ValueError("Solo SELECT permitido")
    if DENY.search(s):
        raise ValueError("Operación prohibida")
    if " limit " not in s:
        raise ValueError("Falta LIMIT")

    # Tabla del FROM
    m = re.search(r"\bfrom\s+([a-z0-9_\.]+)", s, re.I)
    if not m:
        raise ValueError("No se encontró FROM")
    table = m.group(1)
    if table not in DATASETS:
        raise ValueError(f"Tabla/vista no permitida: {table}")

    # No SELECT *
    head = re.search(r"select\s+(.*?)\s+from", s, re.S | re.I)
    if not head: 
        raise ValueError("No se pudo analizar las columnas")
    cols_raw = head.group(1)
    if "*" in cols_raw:
        raise ValueError("SELECT * no permitido")

    allowed = set(DATASETS[table]["columns"])
    cols = [c.strip().split(" as ")[0].split(".")[-1] for c in cols_raw.split(",")]
    for c in cols:
        if c and c not in allowed:
            raise ValueError(f"Columna no permitida: {c}")
