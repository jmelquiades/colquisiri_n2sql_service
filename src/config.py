import os

def _env(name: str, default: str | None = None) -> str:
    v = os.getenv(name, default)
    if v is None or v == "":
        raise RuntimeError(f"Missing required env var: {name}")
    return v

# Azure OpenAI
AZURE_OPENAI_API_KEY = _env("AZURE_OPENAI_API_KEY")
AZURE_OPENAI_ENDPOINT = _env("AZURE_OPENAI_ENDPOINT").rstrip("/")
AZURE_OPENAI_API_VERSION = os.getenv("AZURE_OPENAI_API_VERSION", "2024-06-01")
AZURE_OPENAI_DEPLOYMENT_GPT = _env("AZURE_OPENAI_DEPLOYMENT_GPT")

# Postgres
PG_DEST_DSN = _env("PG_DEST_DSN")

# Dataset → esquema permitido (puedes ampliar)
DATASET_TO_SCHEMA = {
    "odoo": "odoo_replica",
}

# Seguridad
MAX_ROWS = int(os.getenv("MAX_ROWS", "200"))
STATEMENT_TIMEOUT_MS = int(os.getenv("STATEMENT_TIMEOUT_MS", "8000"))

