import re
from openai import AzureOpenAI
from .config import (
    AZURE_OPENAI_API_KEY,
    AZURE_OPENAI_ENDPOINT,
    AZURE_OPENAI_API_VERSION,
    AZURE_OPENAI_DEPLOYMENT_GPT,
    MAX_ROWS,
)
from .prompts import PROMPT_TEMPLATE

# Cliente Azure OpenAI (chat.completions)
client = AzureOpenAI(
    api_key=AZURE_OPENAI_API_KEY,
    api_version=AZURE_OPENAI_API_VERSION,
    azure_endpoint=AZURE_OPENAI_ENDPOINT,
)

# Bloqueos de seguridad
_BLOCKED = re.compile(
    r"\b(INSERT|UPDATE|DELETE|TRUNCATE|ALTER|DROP|CREATE|GRANT|REVOKE|COPY|CALL|DO|SET|SHOW)\b",
    re.IGNORECASE,
)

def sanitize_sql(sql: str) -> str:
    s = sql.strip().rstrip(";")
    if not re.match(r"(?is)^\s*select\b", s):
        raise ValueError("Solo se permiten consultas SELECT")
    if _BLOCKED.search(s):
        raise ValueError("Se detectó una instrucción no permitida")
    return s + ";"

def build_prompt(intent: str, schema_spec: str) -> str:
    return PROMPT_TEMPLATE.format(intent=intent, schema_spec=schema_spec, max_rows=MAX_ROWS)

def _extract_sql(text: str) -> str:
    text = text.strip()
    if "```" in text:
        parts = re.findall(r"```(?:sql)?\s*([\s\S]*?)```", text, flags=re.IGNORECASE)
        if parts:
            text = max(parts, key=len).strip()
    return text

def generate_sql(intent: str, schema_spec: str) -> str:
    prompt = build_prompt(intent, schema_spec)
    resp = client.chat.completions.create(
        model=AZURE_OPENAI_DEPLOYMENT_GPT,
        temperature=0.0,
        max_tokens=600,
        messages=[
            {"role": "system", "content": "Eres un generador de SQL para PostgreSQL."},
            {"role": "user", "content": prompt},
        ],
    )
    text = resp.choices[0].message.content or ""
    text = _extract_sql(text)
    return sanitize_sql(text)
