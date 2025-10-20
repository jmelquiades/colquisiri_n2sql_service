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

client = AzureOpenAI(
    api_key=AZURE_OPENAI_API_KEY,
    api_version=AZURE_OPENAI_API_VERSION,
    azure_endpoint=AZURE_OPENAI_ENDPOINT,
)

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
    return s + ";"  # terminador seguro

def build_prompt(intent: str, schema_spec: str) -> str:
    return PROMPT_TEMPLATE.format(intent=intent, schema_spec=schema_spec, max_rows=MAX_ROWS)

def generate_sql(intent: str, schema_spec: str) -> str:
    prompt = build_prompt(intent, schema_spec)
    resp = client.responses.create(
        model=AZURE_OPENAI_DEPLOYMENT_GPT,
        input=prompt,
        temperature=0.0,
        max_output_tokens=600,
    )
    text = resp.output_text.strip()
    # Extrae si viniera con fences por error
    if "```" in text:
        # toma el bloque de código más largo
        parts = re.findall(r"```(?:sql)?\s*([\s\S]*?)```", text, flags=re.IGNORECASE)
        if parts:
            text = max(parts, key=len).strip()
    return sanitize_sql(text)

