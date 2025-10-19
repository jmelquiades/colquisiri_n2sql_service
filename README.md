# Colquisiri N2SQL Service

Servicio REST para convertir **lenguaje natural → SQL** (Postgres) y **ejecutar** la consulta.

## Endpoints

- `GET /health` → estado del servicio
- `GET /diag/env` → diagnóstico (variables *sin* secretos)
- `POST /api/n2sql` → genera SQL y opcionalmente ejecuta
- `POST /api/exec` → ejecuta SQL directo (por defecto, solo SELECT)

## Variables de entorno

- **Base de datos**
  - `DATABASE_URL` = `postgres://user:pass@host:5432/dbname`

- **OpenAI (api.openai.com)**
  - `OPENAI_PROVIDER=openai`
  - `OPENAI_API_KEY`
  - `OPENAI_MODEL=gpt-4o-mini` (o el que prefieras)

- **Azure OpenAI**
  - `OPENAI_PROVIDER=azure`
  - `AZURE_OPENAI_ENDPOINT=https://<resource>.openai.azure.com`
  - `AZURE_OPENAI_API_KEY`
  - `AZURE_OPENAI_DEPLOYMENT=<deployment-name>`
  - `AZURE_OPENAI_API_VERSION=2024-08-01-preview`

## Probar

### Generar SQL sin ejecutar
```bash
curl -s -X POST $BASE/api/n2sql \
  -H 'Content-Type: application/json' \
  -d '{
    "question": "Total facturado por mes en 2024",
    "execute": false,
    "limit": 50
  }' | jq
