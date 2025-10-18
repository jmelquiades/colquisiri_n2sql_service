# CRITERIA DataTalk - n2sql-service
Servicio FastAPI que genera SQL parametrizado y seguro (solo SELECT) para datasets permitidos.

## Variables de entorno
- PG_DEST_DSN (obligatorio, usuario SOLO LECTURA)
- APP_TZ=America/Lima

## Arranque local
pip install -r requirements.txt
uvicorn src.app:app --reload

## Deploy en Render
- Subir repo
- Setear PG_DEST_DSN en Env Vars
- Deploy con render.yaml
