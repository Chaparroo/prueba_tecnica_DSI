# rpa-worker

Worker de automatización estilo REFramework: toma items de Redis Streams,
consulta una API externa y persiste el resultado en Postgres.

> README en construcción. La versión completa (instalación, despliegue,
> monitoreo, runbook y tabla de errores) se entrega en la fase 5.

## Arranque rápido (desarrollo)

```bash
uv sync
cp config/config.example.yaml config/config.yaml
cp .env.example .env
uv run rpa-worker check-config
```
