# syntax=docker/dockerfile:1.7
#
# Imagen del worker. Dos etapas:
#   builder  resuelve dependencias con uv desde uv.lock (reproducible) en /app/.venv
#   runtime  solo el venv + migraciones; sin uv, sin compiladores, usuario no root
#
# CMD ejecuta el binario del venv directamente (no `uv run`): así SIGTERM llega
# al proceso Python. En compose se usa `init: true` para que un init mínimo
# (PID 1) reenvíe señales y recoja procesos huérfanos.

ARG PYTHON_IMAGE=python:3.12.13-slim-bookworm

FROM ghcr.io/astral-sh/uv:0.11.8 AS uv

# ---------------------------------------------------------------- builder
FROM ${PYTHON_IMAGE} AS builder
COPY --from=uv /uv /usr/local/bin/uv
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/app/.venv
WORKDIR /app

# Capa de dependencias: solo cambia si cambia uv.lock.
COPY pyproject.toml uv.lock README.md ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project

# Capa del proyecto (instalado como paquete, no editable).
COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable

# ---------------------------------------------------------------- runtime
FROM ${PYTHON_IMAGE} AS runtime
RUN groupadd --system --gid 10001 rpa \
 && useradd --system --uid 10001 --gid rpa --no-create-home --shell /usr/sbin/nologin rpa

WORKDIR /app
# El venv pertenece a root y es de solo lectura para el usuario del proceso.
COPY --from=builder /app/.venv /app/.venv
COPY alembic.ini ./
COPY migrations ./migrations

ENV PATH="/app/.venv/bin:${PATH}" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    RPA_CONFIG_PATH=/app/config/config.yaml

USER 10001:10001
EXPOSE 8080
CMD ["/app/.venv/bin/rpa-worker", "run"]
