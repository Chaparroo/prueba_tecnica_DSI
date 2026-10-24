# Atajos para desarrollo. Cada objetivo es un comando uv/docker que también
# puede ejecutarse a mano (p. ej. en Windows sin make).

COMPOSE := docker compose
OBS := $(COMPOSE) -f docker-compose.yml -f docker-compose.observability.yml
PROMTOOL := docker run --rm --entrypoint promtool -v "$(CURDIR)/observability/prometheus:/etc/prometheus:ro" prom/prometheus:v3.15.1

.PHONY: install lint typecheck test test-unit check smoke up up-obs down rules

install:
	uv sync

lint:
	uv run ruff format --check .
	uv run ruff check .

typecheck:
	uv run mypy

test-unit:
	uv run pytest tests/unit

test:  ## unitarias + integración (requiere Docker)
	uv run pytest

check: lint typecheck test rules

smoke:  ## compose real + API falsa + SIGTERM a mitad de item
	uv run python scripts/smoke.py

rules:  ## valida y prueba las reglas de alerta de Prometheus
	$(PROMTOOL) check rules /etc/prometheus/rules/alerts.yml
	$(PROMTOOL) test rules /etc/prometheus/tests/alerts_test.yml

up:
	$(COMPOSE) up -d --build --wait

up-obs:
	$(OBS) up -d --build --wait

down:
	$(OBS) down
