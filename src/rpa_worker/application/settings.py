"""Parámetros que la aplicación necesita, desacoplados del modelo pydantic."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ProcessingSettings:
    max_retry_number: int = 2
    max_attempts: int = 3
    backoff_base_seconds: float = 0.5
    backoff_max_seconds: float = 8.0
    max_retry_after_seconds: float = 10.0
    item_retry_base_seconds: float = 30.0
    max_retry_delay_seconds: float = 3_600.0
    # ready_at corto para items devueltos sin procesar (ShouldStop, Postgres caído).
    release_delay_seconds: float = 5.0
    max_deliveries: int = 5


@dataclass(frozen=True, slots=True)
class RunnerSettings:
    max_consecutive_system_exceptions: int = 3
    circuit_cooldown_seconds: float = 60.0
    # Cada cuánto late el loop mientras espera (cooldown, errores de cola).
    heartbeat_interval_seconds: float = 5.0
    # Espera tras un fallo al obtener trabajo (Redis caído, etc.).
    fetch_error_backoff_seconds: float = 2.0
    # Tope absoluto de la pausa de toma de items tras un 429 final.
    max_rate_limit_pause_seconds: float = 3_600.0
