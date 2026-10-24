"""Carga y validación de configuración (falla rápido al arrancar).

Dos fuentes con responsabilidades separadas:

* ``config/config.yaml``: parámetros operativos (sin secretos). Se valida con
  pydantic y ``extra="forbid"`` para que un typo en una llave sea un error, no
  un valor por defecto silencioso.
* Variables de entorno: credenciales y endpoints con credenciales embebidas
  (``REDIS_URL``, ``POSTGRES_*``). Nunca viven en el YAML.

Nombres REFramework -> llaves YAML:
    MaxConsecutiveSystemExceptions -> worker.max_consecutive_system_exceptions
    MaxRetryNumber                 -> worker.max_retry_number
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path
from typing import Annotated, Literal, Self
from urllib.parse import urlsplit, urlunsplit

import yaml
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    HttpUrl,
    SecretStr,
    ValidationError,
    model_validator,
)

DEFAULT_CONFIG_PATH = Path("config/config.yaml")
CONFIG_PATH_ENV = "RPA_CONFIG_PATH"

PositiveFloat = Annotated[float, Field(gt=0)]
PositiveInt = Annotated[int, Field(gt=0)]


class ConfigError(Exception):
    """Configuración inválida o incompleta. El proceso no debe arrancar."""


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


# --------------------------------------------------------------------- YAML


class BackoffSettings(_Strict):
    """Reintento dentro del item (capa 1): backoff exponencial con jitter."""

    max_attempts: Annotated[int, Field(ge=1, le=10)] = 3
    base_seconds: PositiveFloat = 0.5
    max_seconds: PositiveFloat = 8.0

    @model_validator(mode="after")
    def _base_le_max(self) -> Self:
        if self.base_seconds > self.max_seconds:
            raise ValueError("backoff.base_seconds no puede ser mayor que backoff.max_seconds")
        return self


class HttpSettings(_Strict):
    base_url: HttpUrl
    connect_timeout_seconds: PositiveFloat = 3.0
    read_timeout_seconds: PositiveFloat = 10.0
    # Tope de espera dentro del item. Un Retry-After mayor no se espera en
    # caliente: el item se reencola en el sorted set con ready_at = ahora + valor.
    max_retry_after_seconds: PositiveFloat = 10.0
    user_agent: str = "rpa-worker/0.1"
    backoff: BackoffSettings = BackoffSettings()


class QueueSettings(_Strict):
    stream_prefix: str = "rpa:queue"
    # Orden = prioridad de consulta (la primera se atiende antes).
    priorities: Annotated[list[str], Field(min_length=1)] = ["high", "normal", "low"]
    consumer_group: str = "rpa-workers"
    block_ms: Annotated[int, Field(ge=100, le=60_000)] = 2_000
    retry_zset_key: str = "rpa:retry"
    dlq_stream: str = "rpa:dlq"
    should_stop_key: str = "control:should_stop"
    # Tiempo mínimo que un mensaje debe estar pendiente para que XAUTOCLAIM lo
    # reclame. Debe superar el peor caso de un item, o dos workers lo procesarán.
    item_lease_ms: PositiveInt = 120_000
    # XAUTOCLAIM reclama de a UN item (COUNT 1), solo cuando el worker va a
    # procesarlo; entre revisiones sin hallazgos espera este intervalo.
    claim_interval_ms: Annotated[int, Field(ge=100)] = 15_000

    @model_validator(mode="after")
    def _unique_priorities(self) -> Self:
        if len(set(self.priorities)) != len(self.priorities):
            raise ValueError("queue.priorities contiene valores repetidos")
        return self

    def stream_for(self, priority: str) -> str:
        return f"{self.stream_prefix}:{priority}"


class WorkerSettings(_Strict):
    max_consecutive_system_exceptions: PositiveInt = 3
    max_retry_number: Annotated[int, Field(ge=0)] = 2
    circuit_cooldown_seconds: PositiveFloat = 60.0
    # Presupuesto para terminar el item en vuelo tras ShouldStop. Ver
    # AppConfig.worst_case_item_seconds: 90 s = 69 s de peor caso + ~30 % de
    # margen. El stop_grace_period de Docker (120 s) suma 30 s para End Process.
    stop_grace_seconds: PositiveFloat = 90.0
    # Reintento a nivel de item (capa 2): ready_at = ahora + base * 2^(n-1).
    item_retry_base_seconds: PositiveFloat = 30.0
    # Tope absoluto de cualquier ready_at (backoff de item o Retry-After largo).
    max_retry_delay_seconds: PositiveFloat = 3_600.0
    # ready_at corto para items devueltos sin procesar (ShouldStop durante un
    # backoff, fallo transitorio de Postgres). No consume MaxRetryNumber.
    release_delay_seconds: PositiveFloat = 5.0
    # Guarda contra mensajes venenosos: entregas (XAUTOCLAIM incluidas) tras las
    # cuales el item va al DLQ aunque Postgres no permita registrar el estado.
    max_deliveries: Annotated[int, Field(ge=2)] = 5
    # None -> se usa HOSTNAME (id del contenedor), único por réplica.
    consumer_name: str | None = None

    @model_validator(mode="after")
    def _retry_delays(self) -> Self:
        if self.item_retry_base_seconds > self.max_retry_delay_seconds:
            raise ValueError(
                "worker.item_retry_base_seconds no puede superar worker.max_retry_delay_seconds"
            )
        return self


class PostgresSettings(_Strict):
    pool_min_size: Annotated[int, Field(ge=1)] = 1
    pool_max_size: Annotated[int, Field(ge=1)] = 4
    connect_timeout_seconds: PositiveFloat = 5.0
    statement_timeout_ms: PositiveInt = 5_000

    @model_validator(mode="after")
    def _pool_bounds(self) -> Self:
        if self.pool_min_size > self.pool_max_size:
            raise ValueError("postgres.pool_min_size no puede superar pool_max_size")
        return self


class HealthSettings(_Strict):
    host: str = "0.0.0.0"  # noqa: S104 - dentro del contenedor; el puerto se expone explícitamente
    port: Annotated[int, Field(ge=1, le=65_535)] = 8080
    # /livez falla si el loop no late en este tiempo.
    liveness_max_age_seconds: PositiveFloat = 30.0


class ObservabilitySettings(_Strict):
    service_name: str = "rpa-worker"
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    log_format: Literal["json", "console"] = "json"
    # None -> trazas/métricas deshabilitadas (no-op). El estándar OTEL_* de
    # entorno tiene precedencia si está definido.
    otlp_endpoint: HttpUrl | None = None
    metrics_export_interval_seconds: PositiveFloat = 15.0
    # Llaves cuyo valor se redacta en logs (comparación sin mayúsculas).
    pii_fields: list[str] = ["email", "name", "username", "phone", "address", "password", "token"]


class AppConfig(_Strict):
    worker: WorkerSettings = WorkerSettings()
    queue: QueueSettings = QueueSettings()
    http: HttpSettings
    postgres: PostgresSettings = PostgresSettings()
    health: HealthSettings = HealthSettings()
    observability: ObservabilitySettings = ObservabilitySettings()

    def _http_call_seconds(self) -> float:
        return self.http.connect_timeout_seconds + self.http.read_timeout_seconds

    def _wait_seconds(self) -> float:
        return max(self.http.backoff.max_seconds, self.http.max_retry_after_seconds)

    def _db_seconds(self) -> float:
        return self.postgres.connect_timeout_seconds + self.postgres.statement_timeout_ms / 1000

    def worst_case_item_seconds(self) -> float:
        """Cota superior del tiempo que puede tardar un item en vuelo.

        max_attempts * (connect + read)                 HTTP con timeout acotado
        + (max_attempts - 1) * max(backoff, RetryAfter)  esperas entre intentos
        + (connect + statement_timeout) de Postgres      escritura del resultado

        Con los valores por defecto: 3*(3+10) + 2*10 + (5+5) = 69 s.
        """
        attempts = self.http.backoff.max_attempts
        return (
            attempts * self._http_call_seconds()
            + (attempts - 1) * self._wait_seconds()
            + self._db_seconds()
        )

    @model_validator(mode="after")
    def _liveness_budget(self) -> Self:
        # El loop late entre operaciones; ninguna operación bloqueante aislada
        # puede durar más que la ventana de /livez o el worker parecería colgado.
        longest = max(
            self._http_call_seconds(),
            self._wait_seconds(),
            self._db_seconds(),
            self.queue.block_ms / 1000,
        )
        if self.health.liveness_max_age_seconds <= longest:
            raise ValueError(
                f"health.liveness_max_age_seconds ({self.health.liveness_max_age_seconds}s) "
                f"debe superar la operación bloqueante más larga ({longest:.1f}s)"
            )
        return self

    @model_validator(mode="after")
    def _shutdown_budget(self) -> Self:
        worst = self.worst_case_item_seconds()
        if self.worker.stop_grace_seconds <= worst:
            raise ValueError(
                f"worker.stop_grace_seconds ({self.worker.stop_grace_seconds}s) debe superar "
                f"el peor caso de un item ({worst:.1f}s); si no, un SIGTERM puede cortar un "
                "item a la mitad"
            )
        if self.queue.item_lease_ms / 1000 <= worst:
            raise ValueError(
                f"queue.item_lease_ms ({self.queue.item_lease_ms}ms) debe superar el peor caso "
                f"de un item ({worst:.1f}s); si no, XAUTOCLAIM robará items aún en vuelo"
            )
        return self


# ---------------------------------------------------------------- Entorno


class PostgresCredentials(_Strict):
    host: str
    port: Annotated[int, Field(ge=1, le=65_535)] = 5432
    dbname: str
    user: str
    password: SecretStr

    def conninfo(self) -> str:
        """Cadena libpq. No loguear: contiene el password."""
        pw = self.password.get_secret_value().replace("\\", "\\\\").replace("'", "\\'")
        return (
            f"host={self.host} port={self.port} dbname={self.dbname} "
            f"user={self.user} password='{pw}'"
        )


class Secrets(_Strict):
    redis_url: SecretStr
    postgres: PostgresCredentials

    def redis_url_for_logs(self) -> str:
        """URL de Redis apta para logs: usuario y password sustituidos por ***."""
        parts = urlsplit(self.redis_url.get_secret_value())
        if parts.username is None and parts.password is None:
            return parts.geturl()
        host = parts.hostname or ""
        netloc = f"***@{host}" + (f":{parts.port}" if parts.port else "")
        return urlunsplit(parts._replace(netloc=netloc))


class Settings(_Strict):
    config: AppConfig
    secrets: Secrets


def _secrets_from_env(environ: Mapping[str, str]) -> Secrets:
    required = ["REDIS_URL", "POSTGRES_HOST", "POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD"]
    missing = [k for k in required if not environ.get(k)]
    if missing:
        raise ConfigError(f"Faltan variables de entorno obligatorias: {', '.join(missing)}")
    return Secrets.model_validate(
        {
            "redis_url": environ["REDIS_URL"],
            "postgres": {
                "host": environ["POSTGRES_HOST"],
                "port": environ.get("POSTGRES_PORT", "5432"),
                "dbname": environ["POSTGRES_DB"],
                "user": environ["POSTGRES_USER"],
                "password": environ["POSTGRES_PASSWORD"],
            },
        }
    )


def load_config(path: Path) -> AppConfig:
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ConfigError(
            f"No existe {path}. Copia config/config.example.yaml a {path} y ajústalo."
        ) from exc
    except yaml.YAMLError as exc:
        raise ConfigError(f"{path} no es YAML válido: {exc}") from exc
    if not isinstance(raw, dict):
        raise ConfigError(f"{path} debe contener un mapeo en la raíz")
    try:
        return AppConfig.model_validate(raw)
    except ValidationError as exc:
        raise ConfigError(f"Configuración inválida en {path}:\n{exc}") from exc


def load_settings(path: Path | None = None, environ: Mapping[str, str] | None = None) -> Settings:
    """Punto único de carga. Lanza ConfigError con un mensaje accionable."""
    env = os.environ if environ is None else environ
    cfg_path = path or Path(env.get(CONFIG_PATH_ENV, str(DEFAULT_CONFIG_PATH)))
    config = load_config(cfg_path)
    try:
        secrets = _secrets_from_env(env)
    except ValidationError as exc:
        raise ConfigError(f"Variables de entorno inválidas:\n{exc}") from exc
    return Settings(config=config, secrets=secrets)
