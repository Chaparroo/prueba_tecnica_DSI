# rpa-worker

Worker de automatización en Python con el modelo del REFramework de UiPath.
Toma items de una cola en **Redis Streams**, consulta
`GET https://jsonplaceholder.typicode.com/posts/{id}` por cada uno y persiste
el resultado en **Postgres**. Distingue Business Exceptions (no se reintentan)
de System Exceptions (se reintentan con espera creciente, con frontera
explícita) y se apaga limpio con SIGTERM sin perder el item en vuelo.

Entregables del reto en este repositorio: el servicio (este README),
[REVISION.md](REVISION.md), [ACUERDO.md](ACUERDO.md) y [BITACORA.md](BITACORA.md).

## Contenido

- [Cómo funciona](#cómo-funciona)
- [Tabla de errores](#tabla-de-errores)
- [Instalación para desarrolladores](#instalación-para-desarrolladores)
- [Configuración](#configuración)
- [Despliegue](#despliegue)
- [Monitoreo](#monitoreo)
- [Qué hacer cuando falla a las 3 a.m.](#qué-hacer-cuando-falla-a-las-3-am)
- [Decisiones de diseño](#decisiones-de-diseño)
- [Pendientes y limitaciones](#pendientes-y-limitaciones)

## Cómo funciona

```
productor ──XADD──► rpa:queue:{high,normal,low}  (un stream por prioridad)
                         │ XREADGROUP (grupo rpa-workers)
                         ▼
                 ┌─────────────── worker ───────────────┐
                 │ Init → Get Transaction Data          │◄── ShouldStop
                 │      → Process Transaction           │    (SIGTERM o llave
                 │      → End Process                   │     control:should_stop)
                 └──────┬──────────────┬────────────────┘
          GET /posts/id │              │ INSERT ... ON CONFLICT (reference)
                        ▼              ▼
              jsonplaceholder      Postgres: transaction_results
                                       │ y recién entonces XACK
   reintento de item ─► rpa:retry (sorted set, score = ready_at)
   agotado / venenoso ─► rpa:dlq  (dead-letter stream)
```

Arquitectura hexagonal en `src/rpa_worker/`:

| Capa | Qué tiene | Regla |
|---|---|---|
| `domain/` | `Transaction`, `BusinessException`, `SystemException`, políticas de clasificación y backoff | Sin dependencias externas |
| `application/` | Runner REFramework, caso de uso de procesar un item, puertos, circuit breaker, ShouldStop, requeue, drain-dlq | No importa Redis, HTTP ni la BD (lo impide ruff con `TID251`) |
| `adapters/` | Redis Streams, cliente httpx, repositorio Postgres (psycopg 3), servidor de health, structlog/OpenTelemetry | Implementan los puertos |
| `bootstrap.py`, `cli.py` | Composition root y comandos | Únicos que conectan todo |

**Estados de una transacción:** `New → InProgress → Successful | Failed |
Retried | Abandoned`. Cada cambio se registra en el log
`transaction.status_changed` con `reference` y `correlation_id`.

**Garantías clave:**

- **Idempotencia.** La clave es `reference`. El resultado se escribe con
  `INSERT ... ON CONFLICT (reference)` y el `XACK` va *después* de que la
  escritura confirmó. Una doble entrega no produce dos filas y se acusa normal.
- **Apagado limpio.** SIGTERM/SIGINT solo activan una bandera. El runner la
  revisa *antes* de tomar el siguiente item; el item en vuelo termina su ciclo
  (persistir y luego XACK). Las esperas del backoff son interrumpibles. Si
  llega un SIGKILL, el item queda pendiente en el stream y otro arranque lo
  reclama con `XAUTOCLAIM`.
- **Correlación.** Quien encola genera `correlation_id` (y `traceparent` W3C).
  Viaja en el mensaje y aparece en todos los logs junto con `trace_id`,
  `span_id` y `reference`. Los campos con PII se redactan.

## Tabla de errores

La frontera está en [domain/policies.py](src/rpa_worker/domain/policies.py) y
la cubren [tests/unit/test_policies.py](tests/unit/test_policies.py) y
[tests/unit/test_runner.py](tests/unit/test_runner.py).

| Caso | Tipo | ¿Se reintenta? | Estado final | Motivo (`error_reason`) |
|---|---|---|---|---|
| HTTP 404 (p. ej. `/posts/101`) | Business | No | `Failed` | `not_found` |
| Payload sin `id` o `id <= 0` | Business | No | `Failed` | `invalid_payload` |
| `id` no numérico (`"abc"`, booleano) | Business | No | `Failed` | `non_numeric_id` |
| Timeout o error de conexión | System | Sí | `Retried` → `Abandoned` | `timeout` / `connection_error` |
| HTTP 5xx | System | Sí | `Retried` → `Abandoned` | `server_error` |
| HTTP 408 | System | Sí | `Retried` → `Abandoned` | `request_timeout` |
| HTTP 429 | System | Sí, respetando `Retry-After`; no abre el circuito | `Retried` → `Abandoned` | `rate_limited` |
| Fallo transitorio de Postgres | System | Sí: se libera el item sin tocar la BD ni consumir `MaxRetryNumber` | se reprocesa | — |
| HTTP 400 | System | **No**, alerta `contract` | `Abandoned` (DLQ) | `bad_request` |
| HTTP 401 / 403 | System | **No**, alerta `auth` | `Abandoned` (DLQ) | `unauthorized` / `forbidden` |
| Otros 4xx (409, 410, 422...) | System | **No**, alerta | `Abandoned` (DLQ) | `unexpected_status` |
| 2xx con cuerpo inesperado | System | **No**, alerta `contract` | `Abandoned` (DLQ) | `invalid_response` |
| Mensaje ilegible en la cola | — | **Nunca** | DLQ | `poison` |

**Dos capas de reintento:**

1. **Dentro del item:** hasta 3 intentos con backoff exponencial y *full
   jitter* (`http.backoff`).
2. **A nivel de item:** si se agotan, el item se reencola en `rpa:retry` con
   `ready_at` creciente, sin dormir al worker, hasta `MaxRetryNumber` (2).
   Después pasa a `Abandoned` y al DLQ.

**Circuit breaker (`MaxConsecutiveSystemExceptions = 3`).** Al alcanzarse, el
worker deja de tomar items, `/readyz` reporta `degraded`, espera
`circuit_cooldown_seconds` y prueba con un solo item. El proceso **no** termina.

## Instalación para desarrolladores

Requisitos: Python 3.12, [uv](https://docs.astral.sh/uv/) y Docker (para
integración, smoke y el stack local).

```bash
git clone <este-repo> && cd proyecto_rpa_tech_lead_dsi_factory
uv sync                                        # crea .venv con dependencias de desarrollo
cp config/config.example.yaml config/config.yaml
cp .env.example .env                           # cambia TODOS los valores
uv run pre-commit install                      # ruff, mypy y gitleaks antes de cada commit
uv run rpa-worker check-config                 # valida YAML + entorno y sale
```

Comandos del día a día (en Windows sin `make`, cada objetivo del
[Makefile](Makefile) es un comando que se puede correr a mano):

| Comando | Qué hace |
|---|---|
| `make lint` | `ruff format --check` y `ruff check` |
| `make typecheck` | `mypy` en modo strict |
| `make test-unit` | Pruebas unitarias con fakes en memoria (sin Docker) |
| `make test` | Unitarias + integración con testcontainers (Redis y Postgres reales) |
| `make smoke` | Levanta el compose real con una API falsa y prueba el flujo, SIGTERM y `stop` a mitad de item |
| `make rules` | Valida y prueba las reglas de alerta de Prometheus con `promtool` |
| `make check` | Todo lo anterior menos smoke |

Pruebas que piden el reto y dónde están:

- **Flujo principal:** `test_runner.py` y el escenario 2 del smoke (1-3 `Successful`).
- **Error de negocio:** 404 e id no numérico → `Failed` sin reintento.
- **Error técnico:** reintentos agotados → `Retried` y luego `Abandoned` al DLQ.
- **Idempotencia:** doble entrega → una sola fila y XACK normal.
- **SIGTERM con item en vuelo:** unitario y en el smoke con una señal real a
  un contenedor.

Para abrir un PR sigue la plantilla
[.github/PULL_REQUEST_TEMPLATE.md](.github/PULL_REQUEST_TEMPLATE.md) y el
[ACUERDO.md](ACUERDO.md) del equipo.

## Configuración

- **No secreta:** `config/config.yaml`, copiado de
  [config/config.example.yaml](config/config.example.yaml). Se valida con
  pydantic al arrancar; si algo no cuadra, el worker no inicia. También valida
  que los tiempos sean coherentes entre sí (por ejemplo, que
  `stop_grace_seconds` sea mayor que el peor caso de un item). El ejemplo
  explica cada llave.
- **Secretos:** solo variables de entorno (`.env`, ignorado por git; ver
  [.env.example](.env.example)): `REDIS_URL`, `POSTGRES_HOST`,
  `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`.
  No hay credenciales en el código y gitleaks corre en pre-commit y en CI.

Llaves que más se tocan:

| Llave | Valor | Para qué |
|---|---|---|
| `worker.max_consecutive_system_exceptions` | 3 | Umbral del circuit breaker |
| `worker.max_retry_number` | 2 | Reencolados antes del DLQ |
| `worker.stop_grace_seconds` | 90 | Tiempo para terminar el item en vuelo |
| `http.connect_timeout_seconds` / `read_timeout_seconds` | 3 / 10 | Todo HTTP tiene timeout |
| `http.backoff.max_attempts` | 3 | Intentos dentro del item |
| `queue.item_lease_ms` | 120000 | Cuándo un pendiente se considera huérfano |

## Despliegue

**Requisitos:** Docker con Compose v2. Redis **con AOF** (sin AOF, un reinicio
de Redis pierde la cola, los items en vuelo y los reintentos programados).
Redis Cluster no está soportado: `check-config` lo rechaza.

```bash
cp .env.example .env                             # secretos reales
cp config/config.example.yaml config/config.yaml
docker compose up -d --build --wait              # redis, postgres, migrate, worker
curl -s localhost:8080/readyz                    # {"status":"ok","state":"idle",...}
docker compose exec worker rpa-worker seed --ids 1-10,101,abc
```

Qué hace el compose ([docker-compose.yml](docker-compose.yml)):

1. `redis` (AOF, contraseña) y `postgres` (volumen y healthcheck con `pg_isready`).
2. `migrate` ejecuta `alembic upgrade head` y termina. Es un paso explícito:
   **el worker nunca migra**.
3. `worker` arranca solo cuando todo lo anterior está sano. Imagen
   multi-stage ([Dockerfile](Dockerfile)), usuario no root, sistema de
   archivos de solo lectura, `init: true` para que SIGTERM llegue a Python y
   `stop_grace_period: 120s` (mayor que el peor caso de un item, 69 s).

**Actualizar versión:**
`docker compose build && docker compose run --rm migrate && docker compose up -d worker`.
Docker manda SIGTERM, el worker termina el item en vuelo y sale con código 0.

**Más réplicas:** duplica el servicio `worker` con otro `CONSUMER_NAME` y
`hostname` (`worker-2`...). El nombre tiene que ser estable entre despliegues
y único por proceso.

**Parar sin perder nada:** `docker compose stop worker`, o
`docker compose exec worker rpa-worker stop --ttl 300` para una parada suave
de todos los workers vía Redis. Con `restart: unless-stopped` el worker
vuelve a arrancar e ignora la llave anterior a su arranque, así que no queda
en bucle.

## Monitoreo

### Health

| Endpoint | Responde 200 si | Uso |
|---|---|---|
| `GET /livez` | El loop del worker latió hace menos de 30 s (también en idle) | healthcheck de Docker / liveness |
| `GET /readyz` | Redis y Postgres responden. Devuelve `state`: `starting \| idle \| processing \| degraded \| stopping` | readiness / sondeo externo |

La API externa no cuenta para liveness: si jsonplaceholder se cae, el worker
sigue vivo y lo que se abre es el circuito.

### Stack de observabilidad (opcional)

```bash
docker compose -f docker-compose.yml -f docker-compose.observability.yml up -d --build --wait
```

Levanta Alloy (recolector OTLP y logs de Docker), Loki, Tempo, Prometheus,
blackbox-exporter y Grafana (`localhost:3000`, con el usuario y la contraseña
de `.env`). El dashboard **RPA Worker** viene aprovisionado: estado, circuito,
DLQ, item más viejo, throughput, duración p50/p95, reintentos y logs
filtrables por `correlation_id`.

**Seguir un mensaje de punta a punta** (Grafana → Explore → Loki):

```logql
{service="worker"} | json | correlation_id="<id>"
```

El `trace_id` del log abre la traza en Tempo.

**Métricas** (sin `reference` ni `correlation_id` como label):
`rpa_transactions_total{status,priority}`, `rpa_transaction_duration_seconds`,
`rpa_retries_total{layer,reason}`, `rpa_circuit_state` (0 cerrado, 1 medio
abierto, 2 abierto), `rpa_queue_depth`, `rpa_queue_oldest_item_age_seconds`,
`rpa_retry_scheduled`, `rpa_dlq_length`, `rpa_alerts_total{category}`,
`rpa_worker_up`.

### Alertas (por síntoma)

Definidas en
[observability/prometheus/rules/alerts.yml](observability/prometheus/rules/alerts.yml)
y probadas con `make rules`. Cada una enlaza su sección del runbook.

| Alerta | Se dispara cuando | Runbook |
|---|---|---|
| `RpaWorkerTelemetryAbsent` | No llegan métricas del worker en 5 min | [telemetría ausente](#runbook-telemetría-ausente) |
| `RpaWorkerNotReady` | `/readyz` falla 3 min | [worker no ready](#runbook-worker-no-ready) |
| `RpaQueueStalled` | El item más viejo lleva > 15 min, sostenido 10 min | [cola estancada](#runbook-cola-estancada) |
| `RpaCircuitOpen` | Circuito abierto 5 min | [circuito abierto](#runbook-circuito-abierto) |
| `RpaDlqNotEmpty` | DLQ > 0 durante 5 min | [DLQ](#runbook-dlq) |
| `RpaDlqAuth` / `Contract` / `Poison` / `MaxDeliveries` / `MaxAge` | Llegó al DLQ algo que exige a una persona | [DLQ](#runbook-dlq) |

## Qué hacer cuando falla a las 3 a.m.

### Triage en 5 pasos

1. **¿Está vivo y conectado?**
   `curl -s localhost:8080/readyz` y `docker compose ps`.
   - `degraded` → [circuito abierto](#runbook-circuito-abierto).
   - Falla `redis` o `postgres` → [worker no ready](#runbook-worker-no-ready).
2. **¿Qué dicen los logs?**
   `docker compose logs --since 30m worker | grep -E '"level": "(error|warning)"'`,
   o en Loki `{service="worker", level=~"error|warning"}`. Toma un
   `correlation_id` y sigue ese mensaje:
   `{service="worker"} | json | correlation_id="<id>"`.
3. **¿Cómo está la cola?**
   ```bash
   docker compose exec redis redis-cli XLEN rpa:queue:normal
   docker compose exec redis redis-cli XPENDING rpa:queue:normal rpa-workers
   docker compose exec redis redis-cli ZCARD rpa:retry
   docker compose exec worker rpa-worker drain-dlq        # dry-run: lista el DLQ
   ```
4. **¿Qué quedó en la base?**
   ```bash
   docker compose exec postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c \
     "SELECT status, error_reason, count(*) FROM transaction_results GROUP BY 1,2"'
   ```
5. **¿La API externa responde?**
   `curl -s -o /dev/null -w '%{http_code}\n' https://jsonplaceholder.typicode.com/posts/1`

### Runbook: telemetría ausente

Sin métricas, el resto de alertas no puede dispararse. Revisa `/readyz` por
fuera. Si el worker está bien, el problema es el pipeline:
`docker compose logs alloy prometheus`. Si el worker está caído,
`docker compose up -d worker` y vuelve al paso 1.

### Runbook: worker no ready

`/readyz` dice cuál dependencia falla.

- **Redis:** `docker compose logs redis`. Si se reinició, el AOF recupera la
  cola. Revisa que el volumen `redis-data` exista.
- **Postgres:** `docker compose logs postgres`. Mientras Postgres está caído,
  los items se liberan y vuelven a la cola **sin perderse ni consumir
  reintentos**. Cuando vuelve, el worker sigue solo.

### Runbook: cola estancada

Hay trabajo y no avanza. En orden:

1. ¿El worker corre? `docker compose ps worker`.
2. ¿Hay una parada suave activa?
   `docker compose exec redis redis-cli EXISTS control:should_stop`, o busca
   `stop_origin` en los logs. Si fue un error,
   `docker compose exec worker rpa-worker stop --clear`.
3. ¿Circuito abierto? Ve a [circuito abierto](#runbook-circuito-abierto).
4. ¿Muchos `rate_limited`? La API nos está frenando con 429. Es esperado: el
   worker respeta `Retry-After`. No reinicies en bucle.

### Runbook: circuito abierto

Se alcanzaron 3 System Exceptions seguidas. El worker sigue vivo y prueba un
item después de cada cooldown (60 s).

1. Mira el `error_reason` de los últimos fallos en los logs.
2. `server_error` o `timeout` → la API externa está mal. Espera; el circuito
   se cierra solo cuando un item de prueba sale bien.
3. `unauthorized` / `forbidden` → credenciales. Corrígelas y reinicia el worker.
4. Errores de Postgres → [worker no ready](#runbook-worker-no-ready).

### Runbook: DLQ

Nada en el DLQ se reprocesa solo. Primero entiende **por qué** llegó:

```bash
docker compose exec worker rpa-worker drain-dlq                         # lista y agrupa por motivo
docker compose exec worker rpa-worker drain-dlq --dlq-reason server_error
```

| Motivo | Qué hacer |
|---|---|
| `server_error`, `timeout`, `connection_error` | Si la API ya está bien, reencola |
| `unauthorized`, `forbidden` | Corrige credenciales primero, luego reencola |
| `bad_request`, `invalid_response`, `unexpected_status` | Cambió el contrato. **No reencoles** hasta entender el cambio |
| `poison` (ilegible) | Nunca se reencola. Corrige al productor |
| `max_deliveries` | Item venenoso que tumba al worker. Investiga `last_error` antes de usar `--include-poison` |
| `max_age` | Se liberó por más de 24 h. Posible bug disfrazado de error de conexión |

Reencolar (queda auditado en `requeue_audit` con quién y por qué):

```bash
# un item
docker compose exec worker rpa-worker requeue --reference <ref> \
  --operator "<tu nombre>" --reason "API recuperada tras incidente X"
# en lote, con tope explícito
docker compose exec worker rpa-worker drain-dlq --execute --dlq-reason server_error \
  --limit 100 --operator "<tu nombre>" --reason "API recuperada tras incidente X"
```

### Acciones seguras

- Reiniciar el worker (`docker compose restart worker`): termina el item en
  vuelo y los pendientes se reclaman al volver.
- `rpa-worker stop --ttl <s>` / `stop --clear`.
- `rpa-worker requeue --reference ...` y `drain-dlq` (dry-run por defecto).

### Qué NO hacer

- **No** `FLUSHALL` / `FLUSHDB` en Redis: borra la cola, los items en vuelo,
  los reintentos y el DLQ.
- **No** borres el stream ni el consumer group (`DEL rpa:queue:*`,
  `XGROUP DESTROY`): se pierden los pendientes.
- **No** borres ni edites filas de `transaction_results` a mano: rompes la
  idempotencia y la auditoría. Para reprocesar, usa `requeue`.
- **No** uses `docker kill` ni `docker compose down -v`: el primero corta el
  item a la mitad (se recupera, pero tarda el lease) y el segundo **borra los
  volúmenes**.
- **No** reencoles en masa sin leer el motivo del DLQ.

## Decisiones de diseño

- **Redis Streams en vez de RabbitMQ o una cola en memoria.** Una cola en
  memoria pierde todo con SIGKILL. Streams con consumer groups dan lo mismo
  que necesitamos de RabbitMQ (ack explícito, pendientes que se reclaman tras
  un crash), y con un sorted set los reintentos diferidos salen sin plugins.
  El costo es que Redis tiene que correr con AOF.
- **XACK después de persistir:** entrega *al menos una vez* más idempotencia
  por `reference` = un efecto por item.
- **Migraciones fuera del worker:** dos réplicas arrancando no compiten por
  migrar, y un fallo de migración no tumba al worker en bucle.
- **Versiones pineadas** de `opentelemetry-*==1.45.1`, `structlog==26.1.0` y
  `pytest==9.1.1` por requisito; el resto, acotadas con `uv.lock`.

## Pendientes y limitaciones

Lo que no alcancé y por qué:

- **No hay Alertmanager.** Las alertas se evalúan y se ven en Prometheus y
  Grafana, pero no notifican a nadie (Slack, correo, PagerDuty). El destino
  depende del equipo, y no quise inventar uno.
- **El seguimiento por `correlation_id` en Loki y Tempo no está
  automatizado.** El smoke prueba el flujo, SIGTERM y `stop` sobre el compose
  real, pero no levanta el stack de observabilidad.
- **Una réplica por defecto.** El escalado horizontal funciona (consumers con
  nombre estable), pero no lo probé con carga.
- **Sin Redis Cluster ni alta disponibilidad de Redis/Postgres:** quedan como
  responsabilidad de la infraestructura donde se despliegue.
- **No hay pruebas de carga.** No medí throughput máximo ni cuánto aguanta el
  rate limit real de la API.
