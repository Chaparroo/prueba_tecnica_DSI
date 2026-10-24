# Bitácora

## 1. Qué herramienta de IA usé y para qué

La herramienta que usé fue Claude. La usé para la creación del proyecto y para mejorar la ortografía y la organización de ideas de los archivos .md. También para resolver dudas, como qué es RabbitMQ, entre otras.

## 2. Prompts que de verdad me sirvieron

### Mi bosquejo inicial

Este es el bosquejo con el que empecé:

```text
Hola chat como vas?
me vas a ayudar a iniciar un proyecto de automatizacion por favor 
con los siguientes criterios

-arquitectura hexagonal
-redis como cola de items
-grafana autohospedado  (opentelemetry-api==1.45.1 opentelemetry-sdk==1.45.1(obligatorio por temas de seguridad),structlog==26.1.0) para temas de logs y trazas  (disponible para metricas tambien ) 
-dockerizacion del proyecto
- versión de Python 3.12 con ruff  mypy y la guía oficial de empaquetado
- test con pytest==9.1.1
- en CI vamos a usar gitleaks on el .pre-commit-config.yaml para el desarrollador  con ruff y mypy  tambien agrega PULL_REQUEST_TEMPLATE.md
- la api con la que vamos a trabajar es esta   https://jsonplaceholder.typicode.com
- nuestro config va ir en un folder config por medio de un archivo yaml
-readme obligatorio (descripcion del proyecto,instalcion en mi equipo como parte del equipo de desarrolladores,)
-healcheck de modo activo o esperando items


a continuacion te comentare un poco como vamos a trabajar este proyeto  nos vamos a basar mucho en el reframework de uipath  principalmente por que nos permite trabajar con items y transacciones  junto con bussiines exceptions y systems exceptions en el config vamos a poner MaxConsecutiveSystemExceptions (setiado por defecto en 3 ) MaxRetryNumber(2) y tambien nos va permitir el uso de should stop como bandera antes de tocar el siguiente item  tambien nos permite el manejo de reference,status,priority
```

### El prompt real

A partir del bosquejo armé este, que es el verdadero prompt con el que se construyó el proyecto:

```markdown
# Rol
Actúa como ingeniero de software senior en Python, con experiencia en
arquitectura hexagonal, sistemas basados en colas y operación en producción.
Ayúdame a construir el proyecto por fases, explicando las decisiones
importantes y señalando riesgos antes de escribir código.

# Objetivo
Un worker de automatización en Python que toma items de una cola en Redis,
consulta una API externa por cada uno, persiste el resultado en Postgres y
se puede operar y diagnosticar en producción. El modelo de trabajo replica
el REFramework de UiPath: transacciones, Business Exceptions vs System
Exceptions, y ShouldStop como bandera de parada revisada antes de tomar el
siguiente item.

# Contexto
- API externa: https://jsonplaceholder.typicode.com (`GET /posts/{id}`).
  Nota: `/posts/101` devuelve 404, úsalo como caso de negocio real.
- Persistencia: Postgres, dockerizado.
- Equipo de desarrolladores; el README debe servir para incorporarse.

# Ubicación del proyecto
- Crea el proyecto en el directorio de trabajo actual, sin crear una
  carpeta contenedora adicional. Los archivos van directamente en la raíz
  (`pyproject.toml`, `src/`, `config/`, `tests/`, etc.).
- Antes de crear nada, revisa qué hay en el directorio. Si ya contiene
  archivos, dime cuáles y pregúntame antes de sobrescribir o mezclar.
- Inicializa git si no existe un repositorio, con un `.gitignore` adecuado
  para Python (incluye `.venv/`, `config/config.yaml`, `.env`).
- Puedes usar UV(instalado en maquina) o pip 

# Restricciones técnicas (obligatorias)

- Python 3.12, `pyproject.toml` (PEP 621), layout `src/`, empaquetado
  según la guía oficial de PyPA.
- Calidad: ruff (lint + format) y mypy en modo strict.
- Pruebas: pytest==9.1.1.
- Observabilidad: opentelemetry-api==1.45.1 (obligatorio por seguridad) y
  structlog==26.1.0. Grafana autohospedado para logs, trazas y métricas
  (Loki, Tempo, Prometheus y OTel Collector/Alloy en un
  `docker-compose.observability.yml` opcional). Incluye el SDK y el
  exporter OTLP alineados con la versión del API.
- Si alguna versión pineada no existe o es incompatible, dímelo antes de
  continuar. No la sustituyas en silencio.
- Docker: Dockerfile multi-stage, usuario no root, CMD en forma exec.
- Sin credenciales en el código: secretos por variables de entorno;
  `config.example.yaml` commiteado, nunca valores reales.
- Configuración en `config/config.yaml`, validada con pydantic al arrancar
  (falla rápido).

# Arquitectura
Hexagonal:
- `domain/`: Transaction (reference, status, priority, retry_count,
  payload), BusinessException, SystemException y las políticas que
  clasifican errores. Sin dependencias externas.
- `application/`: casos de uso, el runner con los estados de REFramework
  (Init → Get Transaction Data → Process Transaction → End Process) y los
  puertos (queue, external API, repository, ShouldStop, clock/sleeper).
- `adapters/`: Redis, cliente HTTP, repositorio Postgres, servidor de
  health, configuración de OpenTelemetry/structlog.
El dominio y la aplicación no importan Redis, HTTP ni la base de datos.

# Comportamiento requerido

## Cola (Redis)
- Redis Streams con consumer groups: XREADGROUP, XACK solo después de
  persistir el resultado y XAUTOCLAIM para recuperar pendientes tras un
  crash.
- Priority: un stream por prioridad, consultados en orden.
- Reintentos diferidos con sorted set (score = ready_at), sin dormir al
  worker. Dead-letter stream para items agotados.
- Redis con AOF documentado como requisito de despliegue.

## Estados de la transacción (SetTransactionStatus)
New → InProgress → Successful | Failed (business) | Retried (system,
reencolado) | Abandoned (agotó MaxRetryNumber, va al DLQ). Cada cambio de
estado se registra en un log con reference y correlation_id.

## Errores (frontera explícita)
- Business Exception → no se reintenta, status Failed: 404, payload
  inválido, id no numérico.
- System Exception retryable → se reintenta: timeout, errores de
  conexión, 5xx, 408, 429 (respetando Retry-After), fallo transitorio de
  Postgres.
- System Exception no retryable → no se reintenta, alerta: 400/401/403.
- Dos capas de reintento: backoff exponencial con jitter dentro del item
  (máx. 3 intentos) y MaxRetryNumber a nivel de item (reencolado).
- Documenta esta tabla en el README y cúbrela con pruebas.

## Config (YAML)
MaxConsecutiveSystemExceptions=3, MaxRetryNumber=2, más: backoff base/máx,
timeout HTTP, block_ms, lease del item y grace de apagado.
MaxConsecutiveSystemExceptions funciona como circuit breaker: al
alcanzarse deja de tomar items, el health pasa a `degraded`, espera un
cooldown y prueba con un item. El proceso NO termina.

## Persistencia (Postgres)
- Servicio en docker-compose con volumen persistente y healthcheck
  (`pg_isready`); el worker espera a que esté sano.
- Migraciones versionadas (Alembic o equivalente), ejecutadas como paso
  explícito de despliegue, no escondidas en el arranque del worker.
- Driver psycopg 3. Tabla de resultados con: reference (UNIQUE), status,
  priority, retry_count, resultado de la API (JSONB), correlation_id,
  timestamps de creación y de fin.
- Credenciales solo por variables de entorno (usuario, password, host,
  base de datos).

## Idempotencia
Clave = `reference`. INSERT ... ON CONFLICT DO NOTHING en la misma
transacción de base de datos que marca el item como Successful. Una doble
entrega no produce dos escrituras y se acusa (XACK) normalmente.

## ShouldStop (apagado controlado)
- ShouldStop es una única bandera (threading.Event) con dos orígenes:
  el handler de SIGTERM/SIGINT, que solo la activa y no ejecuta lógica, y
  la llave `control:should_stop` en Redis para un stop suave del operador.
- El runner la consulta en el estado Get Transaction Data, antes de tomar
  el siguiente item. Si está activa, pasa a End Process: cierra
  conexiones, hace flush de logs/trazas y sale con código 0.
- El item en vuelo termina su ciclo completo: persistir y luego XACK. No
  se interrumpe a la mitad.
- Las esperas del backoff son interrumpibles (`event.wait(timeout)`) y todo
  HTTP lleva timeout acotado. `stop_grace_period` de Docker mayor que el
  peor caso de un item.
- Si hay SIGKILL, el item queda pendiente en el stream y se reclama al
  arrancar.

## Observabilidad
- `correlation_id` generado por quien encola, viaja en el mensaje y
  aparece en todos los logs. Propaga también `traceparent` (W3C).
- structlog con contextvars; cada log incluye correlation_id, trace_id,
  span_id y reference.
- Métricas: items procesados por resultado, duración, reintentos, estado
  del circuito, edad del item más viejo. Nunca uses reference ni
  correlation_id como label.
- Redacta PII en los logs.

## Health
Servidor HTTP mínimo:
- `/livez`: heartbeat del loop reciente (también en idle).
- `/readyz`: Redis y Postgres responden; reporta el estado
  `starting | idle | processing | degraded | stopping`.
- La API externa no cuenta para liveness.

## Operación (las 3 a.m.)
Diseña esto antes de escribir el README: alertas por síntoma (cola
estancada, circuito abierto, DLQ > 0, health caído), runbook de triage
paso a paso (health → logs por correlation_id → Redis/Postgres/API),
acciones seguras (reinicio, `requeue --reference`, `drain-dlq`) y qué NO
hacer (FLUSHALL, borrar el stream o el consumer group, borrar filas a
mano en Postgres).

# Entregables
1. Estructura del repo y `pyproject.toml`.
2. Núcleo de dominio, casos de uso y puertos.
3. Adaptadores (Redis, HTTP, repositorio Postgres, health,
   observabilidad).
4. `config/config.example.yaml` y su validación.
5. Dockerfile y `docker-compose.yml` (worker, Redis con AOF, Postgres)
   más el compose de observabilidad.
6. Migraciones de base de datos.
7. `.pre-commit-config.yaml` con ruff, mypy (hook local) y gitleaks; CI
   con gitleaks, ruff, mypy, pytest y pip-audit.
8. `PULL_REQUEST_TEMPLATE.md`.
9. Pruebas con fakes en memoria: flujo feliz, 404 (business), reintentos
   agotados (system), doble entrega (idempotencia) y ShouldStop activado
   con item en vuelo. Adaptadores con testcontainers (Redis y Postgres).
10. `README.md`: descripción, instalación local para desarrolladores,
    configuración, despliegue, monitoreo, runbook y tabla de errores.
11. Un CLI `seed` para encolar items de prueba.

# Modo de trabajo
- Trabaja por fases en este orden: (1) estructura + pyproject + config,
  (2) dominio + puertos + runner con pruebas, (3) adaptadores, (4)
  Docker + observabilidad, (5) CI/pre-commit/README/runbook.
- Al terminar cada fase, detente, resume lo hecho, lista supuestos y
  espera mi confirmación.
- Si una decisión cambia el diseño y no está aquí, pregúntame antes de
  asumirla (máximo una pregunta a la vez).
- Dime lo que no puedas verificar. No inventes versiones, flags ni APIs.

# Criterios de aceptación
- `ruff check`, `mypy --strict` y `pytest` pasan en limpio.
- `docker compose up` levanta worker, Redis y Postgres, y `/readyz`
  responde ok.
- Con ShouldStop activado (por SIGTERM o por la llave de Redis) a mitad
  de un item, ese item termina y se persiste, y el proceso sale limpio sin
  tomar otro (probado).
- Un mensaje se puede seguir de punta a punta en Loki por correlation_id.
- No hay secretos en el repo (gitleaks limpio).
```

## 3. Algo que la IA me entregó mal o incompleto, y cómo me di cuenta

- **La parada suave por Redis se podía quedar en bucle.** La primera versión de `control:should_stop` dejaba la llave puesta en Redis. Si Docker reiniciaba el worker, este leía la llave otra vez y se volvía a detener, y así una y otra vez. Lo corregimos con el comando `stop --ttl`, que guarda la hora de Redis, y el worker ignora las llaves que son anteriores a su arranque.

- **Los fallos transitorios no tenían tope.** Un item que fallaba por algo transitorio se podía liberar y volver a la cola sin límite. Agregamos `release_count` y `first_enqueued_at` al mensaje, una espera exponencial con tope, y el envío al DLQ por antigüedad (`reason=max_age`).

- **Una carrera en `reopen` con el DLQ.** Cuando una entrada del DLQ no tenía fila en Postgres, reabrirla podía fallar. Se corrigió en el mismo commit.

- **`drain-dlq` era peligroso por defecto.** Lo dejamos en dry-run por defecto, con un tope fijo de recorrido, sin tocar mensajes venenosos o ilegibles ni los que ya tienen `requeue_count >= 3`, y con auditoría por item.


## 4. Qué decidí yo sin consultarle

Lo que decidí yo fue el entendimiento primario de lo que me piden. También, en primera instancia, qué utilizar, como se puede ver en el prompt inicial, e incluso qué referencias y herramientas usar para el desarrollo: arquitectura hexagonal, el modelo del REFramework de UiPath (Business y System Exceptions, MaxRetryNumber, MaxConsecutiveSystemExceptions, ShouldStop), Redis como cola, Postgres, Grafana autohospedado y las versiones pineadas.

También decidí, durante cada suceso, ver cómo lo solucionaba y por qué lo tenía que solucionar así. Por eso trabajamos por fases: al final de cada una yo revisaba y confirmaba antes de seguir.
