## Qué cambia y por qué

<!-- Explícalo con tus palabras. Si no lo puedes explicar, todavía no está listo (ver ACUERDO.md). -->

## Cómo lo probé

<!-- Comandos que corriste y qué viste. "Está probado" no cuenta: muestra cómo. -->

- [ ] `make lint` y `make typecheck` pasan
- [ ] `make test` pasa (unitarias + integración)
- [ ] Probé al menos un caso de error, no solo el camino feliz

## Uso de IA

- [ ] Parte de este código lo escribió un agente
- [ ] Revisé y entiendo cada línea; puedo explicarla y modificarla sin el agente
- Herramienta y para qué: <!-- p. ej. Claude, para generar las pruebas de X -->

## Riesgos

- [ ] Toca la tabla de errores (qué se reintenta y qué no). Si es así, actualicé el README y `test_policies.py`
- [ ] Toca el orden persistir → XACK, la idempotencia o el apagado
- [ ] Cambia el esquema de la BD (hay migración nueva)
- [ ] Sin credenciales ni datos reales en el código, los logs o las pruebas
