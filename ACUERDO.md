# Acuerdo de trabajo con agentes de IA

## ¿Qué tiene que haber hecho una persona antes de abrir un pull request con código que escribió un agente?

Primero, antes de abrir el PR tengo que revisar que cumpla del todo con lo que debe ser un PR. También tengo que verificar que pasen todos los CI que tenga el proyecto. Y tengo que intentar entender qué hizo el agente en cada paso, no solo mirar el resultado final.

## ¿Qué no se delega nunca?

Lo que nunca se le delega a la IA es el inicio de la tarea. Ahí hay que evaluar con un enfoque humano antes de empezar a escribir código. Tampoco se delegan las decisiones sobre el core del proyecto cuando no hay una especificación que las respalde. Y no se delegan las responsabilidades con contraseñas.

## ¿Cómo evitas que en seis meses el equipo tenga un sistema que funciona y que nadie entiende?

Lo evitaría con documentación y con flujos claros. También buscaría que el código se mantenga lo más libre posible de code smells. Haría una evaluación mensual: si me surge una duda, tengo que ser capaz de conocer el sistema y responderla. Además, en cada actividad se registra el porqué. La idea es que entender el sistema sea un requisito para avanzar.

## ¿Qué haces con el junior que entrega rápido y bien, pero no sabe explicar lo que entregó?

Si un junior entrega rápido y con calidad, primero tengo que saber si entiende lo que hizo y no sabe explicarlo, o si de verdad está adivinando cuando explica lo que hace. No le prohibiría usar IA, pero sí le pediría que cada línea que escriba tenga un sentido claro dentro del sistema. Y en el PR tendría que explicar qué hace cada cambio, y si se puede, cada commit.
