# Revisión del PR: procesador_cuentas.py

```python
# procesador_cuentas.py
"""[sugerencia]  organiza los imports"""
import requests
import logging
import time
import pika
import json

"""[sugerencia] corrige las funciones usando type hints para saber que reciben y que devuelven"""
"""[sugerencia] en los logs usa f-string en vez de formatear con %"""

"""[bloqueante] las credenciales estan expuestas, tienes que rotarlas y revocarlas debido a que ya quedaron registradas en github,
y usarlas como variables de entorno. tampoco usemos el usuario admin de rabbit, mejor uno solo con permisos a la cola"""
API_KEY = "sk_live_7f3a9c21b4e8d05f6a1c"
API_URL = "https://api.proveedor-salud.co/v2/cuentas"
RABBIT_URL = "amqp://admin:Admin2026@10.0.1.14:5672/"

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)


"""[sugerencia] define que tipo de dato vamos a recibir y devolver para no ocasionar problemas en el resto de la ejecucion"""
def consultar_cuenta(numero, intentos=5):
    for i in range(intentos):
        try:
            """[bloqueante] agrega timeout por si la api se cuelga"""
            r = requests.get(
                f"{API_URL}/{numero}",
                headers={"Authorization": f"Bearer {API_KEY}"},
            )
            """[bloqueante] ¿como manejas las respuestas de error de la api? un 401 o 500 no lanza excepcion
            y lo guardamos como si fuera valido, usa r.raise_for_status()"""
            return r.json()
        except Exception as e:
            """[sugerencia] para expresar fallos con logs usa log.warning o log.error y usa f-string en vez de %
            ej: log.warning(f"fallo, reintentando: error={e}")"""
            log.info("fallo, reintentando: %s" % e)
            time.sleep(2)
    """[bloqueante] si se acaban los intentos devolvemos None y guardar revienta"""
    return None


"""[bloqueante] parametros/variables sin utilizar que crecen en memoria y sin poder accederse se eliminan"""
def guardar(resultado, cache={}):
    cache[resultado["id"]] = resultado
    """[bloqueante] estamos guardando datos de salud en texto plano en el archivo, hay que definir donde se guarda esto"""
    """[sugerencia] abre el archivo con encoding="utf-8" """
    with open("resultados.json", "a") as f:
        f.write(json.dumps(resultado) + "\n")


"""[sugerencia] properties no se usa explicitamente, renombrar a _properties"""
def procesar(ch, method, properties, body):
    """[bloqueante] ¿que pasa si un mensaje esta mal formado? se cae el consumidor y se reintenta en bucle"""
    mensaje = json.loads(body)
    """[bloqueante] no se deberia de loguear (debug) el mensaje completo, no sabemos si puede ser sensible"""
    log.debug("mensaje recibido: %s" % mensaje)
    try:
        resultado = consultar_cuenta(mensaje["numero_cuenta"])
        guardar(resultado)
    except Exception:
        """[bloqueante] muestra el error aqui, nos tragamos el error sin saber que sucedio y el flujo sigue como si nada,
        usa log.exception para conservar la excepcion"""
        pass

    """[bloqueante - el mas grave] hacemos ack aunque haya fallado todo y rabbit borra el mensaje, esa cuenta se pierde
    y nadie se entera. solo se hace cuando se guardo satisfactoriamente, si falla usa basic_nack"""
    ch.basic_ack(delivery_tag=method.delivery_tag)


"""[sugerencia] pon un punto de ingreso por si se importa y para testear sin conectarnos necesariamente a rabbit"""
conexion = pika.BlockingConnection(pika.URLParameters(RABBIT_URL))
canal = conexion.channel()
canal.queue_declare(queue="cuentas")
canal.basic_consume(queue="cuentas", on_message_callback=procesar)
log.info("escuchando...")
canal.start_consuming()
```


este seria el comentario con el que responderia a el pr 
 gracias por el PR, el flujo se entiende bien. antes del merge hay que resolver los [bloqueante],
lo mas grave es el ack en procesar porque estamos perdiendo mensajes sin darnos cuenta.
dices que esta probado, ¿como lo probaste? me gustaria ver que pasa con la api caida, un 404 y un mensaje mal formado.
para un Pull Request se tiene que seguir la plantilla .github/PULL_REQUEST_TEMPLATE.md"