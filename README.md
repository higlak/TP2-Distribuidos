# TP Sistemas Distribuidos
## Escalabilidad

### Alumnos
- Martin Ugarte (107870)  
- Juan Pablo Aschieri (108000)  

### Como levantar el proyecto
Para levantar los containers se brindan los siguientes scripts que deberan ser ejecutados desde el root del proyecto:
- **run.sh** levantar los container y los ejecutara, segun esten dispuestos en el docker compose
- **stop.sh**: frenara la ejecucion del programa con un gracefull finish y bajara los contenedores
 

El sistema presentado es configurable utilizando las herramientas provistas en la carpeta config. Habrá un archivo por query que indique las configuraciones de los workers, un archivo de configuracion para el cliente, y otro para el gateway. Luego corriendo load_docker_config.py esta configuraciones seran plasmadas en el docker-compose-dev.yaml

### Configuracion Workers
Para configurar un pipeline, se deben indicar las pools a usar como una sección en un archivo **config_queryN.ini** y asignarle los siguientes atributos:
- `WORKER_AMOUNT`: cantidad de workers que participarán de la pool, se levantara un container de docker por cada uno
- `WORKER_TYPE`: tipo de worker (filter o accumulator)
- `WORKER_FIELD`: campo por el que se filtra si el tipo de worker es filtro, o campo que se acumula si es un acumulador.
- `WORKER_VALUE`: valor por el que se filtra si el tipo de worker es filtro, o valor al que se quiere llegar si es un acumulador. No es obligatorio. Si se quiere filtrar valores en un rango, establecer un mínimo y máximo separado por comas.
- `ACCUMULATE_BY`: campo por el que se quiere acumular. Solo es válido para los workers de tipo accumulator.
- `FORWARD_TO`: grupos o pools a los que se rediriga el resultado del proceso. Se deben separar por comas si es más de uno.

A continuación un ejemplo completo para el pipeline de la consulta nro 3:

```ini
[POOL0]
WORKER_AMOUNT = 4
WORKER_TYPE = filter
WORKER_FIELD = year
WORKER_VALUE = 1990,1999
FORWARD_TO = 3.1

[POOL1]
WORKER_AMOUNT = 4
WORKER_TYPE = accumulator
WORKER_FIELD = review_count
WORKER_VALUE = 500
ACCUMULATE_BY = title
FORWARD_TO = Gateway,4.0
```

### Configuracion Cliente

Se debe indicar en el archivo **config_client.ini** el tama;o de batchs que se usara en elsistema y el path donde se guardaraan los resultados.

Ejemplo:
```ini
[DEFAULT]
BATCH_SIZE = 25
QUERY_RESULTS_PATH = /data/query_results
```
### Configuracion Gateway

Se deberá indicar en el archivo **config_gateway.ini** el puerto en el que el gateway quedará escuchando conexiones de nuevos clientes, las queries (de la 1 a la 5 separadas por coma) que van a recibir libros del gateway, y las queries que van a recibir reseñas del gateway.

Ejemplo:
```ini
[DEFAULT]
PORT = 12345
BOOK_QUERIES = 1,2,3,5
REVIEW_QUERIES = 3,5
```