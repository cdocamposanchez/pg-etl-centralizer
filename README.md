# ETL Centralizador

ETL automático que sincroniza múltiples bases Postgres de origen hacia una base central usando **PySpark** para el procesamiento, **Airflow** para la orquestación y **Postgres** como repositorio central de datos y metadatos.

El proyecto está diseñado para descubrir tablas de forma automática, decidir el modo de carga por tabla y mantener trazabilidad de cada corrida sin requerir configuración manual por cada objeto.

## Qué hace este proyecto

- Descubre tablas directamente desde `information_schema`.
- Ejecuta un DAG horario en Airflow con una tarea por fuente activa.
- Sincroniza en modo **incremental** cuando existe una columna adecuada.
- Hace **full refresh** cuando no hay columna incremental confiable.
- Aplica **UPSERT** por clave segura y propaga **borrados** cuando corresponde.
- Evoluciona el esquema destino si aparecen columnas nuevas.
- Registra estado y ejecución en `etl_meta.sync_state` y `etl_meta.sync_runs`.

## Flujo general de ejecución

```text
Airflow DAG (postgres_hourly_sync)
        │
        ├── una tarea por fuente activa: sync_db1, sync_db2, ...
        │
        └── cada tarea ejecuta: python -m etl.run_sync --source <ALIAS>
                 │
                 ├── carga configuración desde .env
                 ├── descubre tablas de los esquemas configurados
                 ├── decide incremental o full_refresh
                 ├── lee datos por JDBC con Spark
                 ├── normaliza columnas y agrega auditoría ETL
                 ├── crea o evoluciona tablas destino en synced.*
                 └── actualiza metadatos y logs de corrida
```

## Arquitectura de salida

Las tablas finales se escriben en el esquema `synced` con este patrón:

```text
synced.<alias>_<schema>_<tabla>
```

Ejemplos:

- `synced.db1_esquema1_clientes`
- `synced.db2_public_facturas`

Además, el ETL agrega columnas de auditoría para trazabilidad:

- `_etl_synced_at`
- `_etl_source_alias`
- `_etl_source_schema`
- `_etl_source_table`

## Estructura del proyecto y responsabilidad de cada archivo

| Archivo | Responsabilidad | Cuándo modificarlo |
| --- | --- | --- |
| `dags/postgres_hourly_sync.py` | Define el DAG de Airflow, su frecuencia, sus tareas y el disparo por fuente | Para cambiar horario, concurrencia, nombres de tareas o comportamiento del scheduler |
| `src/etl/config.py` | Lee `.env`, valida variables y arma la configuración de central, fuentes, Spark y ETL | Para agregar nuevas variables, cambiar defaults o introducir nuevas opciones de configuración |
| `src/etl/run_sync.py` | Punto de entrada principal del ETL; coordina descubrimiento, sincronización y cierre de Spark | Para cambiar argumentos CLI, filtrar tablas o ajustar el flujo de ejecución |
| `src/etl/discovery.py` | Descubre tablas, columnas, PK y UNIQUE desde `information_schema` | Para ajustar reglas de exclusión o detección de clave segura |
| `src/etl/syncer.py` | Contiene la lógica de sincronización por tabla: modo, lectura JDBC, escritura, UPSERT, deletes y evolución de esquema | Para cambiar cómo se carga cada tabla o cómo se construye el destino |
| `src/etl/metadata.py` | Administra `etl_meta.sync_state` y `etl_meta.sync_runs`, además de operaciones DDL/DML auxiliares en la base central | Para modificar auditoría, estado incremental o estrategia de merge |
| `src/etl/spark_utils.py` | Utilidades de Spark y mapeo de tipos hacia Postgres | Para ajustar memoria, batch size o conversiones de tipos |
| `sql/01_central_bootstrap.sql` | Inicializa el esquema central y las tablas de metadatos | Para cambiar el bootstrap de la base destino |
| `sql/seed_db1.sql`, `sql/seed_db2.sql` | Datos de ejemplo para las bases origen locales | Para crear casos de prueba o demo |
| `docker-compose.yml` | Define el stack local: central, metadatos de Airflow, fuentes de ejemplo y servicios de Airflow | Para cambiar puertos, volúmenes, variables de entorno o agregar servicios |

## Cómo está configurado el DAG

El DAG real vive en `dags/postgres_hourly_sync.py`.

```text
with DAG(
    dag_id="postgres_hourly_sync",
    description="Sincronización horaria Postgres -> base central con PySpark",
    start_date=datetime(2025, 1, 1),
    schedule="0 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["etl", "pyspark", "postgres"],
) as dag:
```

### Qué significa cada ajuste

- `schedule="0 * * * *"`: el DAG se ejecuta cada hora, en punto.
- `catchup=False`: Airflow no intenta reejecutar historiales pendientes.
- `max_active_runs=1`: evita que una corrida se solape con otra del mismo DAG.
- `default_args`: define reintentos, pausa entre reintentos y propietario lógico.
- `tags`: ayuda a clasificar el DAG en la UI de Airflow.

### Cómo se crean las tareas

El DAG no tiene tareas fijas por tabla; genera una tarea por fuente activa definida en `ACTIVE_SOURCES`.

```text
sources = _active_sources()
if not sources:
    warn = BashOperator(
        task_id="no_sources_configured",
        bash_command='echo "ACTIVE_SOURCES vacío. Revisa el .env"; exit 1',
    )
    start >> warn >> end
else:
    for alias in sources:
        task = BashOperator(
            task_id=f"sync_{alias.lower()}",
            bash_command=(
                "cd /opt/airflow/app && "
                "python -m etl.run_sync --source {{ params.alias }} "
                "--run-id {{ run_id }}"
            ),
            params={"alias": alias},
            execution_timeout=timedelta(minutes=55),
        )
        start >> task >> end
```

### Puntos importantes del comportamiento del DAG

- Si `ACTIVE_SOURCES` está vacío, el DAG sigue siendo válido, pero expone el problema con la tarea `no_sources_configured`.
- Cada fuente se ejecuta en una tarea independiente, lo que permite paralelismo entre fuentes.
- `execution_timeout=55 minutos` deja margen antes de la siguiente corrida horaria.
- El valor de `run_id` se pasa al proceso ETL para registrar la corrida en metadatos.

## Cómo funciona la sincronización interna

La entrada principal es `src/etl/run_sync.py`.

```powershell
python -m etl.run_sync --source DB1
python -m etl.run_sync --source DB1 --schema esquema1
python -m etl.run_sync --all
```

### Flujo por fuente

1. Carga configuración desde `.env`.
2. Descubre tablas de los esquemas configurados.
3. Filtra por esquema si se indicó `--schema`.
4. Para cada tabla, invoca `TableSyncer.sync_table(...)`.
5. Cierra Spark y devuelve un resumen por fuente.

### Decisión del modo de carga

El modo se decide en `src/etl/syncer.py`:

- **incremental**: cuando la tabla tiene una columna candidata de control.
- **full_refresh**: cuando no existe columna incremental o cuando no hay clave segura.

Las columnas candidatas se leen desde `INCREMENTAL_COLUMNS`.

Por defecto:

```dotenv
INCREMENTAL_COLUMNS=updated_at,modified_at,fecha_modificacion,created_at,id
FALLBACK_TO_FULL_REFRESH=true
```

## Cómo se identifican las claves seguras

El ETL intenta operar con precisión usando esta prioridad:

1. `PRIMARY KEY`
2. `UNIQUE constraint` compuesta solo por columnas `NOT NULL`
3. Si no existe una clave confiable, se usa `full refresh`

Esto evita falsos UPSERTs y reduce el riesgo de inconsistencias.

## Metadatos y auditoría

El proyecto guarda estado de sincronización en la base central.

### `etl_meta.sync_state`

Guarda el estado actual de cada tabla:

- modo de sincronización
- columna incremental usada
- último valor procesado
- estado de la última corrida
- filas cargadas en la última ejecución y acumuladas
- error de la última ejecución, si existió

### `etl_meta.sync_runs`

Guarda el histórico de corridas:

- `run_id`
- origen (`source_alias`, `source_schema`, `source_table`)
- tabla destino
- modo
- estado
- filas cargadas
- timestamps de inicio y fin
- mensaje de error

## Configuración

La configuración se toma desde `.env` y se recarga en cada ejecución.

### Variables principales

```dotenv
CENTRAL_DB_HOST=postgres-central
CENTRAL_DB_PORT=5432
CENTRAL_DB_NAME=central_warehouse
CENTRAL_DB_USER=etl_writer
CENTRAL_DB_PASSWORD=central

ACTIVE_SOURCES=DB1,DB2

SOURCE_DB1_HOST=source-db1
SOURCE_DB1_PORT=5432
SOURCE_DB1_DBNAME=db1
SOURCE_DB1_USER=readonly_user
SOURCE_DB1_PASSWORD=changeme_db1
SOURCE_DB1_SCHEMAS=esquema1,esquema2

SOURCE_DB2_HOST=source-db2
SOURCE_DB2_PORT=5432
SOURCE_DB2_DBNAME=db2
SOURCE_DB2_USER=readonly_user
SOURCE_DB2_PASSWORD=changeme_db2
SOURCE_DB2_SCHEMAS=public,ventas

INCREMENTAL_COLUMNS=updated_at,modified_at,fecha_modificacion,created_at,id
FALLBACK_TO_FULL_REFRESH=true
EXCLUDED_TABLES=db1.esquema2.tabla_temporal

SPARK_MASTER=local[*]
SPARK_DRIVER_MEMORY=2g
SPARK_EXECUTOR_MEMORY=2g
SPARK_JDBC_FETCHSIZE=10000
SPARK_JDBC_BATCHSIZE=5000
SPARK_JDBC_NUM_PARTITIONS=4
```

### Cómo se usa `.env`

- El archivo local `.env` se monta en los contenedores de Airflow como `/opt/airflow/.env`.
- La variable `RUNTIME_ENV_FILE` apunta a ese archivo y permite recargar configuración por corrida.
- Si cambias credenciales, esquemas, exclusiones o fuentes activas, el ETL toma el nuevo valor en la siguiente ejecución.

## Qué archivos tocar para modificar el comportamiento

### 1) Agregar o quitar una fuente

Edita `ACTIVE_SOURCES` y crea las variables `SOURCE_<ALIAS>_*` correspondientes.

```dotenv
ACTIVE_SOURCES=DB1,DB2,DB3

SOURCE_DB3_HOST=source-db3
SOURCE_DB3_PORT=5432
SOURCE_DB3_DBNAME=db3
SOURCE_DB3_USER=readonly_user
SOURCE_DB3_PASSWORD=changeme_db3
SOURCE_DB3_SCHEMAS=public,ventas
```

### 2) Cambiar los esquemas que se sincronizan

Modifica `SOURCE_<ALIAS>_SCHEMAS`.

```dotenv
SOURCE_DB1_SCHEMAS=esquema1,esquema2,esquema3
```

### 3) Excluir tablas puntuales

Agrega el nombre completo `schema.tabla` en `EXCLUDED_TABLES`.

```dotenv
EXCLUDED_TABLES=esquema4.tabla_temporal,public.logs_historicos
```

### 4) Ajustar columnas candidatas para incremental

Si una fuente usa otro nombre para control de cambios, amplía `INCREMENTAL_COLUMNS`.

```dotenv
INCREMENTAL_COLUMNS=updated_at,modified_at,fecha_modificacion,fecha_actualizacion,last_update_at
```

### 5) Cambiar la frecuencia del DAG

Edita `schedule` en `dags/postgres_hourly_sync.py`.

```text
with DAG(
    dag_id="postgres_hourly_sync",
    start_date=datetime(2025, 1, 1),
    schedule="0 * * * *",  # cada hora
    catchup=False,
    max_active_runs=1,
):
    ...
```

Ejemplos de schedule válidos:

```text
"0 * * * *"   # cada hora
"0 2 * * *"   # todos los días a las 02:00
"*/15 * * * *" # cada 15 minutos
```

### 6) Ajustar el comportamiento del ETL

Los cambios más comunes se hacen en:

- `src/etl/config.py`: defaults, lectura de `.env`, validación de variables.
- `src/etl/syncer.py`: lógica de incremental/full refresh, UPSERT y deletes.
- `src/etl/discovery.py`: reglas para detectar PK/UNIQUE válidas.
- `src/etl/metadata.py`: esquema de estado, histórico y DDL de destino.

## Puesta en marcha

### 1) Preparar variables de entorno

Genera tu archivo `.env` con las variables necesarias del proyecto.

### 2) Generar la Fernet key de Airflow

```powershell
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

### 3) Levantar el stack local

```powershell
docker compose up -d --build
```

Servicios principales del `docker-compose.yml`:

- `postgres-central`: base central de destino
- `postgres-airflow`: base de metadatos de Airflow
- `source-db1`, `source-db2`: fuentes locales de ejemplo
- `airflow-webserver`: interfaz web de Airflow
- `airflow-scheduler`: ejecución de tareas y DAGs
- `airflow-init`: inicialización de la base de Airflow y usuario administrador

### 4) Abrir la UI de Airflow

```text
http://localhost:8080
```

El DAG `postgres_hourly_sync` queda programado para ejecutarse cada hora.

## Ejecución manual

### Sincronizar una fuente completa

```powershell
docker exec airflow-scheduler bash -lc "cd /opt/airflow/app && python -m etl.run_sync --source DB1"
docker exec airflow-scheduler bash -lc "cd /opt/airflow/app && python -m etl.run_sync --source DB2"
```

### Sincronizar una fuente filtrando por esquema

```powershell
docker exec airflow-scheduler bash -lc "cd /opt/airflow/app && python -m etl.run_sync --source DB2 --schema ventas"
```

### Sincronizar todas las fuentes activas

```powershell
docker exec airflow-scheduler bash -lc "cd /opt/airflow/app && python -m etl.run_sync --all"
```

### Disparar el DAG desde la CLI de Airflow

```powershell
docker exec airflow-scheduler airflow dags trigger postgres_hourly_sync
```

## Verificación de resultados

### Consultar tablas sincronizadas

```powershell
docker exec -it postgres-central psql -U etl_writer -d central_warehouse
\dt synced.*
```

### Consultar estado de sincronización

```text
SELECT source_alias,
       source_schema,
       source_table,
       target_table,
       mode,
       last_value,
       last_run_at,
       last_status,
       rows_loaded_last,
       rows_loaded_total
FROM etl_meta.sync_state
ORDER BY source_alias, source_schema, source_table;
```

### Consultar histórico de corridas

```text
SELECT run_id,
       source_alias,
       source_schema,
       source_table,
       target_table,
       mode,
       status,
       rows_loaded,
       started_at,
       finished_at
FROM etl_meta.sync_runs
ORDER BY started_at DESC
LIMIT 50;
```

## Permisos recomendados en las fuentes

```text
CREATE USER readonly_user WITH PASSWORD 'xxx';
GRANT CONNECT ON DATABASE db1 TO readonly_user;
GRANT USAGE ON SCHEMA esquema1, esquema2 TO readonly_user;
GRANT SELECT ON ALL TABLES IN SCHEMA esquema1, esquema2 TO readonly_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA esquema1, esquema2
  GRANT SELECT ON TABLES TO readonly_user;
```

## Tuning y rendimiento

- `SPARK_DRIVER_MEMORY` y `SPARK_EXECUTOR_MEMORY`: úsalos para tablas grandes o anchas.
- `SPARK_JDBC_FETCHSIZE`: ajusta el tamaño de lectura por lote.
- `SPARK_JDBC_BATCHSIZE`: ajusta el tamaño de escritura por lote.
- `SPARK_JDBC_NUM_PARTITIONS`: útil si luego incorporas particionamiento JDBC más avanzado.
- El DAG ya procesa una tarea por fuente, por lo que DB1, DB2 y futuras fuentes pueden correr en paralelo según la capacidad del entorno.

## Casos cubiertos por el ETL

- Inserciones nuevas en la fuente → aparecen en la base central.
- Updates en registros existentes → se reflejan por `UPSERT`.
- Borrados en la fuente → se eliminan en central cuando existe una clave segura.
- Tablas sin PK pero con `UNIQUE constraint` válida → también se sincronizan con precisión.
- Tablas sin clave segura → `full refresh` para evitar inconsistencias.
- Tablas con columnas nuevas → el destino evoluciona automáticamente.

## Limitaciones conocidas

- Si una tabla no tiene PK ni una `UNIQUE constraint` segura, se usa `full refresh`.
- Las columnas eliminadas del origen no se eliminan del destino.
- No se aplica particionamiento JDBC avanzado por defecto.

## Troubleshooting rápido

- Si `postgres-central` no responde, revisa que el puerto `5432` no esté ocupado.
- Si el DAG no aparece, verifica que `airflow-scheduler` esté activo y que `ACTIVE_SOURCES` tenga valor.
- Si una fuente falla, revisa `etl_meta.sync_runs` y los logs de Airflow.

## Resumen operativo

Este proyecto prioriza:

- descubrimiento automático de tablas
- sincronización incremental cuando es posible
- precisión en updates y deletes
- fallback seguro cuando no hay una clave confiable

Eso permite mantener sincronizada la base central con los orígenes sin depender de configuración manual por tabla.
