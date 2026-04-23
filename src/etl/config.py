"""Carga y valida la configuración desde variables de entorno."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import List


def _load_env_file(path: str) -> None:
    """Carga variables desde .env y sobreescribe os.environ para el proceso actual."""
    if not path or not os.path.exists(path):
        return

    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip()
            if not key:
                continue
            if (value.startswith('"') and value.endswith('"')) or (
                value.startswith("'") and value.endswith("'")
            ):
                value = value[1:-1]
            os.environ[key] = value


def reload_runtime_env() -> None:
    """Permite que cada corrida tome cambios del .env sin reiniciar contenedores."""
    env_file = os.getenv("RUNTIME_ENV_FILE", "/opt/airflow/.env")
    _load_env_file(env_file)


@dataclass
class DBConfig:
    """Configuración de conexión a una base de datos Postgres."""
    alias: str           # Ej: "DB1" -> se usa como prefijo
    host: str
    port: int
    dbname: str
    user: str
    password: str
    schemas: List[str] = field(default_factory=list)

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.dbname}"

    @property
    def jdbc_properties(self) -> dict:
        return {
            "user": self.user,
            "password": self.password,
            "driver": "org.postgresql.Driver",
            "stringtype": "unspecified",
        }


@dataclass
class SparkSettings:
    master: str = "local[*]"
    driver_memory: str = "2g"
    executor_memory: str = "2g"
    fetchsize: int = 10000
    batchsize: int = 5000
    num_partitions: int = 4


@dataclass
class ETLSettings:
    incremental_columns: List[str]
    fallback_to_full_refresh: bool
    excluded_tables: List[str]


@dataclass
class AppConfig:
    central: DBConfig
    sources: List[DBConfig]
    spark: SparkSettings
    etl: ETLSettings


def _require(var: str) -> str:
    val = os.getenv(var)
    if not val:
        raise ValueError(f"Variable de entorno obligatoria no definida: {var}")
    return val


def _get_list(var: str, default: str = "") -> List[str]:
    raw = os.getenv(var, default)
    return [x.strip() for x in raw.split(",") if x.strip()]


def load_central_db() -> DBConfig:
    return DBConfig(
        alias="CENTRAL",
        host=_require("CENTRAL_DB_HOST"),
        port=int(os.getenv("CENTRAL_DB_PORT", "5432")),
        dbname=_require("CENTRAL_DB_NAME"),
        user=_require("CENTRAL_DB_USER"),
        password=_require("CENTRAL_DB_PASSWORD"),
    )


def load_source_dbs() -> List[DBConfig]:
    """Lee todas las fuentes activas desde las variables SOURCE_<ALIAS>_*."""
    active = _get_list("ACTIVE_SOURCES")
    if not active:
        raise ValueError("ACTIVE_SOURCES vacío. Define al menos una fuente.")

    sources: List[DBConfig] = []
    for alias in active:
        prefix = f"SOURCE_{alias}_"
        schemas = _get_list(f"{prefix}SCHEMAS")
        if not schemas:
            raise ValueError(f"La fuente {alias} no tiene esquemas definidos.")
        sources.append(
            DBConfig(
                alias=alias.lower(),  # el prefijo en tabla destino va en minúsculas
                host=_require(f"{prefix}HOST"),
                port=int(os.getenv(f"{prefix}PORT", "5432")),
                dbname=_require(f"{prefix}DBNAME"),
                user=_require(f"{prefix}USER"),
                password=_require(f"{prefix}PASSWORD"),
                schemas=schemas,
            )
        )
    return sources


def load_spark_settings() -> SparkSettings:
    return SparkSettings(
        master=os.getenv("SPARK_MASTER", "local[*]"),
        driver_memory=os.getenv("SPARK_DRIVER_MEMORY", "2g"),
        executor_memory=os.getenv("SPARK_EXECUTOR_MEMORY", "2g"),
        fetchsize=int(os.getenv("SPARK_JDBC_FETCHSIZE", "10000")),
        batchsize=int(os.getenv("SPARK_JDBC_BATCHSIZE", "5000")),
        num_partitions=int(os.getenv("SPARK_JDBC_NUM_PARTITIONS", "4")),
    )


def load_etl_settings() -> ETLSettings:
    return ETLSettings(
        incremental_columns=_get_list(
            "INCREMENTAL_COLUMNS",
            "updated_at,modified_at,fecha_modificacion,created_at,id",
        ),
        fallback_to_full_refresh=os.getenv(
            "FALLBACK_TO_FULL_REFRESH", "true"
        ).lower() == "true",
        excluded_tables=_get_list("EXCLUDED_TABLES"),
    )


def load_config() -> AppConfig:
    # Recarga .env al inicio para tomar cambios de configuración en cada ejecución.
    reload_runtime_env()
    return AppConfig(
        central=load_central_db(),
        sources=load_source_dbs(),
        spark=load_spark_settings(),
        etl=load_etl_settings(),
    )
