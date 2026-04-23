"""Creación de SparkSession y utilidades de mapeo de tipos Spark->Postgres."""
from __future__ import annotations

import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BinaryType, BooleanType, ByteType, DataType, DateType, DecimalType,
    DoubleType, FloatType, IntegerType, LongType, ShortType, StringType,
    TimestampType,
)

from .config import SparkSettings

log = logging.getLogger(__name__)

# Ruta al driver JDBC empaquetado dentro del contenedor
POSTGRES_JDBC_JAR = "/opt/spark/jars/postgresql.jar"


def build_spark(app_name: str, settings: SparkSettings) -> SparkSession:
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(settings.master)
        .config("spark.driver.memory", settings.driver_memory)
        .config("spark.executor.memory", settings.executor_memory)
        .config("spark.jars", POSTGRES_JDBC_JAR)
        .config("spark.driver.extraClassPath", POSTGRES_JDBC_JAR)
        .config("spark.executor.extraClassPath", POSTGRES_JDBC_JAR)
        .config("spark.sql.session.timeZone", "UTC")
        # Reduce ruido en logs
        .config("spark.ui.showConsoleProgress", "false")
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def spark_type_to_postgres(dtype: DataType) -> str:
    """Mapea un tipo Spark a un tipo Postgres compatible."""
    if isinstance(dtype, BooleanType):
        return "BOOLEAN"
    if isinstance(dtype, (ByteType, ShortType, IntegerType)):
        return "INTEGER"
    if isinstance(dtype, LongType):
        return "BIGINT"
    if isinstance(dtype, FloatType):
        return "REAL"
    if isinstance(dtype, DoubleType):
        return "DOUBLE PRECISION"
    if isinstance(dtype, DecimalType):
        return f"NUMERIC({dtype.precision},{dtype.scale})"
    if isinstance(dtype, DateType):
        return "DATE"
    if isinstance(dtype, TimestampType):
        return "TIMESTAMP"
    if isinstance(dtype, BinaryType):
        return "BYTEA"
    if isinstance(dtype, StringType):
        return "TEXT"
    # tipos complejos -> texto (JSON serializado por Spark)
    return "TEXT"
