"""Sincronizador por tabla: decide modo, lee con Spark, evoluciona esquema y escribe."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import AppConfig, DBConfig
from .discovery import TableInfo
from .metadata import MetadataStore, TableState
from .spark_utils import spark_type_to_postgres

log = logging.getLogger(__name__)

CENTRAL_SCHEMA = "synced"  # todas las tablas destino viven aquí
STAGE_SCHEMA = "etl_meta"


@dataclass
class SyncResult:
    target_table: str
    mode: str
    rows_loaded: int
    status: str
    error: Optional[str] = None
    new_last_value: Optional[str] = None


class TableSyncer:
    """Orquesta la sincronización de una única tabla origen."""

    def __init__(
        self,
        spark: SparkSession,
        config: AppConfig,
        metadata: MetadataStore,
    ):
        self.spark = spark
        self.config = config
        self.metadata = metadata

    # ---------- naming ----------

    @staticmethod
    def target_table_name(source_alias: str, schema: str, table: str) -> str:
        """db1 + esquema1 + clientes -> db1_esquema1_clientes (lowercase, seguro)."""
        def clean(s: str) -> str:
            return "".join(c if c.isalnum() else "_" for c in s.lower())
        return f"{clean(source_alias)}_{clean(schema)}_{clean(table)}"

    # ---------- lectura ----------

    def _read_jdbc(
        self, source: DBConfig, schema: str, table: str,
        where_clause: Optional[str] = None,
        select_columns: Optional[list[str]] = None,
        distinct: bool = False,
    ) -> DataFrame:
        """Lee una tabla origen con Spark JDBC, con predicate pushdown opcional."""
        select_expr = "*"
        if select_columns:
            quoted_cols = ", ".join(f'"{c}"' for c in select_columns)
            select_expr = f"DISTINCT {quoted_cols}" if distinct else quoted_cols

        dbtable = (
            f'(SELECT {select_expr} FROM "{schema}"."{table}"'
            + (f" WHERE {where_clause}" if where_clause else "")
            + ") AS src"
        )
        reader = (
            self.spark.read.format("jdbc")
            .option("url", source.jdbc_url)
            .option("dbtable", dbtable)
            .option("user", source.user)
            .option("password", source.password)
            .option("driver", "org.postgresql.Driver")
            .option("fetchsize", self.config.spark.fetchsize)
            .option("stringtype", "unspecified")
        )
        return reader.load()

    # ---------- decisión de modo ----------

    def _decide_mode(
        self, table: TableInfo, state: Optional[TableState]
    ) -> tuple[str, Optional[str]]:
        """Devuelve (modo, columna_incremental). Respeta el modo previo si lo hubo."""
        incremental_col = table.pick_incremental_column(
            self.config.etl.incremental_columns
        )
        if incremental_col:
            return "incremental", incremental_col
        if self.config.etl.fallback_to_full_refresh:
            return "full_refresh", None
        raise RuntimeError(
            f"Tabla {table.schema}.{table.name} sin columna incremental y "
            "fallback_to_full_refresh=false"
        )

    # ---------- evolución / creación de esquema ----------

    def _ensure_target_schema(self, df: DataFrame, target_table: str, schema: str = CENTRAL_SCHEMA) -> None:
        """
        Si la tabla destino NO existe, la crea explícitamente vía DDL antes de
        que Spark escriba. Esto garantiza que la tabla quede en synced.<target_table>
        y no en un esquema incorrecto por la interpretación del punto en el nombre.

        Si ya existe, agrega columnas nuevas (auto-evolución).
        """
        expected = {
            field.name.lower(): spark_type_to_postgres(field.dataType)
            for field in df.schema.fields
        }

        if not self.metadata.table_exists(schema, target_table):
            log.info(
                "Tabla destino %s.%s no existe; creándola explícitamente via DDL.",
                schema, target_table,
            )
            self.metadata.create_table(schema, target_table, expected)
            return

        # Tabla ya existe: agregar columnas nuevas
        existing = {
            k.lower(): v
            for k, v in self.metadata.get_columns(schema, target_table).items()
        }
        for col, pg_type in expected.items():
            if col not in existing:
                log.info(
                    "Evolucionando esquema: agregando columna %s (%s) a %s.%s",
                    col, pg_type, schema, target_table,
                )
                self.metadata.add_column(schema, target_table, col, pg_type)

    # ---------- escritura ----------

    def _write(self, df: DataFrame, target_table: str, write_mode: str) -> None:
        """Escribe vía JDBC. write_mode: 'append' | 'overwrite'."""
        central = self.config.central
        # Usamos comillas dobles explícitas en el nombre de tabla para que JDBC
        # no interprete el punto como separador schema.tabla dentro de synced.
        # La tabla ya fue creada en _ensure_target_schema, así que Spark solo
        # hace INSERT (append) o TRUNCATE+INSERT (overwrite con truncate=true).
        fqn = f'"{CENTRAL_SCHEMA}"."{target_table}"'

        writer = (
            df.write.format("jdbc")
            .option("url", central.jdbc_url)
            .option("dbtable", fqn)
            .option("user", central.user)
            .option("password", central.password)
            .option("driver", "org.postgresql.Driver")
            .option("batchsize", self.config.spark.batchsize)
            .option("stringtype", "unspecified")
            # truncate=true evita DROP/CREATE cuando usamos overwrite -> preserva tipos y ALTERs
            .option("truncate", "true")
            .mode(write_mode)
        )
        writer.save()
        log.info("Escritos datos en %s.%s (modo=%s)", CENTRAL_SCHEMA, target_table, write_mode)

    @staticmethod
    def _stage_table_name(target_table: str, suffix: str) -> str:
        return f"_stg_{target_table}_{suffix}"

    def _write_stage_merge_upsert(
        self,
        df: DataFrame,
        target_table: str,
        pk_columns: list[str],
    ) -> None:
        stage = self._stage_table_name(target_table, "rows")

        self._ensure_target_schema(df, stage, schema=STAGE_SCHEMA)
        self.metadata.truncate_table(STAGE_SCHEMA, stage)
        self._write_to_schema(df, STAGE_SCHEMA, stage, "append")
        self.metadata.ensure_unique_index(CENTRAL_SCHEMA, target_table, pk_columns)
        self.metadata.merge_stage_into_target(
            target_schema=CENTRAL_SCHEMA,
            target_table=target_table,
            stage_schema=STAGE_SCHEMA,
            stage_table=stage,
            all_columns=[c.lower() for c in df.columns],
            pk_columns=pk_columns,
        )

    def _write_to_schema(self, df: DataFrame, schema: str, table: str, write_mode: str) -> None:
        central = self.config.central
        fqn = f'"{schema}"."{table}"'
        (
            df.write.format("jdbc")
            .option("url", central.jdbc_url)
            .option("dbtable", fqn)
            .option("user", central.user)
            .option("password", central.password)
            .option("driver", "org.postgresql.Driver")
            .option("batchsize", self.config.spark.batchsize)
            .option("stringtype", "unspecified")
            .option("truncate", "true")
            .mode(write_mode)
            .save()
        )

    def _sync_hard_deletes(
        self,
        source: DBConfig,
        table: TableInfo,
        target_table: str,
        pk_columns: list[str],
    ) -> int:
        stage_keys = self._stage_table_name(target_table, "keys")
        source_cols_map = {c.lower(): c for c in table.columns.keys()}
        pk_source = [source_cols_map.get(c, c) for c in pk_columns]

        keys_df = self._read_jdbc(
            source,
            table.schema,
            table.name,
            select_columns=pk_source,
            distinct=True,
        )
        keys_df = keys_df.select([F.col(c).alias(c.lower()) for c in keys_df.columns])

        self._ensure_target_schema(keys_df, stage_keys, schema=STAGE_SCHEMA)
        self.metadata.truncate_table(STAGE_SCHEMA, stage_keys)
        self._write_to_schema(keys_df, STAGE_SCHEMA, stage_keys, "append")
        return self.metadata.delete_missing_using_stage_keys(
            target_schema=CENTRAL_SCHEMA,
            target_table=target_table,
            stage_schema=STAGE_SCHEMA,
            stage_table=stage_keys,
            pk_columns=pk_columns,
        )

    # ---------- API pública ----------

    def sync_table(self, source: DBConfig, table: TableInfo, run_id: str) -> SyncResult:
        target = self.target_table_name(source.alias, table.schema, table.name)
        state = self.metadata.get_state(source.alias, table.schema, table.name)
        mode, inc_col = self._decide_mode(table, state)
        pk_columns = table.pick_effective_key()

        # Para garantizar precisión en updates/deletes, si no hay PK/UNIQUE segura
        # hacemos full refresh.
        if mode == "incremental" and not pk_columns:
            log.warning(
                "Tabla %s.%s sin PK/UNIQUE segura: se fuerza full_refresh.",
                table.schema,
                table.name,
            )
            mode = "full_refresh"
            inc_col = None

        log.info(
            "Sincronizando %s.%s.%s -> %s.%s (modo=%s, col_incremental=%s)",
            source.alias, table.schema, table.name, CENTRAL_SCHEMA, target, mode, inc_col,
        )

        try:
            if mode == "incremental":
                df, new_last = self._sync_incremental(source, table, inc_col, state)
            else:
                df = self._read_jdbc(source, table.schema, table.name)
                new_last = None

            # Normalizamos nombres de columnas a minúsculas para consistencia
            df = df.select([F.col(c).alias(c.lower()) for c in df.columns])

            # Columnas de auditoría
            df = df.withColumn("_etl_synced_at", F.current_timestamp()) \
                   .withColumn("_etl_source_alias", F.lit(source.alias)) \
                   .withColumn("_etl_source_schema", F.lit(table.schema)) \
                   .withColumn("_etl_source_table", F.lit(table.name))

            # Crear tabla si no existe, o evolucionar esquema si ya existe
            self._ensure_target_schema(df, target, schema=CENTRAL_SCHEMA)

            if mode == "incremental":
                row_count = df.count()
                if row_count > 0:
                    self._write_stage_merge_upsert(df, target, pk_columns)
                deleted = self._sync_hard_deletes(source, table, target, pk_columns)
                log.info(
                    "Incremental con merge aplicado en %s.%s: upsert_rows=%d, deleted_rows=%d",
                    CENTRAL_SCHEMA,
                    target,
                    row_count,
                    deleted,
                )
            else:
                self._write(df, target, "overwrite")
                row_count = df.count()

            self.metadata.upsert_state(
                source_alias=source.alias,
                source_schema=table.schema,
                source_table=table.name,
                target_table=target,
                mode=mode,
                incremental_column=inc_col,
                last_value=new_last if new_last else (state.last_value if state else None),
                rows_loaded=row_count,
                status="ok",
            )
            self.metadata.log_run(
                run_id, source.alias, table.schema, table.name, target,
                mode, "ok", row_count,
            )
            return SyncResult(target, mode, row_count, "ok", new_last_value=new_last)

        except Exception as exc:  # noqa: BLE001
            log.exception("Error sincronizando %s.%s", table.schema, table.name)
            err = str(exc)[:2000]
            try:
                self.metadata.upsert_state(
                    source_alias=source.alias,
                    source_schema=table.schema,
                    source_table=table.name,
                    target_table=target,
                    mode=mode,
                    incremental_column=inc_col,
                    last_value=state.last_value if state else None,
                    rows_loaded=0,
                    status="error",
                    error=err,
                )
                self.metadata.log_run(
                    run_id, source.alias, table.schema, table.name, target,
                    mode, "error", 0, err,
                )
            except Exception:
                log.exception("Además falló el registro de error en metadata")
            return SyncResult(target, mode, 0, "error", error=err)

    # ---------- incremental ----------

    def _sync_incremental(
        self, source: DBConfig, table: TableInfo,
        inc_col: str, state: Optional[TableState],
    ) -> tuple[DataFrame, Optional[str]]:
        """Lee filas con <inc_col> > last_value y devuelve (df, nuevo_last_value)."""
        where = None
        if state and state.last_value and state.incremental_column == inc_col:
            where = f'"{inc_col}" > \'{state.last_value}\''

        df = self._read_jdbc(source, table.schema, table.name, where_clause=where)

        max_row = df.agg(F.max(F.col(inc_col)).alias("mx")).collect()
        new_last = None
        if max_row and max_row[0]["mx"] is not None:
            new_last = str(max_row[0]["mx"])

        if new_last is None and state:
            new_last = state.last_value

        return df, new_last