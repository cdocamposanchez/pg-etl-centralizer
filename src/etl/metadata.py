"""Gestor de metadatos del ETL sobre la base central (vía psycopg2)."""
from __future__ import annotations

import logging
import hashlib
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Dict, Iterator, Optional

import psycopg2
from psycopg2.extras import RealDictCursor

from .config import DBConfig

log = logging.getLogger(__name__)


@dataclass
class TableState:
    source_alias: str
    source_schema: str
    source_table: str
    target_table: str
    mode: str
    incremental_column: Optional[str]
    last_value: Optional[str]


class MetadataStore:
    """Encapsula operaciones contra etl_meta.* en la base central."""

    def __init__(self, central: DBConfig):
        self._cfg = central

    @contextmanager
    def _conn(self) -> Iterator[psycopg2.extensions.connection]:
        conn = psycopg2.connect(
            host=self._cfg.host,
            port=self._cfg.port,
            dbname=self._cfg.dbname,
            user=self._cfg.user,
            password=self._cfg.password,
        )
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    # ---------- estado ----------

    def get_state(
        self, source_alias: str, source_schema: str, source_table: str
    ) -> Optional[TableState]:
        sql = """
            SELECT source_alias, source_schema, source_table, target_table,
                   mode, incremental_column, last_value
            FROM etl_meta.sync_state
            WHERE source_alias = %s AND source_schema = %s AND source_table = %s
        """
        with self._conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (source_alias, source_schema, source_table))
            row = cur.fetchone()
            if not row:
                return None
            return TableState(**row)

    def upsert_state(
        self,
        source_alias: str,
        source_schema: str,
        source_table: str,
        target_table: str,
        mode: str,
        incremental_column: Optional[str],
        last_value: Optional[str],
        rows_loaded: int,
        status: str,
        error: Optional[str] = None,
    ) -> None:
        sql = """
            INSERT INTO etl_meta.sync_state (
                source_alias, source_schema, source_table, target_table,
                mode, incremental_column, last_value,
                last_run_at, last_status, last_error,
                rows_loaded_last, rows_loaded_total
            ) VALUES (
                %s, %s, %s, %s,
                %s, %s, %s,
                NOW(), %s, %s,
                %s, %s
            )
            ON CONFLICT (source_alias, source_schema, source_table) DO UPDATE SET
                target_table       = EXCLUDED.target_table,
                mode               = EXCLUDED.mode,
                incremental_column = EXCLUDED.incremental_column,
                last_value         = COALESCE(EXCLUDED.last_value, etl_meta.sync_state.last_value),
                last_run_at        = NOW(),
                last_status        = EXCLUDED.last_status,
                last_error         = EXCLUDED.last_error,
                rows_loaded_last   = EXCLUDED.rows_loaded_last,
                rows_loaded_total  = etl_meta.sync_state.rows_loaded_total + EXCLUDED.rows_loaded_last
        """
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                sql,
                (
                    source_alias, source_schema, source_table, target_table,
                    mode, incremental_column, last_value,
                    status, error,
                    rows_loaded, rows_loaded,
                ),
            )

    # ---------- log ----------

    def log_run(
        self,
        run_id: str,
        source_alias: str,
        source_schema: str,
        source_table: str,
        target_table: str,
        mode: str,
        status: str,
        rows_loaded: int,
        error: Optional[str] = None,
    ) -> None:
        sql = """
            INSERT INTO etl_meta.sync_runs (
                run_id, source_alias, source_schema, source_table, target_table,
                mode, started_at, finished_at, status, rows_loaded, error_message
            ) VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW(), %s, %s, %s)
        """
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                sql,
                (run_id, source_alias, source_schema, source_table, target_table,
                 mode, status, rows_loaded, error),
            )

    # ---------- utilidades de esquema en central ----------

    def table_exists(self, schema: str, table: str) -> bool:
        sql = """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
            )
        """
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(sql, (schema, table))
            return cur.fetchone()[0]

    def get_columns(self, schema: str, table: str) -> dict[str, str]:
        """Devuelve {columna: tipo_postgres} para la tabla central."""
        sql = """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
        """
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(sql, (schema, table))
            return {r[0]: r[1] for r in cur.fetchall()}

    def create_table(self, schema: str, table: str, columns: Dict[str, str]) -> None:
        """
        Crea la tabla destino explícitamente vía DDL.
        Esto garantiza que Spark escriba en synced.<table> y no en un schema incorrecto.
        columns: {nombre_columna: tipo_postgres}
        """
        from psycopg2 import sql as psql

        col_defs = psql.SQL(", ").join(
            psql.SQL("{} {}").format(
                psql.Identifier(col_name),
                psql.SQL(pg_type),
            )
            for col_name, pg_type in columns.items()
        )

        stmt = psql.SQL("CREATE TABLE IF NOT EXISTS {}.{} ({})").format(
            psql.Identifier(schema),
            psql.Identifier(table),
            col_defs,
        )

        with self._conn() as conn, conn.cursor() as cur:
            log.info("CREATE TABLE %s.%s con %d columnas", schema, table, len(columns))
            cur.execute(stmt)

    def add_column(self, schema: str, table: str, column: str, pg_type: str) -> None:
        from psycopg2 import sql as psql
        stmt = psql.SQL("ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS {} {}").format(
            psql.Identifier(schema),
            psql.Identifier(table),
            psql.Identifier(column),
            psql.SQL(pg_type),
        )
        with self._conn() as conn, conn.cursor() as cur:
            log.info("ALTER TABLE %s.%s ADD COLUMN %s %s", schema, table, column, pg_type)
            cur.execute(stmt)

    def ensure_unique_index(self, schema: str, table: str, columns: list[str]) -> None:
        from psycopg2 import sql as psql

        if not columns:
            return

        cols_sig = "_".join(columns)
        suffix = hashlib.md5(cols_sig.encode("utf-8")).hexdigest()[:8]
        index_name = f"ux_{table[:40]}_{suffix}"

        stmt = psql.SQL("CREATE UNIQUE INDEX IF NOT EXISTS {} ON {}.{} ({})").format(
            psql.Identifier(index_name),
            psql.Identifier(schema),
            psql.Identifier(table),
            psql.SQL(", ").join(psql.Identifier(c) for c in columns),
        )
        with self._conn() as conn, conn.cursor() as cur:
            log.info("Asegurando índice único %s en %s.%s", index_name, schema, table)
            cur.execute(stmt)

    def merge_stage_into_target(
        self,
        target_schema: str,
        target_table: str,
        stage_schema: str,
        stage_table: str,
        all_columns: list[str],
        pk_columns: list[str],
    ) -> None:
        from psycopg2 import sql as psql

        non_pk = [c for c in all_columns if c not in set(pk_columns)]
        insert_cols = psql.SQL(", ").join(psql.Identifier(c) for c in all_columns)
        conflict_cols = psql.SQL(", ").join(psql.Identifier(c) for c in pk_columns)

        if non_pk:
            update_set = psql.SQL(", ").join(
                psql.SQL("{} = EXCLUDED.{}").format(psql.Identifier(c), psql.Identifier(c))
                for c in non_pk
            )
            conflict_action = psql.SQL("DO UPDATE SET {}")
            conflict_action = conflict_action.format(update_set)
        else:
            conflict_action = psql.SQL("DO NOTHING")

        stmt = psql.SQL(
            """
            INSERT INTO {}.{} ({})
            SELECT {} FROM {}.{}
            ON CONFLICT ({}) {}
            """
        ).format(
            psql.Identifier(target_schema),
            psql.Identifier(target_table),
            insert_cols,
            insert_cols,
            psql.Identifier(stage_schema),
            psql.Identifier(stage_table),
            conflict_cols,
            conflict_action,
        )

        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(stmt)

    def delete_missing_using_stage_keys(
        self,
        target_schema: str,
        target_table: str,
        stage_schema: str,
        stage_table: str,
        pk_columns: list[str],
    ) -> int:
        from psycopg2 import sql as psql

        if not pk_columns:
            return 0

        join_predicate = psql.SQL(" AND ").join(
            psql.SQL("t.{} = s.{}").format(psql.Identifier(c), psql.Identifier(c))
            for c in pk_columns
        )

        stmt = psql.SQL(
            """
            DELETE FROM {}.{} t
            WHERE NOT EXISTS (
                SELECT 1 FROM {}.{} s
                WHERE {}
            )
            """
        ).format(
            psql.Identifier(target_schema),
            psql.Identifier(target_table),
            psql.Identifier(stage_schema),
            psql.Identifier(stage_table),
            join_predicate,
        )

        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(stmt)
            return cur.rowcount or 0

    def truncate_table(self, schema: str, table: str) -> None:
        from psycopg2 import sql as psql
        stmt = psql.SQL("TRUNCATE TABLE {}.{}").format(
            psql.Identifier(schema), psql.Identifier(table)
        )
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(stmt)