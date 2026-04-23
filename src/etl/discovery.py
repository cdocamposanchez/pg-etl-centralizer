"""Descubre tablas y columnas de las bases origen vía information_schema."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import List, Optional

import psycopg2
from psycopg2.extras import RealDictCursor

from .config import DBConfig

log = logging.getLogger(__name__)


@dataclass
class TableInfo:
    schema: str
    name: str
    columns: dict[str, str]  # {columna: tipo_postgres}
    not_null_columns: List[str]
    primary_key: List[str]
    unique_keys: List[List[str]]

    def pick_incremental_column(self, candidates: List[str]) -> Optional[str]:
        """Elige la primera columna candidata que exista en la tabla."""
        lower_cols = {c.lower(): c for c in self.columns}
        for candidate in candidates:
            real = lower_cols.get(candidate.lower())
            if real:
                return real
        return None

    def pick_effective_key(self) -> List[str]:
        """Devuelve PK o una UNIQUE constraint segura; si no, lista vacía."""
        if self.primary_key:
            return [c.lower() for c in self.primary_key]

        not_null = {c.lower() for c in self.not_null_columns}
        for key in sorted(self.unique_keys, key=lambda cols: (len(cols), cols)):
            lowered = [c.lower() for c in key]
            if lowered and all(c in not_null for c in lowered):
                return lowered
        return []


def discover_tables(cfg: DBConfig, excluded: List[str]) -> List[TableInfo]:
    """Lista todas las tablas de los esquemas configurados en una fuente."""
    excluded_set = {e.lower() for e in excluded}
    tables: List[TableInfo] = []

    conn = psycopg2.connect(
        host=cfg.host, port=cfg.port, dbname=cfg.dbname,
        user=cfg.user, password=cfg.password,
    )
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Tablas (no vistas) dentro de los esquemas indicados
            cur.execute(
                """
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_schema = ANY(%s)
                  AND table_type = 'BASE TABLE'
                ORDER BY table_schema, table_name
                """,
                (cfg.schemas,),
            )
            found = cur.fetchall()

            for row in found:
                schema, name = row["table_schema"], row["table_name"]
                key = f"{schema}.{name}".lower()
                if key in excluded_set:
                    log.info("Tabla excluida por configuración: %s", key)
                    continue

                cur.execute(
                    """
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                    ORDER BY ordinal_position
                    """,
                    (schema, name),
                )
                column_rows = cur.fetchall()
                cols = {r["column_name"]: r["data_type"] for r in column_rows}
                not_null_columns = [
                    r["column_name"] for r in column_rows if r["is_nullable"] == "NO"
                ]

                cur.execute(
                    """
                    SELECT kcu.column_name
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu
                      ON tc.constraint_name = kcu.constraint_name
                     AND tc.table_schema = kcu.table_schema
                     AND tc.table_name = kcu.table_name
                    WHERE tc.constraint_type = 'PRIMARY KEY'
                      AND tc.table_schema = %s
                      AND tc.table_name = %s
                    ORDER BY kcu.ordinal_position
                    """,
                    (schema, name),
                )
                pk_cols = [r["column_name"] for r in cur.fetchall()]

                cur.execute(
                    """
                    SELECT kcu.constraint_name, kcu.column_name
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu
                      ON tc.constraint_name = kcu.constraint_name
                     AND tc.table_schema = kcu.table_schema
                     AND tc.table_name = kcu.table_name
                    WHERE tc.constraint_type = 'UNIQUE'
                      AND tc.table_schema = %s
                      AND tc.table_name = %s
                    ORDER BY kcu.constraint_name, kcu.ordinal_position
                    """,
                    (schema, name),
                )
                unique_keys_map: dict[str, list[str]] = {}
                for row in cur.fetchall():
                    unique_keys_map.setdefault(row["constraint_name"], []).append(row["column_name"])
                unique_keys = list(unique_keys_map.values())

                tables.append(
                    TableInfo(
                        schema=schema,
                        name=name,
                        columns=cols,
                        not_null_columns=not_null_columns,
                        primary_key=pk_cols,
                        unique_keys=unique_keys,
                    )
                )
    finally:
        conn.close()

    log.info("Fuente %s: %d tablas descubiertas", cfg.alias, len(tables))
    return tables
