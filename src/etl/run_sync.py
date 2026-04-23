"""Entrypoint principal: invocado desde Airflow o manualmente.

Uso:
    python -m etl.run_sync --source DB1              # sincroniza todas las tablas de DB1
    python -m etl.run_sync --source DB1 --schema esquema1
    python -m etl.run_sync --all                     # sincroniza todas las fuentes
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import uuid
from typing import List

from .config import AppConfig, DBConfig, load_config
from .discovery import TableInfo, discover_tables
from .metadata import MetadataStore
from .spark_utils import build_spark
from .syncer import TableSyncer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("etl.run_sync")


def _filter_tables(tables: List[TableInfo], schema: str | None) -> List[TableInfo]:
    if schema:
        return [t for t in tables if t.schema == schema]
    return tables


def run_for_source(
    spark, config: AppConfig, metadata: MetadataStore,
    source: DBConfig, schema_filter: str | None, run_id: str,
) -> dict:
    tables = discover_tables(source, config.etl.excluded_tables)
    tables = _filter_tables(tables, schema_filter)

    if not tables:
        log.warning("No hay tablas para sincronizar en %s", source.alias)
        return {"source": source.alias, "tables": 0, "ok": 0, "error": 0, "rows": 0}

    syncer = TableSyncer(spark, config, metadata)
    ok = err = total_rows = 0
    for t in tables:
        result = syncer.sync_table(source, t, run_id)
        if result.status == "ok":
            ok += 1
            total_rows += result.rows_loaded
        else:
            err += 1
    log.info(
        "Fuente %s: %d tablas, ok=%d, error=%d, filas=%d",
        source.alias, len(tables), ok, err, total_rows,
    )
    return {
        "source": source.alias, "tables": len(tables),
        "ok": ok, "error": err, "rows": total_rows,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", help="Alias de la fuente (ej: DB1)")
    parser.add_argument("--schema", help="Filtrar por esquema específico")
    parser.add_argument("--all", action="store_true", help="Todas las fuentes activas")
    parser.add_argument("--run-id", default=os.getenv("AIRFLOW_CTX_DAG_RUN_ID", ""),
                        help="ID del run (Airflow lo inyecta)")
    args = parser.parse_args()

    if not args.source and not args.all:
        parser.error("Debes pasar --source <ALIAS> o --all")

    run_id = args.run_id or f"manual_{uuid.uuid4().hex[:12]}"
    log.info("Iniciando sincronización run_id=%s", run_id)

    config = load_config()
    metadata = MetadataStore(config.central)
    spark = build_spark(f"pg-sync-{run_id}", config.spark)

    try:
        if args.all:
            targets = config.sources
        else:
            alias = args.source.lower()
            targets = [s for s in config.sources if s.alias == alias]
            if not targets:
                log.error("Fuente '%s' no encontrada. Activas: %s",
                          args.source, [s.alias for s in config.sources])
                return 2

        summaries = [
            run_for_source(spark, config, metadata, src, args.schema, run_id)
            for src in targets
        ]
    finally:
        spark.stop()

    total_err = sum(s["error"] for s in summaries)
    log.info("Resumen: %s", summaries)
    return 1 if total_err > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
