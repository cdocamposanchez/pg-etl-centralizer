"""DAG horario que sincroniza todas las fuentes Postgres -> base central.

Genera dinámicamente una tarea por fuente activa (definida en ACTIVE_SOURCES),
permitiendo paralelismo entre fuentes y aislamiento de errores.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def _env_from_file(key: str, env_file: str) -> str | None:
    if not env_file or not os.path.exists(env_file):
        return None
    with open(env_file, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            if k.strip() != key:
                continue
            value = v.strip()
            if (value.startswith('"') and value.endswith('"')) or (
                value.startswith("'") and value.endswith("'")
            ):
                value = value[1:-1]
            return value
    return None


def _active_sources() -> list[str]:
    env_file = os.getenv("RUNTIME_ENV_FILE", "/opt/airflow/.env")
    raw = _env_from_file("ACTIVE_SOURCES", env_file) or os.getenv("ACTIVE_SOURCES", "")
    return [a.strip() for a in raw.split(",") if a.strip()]


with DAG(
    dag_id="postgres_hourly_sync",
    description="Sincronización horaria Postgres -> base central con PySpark",
    start_date=datetime(2025, 1, 1),
    schedule="0 * * * *",          # cada hora en punto
    catchup=False,
    max_active_runs=1,             # no solapar corridas
    default_args=DEFAULT_ARGS,
    tags=["etl", "pyspark", "postgres"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule="all_done")

    sources = _active_sources()
    if not sources:
        # DAG "vacío" pero válido; dejará visible el problema en Airflow.
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
                execution_timeout=timedelta(minutes=55),  # margen antes de la próxima hora
            )
            start >> task >> end
