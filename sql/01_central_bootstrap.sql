-- ==========================================================
-- Bootstrap de la base central
-- Se ejecuta una sola vez al iniciar el contenedor postgres-central
-- ==========================================================

-- Esquema interno para metadatos del ETL
CREATE SCHEMA IF NOT EXISTS etl_meta;

-- Tabla de control: lleva registro de cada tabla sincronizada
CREATE TABLE IF NOT EXISTS etl_meta.sync_state (
    id                  BIGSERIAL PRIMARY KEY,
    source_alias        TEXT        NOT NULL,   -- ej: db1
    source_schema       TEXT        NOT NULL,   -- ej: esquema1
    source_table        TEXT        NOT NULL,   -- ej: clientes
    target_table        TEXT        NOT NULL,   -- ej: db1_esquema1_clientes
    mode                TEXT        NOT NULL,   -- 'incremental' | 'full_refresh'
    incremental_column  TEXT,                   -- columna usada para incremental (si aplica)
    last_value          TEXT,                   -- último valor sincronizado (timestamp o id, como texto)
    last_run_at         TIMESTAMPTZ,
    last_status         TEXT,                   -- 'ok' | 'error'
    last_error          TEXT,
    rows_loaded_last    BIGINT DEFAULT 0,
    rows_loaded_total   BIGINT DEFAULT 0,
    UNIQUE (source_alias, source_schema, source_table)
);

CREATE INDEX IF NOT EXISTS idx_sync_state_alias
    ON etl_meta.sync_state (source_alias);

-- Tabla de log por ejecución (histórico)
CREATE TABLE IF NOT EXISTS etl_meta.sync_runs (
    id              BIGSERIAL PRIMARY KEY,
    run_id          TEXT        NOT NULL,       -- identificador del DAG run de Airflow
    source_alias    TEXT        NOT NULL,
    source_schema   TEXT        NOT NULL,
    source_table    TEXT        NOT NULL,
    target_table    TEXT        NOT NULL,
    mode            TEXT        NOT NULL,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at     TIMESTAMPTZ,
    status          TEXT,
    rows_loaded     BIGINT DEFAULT 0,
    error_message   TEXT
);

CREATE INDEX IF NOT EXISTS idx_sync_runs_run_id
    ON etl_meta.sync_runs (run_id);

-- Esquema donde viven las tablas sincronizadas (opcional: las dejamos en public)
-- pero creamos un esquema dedicado por claridad
CREATE SCHEMA IF NOT EXISTS synced;
