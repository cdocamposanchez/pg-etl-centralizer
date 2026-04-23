-- Semilla de ejemplo para db1: tres esquemas con datos variados
CREATE SCHEMA IF NOT EXISTS esquema1;
CREATE SCHEMA IF NOT EXISTS esquema2;
CREATE SCHEMA IF NOT EXISTS esquema3;

-- Tabla con columna updated_at (se sincronizará incrementalmente)
CREATE TABLE esquema1.clientes (
    id           SERIAL PRIMARY KEY,
    nombre       TEXT NOT NULL,
    email        TEXT,
    created_at   TIMESTAMPTZ DEFAULT NOW(),
    updated_at   TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO esquema1.clientes (nombre, email) VALUES
  ('Ana Torres',    'ana@example.com'),
  ('Luis Pérez',    'luis@example.com'),
  ('María Gómez',   'maria@example.com');

-- Tabla con solo id (incremental por id)
CREATE TABLE esquema2.productos (
    id       SERIAL PRIMARY KEY,
    sku      TEXT UNIQUE,
    nombre   TEXT,
    precio   NUMERIC(10,2)
);

INSERT INTO esquema2.productos (sku, nombre, precio) VALUES
  ('SKU-001', 'Teclado', 89.90),
  ('SKU-002', 'Mouse',   29.90);

-- Tabla sin columna incremental (full refresh)
CREATE TABLE esquema3.configuracion (
    clave   TEXT PRIMARY KEY,
    valor   TEXT
);

INSERT INTO esquema3.configuracion VALUES
  ('moneda',   'USD'),
  ('timezone', 'America/Bogota');
