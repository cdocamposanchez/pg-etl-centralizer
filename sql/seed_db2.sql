CREATE SCHEMA IF NOT EXISTS ventas;

CREATE TABLE public.facturas (
    id            SERIAL PRIMARY KEY,
    numero        TEXT UNIQUE,
    total         NUMERIC(12,2),
    fecha         DATE,
    updated_at    TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO public.facturas (numero, total, fecha) VALUES
  ('F-0001', 120.50, CURRENT_DATE),
  ('F-0002',  87.00, CURRENT_DATE);

CREATE TABLE ventas.detalle (
    id            SERIAL PRIMARY KEY,
    factura_id    INT,
    item          TEXT,
    cantidad      INT,
    subtotal      NUMERIC(12,2),
    created_at    TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO ventas.detalle (factura_id, item, cantidad, subtotal) VALUES
  (1, 'Teclado', 1, 89.90),
  (1, 'Mouse',   1, 30.60),
  (2, 'Monitor', 1, 87.00);
