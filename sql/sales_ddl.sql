CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.sales (
    id SERIAL PRIMARY KEY,
    sale_id        TEXT,
    customer_id    TEXT,
    product_id     TEXT,
    quantity       INTEGER,
    channel        TEXT,
    purchase_date  TIMESTAMP
);