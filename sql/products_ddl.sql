CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.products (
    product_id        TEXT PRIMARY KEY,
    name              TEXT,
    category          TEXT,
    subcategory       TEXT,
    brand             TEXT,
    price             NUMERIC(10,2),
    discount_percent  NUMERIC(5,2)
);