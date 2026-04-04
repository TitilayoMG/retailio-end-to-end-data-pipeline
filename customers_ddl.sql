CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.customers (
    customer_id    TEXT PRIMARY KEY,
    name           TEXT,
    email          TEXT,
    phone          TEXT,
    signup_date    DATE,
    branch_id      INTEGER,
    branch_city    TEXT,
    region         TEXT,
    store_type     TEXT
);