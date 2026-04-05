CREATE TABLE IF NOT EXISTS ingestion_tracker (
    file_name TEXT PRIMARY KEY,
    dataset TEXT,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);