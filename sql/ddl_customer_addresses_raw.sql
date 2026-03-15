-- ============================================================
-- DDL: customer_addresses_raw
-- Data Landing table for daily customer_address CSV ingestion
-- ============================================================

CREATE TABLE IF NOT EXISTS customer_addresses_raw (
    id            INT            PRIMARY KEY,
    customer_id   INT            NOT NULL,
    address       VARCHAR(255),
    city          VARCHAR(100),
    province      VARCHAR(100),
    created_at    DATETIME(3),
    source_file   VARCHAR(100)   COMMENT 'Nama file CSV sumber data',
    ingested_at   DATETIME       DEFAULT CURRENT_TIMESTAMP COMMENT 'Timestamp saat data di-ingest'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
