-- ============================================================
-- Cleaned sales table — price parsed to numeric
-- ============================================================

CREATE TABLE IF NOT EXISTS sales_cleaned (
    vin            VARCHAR(20) PRIMARY KEY,
    customer_id    INT,
    model          VARCHAR(50),
    invoice_date   DATE,
    price_raw      VARCHAR(20)   COMMENT 'Original string price from source',
    price_numeric  BIGINT        COMMENT 'Parsed numeric price in Rupiah',
    created_at     DATETIME(3),
    cleaned_at     DATETIME      DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
