-- ============================================================
-- Cleaned customers table — standardized dob format
-- ============================================================

CREATE TABLE IF NOT EXISTS customers_cleaned (
    id            INT PRIMARY KEY,
    name          VARCHAR(100),
    dob           DATE          COMMENT 'Standardized to YYYY-MM-DD, NULL if placeholder/unknown',
    is_company    TINYINT(1)    DEFAULT 0 COMMENT '1 if entity is a company (PT, CV, etc.)',
    created_at    DATETIME(3),
    cleaned_at    DATETIME      DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
