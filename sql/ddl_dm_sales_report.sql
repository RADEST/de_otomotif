-- ============================================================
-- Datamart: Sales Report
-- Aggregated sales by period, price class, and model
-- ============================================================

CREATE TABLE IF NOT EXISTS dm_sales_report (
    periode   VARCHAR(7)    NOT NULL COMMENT 'Format YYYY-MM',
    class     VARCHAR(10)   NOT NULL COMMENT 'LOW / MEDIUM / HIGH',
    model     VARCHAR(50)   NOT NULL,
    total     BIGINT        NOT NULL COMMENT 'Total nominal penjualan (Rupiah)',
    PRIMARY KEY (periode, class, model)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
