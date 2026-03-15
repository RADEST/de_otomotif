-- ============================================================
-- Datamart: Service Report
-- Customer service frequency with priority classification
-- ============================================================

CREATE TABLE IF NOT EXISTS dm_service_report (
    periode        VARCHAR(4)    NOT NULL COMMENT 'Format YYYY',
    vin            VARCHAR(20)   NOT NULL,
    customer_name  VARCHAR(100),
    address        VARCHAR(255),
    count_service  INT           NOT NULL,
    priority       VARCHAR(10)   NOT NULL COMMENT 'HIGH / MED / LOW',
    PRIMARY KEY (periode, vin)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
