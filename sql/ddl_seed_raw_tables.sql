-- ============================================================
-- Seed data for raw tables (sample from project.md)
-- Run this ONCE to populate tables for testing
-- ============================================================

-- ── customers_raw ───────────────────────────────────────────
CREATE TABLE IF NOT EXISTS customers_raw (
    id         INT PRIMARY KEY,
    name       VARCHAR(100),
    dob        VARCHAR(20),
    created_at DATETIME(3)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO customers_raw (id, name, dob, created_at) VALUES
(1, 'Antonio',       '1998-08-04', '2025-03-01 14:24:40.012'),
(2, 'Brandon',       '2001-04-21', '2025-03-02 08:12:54.003'),
(3, 'Charlie',       '1980/11/15', '2025-03-02 11:20:02.391'),
(4, 'Dominikus',     '14/01/1995', '2025-03-03 09:50:41.852'),
(5, 'Erik',          '1900-01-01', '2025-03-03 17:22:03.198'),
(6, 'PT Black Bird',  NULL,        '2025-03-04 12:52:16.122')
ON DUPLICATE KEY UPDATE
    name = VALUES(name), dob = VALUES(dob), created_at = VALUES(created_at);

-- ── sales_raw ───────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS sales_raw (
    vin          VARCHAR(20) PRIMARY KEY,
    customer_id  INT,
    model        VARCHAR(50),
    invoice_date DATE,
    price        VARCHAR(20),
    created_at   DATETIME(3)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO sales_raw (vin, customer_id, model, invoice_date, price, created_at) VALUES
('JIS8135SAD', 1, 'RAIZA',  '2025-03-01', '350.000.000', '2025-03-01 14:24:40.012'),
('MAS8160POE', 3, 'RANGGO', '2025-05-19', '430.000.000', '2025-05-19 14:29:21.003'),
('JLK1368KDE', 4, 'INNAVO', '2025-05-22', '600.000.000', '2025-05-22 16:10:28.120'),
('JLK1869KDF', 6, 'VELOS',  '2025-08-02', '390.000.000', '2025-08-02 14:04:31.021'),
('JLK1962KOP', 6, 'VELOS',  '2025-08-02', '390.000.000', '2025-08-02 15:21:04.201')
ON DUPLICATE KEY UPDATE
    customer_id = VALUES(customer_id), model = VALUES(model),
    invoice_date = VALUES(invoice_date), price = VALUES(price),
    created_at = VALUES(created_at);

-- ── after_sales_raw ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS after_sales_raw (
    service_ticket VARCHAR(20) PRIMARY KEY,
    vin            VARCHAR(20),
    customer_id    INT,
    model          VARCHAR(50),
    service_date   DATE,
    service_type   VARCHAR(10),
    created_at     DATETIME(3)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO after_sales_raw (service_ticket, vin, customer_id, model, service_date, service_type, created_at) VALUES
('T124-kgu1', 'MAS8160POE', 3, 'RANGGO', '2025-07-11', 'BP', '2025-07-11 09:24:40.012'),
('T560-jga1', 'JLK1368KDE', 4, 'INNAVO', '2025-08-04', 'PM', '2025-08-04 10:12:54.003'),
('T521-oai8', 'POI1059IIK', 5, 'RAIZA',  '2026-09-10', 'GR', '2026-09-10 12:45:02.391')
ON DUPLICATE KEY UPDATE
    vin = VALUES(vin), customer_id = VALUES(customer_id), model = VALUES(model),
    service_date = VALUES(service_date), service_type = VALUES(service_type),
    created_at = VALUES(created_at);
