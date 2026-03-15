# Task 2: Datamart — Data Cleaning & Report Tables

## 2a — Data Quality Issues Found

| Table | Column | Issue | Fix |
|-------|--------|-------|-----|
| `customers_raw` | `dob` | Format tidak konsisten: `1998-08-04`, `1980/11/15`, `14/01/1995` | Standardisasi ke `YYYY-MM-DD` |
| `customers_raw` | `dob` | `1900-01-01` → kemungkinan placeholder | Set `NULL` |
| `customers_raw` | `dob` | `NULL` untuk entity non-person (PT) | Biarkan `NULL` |
| `sales_raw` | `price` | Format Indonesia `350.000.000` (string, pakai titik) | Parse ke integer |
| `customer_addresses_raw` | `city` | Inkonsisten casing: `JAKARTA PUSAT` vs `Jakarta Utara` | Standardisasi title case |
| `customer_addresses_raw` | `province` | Inkonsisten casing: `DKI JAKARTA` vs `DKI Jakarta` | Standardisasi upper case |

## Proposed Changes

### Project Structure (new files)

```
d:\astraworld\
├── sql/
│   ├── ddl_customer_addresses_raw.sql      # existing
│   ├── ddl_seed_raw_tables.sql             # [NEW] seed raw tables with sample data
│   ├── ddl_customers_cleaned.sql           # [NEW] cleaned customers
│   ├── ddl_sales_cleaned.sql               # [NEW] cleaned sales
│   ├── ddl_dm_sales_report.sql             # [NEW] datamart 1
│   └── ddl_dm_service_report.sql           # [NEW] datamart 2
├── data_landing/                           # existing
└── datamart/
    ├── config.py                           # [NEW] shared config (symlink/import)
    ├── clean_data.py                       # [NEW] 2a — cleaning script
    ├── build_datamart.py                   # [NEW] 2b — daily datamart job
    └── requirements.txt                    # [NEW]
```

---

### Seed Data

#### [NEW] ddl_seed_raw_tables.sql

Insert sample data dari [project.md](file:///d:/astraworld/project.md) ke `customers_raw`, `sales_raw`, `after_sales_raw` agar bisa ditest.

---

### 2a — Data Cleaning

#### [NEW] clean_data.py

Script Python yang:
1. Baca `customers_raw` → fix `dob` format → tulis ke `customers_cleaned`
2. Baca `sales_raw` → parse `price` string ke integer → tulis ke `sales_cleaned`
3. Baca `customer_addresses_raw` → standardisasi casing city/province → update in-place

Semua cleaning pakai `INSERT ... ON DUPLICATE KEY UPDATE` (idempotent, aman di-rerun).

---

### 2b — Datamart Reports

#### [NEW] build_datamart.py

**Datamart 1: `dm_sales_report`** — full rebuild daily

```sql
SELECT
    DATE_FORMAT(s.invoice_date, '%Y-%m') AS periode,
    CASE
        WHEN s.price_numeric BETWEEN 100000000 AND 250000000 THEN 'LOW'
        WHEN s.price_numeric BETWEEN 250000001 AND 400000000 THEN 'MEDIUM'
        WHEN s.price_numeric > 400000000 THEN 'HIGH'
    END AS class,
    s.model,
    SUM(s.price_numeric) AS total
FROM sales_cleaned s
GROUP BY periode, class, model
```

**Datamart 2: `dm_service_report`** — full rebuild daily

```sql
SELECT
    YEAR(a.service_date) AS periode,
    a.vin,
    c.name AS customer_name,
    ca.address,
    COUNT(*) AS count_service,
    CASE
        WHEN COUNT(*) > 10 THEN 'HIGH'
        WHEN COUNT(*) BETWEEN 5 AND 10 THEN 'MED'
        ELSE 'LOW'
    END AS priority
FROM after_sales_raw a
JOIN customers_raw c ON c.id = a.customer_id
LEFT JOIN customer_addresses_raw ca ON ca.customer_id = a.customer_id
GROUP BY periode, a.vin, c.name, ca.address
```

---

## Verification Plan

### Automated Tests
1. Jalankan seed data → `clean_data.py` → `build_datamart.py`
2. Query `SELECT * FROM dm_sales_report` dan `dm_service_report`
3. Validasi output sesuai format yang diminta
