# Walkthrough — Task 1 & 2

## Task 1: Data Landing ✅

Pipeline Python untuk ingest `customer_address_YYYYMMDD.csv` → MySQL `customer_addresses_raw`.

| File | Fungsi |
|------|--------|
| [ddl_customer_addresses_raw.sql](file:///d:/astraworld/sql/ddl_customer_addresses_raw.sql) | DDL landing table |
| [config.py](file:///d:/astraworld/data_landing/config.py) | Konfigurasi DB & path |
| [ingest_customer_address.py](file:///d:/astraworld/data_landing/ingest_customer_address.py) | Pipeline: scan → parse → upsert → archive |

---

## Task 2a: Data Cleaning ✅

### Issues Ditemukan & Fixes

| Table | Issue | Fix |
|-------|-------|-----|
| `customers_raw.dob` | Format campur: `1998-08-04`, `1980/11/15`, `14/01/1995` | Standardisasi `YYYY-MM-DD` |
| `customers_raw.dob` | `1900-01-01` placeholder | → `NULL` |
| `customers_raw.name` | Entitas perusahaan (PT) | Flag `is_company = 1` |
| `sales_raw.price` | String `350.000.000` | Parse ke integer `350000000` |
| `customer_addresses_raw` | Casing: `JAKARTA PUSAT` vs `Jakarta Utara` | Standardisasi title/upper case |

| File | Fungsi |
|------|--------|
| [ddl_seed_raw_tables.sql](file:///d:/astraworld/sql/ddl_seed_raw_tables.sql) | CREATE TABLE + INSERT seed data |
| [ddl_customers_cleaned.sql](file:///d:/astraworld/sql/ddl_customers_cleaned.sql) | Cleaned customers DDL |
| [ddl_sales_cleaned.sql](file:///d:/astraworld/sql/ddl_sales_cleaned.sql) | Cleaned sales DDL |
| [clean_data.py](file:///d:/astraworld/datamart/clean_data.py) | Script: baca raw → clean → tulis cleaned tables |

---

## Task 2b: Datamart Reports ✅

### dm_sales_report

```sql
-- Aggregasi penjualan per periode, class, model
SELECT
  DATE_FORMAT(invoice_date, '%Y-%m') AS periode,
  CASE
    WHEN price_numeric BETWEEN 100M-250M THEN 'LOW'
    WHEN price_numeric BETWEEN 250M-400M THEN 'MEDIUM'
    WHEN price_numeric > 400M            THEN 'HIGH'
  END AS class,
  model,
  SUM(price_numeric) AS total
FROM sales_cleaned
GROUP BY periode, class, model
```

### dm_service_report

```sql
-- Frekuensi servis per tahun per VIN
SELECT
  YEAR(service_date) AS periode,
  vin, customer_name, address,
  COUNT(*) AS count_service,
  CASE
    WHEN COUNT(*) > 10         THEN 'HIGH'
    WHEN COUNT(*) BETWEEN 5-10 THEN 'MED'
    ELSE 'LOW'
  END AS priority
FROM after_sales_raw JOIN customers_cleaned ...
GROUP BY periode, vin, customer_name, address
```

| File | Fungsi |
|------|--------|
| [ddl_dm_sales_report.sql](file:///d:/astraworld/sql/ddl_dm_sales_report.sql) | DDL datamart 1 |
| [ddl_dm_service_report.sql](file:///d:/astraworld/sql/ddl_dm_service_report.sql) | DDL datamart 2 |
| [build_datamart.py](file:///d:/astraworld/datamart/build_datamart.py) | Daily job: truncate & reload kedua datamart |

---

## Verification ✅

Semua ditest end-to-end terhadap MySQL lokal:

```
1. SQL seed data → OK (3 raw tables terisi)
2. clean_data.py → OK (3 tables di-clean)
3. build_datamart.py → OK (2 datamart tables terisi)
4. SELECT queries → data sesuai format yang diminta
```

## Cara Run

```bash
# Setup (sekali)
pip install -r datamart/requirements.txt

# 1. Buat table-table cleaned & datamart (dari d:\astraworld)
Get-Content sql/ddl_customers_cleaned.sql | & "C:\Program Files\MySQL\MySQL Server 8.0\bin\mysql.exe" -u root -proot astraworld
Get-Content sql/ddl_sales_cleaned.sql | & "C:\Program Files\MySQL\MySQL Server 8.0\bin\mysql.exe" -u root -proot astraworld
Get-Content sql/ddl_dm_sales_report.sql | & "C:\Program Files\MySQL\MySQL Server 8.0\bin\mysql.exe" -u root -proot astraworld
Get-Content sql/ddl_dm_service_report.sql | & "C:\Program Files\MySQL\MySQL Server 8.0\bin\mysql.exe" -u root -proot astraworld
Get-Content sql/ddl_seed_raw_tables.sql | & "C:\Program Files\MySQL\MySQL Server 8.0\bin\mysql.exe" -u root -proot astraworld

# Daily pipeline
# 2. Run cleaning (2a)
python datamart/clean_data.py

# 3. Run datamart builder (2b)
python datamart/build_datamart.py
```
