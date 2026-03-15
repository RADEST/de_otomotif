# Walkthrough — Task 1, 2 & 3

## Task 1: Data Landing ✅
Pipeline: `customer_address_YYYYMMDD.csv` → MySQL `customer_addresses_raw`

| File | Fungsi |
|------|--------|
| [ingest_customer_address.py](file:///d:/astraworld/data_landing/ingest_customer_address.py) | Scan → parse → upsert → archive |
| [config.py](file:///d:/astraworld/data_landing/config.py) | Konfigurasi DB & path |
| [ddl_customer_addresses_raw.sql](file:///d:/astraworld/sql/ddl_customer_addresses_raw.sql) | DDL landing table |

---

## Task 2: Datamart ✅

**2a — Cleaning** ([clean_data.py](file:///d:/astraworld/datamart/clean_data.py)):
- [dob](file:///d:/astraworld/datamart/clean_data.py#75-96) → standardisasi mixed formats ke `YYYY-MM-DD`, placeholder → NULL, detect company
- [price](file:///d:/astraworld/datamart/clean_data.py#118-128) → parse `350.000.000` → `350000000`
- `city/province` → standardisasi casing

**2b — Reports** ([build_datamart.py](file:///d:/astraworld/datamart/build_datamart.py)):
- `dm_sales_report` → per YYYY-MM, class LOW/MEDIUM/HIGH, model, total
- `dm_service_report` → per YYYY, vin, customer, count_service, priority

---

## Task 3: Data Warehouse Design ✅

Desain 3-level DW architecture:

![Level 1 Raw + Level 2 Cleaned + Level 3 Datamart — bagian atas](file:///C:/Users/LENOVO/.gemini/antigravity/brain/a50e06b6-5bcf-4472-af93-fc7f7a64835c/dw_architecture_top_v2_1773535621288.png)

![Level 2 Cleaned + Level 3 Datamart + Legend + Pipeline — bagian bawah](file:///C:/Users/LENOVO/.gemini/antigravity/brain/a50e06b6-5bcf-4472-af93-fc7f7a64835c/dw_architecture_bottom_1773535631297.png)

HTML file interaktif: [dw_architecture.html](file:///d:/astraworld/design/dw_architecture.html)

### Arsitektur

| Level | Tables | Deskripsi |
|-------|--------|-----------|
| **L1 — Raw** | `customers_raw`, `sales_raw`, `after_sales_raw`, `customer_addresses_raw` | Data as-is dari sumber |
| **L2 — Cleaned** | `customers_cleaned`, `sales_cleaned` | Format standar, tipe data benar |
| **L3 — Datamart** | `dm_sales_report`, `dm_service_report` | Aggregated report, daily rebuild |

### Daily Pipeline Order
```
① ingest_customer_address.py  →  ② clean_data.py  →  ③ build_datamart.py
```
