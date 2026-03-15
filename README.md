# Maju Jaya — Data Warehouse Pipeline

Data engineering pipeline untuk perusahaan retail otomotif "Maju Jaya".  
Meliputi **data landing**, **data cleaning**, dan **datamart** dalam satu Docker Compose environment.

## Architecture

```
Level 1 (Raw/Landing)     →  Level 2 (Cleaned/Staging)  →  Level 3 (Datamart)
─────────────────────        ────────────────────────       ──────────────────
customers_raw                customers_cleaned               dm_sales_report
sales_raw                    sales_cleaned                   dm_service_report
after_sales_raw
customer_addresses_raw
```

## Quick Start

### Dengan Docker (Recommended)

```bash
# Clone repository
git clone <repo-url>
cd astraworld

# Jalankan semua services
docker-compose up --build

# Setelah pipeline selesai, cek data:
docker-compose exec mysql mysql -uroot -proot astraworld -e "SELECT * FROM dm_sales_report;"
docker-compose exec mysql mysql -uroot -proot astraworld -e "SELECT * FROM dm_service_report;"

# Cleanup
docker-compose down -v
```

### Tanpa Docker (Manual)

```bash
# Install dependencies
pip install pandas mysql-connector-python python-dotenv

# Setup MySQL
mysql -u root -p -e "CREATE DATABASE IF NOT EXISTS astraworld;"
mysql -u root -p astraworld < sql/init.sql

# Jalankan pipeline
python run_pipeline.py
```

## Project Structure

```
astraworld/
├── docker-compose.yml          # Docker Compose setup
├── Dockerfile                  # Python pipeline image
├── .env                        # Environment variables
├── run_pipeline.py             # Orchestrator (ingest → clean → datamart)
│
├── data_landing/               # Task 1: CSV Ingestion
│   ├── config.py               # DB & path configuration
│   ├── ingest_customer_address.py
│   └── requirements.txt
│
├── datamart/                   # Task 2: Cleaning & Reports
│   ├── clean_data.py           # 2a: Data cleaning
│   └── build_datamart.py       # 2b: Datamart builder
│
├── sql/                        # DDL & seed data
│   ├── init.sql                # Combined init (Docker)
│   ├── ddl_customer_addresses_raw.sql
│   ├── ddl_customers_cleaned.sql
│   ├── ddl_sales_cleaned.sql
│   ├── ddl_dm_sales_report.sql
│   ├── ddl_dm_service_report.sql
│   └── ddl_seed_raw_tables.sql
│
├── data_input/                 # Drop CSV files here
│   └── customer_address_20260314.csv
├── data_archive/               # Processed files moved here
│
├── design/                     # Task 3: DW Architecture
│   └── dw_architecture.html    # Visual design (open in browser)
│
└── README.md
```

## Pipeline Steps

| Step | Script | Fungsi |
|------|--------|--------|
| 1 | `ingest_customer_address.py` | Ingest CSV harian → `customer_addresses_raw` |
| 2 | `clean_data.py` | Clean raw data: standardize dob, parse price, fix casing |
| 3 | `build_datamart.py` | Build `dm_sales_report` + `dm_service_report` |

## Data Cleaning (Task 2a)

| Issue | Fix |
|-------|-----|
| `customers_raw.dob` — format campur | Standardisasi `YYYY-MM-DD` |
| `customers_raw.dob` — `1900-01-01` placeholder | → `NULL` |
| `sales_raw.price` — string `350.000.000` | Parse ke integer `350000000` |
| `customer_addresses_raw` — casing tidak konsisten | Title/Upper case |

## Datamart Reports (Task 2b)

**dm_sales_report**: Penjualan per bulan per kelas harga

| Klasifikasi | Range |
|-------------|-------|
| LOW | Rp 100jt — 250jt |
| MEDIUM | Rp 250jt — 400jt |
| HIGH | > Rp 400jt |

**dm_service_report**: Frekuensi servis per tahun

| Priority | Frekuensi |
|----------|-----------|
| HIGH | > 10x servis |
| MED | 5 — 10x servis |
| LOW | < 5x servis |
