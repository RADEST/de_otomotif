# Walkthrough — Task 1: Data Landing Pipeline

## What Was Built

Pipeline Python harian untuk ingest file `customer_address_YYYYMMDD.csv` ke MySQL table `customer_addresses_raw`.

### Files Created

| File | Purpose |
|------|---------|
| [ddl_customer_addresses_raw.sql](file:///d:/astraworld/sql/ddl_customer_addresses_raw.sql) | DDL untuk membuat table landing di MySQL |
| [config.py](file:///d:/astraworld/data_landing/config.py) | Konfigurasi DB & path (env-var driven) |
| [ingest_customer_address.py](file:///d:/astraworld/data_landing/ingest_customer_address.py) | Main pipeline script |
| [requirements.txt](file:///d:/astraworld/data_landing/requirements.txt) | Dependencies (`pandas`, `mysql-connector-python`) |
| [customer_address_20260314.csv](file:///d:/astraworld/data_input/customer_address_20260314.csv) | Sample CSV data |

### Pipeline Flow

```mermaid
flowchart LR
    A["📂 data_input/"] -->|scan matching files| B["📄 Parse CSV\n(pandas)"]
    B -->|validate columns| C{"Valid?"}
    C -->|yes| D["🗄️ Upsert MySQL\nON DUPLICATE KEY UPDATE"]
    C -->|no| E["⚠️ Log & skip"]
    D --> F["📦 Archive file\nto data_archive/"]
```

### Key Features

- **Upsert** — `INSERT ... ON DUPLICATE KEY UPDATE` di kolom [id](file:///d:/astraworld/data_landing/ingest_customer_address.py#74-84), aman di-re-run
- **Archive** — file yang sudah diproses dipindah ke `data_archive/`
- **Audit trail** — kolom `source_file` dan `ingested_at` di table
- **Dry-run** — `--dry-run` flag untuk validasi tanpa koneksi MySQL
- **Config via env vars** — `MYSQL_HOST`, `MYSQL_USER`, dll.

## How to Run

```bash
# 1. Install dependencies
pip install -r data_landing/requirements.txt

# 2. Jalankan DDL di MySQL
mysql -u root -p astraworld < sql/ddl_customer_addresses_raw.sql

# 3. Taruh file CSV harian di data_input/
#    Format: customer_address_YYYYMMDD.csv

# 4. Jalankan pipeline
python data_landing/ingest_customer_address.py

# Atau dry-run (tanpa MySQL):
python data_landing/ingest_customer_address.py --dry-run
```

## Verification

Dry-run berhasil — pipeline mendeteksi 1 file, mem-parse 4 baris, exit code 0:

```
Files processed: 1/1  |  Total rows: 4
```
