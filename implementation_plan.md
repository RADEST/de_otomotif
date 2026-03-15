# Task 1: Data Landing — Customer Address CSV to MySQL Pipeline

Pipeline harian untuk ingest file `customer_address_yyyymmdd.csv` ke MySQL table `customer_addresses_raw`.

## Proposed Changes

### Project Structure

```
d:\astraworld\
├── data_landing/
│   ├── ingest_customer_address.py   # Main pipeline script
│   ├── config.py                    # DB & path configuration
│   └── requirements.txt             # Dependencies
├── data_input/                      # Folder tempat CSV harian di-drop
│   └── customer_address_20260314.csv  # Sample file
├── data_archive/                    # CSV yang sudah diproses dipindahkan ke sini
└── sql/
    └── ddl_customer_addresses_raw.sql # CREATE TABLE script
```

---

### SQL DDL

#### [NEW] [ddl_customer_addresses_raw.sql](file:///d:/astraworld/sql/ddl_customer_addresses_raw.sql)

```sql
CREATE TABLE IF NOT EXISTS customer_addresses_raw (
    id            INT PRIMARY KEY,
    customer_id   INT NOT NULL,
    address       VARCHAR(255),
    city          VARCHAR(100),
    province      VARCHAR(100),
    created_at    DATETIME(3),
    source_file   VARCHAR(100),    -- track asal file
    ingested_at   DATETIME DEFAULT CURRENT_TIMESTAMP
);
```

- Kolom `source_file` untuk audit trail (nama file CSV sumber).
- Kolom `ingested_at` untuk timestamp kapan data masuk.

---

### Config

#### [NEW] [config.py](file:///d:/astraworld/data_landing/config.py)

- Konfigurasi koneksi MySQL (host, port, user, password, database) — dibaca dari environment variable dengan default fallback.
- Path `DATA_INPUT_DIR` dan `DATA_ARCHIVE_DIR`.
- Pattern regex untuk validasi nama file: `customer_address_\d{8}\.csv`.

---

### Pipeline Script

#### [NEW] [ingest_customer_address.py](file:///d:/astraworld/data_landing/ingest_customer_address.py)

Alur kerja:

1. **Scan** folder `data_input/` untuk file yang cocok pattern `customer_address_*.csv`.
2. **Parse** CSV menggunakan `pandas`.
3. **Validasi** kolom wajib ada: `id, customer_id, address, city, province, created_at`.
4. **Insert** ke MySQL menggunakan `INSERT ... ON DUPLICATE KEY UPDATE` (upsert pada `id`) — sehingga re-run aman tanpa duplikasi.
5. **Archive** — pindahkan file yang sudah berhasil ke `data_archive/`.
6. **Logging** — semua langkah tercatat di console log.

Dependency: `pandas`, `mysql-connector-python`.

---

### Sample CSV

#### [NEW] [customer_address_20260314.csv](file:///d:/astraworld/data_input/customer_address_20260314.csv)

File sample berisi 4 baris data sesuai contoh di [project.md](file:///d:/astraworld/project.md).

---

### Requirements

#### [NEW] [requirements.txt](file:///d:/astraworld/data_landing/requirements.txt)

```
pandas
mysql-connector-python
python-dotenv
```

---

## Verification Plan

### Automated Tests

1. **Dry-run tanpa MySQL** — jalankan script dengan flag `--dry-run` yang hanya membaca CSV, memvalidasi kolom, dan mencetak data ke console tanpa koneksi DB:
   ```
   cd d:\astraworld
   python data_landing/ingest_customer_address.py --dry-run
   ```
   Expected: script membaca sample CSV, print 4 rows, tidak error.

### Manual Verification

1. Jika ada MySQL berjalan lokal, jalankan DDL lalu run pipeline biasa:
   ```
   python data_landing/ingest_customer_address.py
   ```
2. Cek data di MySQL: `SELECT * FROM customer_addresses_raw;`
3. Cek file sudah pindah ke `data_archive/`.

> [!NOTE]
> Untuk testing tanpa MySQL, mode `--dry-run` sudah cukup memvalidasi bahwa pipeline berjalan benar.
