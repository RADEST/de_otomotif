# Task 4: Docker Compose Environment

Package semua pipeline (Task 1-3) ke dalam docker-compose yang bisa di-run satu perintah.

## Architecture

```
docker-compose up
  ├── mysql       (MySQL 8.0, auto-init DDL + seed data)
  └── pipeline    (Python 3.11, runs: ingest → clean → build_datamart)
```

## Proposed Changes

### [NEW] docker-compose.yml

```yaml
services:
  mysql:
    image: mysql:8.0
    volumes:
      - ./sql/init.sql:/docker-entrypoint-initdb.d/init.sql
      - mysql_data:/var/lib/mysql
    environment: from .env
    healthcheck: mysqladmin ping

  pipeline:
    build: .
    depends_on: mysql (healthy)
    volumes:
      - ./data_input:/app/data_input
      - ./data_archive:/app/data_archive
    environment: from .env
    command: python run_pipeline.py
```

---

### [NEW] Dockerfile

Python 3.11 slim, install dependencies, copy semua code.

---

### [NEW] sql/init.sql

Gabungan semua DDL + seed data dalam satu file (supaya MySQL auto-init saat container pertama kali jalan).

---

### [NEW] run_pipeline.py

Entrypoint: wait for MySQL ready → ingest CSV → clean data → build datamart. Satu script yang orchestrate semua step.

---

### [NEW] .env

```
MYSQL_ROOT_PASSWORD=root
MYSQL_DATABASE=astraworld
```

---

### [NEW] .gitignore, [NEW] README.md

---

### [NEW] data_input/customer_address_20260314.csv

Re-create sample CSV (yang tadi sudah di-archive).

## Verification

```bash
docker-compose up --build
# Expected: MySQL init → pipeline runs → data in all tables
docker-compose exec mysql mysql -uroot -proot astraworld -e "SELECT * FROM dm_sales_report;"
```
