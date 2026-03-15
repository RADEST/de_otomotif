"""
Datamart Builder — Task 2b
===========================
Daily job that rebuilds two datamart tables from cleaned data:

  1. dm_sales_report    — sales aggregated by period, price class, model
  2. dm_service_report  — service frequency per customer/vin with priority

Usage:
    python datamart/build_datamart.py              # run against MySQL
    python datamart/build_datamart.py --dry-run    # preview query results
"""

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd

# ── Config ───────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "data_landing"))
import config  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ── SQL Queries ──────────────────────────────────────────────

QUERY_SALES_REPORT = """
SELECT
    DATE_FORMAT(s.invoice_date, '%%Y-%%m')  AS periode,
    CASE
        WHEN s.price_numeric BETWEEN 100000000 AND 250000000  THEN 'LOW'
        WHEN s.price_numeric BETWEEN 250000001 AND 400000000  THEN 'MEDIUM'
        WHEN s.price_numeric > 400000000                      THEN 'HIGH'
        ELSE 'UNKNOWN'
    END                                     AS class,
    s.model,
    SUM(s.price_numeric)                    AS total
FROM sales_cleaned s
GROUP BY periode, class, s.model
ORDER BY periode, class, s.model
"""

QUERY_SERVICE_REPORT = """
SELECT
    CAST(YEAR(a.service_date) AS CHAR)      AS periode,
    a.vin,
    c.name                                  AS customer_name,
    COALESCE(ca.address, '-')               AS address,
    COUNT(*)                                AS count_service,
    CASE
        WHEN COUNT(*) > 10                  THEN 'HIGH'
        WHEN COUNT(*) BETWEEN 5 AND 10      THEN 'MED'
        ELSE 'LOW'
    END                                     AS priority
FROM after_sales_raw a
    JOIN customers_cleaned c            ON c.id = a.customer_id
    LEFT JOIN customer_addresses_raw ca ON ca.customer_id = a.customer_id
GROUP BY periode, a.vin, c.name, ca.address
ORDER BY periode, a.vin
"""


# ── DB helper ────────────────────────────────────────────────

def get_connection():
    import mysql.connector
    return mysql.connector.connect(
        host=config.MYSQL_HOST,
        port=config.MYSQL_PORT,
        user=config.MYSQL_USER,
        password=config.MYSQL_PASSWORD,
        database=config.MYSQL_DATABASE,
    )


# ── Build functions ──────────────────────────────────────────

def build_sales_report(dry_run: bool = False):
    """Rebuild dm_sales_report from sales_cleaned."""
    logger.info("[1/2] Building dm_sales_report")

    conn = get_connection()
    df = pd.read_sql(QUERY_SALES_REPORT, conn)
    logger.info("  Query returned %d rows", len(df))

    if dry_run:
        print("\n── dm_sales_report preview ──")
        print(df.to_string(index=False))
        print()
        conn.close()
        return

    cursor = conn.cursor()

    # Truncate and reload (full rebuild)
    cursor.execute("TRUNCATE TABLE dm_sales_report")

    insert_sql = """
        INSERT INTO dm_sales_report (periode, class, model, total)
        VALUES (%s, %s, %s, %s)
    """
    for _, row in df.iterrows():
        cursor.execute(insert_sql, (
            row["periode"], row["class"], row["model"], int(row["total"])
        ))

    conn.commit()
    cursor.close()
    conn.close()
    logger.info("  ✓ Loaded %d rows into dm_sales_report", len(df))


def build_service_report(dry_run: bool = False):
    """Rebuild dm_service_report from after_sales_raw + customers_cleaned."""
    logger.info("[2/2] Building dm_service_report")

    conn = get_connection()
    df = pd.read_sql(QUERY_SERVICE_REPORT, conn)
    logger.info("  Query returned %d rows", len(df))

    if dry_run:
        print("\n── dm_service_report preview ──")
        print(df.to_string(index=False))
        print()
        conn.close()
        return

    cursor = conn.cursor()

    # Truncate and reload (full rebuild)
    cursor.execute("TRUNCATE TABLE dm_service_report")

    insert_sql = """
        INSERT INTO dm_service_report
            (periode, vin, customer_name, address, count_service, priority)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    for _, row in df.iterrows():
        cursor.execute(insert_sql, (
            row["periode"], row["vin"], row["customer_name"],
            row["address"], int(row["count_service"]), row["priority"]
        ))

    conn.commit()
    cursor.close()
    conn.close()
    logger.info("  ✓ Loaded %d rows into dm_service_report", len(df))


# ── Main ─────────────────────────────────────────────────────

def run(dry_run: bool = False):
    logger.info("=" * 60)
    logger.info("Datamart Builder — Task 2b (Daily Job)")
    logger.info("=" * 60)
    logger.info("Dry-run: %s", dry_run)
    logger.info("-" * 60)

    build_sales_report(dry_run)
    build_service_report(dry_run)

    logger.info("-" * 60)
    logger.info("Done!")


def main():
    parser = argparse.ArgumentParser(description="Build datamart tables (Task 2b)")
    parser.add_argument("--dry-run", action="store_true", help="Preview only, no writes")
    args = parser.parse_args()
    run(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
