"""
Data Cleaning Script — Task 2a
===============================
Reads raw tables, cleans data quality issues, and writes to cleaned tables.

Cleaning rules:
  - customers_raw.dob : standardize mixed date formats → YYYY-MM-DD
                         set 1900-01-01 → NULL (placeholder)
                         detect company names (PT, CV) → is_company = 1
  - sales_raw.price   : parse '350.000.000' string → 350000000 integer
  - customer_addresses_raw : standardize city/province casing

Usage:
    python datamart/clean_data.py              # run against MySQL
    python datamart/clean_data.py --dry-run    # preview only
"""

import argparse
import logging
import re
import sys
from datetime import datetime
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


def read_table(table_name: str) -> pd.DataFrame:
    """Read an entire table into a DataFrame."""
    conn = get_connection()
    df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
    conn.close()
    logger.info("Read %d rows from %s", len(df), table_name)
    return df


# ── Cleaning functions ───────────────────────────────────────

COMPANY_PREFIXES = re.compile(r"^(PT|CV|UD|Yayasan|Koperasi)\s", re.IGNORECASE)

DATE_PATTERNS = [
    ("%Y-%m-%d", None),       # 1998-08-04
    ("%Y/%m/%d", None),       # 1980/11/15
    ("%d/%m/%Y", None),       # 14/01/1995
    ("%d-%m-%Y", None),       # 14-01-1995
]

PLACEHOLDER_DATE = datetime(1900, 1, 1).date()


def parse_dob(value) -> tuple:
    """
    Try to parse a dob string into a date.
    Returns (date_or_None, is_placeholder: bool).
    """
    if pd.isna(value) or value is None or str(value).strip() == "":
        return None, False

    raw = str(value).strip()

    for fmt, _ in DATE_PATTERNS:
        try:
            dt = datetime.strptime(raw, fmt).date()
            if dt == PLACEHOLDER_DATE:
                return None, True  # placeholder
            return dt, False
        except ValueError:
            continue

    logger.warning("Cannot parse dob: '%s'", raw)
    return None, False


def clean_customers(df: pd.DataFrame) -> pd.DataFrame:
    """Clean customers_raw → customers_cleaned."""
    records = []
    for _, row in df.iterrows():
        dob_parsed, _ = parse_dob(row.get("dob"))
        is_company = 1 if COMPANY_PREFIXES.match(str(row.get("name", ""))) else 0

        records.append({
            "id": int(row["id"]),
            "name": row["name"],
            "dob": dob_parsed,
            "is_company": is_company,
            "created_at": row["created_at"],
        })

    result = pd.DataFrame(records)
    logger.info("Cleaned customers: %d rows", len(result))
    return result


def parse_price(value) -> int | None:
    """Parse '350.000.000' → 350000000."""
    if pd.isna(value) or value is None:
        return None
    cleaned = str(value).replace(".", "").replace(",", "").strip()
    try:
        return int(cleaned)
    except ValueError:
        logger.warning("Cannot parse price: '%s'", value)
        return None


def clean_sales(df: pd.DataFrame) -> pd.DataFrame:
    """Clean sales_raw → sales_cleaned."""
    result = df.copy()
    result["price_raw"] = result["price"]
    result["price_numeric"] = result["price"].apply(parse_price)
    result = result.drop(columns=["price"])
    logger.info("Cleaned sales: %d rows, price parsed to numeric", len(result))
    return result


def standardize_case(value, mode="title") -> str | None:
    """Standardize string casing."""
    if pd.isna(value) or value is None:
        return None
    s = str(value).strip()
    if mode == "title":
        return s.title()
    elif mode == "upper":
        return s.upper()
    return s


def clean_addresses(df: pd.DataFrame) -> pd.DataFrame:
    """Clean customer_addresses_raw — standardize city/province casing."""
    result = df.copy()
    result["city"] = result["city"].apply(lambda x: standardize_case(x, "title"))
    result["province"] = result["province"].apply(lambda x: standardize_case(x, "upper"))
    logger.info("Cleaned addresses: %d rows, casing standardized", len(result))
    return result


# ── Write to MySQL ───────────────────────────────────────────

def upsert_customers(df: pd.DataFrame):
    """Upsert cleaned customers into customers_cleaned."""
    conn = get_connection()
    cursor = conn.cursor()
    sql = """
        INSERT INTO customers_cleaned (id, name, dob, is_company, created_at, cleaned_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            name = VALUES(name), dob = VALUES(dob),
            is_company = VALUES(is_company), created_at = VALUES(created_at),
            cleaned_at = VALUES(cleaned_at)
    """
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for _, row in df.iterrows():
        dob_val = row["dob"].strftime("%Y-%m-%d") if row["dob"] is not None else None
        cursor.execute(sql, (
            row["id"], row["name"], dob_val, row["is_company"], row["created_at"], now
        ))
    conn.commit()
    cursor.close()
    conn.close()
    logger.info("  ✓ Upserted %d rows to customers_cleaned", len(df))


def upsert_sales(df: pd.DataFrame):
    """Upsert cleaned sales into sales_cleaned."""
    conn = get_connection()
    cursor = conn.cursor()
    sql = """
        INSERT INTO sales_cleaned
            (vin, customer_id, model, invoice_date, price_raw, price_numeric, created_at, cleaned_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            customer_id = VALUES(customer_id), model = VALUES(model),
            invoice_date = VALUES(invoice_date), price_raw = VALUES(price_raw),
            price_numeric = VALUES(price_numeric), created_at = VALUES(created_at),
            cleaned_at = VALUES(cleaned_at)
    """
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for _, row in df.iterrows():
        price_num = int(row["price_numeric"]) if pd.notna(row["price_numeric"]) else None
        cursor.execute(sql, (
            row["vin"], int(row["customer_id"]), row["model"],
            row["invoice_date"], row["price_raw"], price_num,
            row["created_at"], now
        ))
    conn.commit()
    cursor.close()
    conn.close()
    logger.info("  ✓ Upserted %d rows to sales_cleaned", len(df))


def update_addresses(df: pd.DataFrame):
    """Update city/province casing in customer_addresses_raw in-place."""
    conn = get_connection()
    cursor = conn.cursor()
    sql = """
        UPDATE customer_addresses_raw
        SET city = %s, province = %s
        WHERE id = %s
    """
    for _, row in df.iterrows():
        cursor.execute(sql, (row["city"], row["province"], int(row["id"])))
    conn.commit()
    cursor.close()
    conn.close()
    logger.info("  ✓ Updated %d rows in customer_addresses_raw (casing)", len(df))


# ── Main ─────────────────────────────────────────────────────

def run(dry_run: bool = False):
    logger.info("=" * 60)
    logger.info("Data Cleaning Pipeline — Task 2a")
    logger.info("=" * 60)
    logger.info("Dry-run: %s", dry_run)
    logger.info("-" * 60)

    # 1. Clean customers
    logger.info("[1/3] Cleaning customers_raw → customers_cleaned")
    cust_raw = read_table("customers_raw")
    cust_cleaned = clean_customers(cust_raw)
    if dry_run:
        print("\n── customers_cleaned preview ──")
        print(cust_cleaned.to_string(index=False))
    else:
        upsert_customers(cust_cleaned)

    # 2. Clean sales
    logger.info("[2/3] Cleaning sales_raw → sales_cleaned")
    sales_raw = read_table("sales_raw")
    sales_cleaned = clean_sales(sales_raw)
    if dry_run:
        print("\n── sales_cleaned preview ──")
        print(sales_cleaned.to_string(index=False))
    else:
        upsert_sales(sales_cleaned)

    # 3. Clean addresses
    logger.info("[3/3] Cleaning customer_addresses_raw (casing)")
    try:
        addr_raw = read_table("customer_addresses_raw")
        addr_cleaned = clean_addresses(addr_raw)
        if dry_run:
            print("\n── customer_addresses_raw (cleaned preview) ──")
            print(addr_cleaned.to_string(index=False))
        else:
            update_addresses(addr_cleaned)
    except Exception as e:
        logger.warning("Skipping address cleaning: %s", e)

    logger.info("-" * 60)
    logger.info("Done!")


def main():
    parser = argparse.ArgumentParser(description="Clean raw tables (Task 2a)")
    parser.add_argument("--dry-run", action="store_true", help="Preview only, no writes")
    args = parser.parse_args()
    run(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
