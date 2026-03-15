"""
Data Landing Pipeline — Customer Address CSV Ingestion
======================================================
Scans data_input/ for files matching `customer_address_YYYYMMDD.csv`,
parses them with pandas, and upserts rows into MySQL table
`customer_addresses_raw`.  Processed files are moved to data_archive/.

Usage:
    python data_landing/ingest_customer_address.py              # normal run
    python data_landing/ingest_customer_address.py --dry-run    # validate only, no DB
"""

import argparse
import logging
import os
import re
import shutil
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

# ── Add project root so we can import config ─────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent))
import config  # noqa: E402

# ── Logging setup ────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── Expected columns ────────────────────────────────────────
EXPECTED_COLUMNS = {"id", "customer_id", "address", "city", "province", "created_at"}

# ── Upsert SQL ───────────────────────────────────────────────
UPSERT_SQL = """
INSERT INTO customer_addresses_raw
    (id, customer_id, address, city, province, created_at, source_file, ingested_at)
VALUES
    (%s, %s, %s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
    customer_id = VALUES(customer_id),
    address     = VALUES(address),
    city        = VALUES(city),
    province    = VALUES(province),
    created_at  = VALUES(created_at),
    source_file = VALUES(source_file),
    ingested_at = VALUES(ingested_at)
"""


# ── Helper functions ─────────────────────────────────────────

def discover_files(input_dir: str, pattern: str) -> list[Path]:
    """Return sorted list of CSV files matching the naming pattern."""
    regex = re.compile(pattern)
    input_path = Path(input_dir)

    if not input_path.exists():
        logger.warning("Input directory does not exist: %s", input_dir)
        return []

    files = sorted(
        f for f in input_path.iterdir()
        if f.is_file() and regex.match(f.name)
    )
    return files


def validate_dataframe(df: pd.DataFrame, filepath: Path) -> bool:
    """Check that the DataFrame has all required columns and is non-empty."""
    missing = EXPECTED_COLUMNS - set(df.columns)
    if missing:
        logger.error("File %s is missing columns: %s", filepath.name, missing)
        return False
    if df.empty:
        logger.warning("File %s is empty – skipping.", filepath.name)
        return False
    return True


def parse_csv(filepath: Path) -> pd.DataFrame | None:
    """Read CSV and return a validated DataFrame, or None on failure."""
    try:
        df = pd.read_csv(filepath, dtype=str)  # read all as string first
        df.columns = df.columns.str.strip().str.lower()
    except Exception as exc:
        logger.error("Failed to read %s: %s", filepath.name, exc)
        return None

    if not validate_dataframe(df, filepath):
        return None

    # Type coercions
    df["id"] = pd.to_numeric(df["id"], errors="coerce").astype("Int64")
    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").astype("Int64")
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")

    return df


def load_to_mysql(df: pd.DataFrame, source_filename: str) -> int:
    """Upsert rows into MySQL. Returns count of rows upserted."""
    import mysql.connector

    conn = mysql.connector.connect(
        host=config.MYSQL_HOST,
        port=config.MYSQL_PORT,
        user=config.MYSQL_USER,
        password=config.MYSQL_PASSWORD,
        database=config.MYSQL_DATABASE,
    )
    cursor = conn.cursor()
    now = datetime.now()
    rows_processed = 0

    try:
        for _, row in df.iterrows():
            created_at_val = row["created_at"]
            if pd.isna(created_at_val):
                created_at_val = None
            else:
                created_at_val = created_at_val.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

            values = (
                int(row["id"]) if pd.notna(row["id"]) else None,
                int(row["customer_id"]) if pd.notna(row["customer_id"]) else None,
                row["address"] if pd.notna(row["address"]) else None,
                row["city"] if pd.notna(row["city"]) else None,
                row["province"] if pd.notna(row["province"]) else None,
                created_at_val,
                source_filename,
                now.strftime("%Y-%m-%d %H:%M:%S"),
            )
            cursor.execute(UPSERT_SQL, values)
            rows_processed += 1

        conn.commit()
        logger.info("  ✓ Upserted %d rows from %s", rows_processed, source_filename)
    except Exception as exc:
        conn.rollback()
        logger.error("  ✗ MySQL error for %s: %s", source_filename, exc)
        raise
    finally:
        cursor.close()
        conn.close()

    return rows_processed


def archive_file(filepath: Path, archive_dir: str) -> None:
    """Move a processed file to the archive directory."""
    dest = Path(archive_dir)
    dest.mkdir(parents=True, exist_ok=True)
    shutil.move(str(filepath), str(dest / filepath.name))
    logger.info("  → Archived to %s", dest / filepath.name)


# ── Main pipeline ────────────────────────────────────────────

def run_pipeline(dry_run: bool = False) -> None:
    logger.info("=" * 60)
    logger.info("Data Landing Pipeline — Customer Address Ingestion")
    logger.info("=" * 60)
    logger.info("Input dir  : %s", config.DATA_INPUT_DIR)
    logger.info("Archive dir: %s", config.DATA_ARCHIVE_DIR)
    logger.info("Dry-run    : %s", dry_run)
    logger.info("-" * 60)

    files = discover_files(config.DATA_INPUT_DIR, config.CSV_FILE_PATTERN)

    if not files:
        logger.info("No new files to process.")
        return

    logger.info("Found %d file(s) to process.", len(files))

    total_rows = 0
    success_count = 0

    for filepath in files:
        logger.info("Processing: %s", filepath.name)
        df = parse_csv(filepath)
        if df is None:
            continue

        logger.info("  Parsed %d rows, %d columns", len(df), len(df.columns))

        if dry_run:
            # Print preview
            logger.info("  [DRY-RUN] Data preview:")
            print(df.to_string(index=False))
            print()
            success_count += 1
            total_rows += len(df)
            continue

        # Real run — load to MySQL and archive
        try:
            loaded = load_to_mysql(df, filepath.name)
            total_rows += loaded
            archive_file(filepath, config.DATA_ARCHIVE_DIR)
            success_count += 1
        except Exception:
            logger.error("  Skipping file %s due to error.", filepath.name)

    logger.info("-" * 60)
    logger.info(
        "Done. Files processed: %d/%d  |  Total rows: %d",
        success_count, len(files), total_rows,
    )


# ── Entry point ──────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Ingest customer_address CSV files into MySQL."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse & validate only — do not connect to MySQL.",
    )
    args = parser.parse_args()

    # Ensure directories exist
    Path(config.DATA_INPUT_DIR).mkdir(parents=True, exist_ok=True)
    Path(config.DATA_ARCHIVE_DIR).mkdir(parents=True, exist_ok=True)

    run_pipeline(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
