"""
Pipeline Orchestrator
=====================
Single entrypoint that runs all pipeline steps in order:
  1. Ingest customer_address CSV files  (Task 1)
  2. Clean raw data                     (Task 2a)
  3. Build datamart reports             (Task 2b)

Waits for MySQL to be ready before starting.
"""

import logging
import os
import sys
import time
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("orchestrator")

# ── Ensure imports work ──────────────────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent / "data_landing"))
sys.path.insert(0, str(Path(__file__).resolve().parent / "datamart"))


def wait_for_mysql(max_retries: int = 30, delay: int = 3):
    """Wait until MySQL is accepting connections."""
    import mysql.connector
    import config

    for attempt in range(1, max_retries + 1):
        try:
            conn = mysql.connector.connect(
                host=config.MYSQL_HOST,
                port=config.MYSQL_PORT,
                user=config.MYSQL_USER,
                password=config.MYSQL_PASSWORD,
                database=config.MYSQL_DATABASE,
            )
            conn.close()
            logger.info("✓ MySQL is ready (attempt %d/%d)", attempt, max_retries)
            return
        except mysql.connector.Error as e:
            logger.info(
                "Waiting for MySQL... (%d/%d) — %s",
                attempt, max_retries, e.msg if hasattr(e, 'msg') else str(e)
            )
            time.sleep(delay)

    logger.error("✗ Could not connect to MySQL after %d attempts", max_retries)
    sys.exit(1)


def main():
    logger.info("=" * 60)
    logger.info("  Maju Jaya — Data Pipeline Orchestrator")
    logger.info("=" * 60)

    # Wait for MySQL
    wait_for_mysql()

    # Step 1: Ingest CSV
    logger.info("")
    logger.info("▶ Step 1/3: Ingest Customer Address CSV")
    logger.info("-" * 60)
    from data_landing import ingest_customer_address
    ingest_customer_address.run_pipeline(dry_run=False)

    # Step 2: Clean raw data
    logger.info("")
    logger.info("▶ Step 2/3: Clean Raw Data")
    logger.info("-" * 60)
    from datamart import clean_data
    clean_data.run(dry_run=False)

    # Step 3: Build datamarts
    logger.info("")
    logger.info("▶ Step 3/3: Build Datamart Reports")
    logger.info("-" * 60)
    from datamart import build_datamart
    build_datamart.run(dry_run=False)

    logger.info("")
    logger.info("=" * 60)
    logger.info("  ✅ All pipeline steps completed successfully!")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
