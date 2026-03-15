"""
Configuration for Data Landing Pipeline.
Values are read from environment variables with sensible defaults.
"""

import os
from pathlib import Path

# ── Base paths ───────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent

DATA_INPUT_DIR = os.getenv("DATA_INPUT_DIR", str(BASE_DIR / "data_input"))
DATA_ARCHIVE_DIR = os.getenv("DATA_ARCHIVE_DIR", str(BASE_DIR / "data_archive"))

# ── MySQL connection ─────────────────────────────────────────
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "root")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "astraworld")

# ── File pattern ─────────────────────────────────────────────
CSV_FILE_PATTERN = r"^customer_address_\d{8}\.csv$"
