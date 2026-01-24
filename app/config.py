"""
Central Configuration for Company Dataset Pipeline
"""
import os
from pathlib import Path

# ============ BASE DIRECTORIES ============
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "outputs"
LOGS_DIR = BASE_DIR / "logs"

# Ensure core directories exist
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

# ============ DATA PATHS ============
# Use "current" snapshot or fallback to a specific month
CURRENT_SNAPSHOT = Path(os.getenv(
    "CURRENT_SNAPSHOT",
    DATA_DIR / "2026-01" / "2026-01.csv"
))

NSPL_PATH = Path(os.getenv(
    "NSPL_PATH",
    DATA_DIR / "reference" / "NSPL_AUG_2025_UK.csv"
))

# ============ DATABASE CONFIGURATION ============
# Default: SQLite (for local development)
# Production: Set DATABASE_URL to PostgreSQL connection string
# Example PostgreSQL: "postgresql://user:password@localhost:5432/company_db"
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    f"sqlite:///{OUTPUT_DIR / 'company_datasets.db'}"
)

# ============ CACHE CONFIGURATION ============
CACHE_DIR = Path(os.getenv("CACHE_DIR", "/tmp/company_dataset_cache"))
CACHE_DIR.mkdir(parents=True, exist_ok=True)
CACHE_MAX_AGE_HOURS = int(os.getenv("CACHE_MAX_AGE_HOURS", "24"))

# ============ APPLICATION SETTINGS ============
DEFAULT_PAGE_SIZE = int(os.getenv("DEFAULT_PAGE_SIZE", "1000"))
MAX_PAGE_SIZE = int(os.getenv("MAX_PAGE_SIZE", "10000"))
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "100000"))

# ============ LOGGING ============
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_FILE = LOGS_DIR / "app.log"

# ============ VALIDATION ============
def validate_config():
    """
    Validate all required files and directories.
    Raises FileNotFoundError if any required resource is missing.
    """
    errors = []

    if not CURRENT_SNAPSHOT.exists():
        errors.append(f"Snapshot file not found: {CURRENT_SNAPSHOT}")

    if not NSPL_PATH.exists():
        errors.append(f"NSPL file not found: {NSPL_PATH}")

    if not DATA_DIR.exists():
        errors.append(f"Data directory not found: {DATA_DIR}")

    if not OUTPUT_DIR.exists():
        errors.append(f"Output directory not found: {OUTPUT_DIR}")

    if not CACHE_DIR.exists():
        errors.append(f"Cache directory not found: {CACHE_DIR}")

    if errors:
        raise FileNotFoundError("\n".join(errors))

    return True