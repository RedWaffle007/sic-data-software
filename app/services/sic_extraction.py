"""
Script A: SIC-Only Extraction Module

Purpose:
    Extract ALL companies matching specified SIC codes from the full Companies House
    snapshot, with NO county or address logic applied.

Design Decisions:
    1. Streams entire CSV in lazy mode to avoid memory overload
    2. Writes directly to Parquet (compressed, columnar, fast)
    3. Returns complete population - no pagination at this stage
    4. Caches result so subsequent filters don't re-scan source
    5. Includes metadata about extraction for traceability

Output:
    - Parquet file: outputs/sic_extracts/{sic_hash}.parquet
    - Metadata JSON: outputs/sic_extracts/{sic_hash}_meta.json
"""

import re
import json
import logging
import hashlib
from pathlib import Path
from typing import List, Dict, Optional
from datetime import datetime
import polars as pl
import psutil
from functools import wraps

logger = logging.getLogger(__name__)

# ============ CONFIGURATION ============
SIC_EXTRACT_DIR = Path("outputs/sic_extracts")
SIC_EXTRACT_DIR.mkdir(parents=True, exist_ok=True)


# ============ UTILITIES ============
def track_performance(func):
    """Track execution time and memory usage."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = datetime.now()
        start_mem = psutil.Process().memory_info().rss / (1024 ** 2)

        logger.info(f"Starting {func.__name__}")
        result = func(*args, **kwargs)

        end_time = datetime.now()
        end_mem = psutil.Process().memory_info().rss / (1024 ** 2)
        duration = (end_time - start_time).total_seconds()
        mem_delta = end_mem - start_mem

        logger.info(
            f"Completed {func.__name__} in {duration:.2f}s | "
            f"Memory: {end_mem:.1f}MB (Δ{mem_delta:+.1f}MB)"
        )
        return result
    return wrapper


def generate_sic_hash(sic_codes: List[str]) -> str:
    """Generate deterministic hash for SIC code combination."""
    normalized = sorted([s.strip() for s in sic_codes])
    key = "|".join(normalized)
    return hashlib.md5(key.encode()).hexdigest()[:12]


def find_existing_extract(sic_codes: List[str]) -> Optional[Dict[str, Path]]:
    """Check if SIC extract already exists in cache."""
    sic_hash = generate_sic_hash(sic_codes)
    data_file = SIC_EXTRACT_DIR / f"{sic_hash}.parquet"
    meta_file = SIC_EXTRACT_DIR / f"{sic_hash}_meta.json"

    if data_file.exists() and meta_file.exists():
        logger.info(f"Found existing SIC extract: {data_file}")
        return {"data": data_file, "metadata": meta_file}

    return None


# ============ CORE EXTRACTION FUNCTION ============
@track_performance
def extract_companies_by_sic(
    sic_codes: List[str],
    csv_path: str,
    force_refresh: bool = False,
) -> Dict[str, any]:
    """
    Extract ALL companies matching SIC codes from Companies House snapshot.

    Stage A of the pipeline:
    - NO county filtering
    - NO postcode filtering
    - NO enrichment
    - SIC filtering ONLY
    - BUT preserves address fields for later stages
    """

    csv_path_obj = Path(csv_path)
    if not csv_path_obj.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    if not sic_codes:
        raise ValueError("At least one SIC code must be provided")

    target_sics = [s.strip() for s in sic_codes]
    sic_hash = generate_sic_hash(target_sics)

    # Check cache
    if not force_refresh:
        existing = find_existing_extract(target_sics)
        if existing:
            with open(existing["metadata"], "r") as f:
                metadata = json.load(f)

            logger.info(
                f"Using cached SIC extract: {metadata['stats']['total_companies']:,} companies"
            )

            return {
                "output_file": str(existing["data"]),
                "metadata_file": str(existing["metadata"]),
                "stats": metadata["stats"],
                "from_cache": True
            }

    output_file = SIC_EXTRACT_DIR / f"{sic_hash}.parquet"
    metadata_file = SIC_EXTRACT_DIR / f"{sic_hash}_meta.json"

    logger.info(f"Extracting SIC codes: {target_sics}")
    logger.info(f"Source: {csv_path}")
    logger.info(f"Output: {output_file}")

    # ============ SCHEMA DETECTION ============
    logger.info("Reading CSV schema...")
    sample = pl.read_csv(csv_path, n_rows=1, infer_schema_length=0)

    name_col = next((c for c in sample.columns if "companyname" in c.lower()), None)
    num_col = next((c for c in sample.columns if "companynumber" in c.lower()), None)
    sic_cols = [c for c in sample.columns if "siccode.sictext" in c.lower()]

    postcode_col = next((c for c in sample.columns if "regaddress.postcode" in c.lower()), None)
    county_col = next((c for c in sample.columns if "regaddress.county" in c.lower()), None)
    addr1_col = next((c for c in sample.columns if "regaddress.addressline1" in c.lower()), None)
    addr2_col = next((c for c in sample.columns if "regaddress.addressline2" in c.lower()), None)
    town_col = next((c for c in sample.columns if "regaddress.posttown" in c.lower()), None)

    if not name_col or not num_col or not sic_cols:
        raise ValueError(
            f"Required columns not found. Available columns: {sample.columns[:10]}"
        )

    logger.info(
        f"Detected columns: name={name_col}, number={num_col}, "
        f"SIC columns={len(sic_cols)}, postcode={postcode_col}, county={county_col}"
    )

    required_cols = [name_col, num_col] + sic_cols

    # Optional address columns
    for c in [postcode_col, county_col, addr1_col, addr2_col, town_col]:
        if c:
            required_cols.append(c)

    # Pre-compile SIC regex
    sic_pattern = "|".join(re.escape(s) for s in target_sics)

    # ============ STREAMING EXTRACTION ============
    logger.info("Building extraction pipeline (streaming mode)...")

    lazy_df = (
        pl.scan_csv(csv_path, infer_schema_length=0)
        .select([pl.col(c) for c in required_cols])
        .with_columns(
            pl.concat_str(
                [pl.col(c).fill_null("") for c in sic_cols],
                separator="|"
            ).alias("all_sics")
        )
        .filter(pl.col("all_sics").str.contains(sic_pattern))
        .drop("all_sics")
        .with_columns([
            pl.col(num_col).str.zfill(8).alias("CompanyNumber"),
            pl.col(name_col).str.strip_chars().alias("BusinessName"),
            pl.lit("; ".join(target_sics)).alias("SIC"),
            pl.col(postcode_col).fill_null("").alias("Postcode") if postcode_col else pl.lit("").alias("Postcode"),
            pl.col(county_col).fill_null("").alias("County") if county_col else pl.lit("").alias("County"),
            pl.col(addr1_col).fill_null("").alias("AddressLine1") if addr1_col else pl.lit("").alias("AddressLine1"),
            pl.col(addr2_col).fill_null("").alias("AddressLine2") if addr2_col else pl.lit("").alias("AddressLine2"),
            pl.col(town_col).fill_null("").alias("Town") if town_col else pl.lit("").alias("Town"),
        ])
        .select([
            "CompanyNumber",
            "BusinessName",
            "SIC",
            "Postcode",
            "County",
            "AddressLine1",
            "AddressLine2",
            "Town"
        ])
    )

    # ============ COLLECT AND WRITE ============
    logger.info("Collecting data (streaming mode)...")
    df = lazy_df.collect(streaming=True)

    total_companies = df.height
    logger.info(f"Extracted {total_companies:,} companies")

    logger.info(f"Writing to {output_file}...")
    df.write_parquet(output_file, compression="zstd")

    metadata = {
        "sic_hash": sic_hash,
        "sic_codes": target_sics,
        "extraction_timestamp": datetime.now().isoformat(),
        "source_file": str(csv_path),
        "stats": {
            "total_companies": int(total_companies)
        }
    }

    with open(metadata_file, "w") as f:
        json.dump(metadata, f, indent=2)

    return {
        "output_file": str(output_file),
        "metadata_file": str(metadata_file),
        "stats": metadata["stats"],
        "from_cache": False
    }
