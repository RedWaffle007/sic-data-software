"""
Script C: County Filtering (Simplified)

LOGIC:
- When NO county filter: Return ALL companies (no resolution, no filtering)
- When county filter IS applied: Return ONLY companies with explicit CSV County matching filter

NO POSTCODE MAPPING - CSV County field only
"""

import re
import json
import pickle
import hashlib
import logging
from pathlib import Path
from typing import List, Dict, Optional
from datetime import datetime
import polars as pl
from functools import wraps

logger = logging.getLogger(__name__)

# ============ CONFIG ============
COUNTY_OUTPUT_DIR = Path("outputs/county_filtered")
COUNTY_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ============ UTILITIES ============
def track_performance(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        import time
        start = time.time()
        logger.info(f"Starting {func.__name__}")
        result = func(*args, **kwargs)
        logger.info(f"Completed {func.__name__} in {time.time() - start:.2f}s")
        return result
    return wrapper


def normalize_county(name: str) -> str:
    """
    Normalize county name to canonical form.
    This is the SINGLE SOURCE OF TRUTH for county name normalization.
    """
    if not name or not isinstance(name, str):
        return ""
    
    s = name.strip().lower()
    
    # Special case: London variants
    if "london" in s:
        return "Greater London"
    
    # Remove common suffixes
    s = re.sub(
        r"\s+(county|unitary|borough|city|metropolitan|royal|district|council|region)$",
        "",
        s,
        flags=re.I,
    )
    
    # Title case for consistency
    return s.strip().title()


def load_county_aliases(config_dir: Path) -> Dict[str, str]:
    """Load county aliases from config file."""
    path = config_dir / "county_aliases.json"
    if path.exists():
        try:
            with open(path, "r") as f:
                raw = json.load(f)
                return {normalize_county(k): normalize_county(v) for k, v in raw.items()}
        except Exception as e:
            logger.warning(f"Failed to load county aliases: {e}")
    return {}


def map_to_canonical(name: str, aliases: Dict[str, str]) -> str:
    """Map county name to canonical form using aliases."""
    norm = normalize_county(name)
    return aliases.get(norm, norm)


def generate_hash(base: str, counties: Optional[List[str]]) -> str:
    """Generate deterministic hash for caching."""
    if not counties:
        key = base
    else:
        # Normalize counties before hashing for consistency
        normalized_counties = sorted([normalize_county(c) for c in counties])
        key = f"{base}|{'|'.join(normalized_counties)}"
    return hashlib.md5(key.encode()).hexdigest()[:12]


# ============ CORE ============
@track_performance
def resolve_and_filter_by_county(
    sic_extract_file: str,
    counties: Optional[List[str]],
    nspl_path: str,
    cache_dir: Path,
    config_dir: Path,
    force_refresh: bool = False,
) -> Dict[str, any]:

    if not Path(sic_extract_file).exists():
        raise FileNotFoundError(f"SIC extract not found: {sic_extract_file}")

    aliases = load_county_aliases(config_dir)

    df = pl.read_parquet(sic_extract_file)
    total_rows = df.height
    if total_rows == 0:
        raise ValueError("Empty SIC extract")

    for col in ["CompanyNumber", "Postcode", "County"]:
        if col not in df.columns:
            raise ValueError(f"Missing required column '{col}' in SIC extract")

    # ============ PREPARE COLUMNS ============
    df = df.with_columns([
        pl.col("County").fill_null("").alias("County"),
    ])

    # ============ SIMPLE LOGIC: FILTER OR RETURN ALL ============
    if counties:
        # ===== FILTER MODE: ONLY USE EXPLICIT CSV COUNTIES =====
        logger.info(f"FILTER MODE: Filtering by counties: {counties}")
        logger.info("Using ONLY explicit CSV County field")
        
        # Normalize user-provided counties
        normalized_targets = {normalize_county(c) for c in counties}
        logger.info(f"Normalized filter targets: {normalized_targets}")
        
        before_filter = df.height
        
        # Normalize the CSV County field and filter
        df = df.with_columns(
            pl.col("County").map_elements(
                lambda x: map_to_canonical(x, aliases), 
                return_dtype=pl.String
            ).alias("NormalizedCounty")
        )
        
        # Filter: must have county in CSV AND match one of the targets
        df = df.filter(
            (pl.col("NormalizedCounty") != "") &
            (pl.col("NormalizedCounty").is_in(list(normalized_targets)))
        )
        
        after_filter = df.height
        companies_with_county = (df["County"] != "").sum()
        
        logger.info(f"Filtered: {before_filter:,} → {after_filter:,} companies")
        logger.info(f"All {after_filter:,} companies had explicit county in CSV")
        
        # Stats for metadata
        stats = {
            "total_rows": int(total_rows),
            "before_filter": int(before_filter),
            "after_filter": int(after_filter),
            "total_companies": int(after_filter),  # For frontend compatibility
            "companies_with_explicit_county": int(companies_with_county),
        }
        
    else:
        # ===== NO FILTER MODE: RETURN ALL COMPANIES =====
        logger.info("NO FILTER MODE: Returning all companies")
        
        companies_with_county = (df["County"] != "").sum()
        companies_without_county = total_rows - companies_with_county
        
        logger.info(f"Total companies: {total_rows:,}")
        logger.info(f"  - With county: {companies_with_county:,}")
        logger.info(f"  - Without county: {companies_without_county:,}")
        
        # Add normalized county column for consistency
        df = df.with_columns(
            pl.col("County").map_elements(
                lambda x: map_to_canonical(x, aliases) if x else "", 
                return_dtype=pl.String
            ).alias("NormalizedCounty")
        )
        
        # Stats for metadata
        stats = {
            "total_rows": int(total_rows),
            "before_filter": int(total_rows),
            "after_filter": int(total_rows),
            "total_companies": int(total_rows),  # For frontend compatibility
            "companies_with_explicit_county": int(companies_with_county),
            "companies_without_county": int(companies_without_county),
        }

    # ============ WRITE OUTPUT ============
    base_hash = Path(sic_extract_file).stem
    out_hash = generate_hash(base_hash, counties)

    output_file = COUNTY_OUTPUT_DIR / f"{out_hash}.parquet"
    meta_file = COUNTY_OUTPUT_DIR / f"{out_hash}_meta.json"

    df.write_parquet(output_file, compression="zstd")

    metadata = {
        "input_file": sic_extract_file,
        "output_file": str(output_file),
        "filter_applied": bool(counties),
        "counties_requested": counties,
        "counties_normalized": list(normalized_targets) if counties else None,
        "timestamp": datetime.now().isoformat(),
        "stats": stats
    }

    with open(meta_file, "w") as f:
        json.dump(metadata, f, indent=2)

    logger.info(f"Output written: {output_file}")
    logger.info(f"Final rows: {stats['after_filter']:,}")

    return {
        "output_file": str(output_file),
        "metadata_file": str(meta_file),
        "stats": stats,
        "from_cache": False,
    }