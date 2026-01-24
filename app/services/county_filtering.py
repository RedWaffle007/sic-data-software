"""
Script C: County Resolution + Optional Filtering (3-Phase Design)

Phase 1: Resolve county for every row
    - Use CSV County if present
    - Else derive from Postcode via NSPL (ONS codes)
    - Else mark as UNRESOLVABLE

Phase 2: Apply filtering ONLY if user supplied counties
    - Filter rows with resolved counties
    - Do NOT include unresolvable rows

Phase 3: Persist + Report stats for UI analysis
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

# ============ STABLE ONS → COUNTY MAPPING (UK-WIDE) ============
# Canonical ceremonial / metro counties

ONS_TO_COUNTY = {
    # London
    "E12000007": "Greater London",

    # Ceremonial counties (England)
    "E10000002": "Buckinghamshire",
    "E10000003": "Cambridgeshire",
    "E10000006": "Cumbria",
    "E10000007": "Derbyshire",
    "E10000008": "Devon",
    "E10000009": "Dorset",
    "E10000010": "Essex",
    "E10000011": "Hampshire",
    "E10000012": "Hertfordshire",
    "E10000013": "Kent",
    "E10000014": "Lancashire",
    "E10000015": "Leicestershire",
    "E10000016": "Lincolnshire",
    "E10000017": "Norfolk",
    "E10000018": "Northamptonshire",
    "E10000019": "Northumberland",
    "E10000020": "Nottinghamshire",
    "E10000021": "Oxfordshire",
    "E10000023": "Somerset",
    "E10000024": "Staffordshire",
    "E10000025": "Suffolk",
    "E10000027": "Surrey",
    "E10000028": "Warwickshire",
    "E10000029": "West Sussex",
    "E10000030": "Wiltshire",
    "E10000031": "Worcestershire",
    "E10000032": "North Yorkshire",
    "E10000034": "Herefordshire",
    "E10000035": "Shropshire",
    "E10000036": "Rutland",

    # Metropolitan counties
    "E08000001": "Greater Manchester",
    "E08000002": "Greater Manchester",
    "E08000003": "Greater Manchester",
    "E08000004": "Greater Manchester",
    "E08000005": "Greater Manchester",
    "E08000006": "Greater Manchester",
    "E08000007": "Greater Manchester",
    "E08000008": "Greater Manchester",
    "E08000009": "Greater Manchester",
    "E08000010": "Greater Manchester",

    "E08000011": "Merseyside",
    "E08000012": "Merseyside",
    "E08000013": "Merseyside",
    "E08000014": "Merseyside",
    "E08000015": "Merseyside",

    "E08000016": "South Yorkshire",
    "E08000017": "South Yorkshire",
    "E08000018": "South Yorkshire",
    "E08000019": "South Yorkshire",

    "E08000020": "Tyne and Wear",
    "E08000021": "Tyne and Wear",
    "E08000022": "Tyne and Wear",
    "E08000023": "Tyne and Wear",
    "E08000024": "Tyne and Wear",

    "E08000025": "West Midlands",
    "E08000026": "West Midlands",
    "E08000027": "West Midlands",
    "E08000028": "West Midlands",
    "E08000029": "West Midlands",
    "E08000030": "West Midlands",
    "E08000031": "West Midlands",

    "E08000032": "West Yorkshire",
    "E08000033": "West Yorkshire",
    "E08000034": "West Yorkshire",
    "E08000035": "West Yorkshire",
    "E08000036": "West Yorkshire",
}

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

    # ============ LOAD NSPL (CODES ONLY) ============
    logger.info("Loading NSPL for postcode → ONS code mapping...")

    nspl = (
        pl.scan_csv(nspl_path, infer_schema_length=0)
        .select([
            pl.col("pcds").str.replace_all(" ", "").str.to_uppercase().alias("postcode"),
            pl.col("cty25cd"),
            pl.col("lad25cd"),
        ])
        .filter(pl.col("postcode").str.len_bytes() >= 4)
        .with_columns(pl.col("postcode").str.slice(0, 4).alias("outward"))
        .unique(subset=["outward"])
        .collect(streaming=True)
    )

    postcode_to_code = {
        row["outward"]: row["cty25cd"] or row["lad25cd"]
        for row in nspl.iter_rows(named=True)
        if row["outward"]
    }

    def resolve_from_code(code: str) -> str:
        """Resolve ONS code to county name."""
        if not code:
            return ""
        if code.startswith("E09"):
            return "Greater London"
        if code.startswith("S"):
            return "Scotland"
        if code.startswith("W"):
            return "Wales"
        if code.startswith("N"):
            return "Northern Ireland"
        return ONS_TO_COUNTY.get(code, "")

    # ============ PHASE 1: RESOLVE ============
    logger.info("Phase 1: Resolving counties for ALL rows...")

    df = df.with_columns([
        pl.col("County").fill_null("").alias("RawCounty"),
        pl.col("Postcode").fill_null("").alias("Postcode"),
    ])

    df = df.with_columns(
        pl.col("Postcode")
        .str.replace_all(r"\s+", "")
        .str.to_uppercase()
        .str.slice(0, 4)
        .alias("outward")
    )

    df = df.with_columns(
        pl.when(pl.col("RawCounty") != "")
        .then(pl.col("RawCounty").map_elements(lambda x: map_to_canonical(x, aliases), return_dtype=pl.String))
        .when(pl.col("outward") != "")
        .then(
            pl.col("outward").map_elements(
                lambda x: map_to_canonical(resolve_from_code(postcode_to_code.get(x, "")), aliases),
                return_dtype=pl.String
            )
        )
        .otherwise(pl.lit(""))
        .alias("ResolvedCounty")
    ).drop("outward")

    direct_county = (df["RawCounty"] != "").sum()
    postcode_resolved = ((df["RawCounty"] == "") & (df["ResolvedCounty"] != "")).sum()
    unresolvable = (df["ResolvedCounty"] == "").sum()

    logger.info(f"Resolution complete: {direct_county} direct, {postcode_resolved} from postcode, {unresolvable} unresolvable")

    # ============ PHASE 2: FILTER (if counties specified) ============
    before_filter = df.height
    
    if counties:
        logger.info(f"Phase 2: Applying county filter for: {counties}")
        
        # Normalize user-provided counties using the SAME function
        normalized_targets = {normalize_county(c) for c in counties}
        logger.info(f"Normalized filter targets: {normalized_targets}")
        
        # Filter: must have resolved county AND match one of the targets (case-insensitive)
        df = df.filter(
            (pl.col("ResolvedCounty") != "") &
            (pl.col("ResolvedCounty").is_in(list(normalized_targets)))
        )
        
        after_filter = df.height
        logger.info(f"Filtered: {before_filter:,} → {after_filter:,} companies")
    else:
        after_filter = df.height
        logger.info("Phase 2: No county filter applied, keeping all resolved companies")

    # ============ PHASE 3: WRITE ============
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
        "stats": {
            "total_rows": int(total_rows),
            "direct_county": int(direct_county),
            "postcode_resolved": int(postcode_resolved),
            "unresolvable": int(unresolvable),
            "before_filter": int(before_filter),
            "after_filter": int(after_filter),
        }
    }

    with open(meta_file, "w") as f:
        json.dump(metadata, f, indent=2)

    logger.info(f"Output written: {output_file}")
    logger.info(f"Final rows: {after_filter:,}")

    return {
        "output_file": str(output_file),
        "metadata_file": str(meta_file),
        "stats": metadata["stats"],
        "from_cache": False,
    }