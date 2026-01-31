"""
Script B: Dataset Analysis
Purpose:
    Analyze dataset (from Script A or Script C) for UI.
    Read-only, no modification.
    Focus: England regions only (excludes Scotland, Wales, Northern Ireland)
"""
import logging
from pathlib import Path
from typing import Dict, List
from datetime import datetime
import polars as pl
from functools import wraps

logger = logging.getLogger(__name__)

# ============ ENGLAND REGIONS CONFIGURATION ============
ENGLAND_REGIONS = {
    "North West": {
        "code": "NW",
        "counties": ["Cheshire", "Cumbria", "Greater Manchester", "Lancashire", "Merseyside"]
    },
    "North East": {
        "code": "NE",
        "counties": ["County Durham", "Northumberland", "Tyne and Wear"]
    },
    "West Midlands": {
        "code": "WM",
        "counties": ["Herefordshire", "Shropshire", "Staffordshire", "Warwickshire", "West Midlands", "Worcestershire"]
    },
    "East Midlands": {
        "code": "EM",
        "counties": ["Derbyshire", "Leicestershire", "Lincolnshire", "Northamptonshire", "Nottinghamshire", "Rutland"]
    },
    "East": {
        "code": "E",
        "counties": ["Bedfordshire", "Cambridgeshire", "Essex", "Hertfordshire", "Norfolk", "Suffolk"]
    },
    "South West": {
        "code": "SW",
        "counties": ["Bristol", "Cornwall", "Devon", "Dorset", "Gloucestershire", "Somerset", "Wiltshire"]
    },
    "South East": {
        "code": "SE",
        "counties": ["Berkshire", "Buckinghamshire", "East Sussex", "Hampshire", "Isle of Wight", "Kent", "Oxfordshire", "Surrey", "West Sussex"]
    },
    "London": {
        "code": "L",
        "counties": ["Greater London"]
    }
}

# Flatten for quick lookup
ALL_ENGLAND_COUNTIES = set()
COUNTY_TO_REGION = {}
for region, data in ENGLAND_REGIONS.items():
    for county in data["counties"]:
        # Store normalized form for consistent matching
        normalized = county.strip().title()
        ALL_ENGLAND_COUNTIES.add(normalized)
        COUNTY_TO_REGION[normalized] = region

# ============ UTIL ============
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

def normalize_county(county: str) -> str:
    """
    Normalize county name to canonical form.
    This MUST match Script C's normalize_county function exactly.
    """
    if not county or not isinstance(county, str):
        return ""
    
    s = county.strip().lower()
    
    # Special case: London variants (City of London and Greater London both map to Greater London)
    if "london" in s:
        return "Greater London"
    
    # Remove common suffixes
    import re
    s = re.sub(
        r"\s+(county|unitary|borough|city|metropolitan|royal|district|council|region)$",
        "",
        s,
        flags=re.I,
    )
    
    # Title case for consistency
    return s.strip().title()

def is_england_county(county: str) -> bool:
    """Check if county is in England regions."""
    normalized = normalize_county(county)
    # Compare normalized form
    return normalized in {normalize_county(c) for c in ALL_ENGLAND_COUNTIES}

def get_region_for_county(county: str) -> str:
    """Get region name for a county."""
    normalized = normalize_county(county)
    return COUNTY_TO_REGION.get(normalized, "")

# ============ CORE ============
@track_performance
def analyze_dataset(dataset_file: str) -> Dict[str, any]:
    if not Path(dataset_file).exists():
        raise FileNotFoundError(f"Dataset not found: {dataset_file}")

    df = pl.read_parquet(dataset_file)
    total = df.height

    if total == 0:
        return {
            "summary": {
                "total_companies": 0,
                "total_england_companies": 0,
                "analysis_timestamp": datetime.now().isoformat(),
                "dataset_file": dataset_file,
            },
            "county_resolution": {},
            "missing_data": {},
            "regional_distribution": [],
            "data_quality_score": 0,
        }

    # ---------- Column Presence ----------
    has_raw_county = "County" in df.columns
    has_resolved = "ResolvedCounty" in df.columns
    has_postcode = "Postcode" in df.columns

    # ---------- Determine County Source ----------
    if has_resolved:
        county_source = "ResolvedCounty"
    elif has_raw_county:
        county_source = "County"
    else:
        county_source = None

    # ---------- Filter for England Only ----------
    if county_source:
        # Filter using normalized county matching
        england_df = df.filter(
            pl.col(county_source)
            .map_elements(lambda x: is_england_county(x), return_dtype=pl.Boolean)
        )
        total_england = england_df.height
    else:
        england_df = df
        total_england = 0

    # ---------- Resolution Stats (England only) ----------
    if has_resolved:
        direct = (england_df["County"] != "").sum() if has_raw_county else 0
        resolved_total = (england_df["ResolvedCounty"] != "").sum()
        postcode_resolved = max(int(resolved_total - direct), 0)
        unresolvable = (england_df["ResolvedCounty"] == "").sum()
    else:
        direct = (england_df["County"] != "").sum() if has_raw_county else 0
        postcode_resolved = 0
        unresolvable = total_england - direct

    # ---------- Missing Data (England only) ----------
    postcode_missing = (england_df["Postcode"] == "").sum() if has_postcode else total_england
    county_missing = (england_df["ResolvedCounty"] == "").sum() if has_resolved else (
        (england_df["County"] == "").sum() if has_raw_county else total_england
    )

    # ---------- Regional Distribution (England only, in specified order) ----------
    regional_distribution = []
    
    if county_source:
        # Get county counts
        county_counts = (
            england_df.filter(pl.col(county_source) != "")
            .group_by(county_source)
            .agg(pl.count().alias("count"))
        )
        
        # Convert to dict for easy lookup (use normalized keys)
        county_dict = {
            normalize_county(row[county_source]): int(row["count"])
            for row in county_counts.iter_rows(named=True)
        }
        
        # Build regional distribution in specified order
        for region_name, region_data in ENGLAND_REGIONS.items():
            region_total = 0
            county_breakdown = []
            
            for county in region_data["counties"]:
                normalized = normalize_county(county)
                count = county_dict.get(normalized, 0)
                region_total += count
                
                if count > 0:  # Only include counties with data
                    county_breakdown.append({
                        "county": county,
                        "count": count
                    })
            
            # Add region entry
            if region_total > 0:  # Only include regions with data
                regional_distribution.append({
                    "region": region_name,
                    "region_code": region_data["code"],
                    "count": region_total,
                    "percentage": f"{region_total / total_england * 100:.1f}%" if total_england > 0 else "0.0%",
                    "counties": county_breakdown
                })

    # ---------- Data Quality (England only) ----------
    if total_england > 0:
        postcode_score = (1 - postcode_missing / total_england) * 40
        county_score = (1 - county_missing / total_england) * 60
        quality = round(postcode_score + county_score, 1)
    else:
        quality = 0

    # ---------- Count unique England counties ----------
    unique_england_counties = len([c for region in regional_distribution for c in region["counties"]])

    # ---------- Result ----------
    return {
        "summary": {
            "total_companies": int(total),
            "total_england_companies": int(total_england),
            "unique_counties": int(unique_england_counties),
            "analysis_timestamp": datetime.now().isoformat(),
            "dataset_file": dataset_file,
        },
        "county_resolution": {
            "direct_from_csv": int(direct),
            "resolved_from_postcode": int(postcode_resolved),
            "unresolvable": int(unresolvable),
        },
        "missing_data": {
            "postcode_missing": int(postcode_missing),
            "county_missing": int(county_missing),
        },
        "regional_distribution": regional_distribution,
        "data_quality_score": quality,
    }