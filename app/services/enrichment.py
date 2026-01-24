"""
Script D: Enrichment Service - Optimized for Memory and Performance

Purpose:
    Enrich company data with Companies House API information.
    This is USER-TRIGGERED via "Enrich" button (async operation).

Design:
    - Streaming processing with batched API calls
    - Checkpoint system to prevent data loss
    - Automatic garbage collection
    - Progress tracking
    - Resumable from checkpoints
"""

import os
import time
import random
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import requests
import polars as pl
from tqdm import tqdm
from functools import wraps
import psutil
import gc

logger = logging.getLogger(__name__)

# ============ CONFIGURATION ============
API_KEY = os.getenv("COMPANIES_HOUSE_API_KEY", "")
BASE_URL = "https://api.company-information.service.gov.uk"
MAX_RETRIES = 3
MIN_DELAY_SEC = 0.6  # ~600 requests / 5 minutes

ENRICHMENT_OUTPUT_DIR = Path("outputs/enriched")
ENRICHMENT_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


FINAL_COL_ORDER = [
    "CompanyNumber",
    "BusinessName",
    "AddressLine1",
    "AddressLine2",
    "Town",
    "County",
    "Postcode",
    "PersonWithSignificantControl",
    "NatureOfControl",
    "Title",
    "Fname",
    "Sname",
    "SelectedPersonSource",
    "SelectedPSCShareTier",
    "SelectedPSCNatureOfControl",
    "Position",
    "SIC",
    "CompanyStatus",
    "CompanyType",
    "DateOfCreation",
    "Website",
    "Phone",
    "Email",
    "WebsiteAddress",
    "AddressMatch(RegVsWeb)",
]


# ============ PERFORMANCE TRACKING ============
def track_performance(func):
    """Decorator to track execution time and memory."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        start_mem = psutil.Process().memory_info().rss / (1024 ** 2)
        logger.info(f"Starting {func.__name__}")
        result = func(*args, **kwargs)
        end_time = time.time()
        end_mem = psutil.Process().memory_info().rss / (1024 ** 2)
        duration = end_time - start_time
        mem_delta = end_mem - start_mem
        logger.info(
            f"Completed {func.__name__} in {duration:.2f}s | "
            f"Memory: {end_mem:.1f}MB (Δ{mem_delta:+.1f}MB)"
        )
        return result
    return wrapper


# ============ API CLIENT ============
class CompaniesHouseClient:
    """Companies House API client with rate limiting and retries."""

    def __init__(self, api_key: str):
        if not api_key:
            raise ValueError(
                "COMPANIES_HOUSE_API_KEY not set. "
                "Get your free API key from: "
                "https://developer.company-information.service.gov.uk/"
            )
        self.session = requests.Session()
        self.session.auth = (api_key, "")
        self.session.headers.update({"Accept": "application/json"})
        self.last_request_time = 0

    def _rate_limit(self):
        elapsed = time.time() - self.last_request_time
        if elapsed < MIN_DELAY_SEC:
            time.sleep(MIN_DELAY_SEC - elapsed)
        self.last_request_time = time.time()

    def safe_get(self, url: str) -> Optional[Dict]:
        for attempt in range(MAX_RETRIES):
            self._rate_limit()
            try:
                response = self.session.get(url, timeout=30)

                if response.status_code == 200:
                    return response.json()

                if response.status_code == 404:
                    return None

                if response.status_code == 429:
                    wait = int(response.headers.get("Retry-After", 15))
                    logger.warning(f"Rate limited. Waiting {wait}s")
                    time.sleep(wait + random.uniform(0, 2))
                    continue

                if response.status_code >= 500:
                    logger.warning(f"Server error {response.status_code}. Retry {attempt + 1}")
                    time.sleep(2 ** attempt)
                    continue

                logger.error(f"Unexpected status {response.status_code}: {url}")
                return None

            except requests.exceptions.RequestException as e:
                logger.warning(f"Request failed: {e}. Retry {attempt + 1}")
                time.sleep(2 ** attempt)

        logger.error(f"Failed after {MAX_RETRIES} attempts: {url}")
        return None

    def get_company_profile(self, company_number: str) -> Dict:
        return self.safe_get(f"{BASE_URL}/company/{company_number}") or {}

    def get_psc(self, company_number: str) -> List[Dict]:
        data = self.safe_get(f"{BASE_URL}/company/{company_number}/persons-with-significant-control") or {}
        return data.get("items", [])

    def get_officers(self, company_number: str) -> List[Dict]:
        data = self.safe_get(f"{BASE_URL}/company/{company_number}/officers") or {}
        return data.get("items", [])


def parse_officer_name(name: str) -> tuple:
    if not name:
        return "", ""
    if "," in name:
        surname, firstname = name.split(",", 1)
        return firstname.strip(), surname.strip()
    return "", name.strip()


# =========== FUNCTION FOR PSC SELECTION ============
def pick_psc_by_ownership(psc_items: list) -> Tuple[Dict, str, str]:
    """
    Returns (psc_dict, share_tier, nature_of_control) based on STRICT ownership priority:
    1) FIRST individual person with ownership-of-shares-75-to-100-percent
    2) FIRST individual person with ownership-of-shares-50-to-75-percent
    3) FIRST individual person with ownership-of-shares-25-to-50-percent
    Only selects individual persons, NOT corporate entities or legal persons.
    Returns ({}, "", "") if no individual PSC found.
    """
    if not psc_items:
        return {}, "", ""

    # Priority 1: First INDIVIDUAL person with 75–100%
    for p in psc_items:
        if (p.get("kind") == "individual-person-with-significant-control" and
            "ownership-of-shares-75-to-100-percent" in p.get("natures_of_control", [])):
            noc = "; ".join(p.get("natures_of_control", []))
            return p, "75-100%", noc

    # Priority 2: First INDIVIDUAL person with 50–75%
    for p in psc_items:
        if (p.get("kind") == "individual-person-with-significant-control" and
            "ownership-of-shares-50-to-75-percent" in p.get("natures_of_control", [])):
            noc = "; ".join(p.get("natures_of_control", []))
            return p, "50-75%", noc

    # Priority 3: First INDIVIDUAL person with 25–50%
    for p in psc_items:
        if (p.get("kind") == "individual-person-with-significant-control" and
            "ownership-of-shares-25-to-50-percent" in p.get("natures_of_control", [])):
            noc = "; ".join(p.get("natures_of_control", []))
            return p, "25-50%", noc

    # No individual PSC found with required ownership
    return {}, "", ""


# ============ TITLE EXTRACTION ============
def extract_title_from_psc(psc_text: str, officer_first: str, officer_last: str) -> str:
    """
    Extracts a title (Mr, Ms, Miss, Mrs, Dr, etc.) from PSC only if:
    - The PSC entry starts with a title
    - AND the name matches the first officer (first + last name)
    Otherwise returns blank.
    """
    if not psc_text or not isinstance(psc_text, str):
        return ""

    if not officer_first or not officer_last:
        return ""

    titles = {"mr", "mrs", "ms", "miss", "dr", "sir", "lady", "prof"}

    officer_first = officer_first.strip().lower()
    officer_last = officer_last.strip().lower()

    parts = [p.strip() for p in psc_text.split(";") if p.strip()]

    for part in parts:
        tokens = part.split()
        if not tokens:
            continue

        first_token = tokens[0].lower().rstrip(".")

        if first_token in titles:
            name_part = " ".join(tokens[1:]).lower()

            if officer_first in name_part and officer_last in name_part:
                return first_token.title()

    return ""


# ============ ENRICHMENT SERVICE ============
@track_performance
def enrich_company_data(
    input_path: str,
    output_path: Optional[str] = None,
    checkpoint_path: Optional[str] = None,
    resume: bool = True,
    batch_size: int = 50,
    progress_callback = None
) -> Dict:
    """
    Enrich company data with Companies House API information.
    """

    input_path = Path(input_path)
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    logger.info(f"Loading input file: {input_path}")
    input_df = pl.read_parquet(input_path)

    # ---- Normalize County Columns (ResolvedCounty → County) ----
    if "ResolvedCounty" in input_df.columns:
        logger.info("Normalizing county columns (ResolvedCounty → County)...")
        input_df = input_df.with_columns(
            pl.col("ResolvedCounty").fill_null(pl.col("County")).alias("County")
        )
        drop_cols = [c for c in ["RawCounty", "ResolvedCounty"] if c in input_df.columns]
        if drop_cols:
            input_df = input_df.drop(drop_cols)
            logger.info(f"Dropped internal columns: {drop_cols}")

    if "CompanyNumber" not in input_df.columns:
        raise ValueError("Input file must have 'CompanyNumber' column")

    total_input = input_df.height
    logger.info(f"Loaded {total_input:,} companies")

    client = CompaniesHouseClient(API_KEY)

    stem = input_path.stem
    timestamp = time.strftime("%Y%m%d_%H%M%S")

    output_path = Path(output_path) if output_path else ENRICHMENT_OUTPUT_DIR / f"{stem}_enriched_{timestamp}.parquet"
    checkpoint_path = Path(checkpoint_path) if checkpoint_path else ENRICHMENT_OUTPUT_DIR / f"{stem}_checkpoint.parquet"

    processed_numbers = set()
    if resume and checkpoint_path.exists():
        logger.info("Resuming from checkpoint...")
        checkpoint_df = pl.read_parquet(checkpoint_path)
        processed_numbers = set(checkpoint_df["CompanyNumber"].to_list())
        logger.info(f"Found {len(processed_numbers)} already processed companies")
    else:
        checkpoint_df = None

    df_to_process = input_df.filter(~pl.col("CompanyNumber").is_in(list(processed_numbers)))
    total_to_process = df_to_process.height
    logger.info(f"Companies to enrich: {total_to_process:,}")

    if total_to_process == 0:
        logger.info("No new companies to process")

        if checkpoint_path.exists():
            logger.info(f"Writing final output from checkpoint to {output_path}")
            checkpoint_df = pl.read_parquet(checkpoint_path)
            checkpoint_df.write_parquet(output_path, compression="zstd")
        else:
            raise FileNotFoundError("Checkpoint file not found, cannot produce output file")

        return {
            "output_file": str(output_path),
            "checkpoint_file": str(checkpoint_path),
            "enrichment_stats": {
                "total_processed": len(processed_numbers),
                "newly_enriched": 0,
                "from_checkpoint": len(processed_numbers)
            }
        }

    stats = {
        "api_success": 0,
        "api_fail": 0,
        "with_psc": 0,
        "with_officers": 0
    }

    batch_rows = []

    for idx, row in enumerate(tqdm(df_to_process.iter_rows(named=True), total=total_to_process, desc="Enriching")):
        company_number = row["CompanyNumber"]

        if progress_callback:
            progress_callback(idx + 1)

        profile = client.get_company_profile(company_number)
        if profile:
            stats["api_success"] += 1
            psc_items = client.get_psc(company_number)
            officers = client.get_officers(company_number)
        else:
            stats["api_fail"] += 1
            psc_items, officers = [], []

        # ===== PSC-based selection with STRICT priority =====
        selected_psc, share_tier, psc_noc = pick_psc_by_ownership(psc_items)
        
        psc_title = ""
        psc_first = ""
        psc_last = ""
        
        # Extract name parts from selected PSC
        if selected_psc:
            selected_psc_name = selected_psc.get("name", "")
            if selected_psc_name:
                tokens = selected_psc_name.split()
                titles = {"mr", "mrs", "ms", "miss", "dr", "sir", "lady", "prof"}
                
                # Extract title if present
                if tokens and tokens[0].lower().rstrip(".") in titles:
                    psc_title = tokens[0].rstrip(".").title()
                    tokens = tokens[1:]  # Remove title from tokens
                
                # Extract first and last name from remaining tokens
                if len(tokens) >= 2:
                    psc_first = tokens[0]
                    psc_last = tokens[-1]
                elif len(tokens) == 1:
                    psc_last = tokens[0]

        if psc_items:
            stats["with_psc"] += 1
        if officers:
            stats["with_officers"] += 1

        # Parse officer names for fallback (only individual persons, not entities)
        officer_names = []
        for o in officers:
            # Skip corporate entities - they typically have "appointed_on" but no "name" with comma
            officer_name = o.get("name", "")
            if officer_name and "," in officer_name:  # Individual format: "SURNAME, Firstname"
                first, last = parse_officer_name(officer_name)
                if first or last:  # Valid individual officer
                    officer_names.append((first, last))

        # All PSC names for PersonWithSignificantControl field
        psc_names = "; ".join(p.get("name", "") for p in psc_items if p.get("name"))

        # First officer details for title extraction fallback
        first_officer_first = officer_names[0][0] if officer_names else ""
        first_officer_last = officer_names[0][1] if officer_names else ""

        # Initialize tracking variables
        selected_source = ""
        selected_share_tier = ""
        selected_noc = ""

        # OVERRIDE: Use PSC data if available, otherwise first INDIVIDUAL officer
        if psc_first or psc_last or psc_title:
            # PSC data available - use it
            final_title = psc_title
            final_first = psc_first
            final_last = psc_last
            selected_source = f"PSC: {selected_psc.get('name', '')}"
            selected_share_tier = share_tier
            selected_noc = psc_noc
        elif officer_names:
            # No PSC, use first individual officer
            final_title = extract_title_from_psc(psc_names, first_officer_first, first_officer_last)
            final_first = first_officer_first
            final_last = first_officer_last
            selected_source = "First Officer"
            selected_share_tier = ""
            selected_noc = ""
        else:
            # No PSC, no individual officers
            final_title = ""
            final_first = ""
            final_last = ""
            selected_source = ""
            selected_share_tier = ""
            selected_noc = ""

        # Position: semicolon-separated list of officer roles
        officer_positions = "; ".join(o.get("officer_role", "") for o in officers if o.get("officer_role"))

        enriched = {
            **row,
            "PersonWithSignificantControl": psc_names,
            "NatureOfControl": "; ".join(set(n for p in psc_items for n in p.get("natures_of_control", []))),
            "Title": final_title,
            "Fname": final_first,
            "Sname": final_last,
            "SelectedPersonSource": selected_source,
            "SelectedPSCShareTier": selected_share_tier,
            "SelectedPSCNatureOfControl": selected_noc,
            "Position": officer_positions,
            "CompanyStatus": profile.get("company_status", ""),
            "CompanyType": profile.get("type", ""),
            "DateOfCreation": profile.get("date_of_creation", ""),
            "Website": "",
            "Phone": "",
            "Email": "",
            "WebsiteAddress": "",
            "AddressMatch(RegVsWeb)": "",
        }

        batch_rows.append(enriched)

        if (idx + 1) % batch_size == 0:
            batch_df = pl.DataFrame(batch_rows)
            if checkpoint_df is not None:
                checkpoint_df = pl.concat([checkpoint_df, batch_df])
            else:
                checkpoint_df = batch_df

            checkpoint_df.write_parquet(checkpoint_path)
            batch_rows.clear()
            gc.collect()
            logger.info(f"✓ Checkpoint saved: {idx + 1}/{total_to_process}")

    if batch_rows:
        final_batch = pl.DataFrame(batch_rows)
        if checkpoint_df is not None:
            result = pl.concat([checkpoint_df, final_batch])
        else:
            result = final_batch
        batch_rows.clear()
    else:
        result = checkpoint_df

    # ===== Column ordering =====
    existing_cols = set(result.columns)
    ordered_cols = [c for c in FINAL_COL_ORDER if c in existing_cols]
    remaining_cols = [c for c in result.columns if c not in FINAL_COL_ORDER]
    result = result.select(ordered_cols + remaining_cols)

    logger.info(f"Writing enriched output to {output_path}")
    result.write_parquet(output_path, compression="zstd")
    result.write_parquet(checkpoint_path)

    return {
        "output_file": str(output_path),
        "checkpoint_file": str(checkpoint_path),
        "enrichment_stats": {
            "total_processed": int(result.height),
            "newly_enriched": int(total_to_process),
            "from_checkpoint": int(len(processed_numbers)),
            "api_success": int(stats["api_success"]),
            "api_failures": int(stats["api_fail"]),
            "coverage": {
                "psc": f"{stats['with_psc']/max(total_to_process,1)*100:.1f}%",
                "officers": f"{stats['with_officers']/max(total_to_process,1)*100:.1f}%"
            }
        }
    }