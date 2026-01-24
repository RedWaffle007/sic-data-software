"""
Script E: Advanced Enrichment (Enrichment 2.0)

Purpose:
    Enrich datasets already processed by Enrichment 1.0 with:
    - Website
    - Phone number
    - Email
    - Website address
    - Address match check
    - Confidence score
    - Review flag

Design:
    - Uses real search layer (Serper.dev / Google)
    - Primary source: Endole
    - Visits website to extract address
    - LLM used only for:
        * Field extraction from pages
        * Address normalization
    - Deterministic confidence scoring
    - Checkpointing
    - Progress callback support
"""

import os
import time
import logging
import requests
from pathlib import Path
from typing import Dict, Optional, List, Callable
import polars as pl
from tqdm import tqdm
import gc

logger = logging.getLogger(__name__)

# ============ CONFIG ============
SERPER_API_KEY = os.getenv("SERPER_API_KEY", "")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
ENRICHMENT_OUTPUT_DIR = Path("outputs/enriched")
ENRICHMENT_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

SERPER_URL = "https://google.serper.dev/search"
HEADERS_SERPER = {"X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json"}

CONFIDENCE_REVIEW_THRESHOLD = 70

API_DELAY = 1.5
# ============ HELPERS ============

def call_serper(query: str) -> Dict:
    payload = {"q": query, "num": 5}
    r = requests.post(SERPER_URL, headers=HEADERS_SERPER, json=payload, timeout=30)
    r.raise_for_status()
    return r.json()


def fetch_url(url: str) -> str:
    try:
        r = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code == 200:
            return r.text
    except Exception as e:
        logger.warning(f"Fetch failed: {url} | {e}")
    return ""


# ---- LLM HELPERS ----
def llm_extract_contact_fields(text: str) -> Dict:
    """
    Extract phone, email, website from raw text.
    Return 'Unreported' if missing.
    """
    from openai import OpenAI
    client = OpenAI(api_key=OPENAI_API_KEY)

    prompt = f"""
You are extracting business contact information.

From the text below, extract:
- Phone
- Email
- Website

Rules:
- If a field is missing, return "Unreported"
- Return JSON only

TEXT:
{text}
"""

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )

    return eval(response.choices[0].message.content)


def llm_normalize_address(address_text: str) -> Dict:
    """
    Convert free-form UK address into:
    AddressLine1, AddressLine2, Town, County, Postcode
    """
    from openai import OpenAI
    client = OpenAI(api_key=OPENAI_API_KEY)

    prompt = f"""
Normalize this UK address into structured fields:
- AddressLine1
- AddressLine2
- Town
- County
- Postcode

Return JSON only. If any field missing, return empty string.

ADDRESS:
{address_text}
"""

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )

    return eval(response.choices[0].message.content)


# ============ CORE LOGIC ============

def search_endole(business_name: str, town: str, postcode: str) -> Optional[str]:
    query = f"{business_name} {town} {postcode} site:endole.co.uk"
    data = call_serper(query)

    for r in data.get("organic", []):
        if "endole.co.uk" in r.get("link", ""):
            return r["link"]
    return None


def calculate_confidence(found_on_endole, has_website, has_phone, has_email,
                         address_match, normalized_address, llm_ok) -> int:
    score = 0
    if found_on_endole: score += 40
    if has_website: score += 15
    if has_phone: score += 15
    if has_email: score += 15
    if address_match: score += 15
    if normalized_address: score += 10
    if llm_ok: score += 5
    return min(score, 100)


# ============ MAIN SERVICE ============

def enrich_company_data_v2(
    input_path: str,
    output_path: Optional[str] = None,
    checkpoint_path: Optional[str] = None,
    resume: bool = True,
    batch_size: int = 10,
    progress_callback = None
) -> Dict:
    """
    Advanced enrichment for datasets already enriched by v1.
    """
    cache_path = Path("outputs/enriched/v2_cache.parquet")

    if cache_path.exists():
        try:
            cache_df = pl.read_parquet(cache_path)
            cached_keys = set(cache_df["CompanyNumber"].to_list())
            logger.info(f"Loaded {len(cached_keys)} cached companies for v2")
        except Exception as e:
            logger.warning(f"⚠️ Cache file corrupted, resetting: {e}")
            cache_df = None
            cached_keys = set()
    else:
        cache_df = None
        cached_keys = set()


    input_path = Path(input_path)
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    df = pl.read_parquet(input_path)

    required_cols = {"CompanyNumber", "BusinessName", "Town", "Postcode"}
    if not required_cols.issubset(set(df.columns)):
        raise ValueError("Dataset must be Enrichment 1.0 output before running Enrichment 2.0")

    stem = input_path.stem
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    output_path = Path(output_path) if output_path else ENRICHMENT_OUTPUT_DIR / f"{stem}_advanced_{timestamp}.parquet"
    checkpoint_path = Path(checkpoint_path) if checkpoint_path else ENRICHMENT_OUTPUT_DIR / f"{stem}_advanced_checkpoint.parquet"

    # Skip if advanced-enriched file already exists
    if output_path.exists():
        logger.info("Advanced enriched file already exists. Skipping.")
        existing_df = pl.read_parquet(output_path)
        return {
            "output_file": str(output_path),
            "checkpoint_file": str(checkpoint_path),
            "advanced_enrichment_stats": {
                "total_processed": int(existing_df.height),
                "from_cache": int(existing_df.height),
                "confidence_threshold": CONFIDENCE_REVIEW_THRESHOLD
            }
        }


    processed = set()
    if resume and checkpoint_path.exists():
        checkpoint_df = pl.read_parquet(checkpoint_path)
        processed = set(checkpoint_df["CompanyNumber"].to_list())
    else:
        checkpoint_df = None

    to_process = df.filter(~pl.col("CompanyNumber").is_in(list(processed)))
    total = to_process.height

    batch_rows = []
    processed_count = 0

    for idx, row in enumerate(tqdm(to_process.iter_rows(named=True), total=total, desc="Advanced Enrichment")):
        company_key = row.get("CompanyNumber")

        if company_key in cached_keys:
            cached_row = cache_df.filter(pl.col("CompanyNumber") == company_key).to_dicts()[0]
            batch_rows.append(cached_row)
            continue

        business = row.get("BusinessName", "")
        town = row.get("Town", "")
        postcode = row.get("Postcode", "")

        found_on_endole = False
        website = phone = email = "Unreported"
        website_address = {}
        address_match = False
        normalized_address = False
        llm_ok = False

        # ---- SEARCH ENDOLE ----
        endole_url = search_endole(business, town, postcode)
        if endole_url:
            found_on_endole = True
            endole_text = fetch_url(endole_url)
            try:
                extracted = llm_extract_contact_fields(endole_text)
                website = extracted.get("Website", "Unreported")
                phone = extracted.get("Phone", "Unreported")
                email = extracted.get("Email", "Unreported")
                llm_ok = True
            except Exception:
                pass

        # ---- VISIT WEBSITE ----
        if website and website != "Unreported":
            site_text = fetch_url(website)
            if site_text:
                try:
                    addr_data = llm_extract_contact_fields(site_text)
                    raw_address = addr_data.get("Address", "")
                    if raw_address:
                        website_address = llm_normalize_address(raw_address)
                        normalized_address = True

                        reg_address = " ".join([
                            str(row.get("AddressLine1", "")),
                            str(row.get("AddressLine2", "")),
                            str(row.get("Town", "")),
                            str(row.get("County", "")),
                            str(row.get("Postcode", ""))
                        ]).lower()

                        website_addr_text = " ".join(website_address.values()).lower()
                        if website_addr_text == reg_address:
                            address_match = True
                except Exception:
                    pass

        # ---- CONFIDENCE ----
        confidence = calculate_confidence(
            found_on_endole,
            website != "Unreported",
            phone != "Unreported",
            email != "Unreported",
            address_match,
            normalized_address,
            llm_ok
        )

        review_flag = confidence < CONFIDENCE_REVIEW_THRESHOLD

        enriched = {
            **row,
            "Website": website,
            "Phone": phone,
            "Email": email,
            "WebsiteAddressLine1": website_address.get("AddressLine1", ""),
            "WebsiteAddressLine2": website_address.get("AddressLine2", ""),
            "WebsiteTown": website_address.get("Town", ""),
            "WebsiteCounty": website_address.get("County", ""),
            "WebsitePostcode": website_address.get("Postcode", ""),
            "WebsiteAddressMatch": "Match" if address_match else "Different" if website_address else "Unreported",
            "ConfidenceScore": confidence,
            "ReviewFlag": review_flag
        }

        batch_rows.append(enriched)
        processed_count += 1

        if progress_callback:
            progress_callback(processed_count, total)

        if processed_count % batch_size == 0:
            batch_df = pl.DataFrame(batch_rows)
            checkpoint_df = pl.concat([checkpoint_df, batch_df]) if checkpoint_df is not None else batch_df
            checkpoint_df.write_parquet(checkpoint_path)
            batch_rows.clear()
            gc.collect()

    if batch_rows:
        final_df = pl.DataFrame(batch_rows)
        result = pl.concat([checkpoint_df, final_df]) if checkpoint_df is not None else final_df
    else:
        result = checkpoint_df


    # Persist new enriched rows to v2 cache
    if batch_rows:
        new_cache_df = pl.DataFrame(batch_rows)
        if cache_df is not None:
            cache_df = pl.concat([cache_df, new_cache_df])
        else:
            cache_df = new_cache_df

        cache_df.write_parquet(cache_path)


    result.write_parquet(output_path)
    result.write_parquet(checkpoint_path)

    return {
        "output_file": str(output_path),
        "checkpoint_file": str(checkpoint_path),
        "advanced_enrichment_stats": {
            "total_processed": int(result.height),
            "confidence_threshold": CONFIDENCE_REVIEW_THRESHOLD
        }
    }
