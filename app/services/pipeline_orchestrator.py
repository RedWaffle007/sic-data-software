"""
Pipeline Orchestrator: Complete Multi-Stage Flow

Conditional Logic:
    - Always runs: A → C (county resolution always happens)
    - C filters ONLY if counties provided

Script Responsibilities:
    A: Extract all companies for SIC codes (cached)
    C: ALWAYS resolve counties, optionally filter (auto-runs every time)
    B: Analyze dataset (user-triggered, read-only)
    D: Enrich dataset (user-triggered)

Job States:
    - sic_extracted
    - county_resolved (always happens now)
"""

import logging
from pathlib import Path
from typing import List, Optional, Dict

logger = logging.getLogger(__name__)


# ============ MAIN PIPELINE ============
def execute_pipeline(
    sic_codes: List[str],
    counties: Optional[List[str]] = None,
    csv_path: Optional[str] = None,
    nspl_path: Optional[str] = None,
    cache_dir: Optional[Path] = None,
    config_dir: Optional[Path] = None,
    force_refresh: bool = False,
) -> Dict[str, any]:
    """
    Execute complete pipeline based on parameters.

    Flow Logic:
        ALWAYS:  A → C (county resolution always happens)
        C filters ONLY if counties parameter provided
    """

    # Local imports to avoid circular dependencies
    from app.config import CURRENT_SNAPSHOT, NSPL_PATH, CACHE_DIR
    from app.services.sic_extraction import extract_companies_by_sic
    from app.services.county_filtering import resolve_and_filter_by_county

    # Defaults
    csv_path = csv_path or CURRENT_SNAPSHOT
    nspl_path = nspl_path or NSPL_PATH
    cache_dir = cache_dir or CACHE_DIR
    config_dir = config_dir or Path("config")

    logger.info("=" * 70)
    logger.info("PIPELINE EXECUTION")
    logger.info("=" * 70)
    logger.info(f"SIC Codes: {sic_codes}")
    logger.info(f"Counties Filter: {counties if counties else 'None (show all resolved counties)'}")
    logger.info("=" * 70)

    stages_completed = []
    stage_results = {}

    # ============ STAGE A: SIC EXTRACTION ============
    logger.info("")
    logger.info("▶ STAGE A: SIC Extraction")
    logger.info("-" * 70)

    sic_result = extract_companies_by_sic(
        sic_codes=sic_codes,
        csv_path=csv_path,
        force_refresh=force_refresh
    )

    stages_completed.append("sic_extraction")
    stage_results["sic_extraction"] = {
        "output_file": sic_result["output_file"],
        "total_companies": sic_result["stats"].get("total_companies", 0),
        "from_cache": sic_result.get("from_cache", False),
        "stats": sic_result["stats"]
    }

    logger.info(f"✓ Stage A complete: {sic_result['stats'].get('total_companies', 0):,} companies")
    logger.info(f"  Output: {sic_result['output_file']}")
    logger.info(f"  From cache: {sic_result.get('from_cache', False)}")

    # ============ STAGE C: COUNTY RESOLUTION (ALWAYS) + OPTIONAL FILTERING ============
    logger.info("")
    if counties:
        logger.info("▶ STAGE C: County Resolution + Filtering")
        logger.info(f"  Filter: {counties}")
    else:
        logger.info("▶ STAGE C: County Resolution (No Filter)")
        logger.info("  All resolved counties will be included")
    logger.info("-" * 70)

    county_result = resolve_and_filter_by_county(
        sic_extract_file=sic_result["output_file"],
        counties=counties,  # Can be None - Script C handles it
        nspl_path=nspl_path,
        cache_dir=cache_dir,
        config_dir=config_dir,
        force_refresh=force_refresh
    )

    stages_completed.append("county_resolution")

    stats = county_result["stats"]
    total_after = stats.get("after_filter", 0)
    total_before = stats.get("before_filter", 0)

    stage_results["county_resolution"] = {
        "output_file": county_result["output_file"],
        "total_companies": total_after,
        "total_before_filter": total_before,
        "from_cache": county_result.get("from_cache", False),
        "stats": stats
    }

    current_dataset = county_result["output_file"]
    pipeline_state = "county_resolved"

    logger.info(f"✓ Stage C complete: {total_after:,} companies")
    logger.info(f"  Output: {current_dataset}")
    if counties:
        logger.info(f"  Filtered: {total_before:,} → {total_after:,}")
    else:
        logger.info(f"  Resolved: {stats.get('direct_county', 0):,} from CSV, {stats.get('postcode_resolved', 0):,} from postcode")
    logger.info(f"  From cache: {county_result.get('from_cache', False)}")

    # ============ PIPELINE COMPLETE ============
    logger.info("")
    logger.info("=" * 70)
    logger.info("PIPELINE COMPLETE")
    logger.info("=" * 70)
    logger.info(f"Active Dataset: {current_dataset}")
    logger.info(f"Pipeline State: {pipeline_state}")
    logger.info(f"Stages Completed: {' → '.join(stages_completed)}")
    logger.info("=" * 70)

    return {
        "current_dataset": str(current_dataset),
        "pipeline_state": pipeline_state,
        "stages_completed": stages_completed,
        "stage_results": stage_results,
        "can_analyze": True,
        "can_enrich": True,
        "sic_codes": sic_codes,
        "counties": counties
    }


# ============ STAGE B: ANALYSIS ============
def analyze_current_dataset(dataset_file: str) -> Dict[str, any]:
    """
    Run analysis on current dataset (Stage B).
    User-triggered via "Analyze" button.
    """

    from app.services.dataset_analysis import analyze_dataset
    from pathlib import Path
    from datetime import datetime

    logger.info("=" * 70)
    logger.info("STAGE B: Dataset Analysis")
    logger.info("=" * 70)
    logger.info(f"Analyzing: {dataset_file}")

    analysis = analyze_dataset(dataset_file)

    # -------------------------------
    # 🧬 Make Dataset Lineage User-Friendly
    # -------------------------------
    try:
        file_name = Path(dataset_file).name

        # Add a readable dataset label
        analysis["summary"]["dataset_label"] = "Filtered company dataset"

        # Keep filename only (hide internal folder paths)
        analysis["summary"]["dataset_file"] = file_name

        # Format timestamp nicely if present
        if "analysis_timestamp" in analysis["summary"]:
            raw_ts = analysis["summary"]["analysis_timestamp"]
            try:
                formatted_ts = datetime.fromisoformat(raw_ts).strftime("%d %b %Y, %I:%M %p")
                analysis["summary"]["analysis_timestamp"] = formatted_ts
            except Exception:
                pass  # keep original if formatting fails

    except Exception as e:
        logger.warning(f"Could not format dataset lineage: {e}")

    logger.info("✓ Analysis complete")
    logger.info(f"  Total companies: {analysis['summary']['total_companies']:,}")
    logger.info(f"  Data quality: {analysis['data_quality_score']}/100")
    logger.info("=" * 70)

    return analysis



# ============ STAGE D: ENRICHMENT ============
def enrich_current_dataset(
    dataset_file: str,
    output_path: Optional[str] = None,
    progress_callback = None
) -> Dict[str, any]:
    """
    Run enrichment on current dataset (Stage D).
    User-triggered via "Enrich" button.
    """

    from app.services.enrichment import enrich_company_data

    logger.info("=" * 70)
    logger.info("STAGE D: Dataset Enrichment")
    logger.info("=" * 70)
    logger.info(f"Enriching: {dataset_file}")

    result = enrich_company_data(
        input_path=dataset_file,
        output_path=output_path,
        progress_callback=progress_callback
    )

    logger.info("✓ Enrichment complete")
    logger.info(f"  Output: {result.get('output_file')}")
    logger.info(f"  Total processed: {result.get('enrichment_stats', {}).get('total_processed', 0):,}")
    logger.info("=" * 70)

    return result


# ============ STAGE D2: ADVANCED ENRICHMENT (V2) ============
def enrich_current_dataset_v2(
    dataset_file: str,
    output_path: Optional[str] = None,
    progress_callback = None
) -> Dict[str, any]:
    """
    Run advanced enrichment on an already enriched dataset (Stage D2 - V2).
    User-triggered via "Advanced Enrich" button.
    """

    from app.services.enrichment_v2 import enrich_company_data_v2

    logger.info("=" * 70)
    logger.info("STAGE D2: Advanced Dataset Enrichment (V2)")
    logger.info("=" * 70)
    logger.info(f"Advanced enriching: {dataset_file}")

    result = enrich_company_data_v2(
        input_path=dataset_file,
        output_path=output_path,
        progress_callback=progress_callback
    )

    logger.info("✓ Advanced enrichment complete")
    logger.info(f"  Output: {result.get('output_file')}")
    logger.info(f"  Total processed: {result.get('enrichment_stats', {}).get('total_processed', 0):,}")
    logger.info("=" * 70)

    return result