"""
Dataset Management Service
Handles importing parquet files into database
"""
import logging
from pathlib import Path
from typing import Dict, Optional, List
import polars as pl
from sqlalchemy.orm import Session
from app.database import crud
from app.services.dataset_analysis import analyze_dataset

logger = logging.getLogger(__name__)

# ============ DATASET IMPORT ============

def import_parquet_to_dataset(
    db: Session,
    parquet_file: str,
    dataset_name: str,
    sic_codes: List[str],
    counties: Optional[List[str]] = None,
    description: Optional[str] = None
) -> Dict:
    """
    Import a parquet file (from extraction/enrichment) into database as a dataset.
    """
    parquet_path = Path(parquet_file)
    if not parquet_path.exists():
        raise FileNotFoundError(f"Parquet file not found: {parquet_file}")
    
    logger.info(f"Importing {parquet_file} as dataset '{dataset_name}'...")
    
    # Check if dataset name already exists
    existing = crud.get_dataset_by_name(db, dataset_name)
    if existing:
        raise ValueError(f"Dataset '{dataset_name}' already exists")
    
    # Read parquet
    df = pl.read_parquet(parquet_file)
    total_rows = df.height
    
    logger.info(f"Read {total_rows:,} companies from parquet")
    
    # Create dataset
    dataset = crud.create_dataset(
        db=db,
        name=dataset_name,
        sic_codes=sic_codes,
        counties=counties,
        description=description,
        source_file=str(parquet_file)
    )
    
    # Prepare company data
    companies_data = []
    for row in df.iter_rows(named=True):
        company = {
            "company_number": row.get("CompanyNumber", ""),
            "business_name": row.get("BusinessName", ""),
            "address_line1": row.get("AddressLine1", ""),
            "address_line2": row.get("AddressLine2", ""),
            "town": row.get("Town", ""),
            "county": row.get("County") or row.get("ResolvedCounty", ""),
            "postcode": row.get("Postcode", ""),
            "person_with_significant_control": row.get("PersonWithSignificantControl", ""),
            "nature_of_control": row.get("NatureOfControl", ""),
            "title": row.get("Title", ""),
            "fname": row.get("Fname") or row.get("OfficerFirstName", ""),
            "sname": row.get("Sname") or row.get("OfficerSurname", ""),
            
            # NEW: Enrichment explanation columns
            "selected_person_source": row.get("SelectedPersonSource", ""),
            "selected_psc_share_tier": row.get("SelectedPSCShareTier", ""),
            "selected_psc_nature_of_control": row.get("SelectedPSCNatureOfControl", ""),
            
            "position": row.get("Position") or row.get("OfficerPosition", ""),
            "sic": row.get("SIC", ""),
            "company_status": row.get("CompanyStatus", ""),
            "company_type": row.get("CompanyType", ""),
            "date_of_creation": row.get("DateOfCreation", ""),
            "website": row.get("Website", ""),
            "phone": row.get("Phone", ""),
            "email": row.get("Email", ""),
            "website_address": row.get("WebsiteAddress", ""),
            "address_match": row.get("AddressMatch(RegVsWeb)", "")
        }
        companies_data.append(company)
    
    # Bulk insert
    inserted_count = crud.bulk_create_companies(db, dataset.id, companies_data)
    
    logger.info(f"✓ Dataset '{dataset_name}' created with {inserted_count:,} companies")
    
    return {
        "dataset_id": dataset.id,
        "dataset_name": dataset_name,
        "total_companies": inserted_count,
        "source_file": str(parquet_file)
    }


# ============ ANALYSIS REGENERATION ============

def regenerate_analysis(db: Session, dataset_id: int) -> Dict:
    """
    Regenerate analysis for a dataset after edits.
    """
    logger.info(f"Regenerating analysis for dataset {dataset_id}...")
    
    dataset = crud.get_dataset(db, dataset_id)
    if not dataset:
        raise ValueError(f"Dataset {dataset_id} not found")
    
    # Export to temporary parquet for analysis
    import tempfile
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = tmp.name
    
    try:
        # Fetch all companies
        companies = crud.get_companies(db, dataset_id, limit=1000000)
        
        # Convert to polars DataFrame
        data = []
        for c in companies:
            data.append({
                "CompanyNumber": c.company_number,
                "BusinessName": c.business_name,
                "County": c.county,
                "Postcode": c.postcode,
                "Town": c.town,
                # Add other fields as needed for analysis
            })
        
        df = pl.DataFrame(data)
        df.write_parquet(tmp_path)
        
        # Run analysis
        analysis_result = analyze_dataset(tmp_path)
        
        # Save to database
        crud.save_analysis(db, dataset_id, analysis_result)
        
        logger.info(f"✓ Analysis regenerated for dataset {dataset_id}")
        return analysis_result
        
    finally:
        Path(tmp_path).unlink(missing_ok=True)