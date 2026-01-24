"""
CRUD Operations for Database
"""
import logging
from typing import List, Optional, Dict
from sqlalchemy.orm import Session
from sqlalchemy import func, or_
from app.database.models import Dataset, Company, DatasetAnalysis

logger = logging.getLogger(__name__)

# ============ DATASET OPERATIONS ============

def create_dataset(
    db: Session,
    name: str,
    sic_codes: List[str],
    counties: Optional[List[str]] = None,
    description: Optional[str] = None,
    source_file: Optional[str] = None
) -> Dataset:
    """Create a new dataset."""
    dataset = Dataset(
        name=name,
        description=description,
        sic_codes=sic_codes,
        counties=counties,
        source_file=source_file,
        total_companies=0
    )
    db.add(dataset)
    db.commit()
    db.refresh(dataset)
    logger.info(f"Created dataset: {name} (ID: {dataset.id})")
    return dataset


def get_dataset(db: Session, dataset_id: int) -> Optional[Dataset]:
    """Get dataset by ID."""
    return db.query(Dataset).filter(Dataset.id == dataset_id).first()


def get_dataset_by_name(db: Session, name: str) -> Optional[Dataset]:
    """Get dataset by name."""
    return db.query(Dataset).filter(Dataset.name == name).first()


def list_datasets(db: Session, skip: int = 0, limit: int = 100) -> List[Dataset]:
    """List all datasets with pagination."""
    return db.query(Dataset).order_by(Dataset.updated_at.desc()).offset(skip).limit(limit).all()


def update_dataset(db: Session, dataset_id: int, **kwargs) -> Optional[Dataset]:
    """Update dataset fields."""
    dataset = get_dataset(db, dataset_id)
    if not dataset:
        return None
    
    for key, value in kwargs.items():
        if hasattr(dataset, key):
            setattr(dataset, key, value)
    
    db.commit()
    db.refresh(dataset)
    logger.info(f"Updated dataset: {dataset.name}")
    return dataset


def delete_dataset(db: Session, dataset_id: int) -> bool:
    """Delete dataset and all associated companies."""
    dataset = get_dataset(db, dataset_id)
    if not dataset:
        return False
    
    name = dataset.name
    db.delete(dataset)
    db.commit()
    logger.info(f"Deleted dataset: {name}")
    return True


# ============ COMPANY OPERATIONS ============

def bulk_create_companies(db: Session, dataset_id: int, companies_data: List[Dict]) -> int:
    """Bulk insert companies into a dataset."""
    companies = [
        Company(dataset_id=dataset_id, **company_data)
        for company_data in companies_data
    ]
    
    db.bulk_save_objects(companies)
    db.commit()
    
    # Update dataset company count
    count = len(companies)
    dataset = get_dataset(db, dataset_id)
    if dataset:
        dataset.total_companies = count
        db.commit()
    
    logger.info(f"Inserted {count} companies into dataset {dataset_id}")
    return count


def get_companies(
    db: Session,
    dataset_id: int,
    skip: int = 0,
    limit: int = 10000,
    county: Optional[str] = None
) -> List[Company]:
    """Get companies from a dataset with pagination."""
    query = db.query(Company).filter(Company.dataset_id == dataset_id)
    
    if county:
        query = query.filter(Company.county.ilike(f"%{county}%"))
    
    return query.offset(skip).limit(limit).all()


def get_company_count(db: Session, dataset_id: int, county: Optional[str] = None) -> int:
    """Get total count of companies in a dataset (for pagination)."""
    query = db.query(func.count(Company.id)).filter(Company.dataset_id == dataset_id)
    
    if county:
        query = query.filter(Company.county.ilike(f"%{county}%"))
    
    return query.scalar()


def update_company(db: Session, company_id: int, **kwargs) -> Optional[Company]:
    """Update company fields."""
    company = db.query(Company).filter(Company.id == company_id).first()
    if not company:
        return None
    
    for key, value in kwargs.items():
        if hasattr(company, key):
            setattr(company, key, value)
    
    db.commit()
    db.refresh(company)
    logger.info(f"Updated company: {company.company_number}")
    return company


def delete_company(db: Session, company_id: int) -> bool:
    """Delete a company and update dataset count."""
    company = db.query(Company).filter(Company.id == company_id).first()
    if not company:
        return False
    
    dataset_id = company.dataset_id
    db.delete(company)
    db.commit()
    
    # Update dataset count
    count = db.query(Company).filter(Company.dataset_id == dataset_id).count()
    dataset = get_dataset(db, dataset_id)
    if dataset:
        dataset.total_companies = count
        db.commit()
    
    logger.info(f"Deleted company ID: {company_id}")
    return True


# ============ COMPREHENSIVE SEARCH OPERATIONS ============

def search_companies_comprehensive(
    db: Session,
    query: str,
    skip: int = 0,
    limit: int = 500
) -> List[Company]:
    """
    COMPREHENSIVE SEARCH: Search across ALL company fields in ALL datasets.
    """
    search_term = f"%{query}%"
    
    # Build OR conditions for ALL searchable text fields
    search_conditions = [
        # Core fields
        Company.business_name.ilike(search_term),
        Company.company_number.ilike(search_term),
        
        # Address fields
        Company.address_line1.ilike(search_term),
        Company.address_line2.ilike(search_term),
        Company.town.ilike(search_term),
        Company.county.ilike(search_term),
        Company.postcode.ilike(search_term),
        
        # PSC & Ownership fields
        Company.person_with_significant_control.ilike(search_term),
        Company.nature_of_control.ilike(search_term),
        
        # Person fields
        Company.title.ilike(search_term),
        Company.fname.ilike(search_term),
        Company.sname.ilike(search_term),
        Company.position.ilike(search_term),
        
        # Company details
        Company.sic.ilike(search_term),
        Company.company_status.ilike(search_term),
        Company.company_type.ilike(search_term),
        Company.date_of_creation.ilike(search_term),
        
        # Contact fields
        Company.website.ilike(search_term),
        Company.phone.ilike(search_term),
        Company.email.ilike(search_term),
        Company.website_address.ilike(search_term),
        Company.address_match.ilike(search_term),
        
        # Enrichment explanation fields
        Company.selected_person_source.ilike(search_term),
        Company.selected_psc_share_tier.ilike(search_term),
        Company.selected_psc_nature_of_control.ilike(search_term),
    ]
    
    results = db.query(Company).filter(
        or_(*search_conditions)
    ).offset(skip).limit(limit).all()
    
    logger.info(f"Comprehensive search '{query}': {len(results)} results across ALL fields")
    return results


def get_comprehensive_search_count(db: Session, query: str) -> int:
    """
    Get total count for comprehensive search across ALL fields.
    """
    search_term = f"%{query}%"
    
    search_conditions = [
        Company.business_name.ilike(search_term),
        Company.company_number.ilike(search_term),
        Company.address_line1.ilike(search_term),
        Company.address_line2.ilike(search_term),
        Company.town.ilike(search_term),
        Company.county.ilike(search_term),
        Company.postcode.ilike(search_term),
        Company.person_with_significant_control.ilike(search_term),
        Company.nature_of_control.ilike(search_term),
        Company.title.ilike(search_term),
        Company.fname.ilike(search_term),
        Company.sname.ilike(search_term),
        Company.position.ilike(search_term),
        Company.sic.ilike(search_term),
        Company.company_status.ilike(search_term),
        Company.company_type.ilike(search_term),
        Company.date_of_creation.ilike(search_term),
        Company.website.ilike(search_term),
        Company.phone.ilike(search_term),
        Company.email.ilike(search_term),
        Company.website_address.ilike(search_term),
        Company.address_match.ilike(search_term),
        Company.selected_person_source.ilike(search_term),
        Company.selected_psc_share_tier.ilike(search_term),
        Company.selected_psc_nature_of_control.ilike(search_term),
    ]
    
    return db.query(func.count(Company.id)).filter(
        or_(*search_conditions)
    ).scalar()


def search_within_dataset_comprehensive(
    db: Session,
    dataset_id: int,
    query: str,
    skip: int = 0,
    limit: int = 500
) -> List[Company]:
    """
    Comprehensive search within a specific dataset.
    """
    search_term = f"%{query}%"
    
    search_conditions = [
        Company.business_name.ilike(search_term),
        Company.company_number.ilike(search_term),
        Company.address_line1.ilike(search_term),
        Company.address_line2.ilike(search_term),
        Company.town.ilike(search_term),
        Company.county.ilike(search_term),
        Company.postcode.ilike(search_term),
        Company.person_with_significant_control.ilike(search_term),
        Company.nature_of_control.ilike(search_term),
        Company.title.ilike(search_term),
        Company.fname.ilike(search_term),
        Company.sname.ilike(search_term),
        Company.position.ilike(search_term),
        Company.sic.ilike(search_term),
        Company.company_status.ilike(search_term),
        Company.company_type.ilike(search_term),
        Company.date_of_creation.ilike(search_term),
        Company.website.ilike(search_term),
        Company.phone.ilike(search_term),
        Company.email.ilike(search_term),
        Company.website_address.ilike(search_term),
        Company.address_match.ilike(search_term),
        Company.selected_person_source.ilike(search_term),
        Company.selected_psc_share_tier.ilike(search_term),
        Company.selected_psc_nature_of_control.ilike(search_term),
    ]
    
    results = db.query(Company).filter(
        Company.dataset_id == dataset_id,
        or_(*search_conditions)
    ).offset(skip).limit(limit).all()
    
    logger.info(f"Dataset {dataset_id} search '{query}': {len(results)} results")
    return results


# ============ ANALYSIS OPERATIONS ============

def save_analysis(
    db: Session,
    dataset_id: int,
    analysis_data: Dict
) -> DatasetAnalysis:
    """Save or update analysis for a dataset."""
    # Delete existing analysis
    db.query(DatasetAnalysis).filter(DatasetAnalysis.dataset_id == dataset_id).delete()
    
    analysis = DatasetAnalysis(
        dataset_id=dataset_id,
        total_companies=analysis_data.get("summary", {}).get("total_companies", 0),
        unique_counties=analysis_data.get("summary", {}).get("unique_counties", 0),
        data_quality_score=analysis_data.get("data_quality_score", 0),
        regional_distribution=analysis_data.get("regional_distribution", []),
        county_resolution=analysis_data.get("county_resolution", {}),
        missing_data=analysis_data.get("missing_data", {})
    )
    
    db.add(analysis)
    db.commit()
    db.refresh(analysis)
    logger.info(f"Saved analysis for dataset {dataset_id}")
    return analysis


def get_analysis(db: Session, dataset_id: int) -> Optional[DatasetAnalysis]:
    """Get analysis for a dataset."""
    return db.query(DatasetAnalysis).filter(DatasetAnalysis.dataset_id == dataset_id).first()


# ============ DATASET STATISTICS ============

def get_dataset_stats(db: Session, dataset_id: int) -> Dict:
    """Get statistics for a dataset."""
    stats = {}
    
    # Total companies
    stats['total_companies'] = get_company_count(db, dataset_id)
    
    # County distribution
    county_distribution = (
        db.query(Company.county, func.count(Company.id).label('count'))
        .filter(Company.dataset_id == dataset_id, Company.county != '')
        .group_by(Company.county)
        .order_by(func.count(Company.id).desc())
        .all()
    )
    stats['county_distribution'] = [
        {"county": c[0], "count": c[1], "percentage": round((c[1] / stats['total_companies']) * 100, 1)}
        for c in county_distribution
    ]
    
    # Data completeness
    total = stats['total_companies']
    
    completeness = {}
    for field in ['county', 'postcode', 'phone', 'email', 'website']:
        field_attr = getattr(Company, field)
        count = db.query(func.count(Company.id)).filter(
            Company.dataset_id == dataset_id,
            field_attr != '',
            field_attr.isnot(None)
        ).scalar()
        completeness[field] = round((count / total) * 100, 1) if total > 0 else 0
    
    stats['data_completeness'] = completeness
    
    return stats