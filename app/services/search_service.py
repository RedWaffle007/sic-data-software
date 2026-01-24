"""
Enhanced Search Service - Search across ALL fields in ALL datasets
"""

import logging
from typing import List, Dict
from sqlalchemy.orm import Session
from sqlalchemy import or_
from app.database import crud
from app.database.models import Company, Dataset

logger = logging.getLogger(__name__)


def search_all_datasets(
    db: Session,
    query: str,
    skip: int = 0,
    limit: int = 500
) -> Dict:
    """
    COMPREHENSIVE CENTRALIZED SEARCH: Search across ALL datasets in ALL company fields.
    
    Now searches in EVERY single text field including:
    - All address fields (line1, line2, town, county, postcode)
    - All person fields (title, fname, sname, position)
    - All PSC/ownership fields
    - All company details (SIC, status, type)
    - All contact info (website, phone, email, website_address)
    - All enrichment fields (selected_person_source, etc.)
    - Address match field
    """
    logger.info(f"COMPREHENSIVE search across ALL datasets for: '{query}' (searching ALL 25+ fields)")
    
    # Use the comprehensive search function from crud
    companies = crud.search_companies_comprehensive(db, query, skip, limit)
    total_count = crud.get_comprehensive_search_count(db, query)
    
    if not companies:
        return {
            "query": query,
            "total_results": 0,
            "datasets": [],
            "message": f"No results found for '{query}' across any field"
        }
    
    # Group by dataset for better organization
    results_by_dataset = {}
    
    for company in companies:
        dataset_id = company.dataset_id
        
        if dataset_id not in results_by_dataset:
            dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
            results_by_dataset[dataset_id] = {
                "dataset_name": dataset.name if dataset else f"Dataset {dataset_id}",
                "dataset_id": dataset_id,
                "dataset_description": dataset.description if dataset else None,
                "sic_codes": dataset.sic_codes if dataset else None,
                "counties": dataset.counties if dataset else None,
                "created_at": dataset.created_at.isoformat() if dataset and dataset.created_at else None,
                "companies": []
            }
        
        # Build comprehensive company result with ALL fields
        company_result = {
            "id": company.id,
            "company_number": company.company_number,
            "business_name": company.business_name,
            
            # Address fields
            "address_line1": company.address_line1,
            "address_line2": company.address_line2,
            "town": company.town,
            "county": company.county,
            "postcode": company.postcode,
            
            # Ownership fields
            "person_with_significant_control": company.person_with_significant_control,
            "nature_of_control": company.nature_of_control,
            
            # Person fields
            "title": company.title,
            "fname": company.fname,
            "sname": company.sname,
            "position": company.position,
            
            # Company details
            "sic": company.sic,
            "company_status": company.company_status,
            "company_type": company.company_type,
            "date_of_creation": company.date_of_creation,
            
            # Contact info
            "website": company.website,
            "phone": company.phone,
            "email": company.email,
            "website_address": company.website_address,
            "address_match": company.address_match,
            
            # Enrichment explanation fields
            "selected_person_source": company.selected_person_source,
            "selected_psc_share_tier": company.selected_psc_share_tier,
            "selected_psc_nature_of_control": company.selected_psc_nature_of_control,
            
            # Search match indicators (for UI highlighting)
            "search_match_info": get_search_match_info(company, query)
        }
        
        results_by_dataset[dataset_id]["companies"].append(company_result)
    
    # Convert to list and sort by number of matches
    datasets_list = sorted(
        results_by_dataset.values(),
        key=lambda x: len(x["companies"]),
        reverse=True
    )
    
    return {
        "query": query,
        "total_results": total_count,
        "returned_results": len(companies),
        "datasets_with_matches": len(datasets_list),
        "datasets": datasets_list,
        "search_fields_covered": [
            "business_name", "company_number", "address_line1", "address_line2",
            "town", "county", "postcode", "person_with_significant_control",
            "nature_of_control", "title", "fname", "sname", "position",
            "sic", "company_status", "company_type", "date_of_creation",
            "website", "phone", "email", "website_address", "address_match",
            "selected_person_source", "selected_psc_share_tier", "selected_psc_nature_of_control"
        ],
        "message": f"Found {total_count} results across {len(datasets_list)} dataset(s) in ALL fields"
    }


def get_search_match_info(company, query: str) -> Dict:
    """
    Identify which fields contain the search term for highlighting.
    """
    query_lower = query.lower()
    match_info = {
        "matched_fields": [],
        "match_count": 0
    }
    
    # Check each field
    fields_to_check = [
        ("business_name", company.business_name),
        ("company_number", company.company_number),
        ("address_line1", company.address_line1),
        ("address_line2", company.address_line2),
        ("town", company.town),
        ("county", company.county),
        ("postcode", company.postcode),
        ("person_with_significant_control", company.person_with_significant_control),
        ("nature_of_control", company.nature_of_control),
        ("title", company.title),
        ("fname", company.fname),
        ("sname", company.sname),
        ("position", company.position),
        ("sic", company.sic),
        ("company_status", company.company_status),
        ("company_type", company.company_type),
        ("date_of_creation", company.date_of_creation),
        ("website", company.website),
        ("phone", company.phone),
        ("email", company.email),
        ("website_address", company.website_address),
        ("address_match", company.address_match),
        ("selected_person_source", company.selected_person_source),
        ("selected_psc_share_tier", company.selected_psc_share_tier),
        ("selected_psc_nature_of_control", company.selected_psc_nature_of_control),
    ]
    
    for field_name, field_value in fields_to_check:
        if field_value and query_lower in str(field_value).lower():
            match_info["matched_fields"].append(field_name)
            match_info["match_count"] += 1
    
    return match_info


def search_within_dataset(
    db: Session,
    dataset_id: int,
    query: str,
    skip: int = 0,
    limit: int = 500
) -> Dict:
    """
    Comprehensive search within a specific dataset only.
    Searches ALL fields within the specified dataset.
    """
    logger.info(f"Comprehensive search within dataset {dataset_id} for: '{query}'")
    
    # Verify dataset exists
    dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
    if not dataset:
        return {
            "error": "Dataset not found",
            "dataset_id": dataset_id
        }
    
    # Use dataset-specific comprehensive search
    companies = crud.search_within_dataset_comprehensive(db, dataset_id, query, skip, limit)
    
    # Get total count
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
    
    total_count = (
        db.query(Company)
        .filter(Company.dataset_id == dataset_id, or_(*search_conditions))
        .count()
    )
    
    results = []
    for company in companies:
        results.append({
            "id": company.id,
            "company_number": company.company_number,
            "business_name": company.business_name,
            
            # Address
            "address_line1": company.address_line1,
            "address_line2": company.address_line2,
            "town": company.town,
            "county": company.county,
            "postcode": company.postcode,
            
            # Person
            "title": company.title,
            "fname": company.fname,
            "sname": company.sname,
            "position": company.position,
            
            # Ownership
            "person_with_significant_control": company.person_with_significant_control,
            "nature_of_control": company.nature_of_control,
            
            # Contact
            "phone": company.phone,
            "email": company.email,
            "website": company.website,
            
            # Quick match indicators for UI
            "match_info": get_search_match_info(company, query)
        })
    
    return {
        "query": query,
        "dataset_id": dataset_id,
        "dataset_name": dataset.name,
        "total_results": total_count,
        "returned_results": len(results),
        "companies": results,
        "search_fields": [
            "business_name", "company_number", "address_line1", "address_line2",
            "town", "county", "postcode", "person_with_significant_control",
            "nature_of_control", "title", "fname", "sname", "position",
            "sic", "company_status", "company_type", "date_of_creation",
            "website", "phone", "email", "website_address", "address_match",
            "selected_person_source", "selected_psc_share_tier", "selected_psc_nature_of_control"
        ]
    }