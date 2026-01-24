"""
FastAPI Routes for Dataset Management (PWA Backend)

Endpoints:
    POST   /api/datasets/save          - Save extraction result as dataset
    GET    /api/datasets               - List all datasets
    GET    /api/datasets/{id}          - Get dataset details
    PUT    /api/datasets/{id}          - Update dataset metadata
    DELETE /api/datasets/{id}          - Delete dataset
    
    GET    /api/datasets/{id}/companies - Get companies in dataset
    PUT    /api/datasets/{id}/companies/{company_id} - Update company
    DELETE /api/datasets/{id}/companies/{company_id} - Delete company
    
    POST   /api/datasets/{id}/analyze  - Regenerate analysis
    GET    /api/datasets/{id}/analysis - Get cached analysis
    
    GET    /api/search                 - Global search across datasets
    GET    /api/datasets/{id}/export   - Export dataset to CSV/Excel
"""

import logging
from pathlib import Path
from typing import List, Optional
from fastapi import APIRouter, HTTPException, Depends, Response, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session
import polars as pl
from io import StringIO, BytesIO

from app.database import get_db, crud
from app.services.dataset_manager import import_parquet_to_dataset, regenerate_analysis
from app.services.search_service import search_all_datasets

logger = logging.getLogger(__name__)
router = APIRouter()

# ============ REQUEST MODELS ============

class SaveDatasetRequest(BaseModel):
    dataset_name: str = Field(..., description="Unique name for this dataset")
    parquet_file: str = Field(..., description="Path to parquet file from extraction/enrichment")
    sic_codes: List[str] = Field(..., description="SIC codes used for extraction")
    counties: Optional[List[str]] = Field(None, description="Counties used for filtering")
    description: Optional[str] = Field(None, description="Optional description")


class UpdateDatasetRequest(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None


class UpdateCompanyRequest(BaseModel):
    business_name: Optional[str] = None
    address_line1: Optional[str] = None
    address_line2: Optional[str] = None
    town: Optional[str] = None
    county: Optional[str] = None
    postcode: Optional[str] = None
    title: Optional[str] = None
    fname: Optional[str] = None
    sname: Optional[str] = None
    position: Optional[str] = None
    company_status: Optional[str] = None
    # Add other fields as needed
    website: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    website_address: Optional[str] = None
    address_match: Optional[str] = None
    

# ============ DATASET ENDPOINTS ============

@router.post("/api/datasets/save")
async def save_dataset(request: SaveDatasetRequest, db: Session = Depends(get_db)):
    """
    Save an extraction/enrichment result as a named dataset.
    This is called after user clicks "Save to Database" button.
    """
    try:
        logger.info(f"Saving dataset: {request.dataset_name}")
        
        result = import_parquet_to_dataset(
            db=db,
            parquet_file=request.parquet_file,
            dataset_name=request.dataset_name,
            sic_codes=request.sic_codes,
            counties=request.counties,
            description=request.description
        )
        
        return {
            "success": True,
            "dataset_id": result["dataset_id"],
            "dataset_name": result["dataset_name"],
            "total_companies": result["total_companies"],
            "message": f"Dataset '{request.dataset_name}' saved successfully"
        }
        
    except ValueError as e:
        logger.error(f"Validation error: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    
    except FileNotFoundError as e:
        logger.error(f"File not found: {e}")
        raise HTTPException(status_code=404, detail=str(e))
    
    except Exception as e:
        logger.error(f"Failed to save dataset: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Save failed: {str(e)}")


@router.get("/api/datasets")
async def list_datasets(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    db: Session = Depends(get_db)
):
    """
    List all saved datasets with pagination.
    """
    try:
        datasets = crud.list_datasets(db, skip=skip, limit=limit)
        
        return {
            "success": True,
            "total": len(datasets),
            "datasets": [
                {
                    "id": d.id,
                    "name": d.name,
                    "description": d.description,
                    "total_companies": d.total_companies,
                    "sic_codes": d.sic_codes,
                    "counties": d.counties,
                    "created_at": d.created_at.isoformat() if d.created_at else None,
                    "updated_at": d.updated_at.isoformat() if d.updated_at else None
                }
                for d in datasets
            ]
        }
        
    except Exception as e:
        logger.error(f"Failed to list datasets: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/datasets/{dataset_id}")
async def get_dataset(dataset_id: int, db: Session = Depends(get_db)):
    """
    Get detailed information about a specific dataset.
    """
    try:
        dataset = crud.get_dataset(db, dataset_id)
        
        if not dataset:
            raise HTTPException(status_code=404, detail="Dataset not found")
        
        return {
            "success": True,
            "dataset": {
                "id": dataset.id,
                "name": dataset.name,
                "description": dataset.description,
                "total_companies": dataset.total_companies,
                "sic_codes": dataset.sic_codes,
                "counties": dataset.counties,
                "source_file": dataset.source_file,
                "created_at": dataset.created_at.isoformat() if dataset.created_at else None,
                "updated_at": dataset.updated_at.isoformat() if dataset.updated_at else None
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get dataset: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/api/datasets/{dataset_id}")
async def update_dataset(
    dataset_id: int,
    request: UpdateDatasetRequest,
    db: Session = Depends(get_db)
):
    """
    Update dataset metadata (name, description).
    """
    try:
        update_data = request.dict(exclude_unset=True)
        
        dataset = crud.update_dataset(db, dataset_id, **update_data)
        
        if not dataset:
            raise HTTPException(status_code=404, detail="Dataset not found")
        
        return {
            "success": True,
            "message": "Dataset updated successfully",
            "dataset": {
                "id": dataset.id,
                "name": dataset.name,
                "description": dataset.description
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to update dataset: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/api/datasets/{dataset_id}")
async def delete_dataset(dataset_id: int, db: Session = Depends(get_db)):
    """
    Delete a dataset and all its companies.
    """
    try:
        success = crud.delete_dataset(db, dataset_id)
        
        if not success:
            raise HTTPException(status_code=404, detail="Dataset not found")
        
        return {
            "success": True,
            "message": "Dataset deleted successfully"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete dataset: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ============ COMPANY ENDPOINTS ============

@router.get("/api/datasets/{dataset_id}/companies")
async def get_companies(
    dataset_id: int,
    skip: int = Query(0, ge=0),
    limit: int = Query(10000, ge=1, le=50000),  # FIXED: Increased max limit to 50k
    county: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    """
    Get companies from a dataset with pagination and optional county filter.
    Now returns up to 10,000 companies by default (max 50,000).
    """
    try:
        # Verify dataset exists
        dataset = crud.get_dataset(db, dataset_id)
        if not dataset:
            raise HTTPException(status_code=404, detail="Dataset not found")
        
        companies = crud.get_companies(db, dataset_id, skip=skip, limit=limit, county=county)
        total_count = crud.get_company_count(db, dataset_id, county=county)
        
        return {
            "success": True,
            "dataset_id": dataset_id,
            "dataset_name": dataset.name,
            "total": total_count,
            "returned": len(companies),
            "skip": skip,
            "limit": limit,
           "companies": [
                {
                    "id": c.id,

                    "company_number": c.company_number,
                    "business_name": c.business_name,

                    "address_line1": c.address_line1,
                    "address_line2": c.address_line2,
                    "town": c.town,
                    "county": c.county,
                    "postcode": c.postcode,

                    "person_with_significant_control": c.person_with_significant_control,
                    "nature_of_control": c.nature_of_control,

                    "title": c.title,
                    "fname": c.fname,
                    "sname": c.sname,
                    "position": c.position,

                    "sic": c.sic,
                    "company_status": c.company_status,
                    "company_type": c.company_type,
                    "date_of_creation": c.date_of_creation,

                    "website": c.website,
                    "phone": c.phone,
                    "email": c.email,
                    "website_address": c.website_address,
                    "address_match": c.address_match,
                }
                for c in companies
            ]
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get companies: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
    

@router.put("/api/datasets/{dataset_id}/companies/{company_id}")
async def update_company(
    dataset_id: int,
    company_id: int,
    request: UpdateCompanyRequest,
    db: Session = Depends(get_db)
):
    """
    Update a company's details.
    Analysis should be regenerated after edits.
    """
    try:
        update_data = request.dict(exclude_unset=True)
        
        company = crud.update_company(db, company_id, **update_data)
        
        if not company:
            raise HTTPException(status_code=404, detail="Company not found")
        
        if company.dataset_id != dataset_id:
            raise HTTPException(status_code=400, detail="Company does not belong to this dataset")
        
        return {
            "success": True,
            "message": "Company updated successfully. Consider regenerating analysis.",
            "company": {
                "id": company.id,
                "company_number": company.company_number,
                "business_name": company.business_name
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to update company: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.patch("/api/companies/{company_id}")
async def patch_company(
    company_id: int,
    request: UpdateCompanyRequest,
    db: Session = Depends(get_db)
):
    """
    PARTIAL UPDATE (Excel-style cell editing).

    This endpoint is intentionally dataset-agnostic.
    It allows updating a single field at a time (PATCH semantics),
    which is ideal for inline table editing in the UI.

    Example:
        PATCH /api/companies/123
        {
            "county": "Essex"
        }
    """
    try:
        update_data = request.dict(exclude_unset=True)

        if not update_data:
            raise HTTPException(
                status_code=400,
                detail="No fields provided for update"
            )

        company = crud.update_company(db, company_id, **update_data)

        if not company:
            raise HTTPException(status_code=404, detail="Company not found")

        return {
            "success": True,
            "message": "Company updated successfully",
            "updated_fields": list(update_data.keys()),
            "company": {
                "id": company.id,
                "company_number": company.company_number,
                "business_name": company.business_name,
                "county": company.county,
                "postcode": company.postcode,
                "title": company.title,
                "fname": company.fname,
                "sname": company.sname,
                "position": company.position,
                "company_status": company.company_status
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to patch company: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/api/datasets/{dataset_id}/companies/{company_id}")
async def delete_company(
    dataset_id: int,
    company_id: int,
    db: Session = Depends(get_db)
):
    """
    Delete a company from a dataset.
    """
    try:
        success = crud.delete_company(db, company_id)
        
        if not success:
            raise HTTPException(status_code=404, detail="Company not found")
        
        return {
            "success": True,
            "message": "Company deleted successfully"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete company: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ============ ANALYSIS ENDPOINTS ============

@router.post("/api/datasets/{dataset_id}/analyze")
async def analyze_dataset_endpoint(dataset_id: int, db: Session = Depends(get_db)):
    """
    Regenerate analysis for a dataset (after edits).
    """
    try:
        analysis = regenerate_analysis(db, dataset_id)
        
        return {
            "success": True,
            "message": "Analysis regenerated successfully",
            "analysis": analysis
        }
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to regenerate analysis: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/datasets/{dataset_id}/analysis")
async def get_analysis(dataset_id: int, db: Session = Depends(get_db)):
    """
    Get cached analysis for a dataset.
    """
    try:
        analysis = crud.get_analysis(db, dataset_id)
        
        if not analysis:
            raise HTTPException(
                status_code=404,
                detail="Analysis not found. Run POST /api/datasets/{id}/analyze first."
            )
        
        return {
            "success": True,
            "analysis": {
                "total_companies": analysis.total_companies,
                "unique_counties": analysis.unique_counties,
                "data_quality_score": analysis.data_quality_score,
                "regional_distribution": analysis.regional_distribution,
                "county_resolution": analysis.county_resolution,
                "missing_data": analysis.missing_data,
                "generated_at": analysis.generated_at.isoformat() if analysis.generated_at else None
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get analysis: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ============ SEARCH ENDPOINT ============

@router.get("/api/search")
async def search_global(
    q: str = Query(..., min_length=1, description="Search query"),
    skip: int = Query(0, ge=0),
    limit: int = Query(500, ge=1, le=2000),
    db: Session = Depends(get_db)
):
    """
    COMPREHENSIVE SEARCH: Search across ALL datasets in ALL company fields.
    Now searches in 25+ fields including addresses, contact info, PSC details, etc.
    """
    try:
        results = search_all_datasets(db, q, skip=skip, limit=limit)
        
        return {
            "success": True,
            "total_matching": results["total_results"],
            "returned": results["returned_results"],
            "datasets": results["datasets"],
            "search_info": {
                "query": q,
                "fields_searched": results.get("search_fields_covered", []),
                "datasets_with_matches": results.get("datasets_with_matches", 0)
            }
        }
        
    except Exception as e:
        logger.error(f"Search failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

# ============ EXPORT ENDPOINT ============

@router.get("/api/datasets/{dataset_id}/export")
async def export_dataset(
    dataset_id: int,
    format: str = Query("csv", regex="^(csv|xlsx)$"),
    db: Session = Depends(get_db)
):
    """
    Export dataset to CSV or Excel.
    Includes all fields including new enrichment explanation columns.
    """
    try:
        dataset = crud.get_dataset(db, dataset_id)
        if not dataset:
            raise HTTPException(status_code=404, detail="Dataset not found")
        
        # Fetch all companies
        companies = crud.get_companies(db, dataset_id, limit=1000000)
        
        # Convert to DataFrame with ALL fields including new ones
        data = []
        for c in companies:
            data.append({
                "CompanyNumber": c.company_number,
                "BusinessName": c.business_name,
                "AddressLine1": c.address_line1,
                "AddressLine2": c.address_line2,
                "Town": c.town,
                "County": c.county,
                "Postcode": c.postcode,
                "PSC": c.person_with_significant_control,
                "NatureOfControl": c.nature_of_control,
                "Title": c.title,
                "FirstName": c.fname,
                "Surname": c.sname,
                "SelectedPersonSource": c.selected_person_source,
                "SelectedPSCShareTier": c.selected_psc_share_tier,
                "SelectedPSCNatureOfControl": c.selected_psc_nature_of_control,
                "Position": c.position,
                "SIC": c.sic,
                "CompanyStatus": c.company_status,
                "CompanyType": c.company_type,
                "DateOfCreation": c.date_of_creation,
                "Website": c.website,
                "Phone": c.phone,
                "Email": c.email
            })
        
        df = pl.DataFrame(data)
        
        if format == "csv":
            buffer = StringIO()
            df.write_csv(buffer)
            content = buffer.getvalue().encode()
            media_type = "text/csv"
            filename = f"{dataset.name.replace(' ', '_')}.csv"
            
        else:  # xlsx
            buffer = BytesIO()
            df.to_pandas().to_excel(buffer, index=False, engine='openpyxl')
            content = buffer.getvalue()
            media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            filename = f"{dataset.name.replace(' ', '_')}.xlsx"
        
        return Response(
            content=content,
            media_type=media_type,
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Export failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Export failed: {str(e)}")


@router.get("/api/datasets/health")
async def health_check():
    """Health check for database routes."""
    return {"status": "healthy", "service": "dataset-management"}