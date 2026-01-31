"""
FastAPI Routes for Company Dataset Pipeline

Endpoints:
    POST /api/extract - Execute pipeline (SIC + optional counties)
    POST /api/analyze - Analyze current dataset
    POST /api/enrich - Enrich current dataset (async)
    GET /api/status/{job_id} - Get job status
    GET /api/download/{job_id} - Download results
"""

import logging
from pathlib import Path
from typing import List, Optional
from unittest import result
from fastapi import APIRouter, HTTPException, BackgroundTasks, Response
from pydantic import BaseModel, Field
import polars as pl
from fastapi import UploadFile, File
import io
from datetime import datetime

from app.services.pipeline_orchestrator import (
    execute_pipeline,
    analyze_current_dataset,
    enrich_current_dataset,
    enrich_current_dataset_v2
)

logger = logging.getLogger(__name__)
router = APIRouter()

# ============ REQUEST MODELS ============
class ExtractRequest(BaseModel):
    sic_codes: List[str] = Field(..., description="List of SIC codes to extract")
    counties: Optional[List[str]] = Field(None, description="Optional counties to filter")
    force_refresh: bool = Field(False, description="Force cache refresh")


class AnalyzeRequest(BaseModel):
    dataset_file: str = Field(..., description="Path to dataset file")


class EnrichRequest(BaseModel):
    dataset_file: str = Field(..., description="Path to dataset file")
    output_format: str = Field("parquet", description="Output format: parquet, csv, or xlsx")


# ============ IN-MEMORY JOB TRACKING ============
# In production, use Redis or a database
JOBS = {}


def generate_job_id() -> str:
    import uuid
    return str(uuid.uuid4())[:8]


# ============ ENDPOINTS ============
@router.post("/api/extract")
async def extract_companies(request: ExtractRequest):
    """
    Execute pipeline: Extract companies by SIC codes (and optionally filter by counties).
    """
    try:
        logger.info(f"Extract request: SIC={request.sic_codes}, Counties={request.counties}")

        result = execute_pipeline(
            sic_codes=request.sic_codes,
            counties=request.counties,
            force_refresh=request.force_refresh
        )

        job_id = generate_job_id()
        JOBS[job_id] = {
            "job_id": job_id,
            "type": "extract",
            "status": "completed",
            "result": result
        }

        return {
            "success": True,
            "job_id": job_id,
            "pipeline_state": result["pipeline_state"],
            "current_dataset": result["current_dataset"],
            "stages_completed": result["stages_completed"],
            "stage_results": result["stage_results"],
            "can_analyze": result["can_analyze"],
            "can_enrich": result["can_enrich"]
        }

    except FileNotFoundError as e:
        logger.error(f"File not found: {e}")
        raise HTTPException(status_code=404, detail=str(e))

    except ValueError as e:
        logger.error(f"Validation error: {e}")
        raise HTTPException(status_code=400, detail=str(e))

    except Exception as e:
        logger.error(f"Extract failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Pipeline failed: {str(e)}")


@router.post("/api/analyze")
async def analyze_dataset(request: AnalyzeRequest):
    """
    Analyze current dataset (Stage B).
    """
    try:
        logger.info(f"Analyze request: {request.dataset_file}")

        if not Path(request.dataset_file).exists():
            raise HTTPException(status_code=404, detail="Dataset file not found")

        analysis = analyze_current_dataset(request.dataset_file)

        job_id = generate_job_id()
        JOBS[job_id] = {
            "job_id": job_id,
            "type": "analyze",
            "status": "completed",
            "result": analysis
        }

        return {
            "success": True,
            "job_id": job_id,
            "analysis": analysis
        }

    except Exception as e:
        logger.error(f"Analysis failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Analysis failed: {str(e)}")


# ============ BACKGROUND TASK ============
def run_enrichment_background(job_id: str, dataset_file: str, output_format: str):
    """Background task for enrichment."""
    try:
        JOBS[job_id]["status"] = "processing"

        # 🔹 Read dataset to get total rows
        df = pl.read_parquet(dataset_file)
        JOBS[job_id]["total"] = df.height
        JOBS[job_id]["processed"] = 0

        # Determine output path
        output_path = None
        if output_format != "parquet":
            from app.config import OUTPUT_DIR
            import time

            OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            output_path = OUTPUT_DIR / f"enriched_{timestamp}.{output_format}"

        # 🔹 Call enrichment with a progress callback
        result = enrich_current_dataset(
            dataset_file=dataset_file,
            output_path=str(output_path) if output_path else None,
            progress_callback=lambda n: JOBS[job_id].update({"processed": n})
        )


        JOBS[job_id]["status"] = "completed"
        JOBS[job_id]["result"] = result

    except Exception as e:
        logger.error(f"Enrichment failed for job {job_id}: {e}", exc_info=True)
        JOBS[job_id]["status"] = "failed"
        JOBS[job_id]["error"] = str(e)


def run_enrichment_v2_background(job_id: str, dataset_file: str, output_format: str):
    """Background task for advanced enrichment (V2)."""
    try:
        JOBS[job_id]["status"] = "processing"
        JOBS[job_id]["processed"] = 0

        output_path = None
        if output_format != "parquet":
            from app.config import OUTPUT_DIR
            import time
            OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            output_path = OUTPUT_DIR / f"enriched_v2_{timestamp}.{output_format}"

        result = enrich_current_dataset_v2(
            dataset_file=dataset_file,
            output_path=str(output_path) if output_path else None,
            progress_callback=lambda processed, total: JOBS[job_id].update({
                "processed": processed,
                "total": total
            })
        )

        JOBS[job_id]["status"] = "completed"
        JOBS[job_id]["result"] = result

    except Exception as e:
        logger.error(f"V2 Enrichment failed for job {job_id}: {e}", exc_info=True)
        JOBS[job_id]["status"] = "failed"
        JOBS[job_id]["error"] = str(e)


@router.post("/api/enrich")
async def enrich_dataset(request: EnrichRequest, background_tasks: BackgroundTasks):
    """
    Enrich current dataset (Stage D).
    Runs as background task due to long execution time.
    """
    try:
        logger.info(f"Enrich request: {request.dataset_file}")

        if not Path(request.dataset_file).exists():
            raise HTTPException(status_code=404, detail="Dataset file not found")

        job_id = generate_job_id()
        JOBS[job_id] = {
            "job_id": job_id,
            "type": "enrich",
            "status": "queued",
            "dataset_file": request.dataset_file,
            "output_format": request.output_format,
            "total": 0,
            "processed": 0
        }

        background_tasks.add_task(
            run_enrichment_background,
            job_id,
            request.dataset_file,
            request.output_format
        )

        return {
            "success": True,
            "job_id": job_id,
            "message": "Enrichment started in background",
            "status": "queued"
        }

    except Exception as e:
        logger.error(f"Enrich request failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Enrichment failed: {str(e)}")


@router.post("/api/enrich-v2")
async def enrich_dataset_v2(request: EnrichRequest, background_tasks: BackgroundTasks):
    """
    Advanced Enrichment (Stage D2).
    Uses search + LLM to find website, phone, email, address match, confidence score.
    """
    try:
        logger.info(f"Advanced Enrich request: {request.dataset_file}")

        if not Path(request.dataset_file).exists():
            raise HTTPException(status_code=404, detail="Dataset file not found")

        job_id = generate_job_id()
        JOBS[job_id] = {
            "job_id": job_id,
            "type": "enrich_v2",
            "status": "queued",
            "dataset_file": request.dataset_file,
            "output_format": request.output_format,
            "processed": 0
        }

        background_tasks.add_task(
            run_enrichment_v2_background,
            job_id,
            request.dataset_file,
            request.output_format
        )

        return {
            "success": True,
            "job_id": job_id,
            "message": "Advanced enrichment started",
            "status": "queued"
        }

    except Exception as e:
        logger.error(f"Advanced enrich request failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Enrichment V2 failed: {str(e)}")


@router.get("/api/status/{job_id}")
async def get_job_status(job_id: str):
    if job_id not in JOBS:
        raise HTTPException(status_code=404, detail="Job not found")

    job = JOBS[job_id]
    response = {
        "job_id": job_id,
        "type": job["type"],
        "status": job["status"]
    }

    if job["status"] == "processing":
        response["processed"] = job.get("processed", 0)
        response["total"] = job.get("total", 0)

    if job["status"] == "completed":
        response["result"] = job.get("result")
    elif job["status"] == "failed":
        response["error"] = job.get("error")

    return response



@router.get("/api/download/{job_id}")
async def download_result(job_id: str, format: str = "csv"):
    """Download result file from completed job."""
    if job_id not in JOBS:
        raise HTTPException(status_code=404, detail="Job not found")

    job = JOBS[job_id]

    if job["status"] != "completed":
        raise HTTPException(status_code=400, detail=f"Job status: {job['status']}")

    try:
        result = job["result"]

        if job["type"] == "extract":
            dataset_file = result["current_dataset"]
        elif job["type"] in ("enrich", "enrich_v2"):
            dataset_file = result["output_file"]
        else:
            raise HTTPException(status_code=400, detail="This job type does not produce downloadable files")

        # 🔧 FIX: Resolve dataset path safely to absolute path
        BASE_DIR = Path(__file__).resolve().parent.parent
        dataset_path = BASE_DIR / dataset_file

        if not dataset_path.exists():
            raise FileNotFoundError(f"No such file or directory: {dataset_path}")

        df = pl.read_parquet(dataset_path)

        if format == "csv":
            from io import StringIO
            buffer = StringIO()
            df.write_csv(buffer)
            content = buffer.getvalue().encode()
            media_type = "text/csv"
            filename = f"companies_{job_id}.csv"

        elif format == "xlsx":
            from io import BytesIO
            buffer = BytesIO()
            df.to_pandas().to_excel(buffer, index=False)
            content = buffer.getvalue()
            media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            filename = f"companies_{job_id}.xlsx"

        else:
            raise HTTPException(status_code=400, detail="Unsupported format")

        return Response(
            content=content,
            media_type=media_type,
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )

    except Exception as e:
        logger.error(f"Download failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Download failed: {str(e)}")


@router.post("/api/comparison/analyze-final")
async def analyze_final_dataset(file: UploadFile = File(...)):
    """
    Analyze uploaded final dataset (Excel or CSV) for comparison.
    Returns same format as analyze_dataset for side-by-side comparison.
    """
    try:
        logger.info(f"Comparison analysis started for file: {file.filename}")
        
        # Import required functions from dataset_analysis
        from app.services.dataset_analysis import (
            normalize_county,
            is_england_county,
            ENGLAND_REGIONS
        )
        
        # Read the uploaded file
        contents = await file.read()
        
        # Determine file type and read accordingly
        if file.filename.endswith('.csv'):
            df = pl.read_csv(io.BytesIO(contents))
        elif file.filename.endswith(('.xlsx', '.xls')):
            df = pl.read_excel(io.BytesIO(contents))
        else:
            raise HTTPException(status_code=400, detail="Unsupported file format. Use CSV or Excel.")
        
        logger.info(f"File loaded: {df.height} rows, {len(df.columns)} columns")
        
        # Check for county column
        county_col = None
        for col in df.columns:
            col_lower = col.lower().strip()
            if col_lower in ['county', 'resolvedcounty', 'resolved_county', 'resolved county']:
                county_col = col
                break
        
        if not county_col:
            logger.error(f"No county column found. Available columns: {df.columns}")
            raise HTTPException(
                status_code=400, 
                detail=f"File must contain a 'County' or 'ResolvedCounty' column. Found columns: {', '.join(df.columns)}"
            )
        
        logger.info(f"Using county column: {county_col}")
        
        # Analyze the dataframe
        total = df.height
        
        if total == 0:
            return {
                "success": True,
                "analysis": {
                    "summary": {
                        "total_companies": 0,
                        "total_england_companies": 0,
                        "unique_counties": 0,
                        "analysis_timestamp": datetime.now().isoformat(),
                    },
                    "regional_distribution": [],
                    "data_quality_score": 0,
                },
                "filename": file.filename,
                "total_rows": 0
            }
        
        # Filter for England only
        england_df = df.filter(
            pl.col(county_col)
            .map_elements(
                lambda x: is_england_county(str(x)) if x and str(x).strip() else False, 
                return_dtype=pl.Boolean
            )
        )
        
        total_england = england_df.height
        logger.info(f"England companies: {total_england} out of {total}")
        
        # Regional Distribution
        regional_distribution = []
        
        if total_england > 0:
            # Get county counts
            county_counts = (
                england_df.filter(pl.col(county_col) != "")
                .group_by(county_col)
                .agg(pl.count().alias("count"))
            )
            
            # Convert to dict (normalized keys)
            county_dict = {}
            for row in county_counts.iter_rows(named=True):
                county_name = row[county_col]
                normalized = normalize_county(str(county_name))
                count = int(row["count"])
                county_dict[normalized] = count
            
            logger.info(f"County distribution: {county_dict}")
            
            # Build regional distribution
            for region_name, region_data in ENGLAND_REGIONS.items():
                region_total = 0
                county_breakdown = []
                
                for county in region_data["counties"]:
                    normalized = normalize_county(county)
                    count = county_dict.get(normalized, 0)
                    region_total += count
                    
                    if count > 0:
                        county_breakdown.append({
                            "county": county,
                            "count": count
                        })
                
                if region_total > 0:
                    regional_distribution.append({
                        "region": region_name,
                        "region_code": region_data["code"],
                        "count": region_total,
                        "percentage": f"{region_total / total_england * 100:.1f}%",
                        "counties": county_breakdown
                    })
        
        unique_england_counties = len([c for region in regional_distribution for c in region["counties"]])
        
        analysis = {
            "summary": {
                "total_companies": int(total),
                "total_england_companies": int(total_england),
                "unique_counties": int(unique_england_counties),
                "analysis_timestamp": datetime.now().isoformat(),
            },
            "regional_distribution": regional_distribution,
            "data_quality_score": 100,
        }
        
        logger.info(f"Analysis complete: {total_england} England companies across {len(regional_distribution)} regions")
        
        return {
            "success": True,
            "analysis": analysis,
            "filename": file.filename,
            "total_rows": df.height
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Comparison analysis failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Analysis failed: {str(e)}")


@router.get("/api/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": "company-dataset-pipeline"}
