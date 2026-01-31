"""
Routes for letter generation functionality.
"""
from fastapi import APIRouter, HTTPException, Query, UploadFile, File, Form, BackgroundTasks
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from typing import Optional, List
from pathlib import Path
import tempfile
import os
import io
import logging
import shutil

# Import the letter generation service
from app.services.letter_generation import LetterGenerationService

# Set up logging
logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/letters", tags=["letters"])

@router.post("/generate/upload")
async def generate_letters_from_upload(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    template: Optional[UploadFile] = File(None),
    mode: str = Form("zip"),
    letters_per_file: int = Form(None),
):
    """
    Generate letters from uploaded file (Excel or CSV).
    
    Args:
        file: Excel or CSV file with company data
        template: REQUIRED custom .docx template file (no fallback to stored template)
        mode: "zip" = one letter per file in ZIP, "combined" = N letters per DOCX
        letters_per_file: How many letters per DOCX (only for "combined" mode)
    
    NOTE: ALL rows in the file will be processed. No limit parameter.
    """
    try:
        # Validate data file type
        filename = file.filename.lower()
        if not (filename.endswith(('.xlsx', '.xls', '.csv'))):
            raise HTTPException(400, "File must be Excel (.xlsx, .xls) or CSV (.csv)")
        
        # Validate template is provided
        if not template:
            raise HTTPException(400, "Template file is required. Please upload a .docx template file.")
        
        # Validate template file type
        template_filename = template.filename.lower()
        if not template_filename.endswith('.docx'):
            raise HTTPException(400, "Template must be a .docx file")
        
        # Validate mode
        if mode not in ["zip", "combined"]:
            raise HTTPException(400, f"Invalid mode '{mode}'. Use 'zip' or 'combined'")
        
        # Set default letters_per_file based on mode
        if letters_per_file is None:
            letters_per_file = 5 if mode == "combined" else 1
        
        # Validate letters_per_file
        if letters_per_file < 1:
            raise HTTPException(400, "letters_per_file must be at least 1")
        
        # Log the parameters for debugging
        logger.info(f"Generating letters - Mode: {mode}, Letters per file: {letters_per_file}, File: {filename}, Template: {template_filename}")
        
        # Read data file content
        content = await file.read()
        logger.info(f"Read {len(content)} bytes from {filename}")
        
        # Create temporary file for data
        with tempfile.NamedTemporaryFile(delete=False, suffix=Path(filename).suffix) as tmp:
            tmp.write(content)
            tmp_path = tmp.name
        
        logger.info(f"Created temporary data file: {tmp_path}")
        
        # Read template file content
        template_content = await template.read()
        logger.info(f"Read {len(template_content)} bytes from template {template_filename}")
        
        # Create temporary file for template
        with tempfile.NamedTemporaryFile(delete=False, suffix='.docx') as tmp_template:
            tmp_template.write(template_content)
            tmp_template_path = tmp_template.name
        
        logger.info(f"Created temporary template file: {tmp_template_path}")
        
        try:
            # Initialize service with user's template
            logger.info(f"Initializing service with user template: {tmp_template_path}")
            service = LetterGenerationService(tmp_template_path)
            
            # Generate letters based on file type (NO LIMIT PARAMETER)
            logger.info(f"Starting letter generation for {filename}...")
            
            if filename.endswith('.csv'):
                result = service.generate_from_csv(tmp_path, mode, letters_per_file)
            else:  # Excel
                result = service.generate_from_excel(tmp_path, mode, letters_per_file)
            
            logger.info(f"Letter generation completed. Total letters: {result.get('total_letters', 0)}")
            
            # Schedule cleanup of temp files
            background_tasks.add_task(cleanup_temp_files, [tmp_path, tmp_template_path])
            
            # Check if there's an error
            if "error" in result:
                logger.error(f"Letter generation error: {result['error']}")
                return JSONResponse(content={"error": result["error"]}, status_code=400)
            
            # Return file as streaming response
            logger.info(f"Returning file: {result['filename']} ({result.get('content_type', 'application/zip')})")
            
            return StreamingResponse(
                io.BytesIO(result["content"]),
                media_type=result.get("content_type", "application/zip"),
                headers={
                    "Content-Disposition": f"attachment; filename={result['filename']}",
                    "X-Total-Letters": str(result.get("total_letters", 0)),
                    "X-Files-Created": str(result.get("files_created", 0)),
                }
            )
            
        except Exception as e:
            logger.error(f"Letter generation failed: {str(e)}", exc_info=True)
            # Clean up temp files on error
            cleanup_temp_files_sync([tmp_path, tmp_template_path])
            raise HTTPException(500, f"Letter generation failed: {str(e)}")
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in generate_letters_from_upload: {str(e)}", exc_info=True)
        raise HTTPException(500, f"Processing failed: {str(e)}")

@router.post("/generate/dataset/{dataset_id}")
async def generate_letters_from_dataset(
    dataset_id: str,
    mode: str = Query("zip"),
    letters_per_file: int = Query(1),
):
    """
    Generate letters from an existing dataset in the system.
    
    Args:
        dataset_id: ID of the dataset to use
        mode: "zip" = one letter per file in ZIP, "combined" = N letters per DOCX
        letters_per_file: How many letters per DOCX (only for "combined" mode)
    
    NOTE: ALL rows in the dataset will be processed. No limit parameter.
    """
    try:
        # TODO: Replace with actual dataset retrieval from your system
        # For now, this is a placeholder
        logger.info(f"Dataset letter generation requested for dataset_id: {dataset_id}, mode: {mode}")
        raise HTTPException(501, "Dataset integration not implemented yet. Use the upload endpoint instead.")
        
        # When you implement this, it should look like:
        # 1. Get dataset from your database
        # 2. Convert to DataFrame
        # 3. Call service.generate_from_dataframe(df, mode, letters_per_file)
        # 4. Return the result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Dataset letter generation failed: {str(e)}", exc_info=True)
        raise HTTPException(500, f"Dataset letter generation failed: {str(e)}")

@router.get("/download/{filename}")
async def download_letters(filename: str):
    """
    Download generated letters.
    """
    try:
        file_path = Path("outputs/letters") / filename
        
        if not file_path.exists():
            logger.warning(f"File not found: {file_path}")
            raise HTTPException(404, "File not found")
        
        # Determine content type
        if filename.endswith('.docx'):
            media_type = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        elif filename.endswith('.zip'):
            media_type = "application/zip"
        else:
            media_type = "application/octet-stream"
        
        logger.info(f"Serving file: {filename} ({media_type})")
        
        return FileResponse(
            path=file_path,
            filename=filename,
            media_type=media_type
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error downloading file {filename}: {str(e)}", exc_info=True)
        raise HTTPException(500, f"Error downloading file: {str(e)}")

@router.get("/template")
async def download_template():
    """
    Download a sample letter template for reference.
    This is just an example - users should upload their own templates.
    """
    try:
        # Look for an example template
        example_template_path = find_example_template()
        
        if not example_template_path or not os.path.exists(example_template_path):
            logger.error("Example template not found")
            raise HTTPException(404, "Example template not found. Please use your own .docx template.")
        
        logger.info(f"Serving example template: {example_template_path}")
        
        return FileResponse(
            path=example_template_path,
            filename="example_letter_template.docx",
            media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error serving template: {str(e)}", exc_info=True)
        raise HTTPException(500, f"Error serving template: {str(e)}")

@router.get("/templates")
async def get_available_templates():
    """
    Get list of available example letter templates for reference.
    Users should upload their own templates.
    """
    try:
        templates_dir = Path("app/templates/letters")
        
        templates = []
        if templates_dir.exists():
            for file in templates_dir.glob("*.docx"):
                templates.append({
                    "id": file.stem,
                    "name": file.stem.replace("_", " ").title(),
                    "filename": file.name,
                    "path": str(file),
                    "size": file.stat().st_size if file.exists() else 0,
                    "modified": file.stat().st_mtime,
                    "note": "Example template - please upload your own"
                })
        
        # If no templates found, return info message
        if not templates:
            templates = [{
                "id": "custom",
                "name": "Custom Template Required",
                "filename": "your_template.docx",
                "description": "Please upload your own .docx template file",
                "note": "No example templates available"
            }]
        
        logger.info(f"Found {len(templates)} example templates")
        return templates
        
    except Exception as e:
        logger.error(f"Error getting templates: {str(e)}", exc_info=True)
        raise HTTPException(500, f"Failed to get templates: {str(e)}")

@router.post("/cleanup")
async def cleanup_old_letters(days_old: int = 7):
    """
    Clean up old generated letter files.
    """
    try:
        import time
        from datetime import datetime, timedelta
        
        letters_dir = Path("outputs/letters")
        if not letters_dir.exists():
            logger.info("No letters directory found")
            return {"success": True, "message": "No letters directory found"}
        
        cutoff_time = time.time() - (days_old * 86400)
        deleted_count = 0
        deleted_files = []
        
        for file_path in letters_dir.iterdir():
            if file_path.is_file():
                if file_path.stat().st_mtime < cutoff_time:
                    try:
                        file_path.unlink()
                        deleted_count += 1
                        deleted_files.append(file_path.name)
                        logger.info(f"Deleted old file: {file_path.name}")
                    except Exception as e:
                        logger.warning(f"Failed to delete {file_path.name}: {str(e)}")
        
        logger.info(f"Cleaned up {deleted_count} files older than {days_old} days")
        
        return {
            "success": True, 
            "message": f"Cleaned up {deleted_count} files older than {days_old} days",
            "deleted_count": deleted_count,
            "deleted_files": deleted_files
        }
    except Exception as e:
        logger.error(f"Cleanup failed: {str(e)}", exc_info=True)
        raise HTTPException(500, f"Cleanup failed: {str(e)}")

@router.get("/recent")
async def get_recent_letters(limit: int = Query(10, ge=1, le=50)):
    """
    Get recently generated letters.
    """
    try:
        letters_dir = Path("outputs/letters")
        
        if not letters_dir.exists():
            logger.info("No letters directory found")
            return {"files": []}
        
        # Get all files, sort by modification time
        files = []
        for file_path in letters_dir.iterdir():
            if file_path.is_file():
                files.append({
                    "filename": file_path.name,
                    "path": str(file_path),
                    "size": file_path.stat().st_size,
                    "modified": file_path.stat().st_mtime,
                    "file_type": "docx" if file_path.suffix == ".docx" else "zip",
                    "download_url": f"/api/letters/download/{file_path.name}"
                })
        
        # Sort by modification time (newest first)
        files.sort(key=lambda x: x["modified"], reverse=True)
        
        # Limit results
        files = files[:limit]
        
        logger.info(f"Returning {len(files)} recent files")
        return {"files": files}
        
    except Exception as e:
        logger.error(f"Failed to get recent files: {str(e)}", exc_info=True)
        raise HTTPException(500, f"Failed to get recent files: {str(e)}")

@router.get("/status")
async def get_letter_generation_status():
    """
    Get status of letter generation system.
    """
    try:
        example_template_path = find_example_template()
        letters_dir = Path("outputs/letters")
        
        status = {
            "example_template_available": example_template_path is not None and os.path.exists(example_template_path),
            "example_template_path": example_template_path,
            "note": "Users must upload their own .docx template for letter generation",
            "outputs_dir_exists": letters_dir.exists(),
            "outputs_dir": str(letters_dir),
        }
        
        if letters_dir.exists():
            files = list(letters_dir.iterdir())
            status["total_files"] = len(files)
            status["total_size_bytes"] = sum(f.stat().st_size for f in files if f.is_file())
        
        return status
        
    except Exception as e:
        logger.error(f"Failed to get status: {str(e)}", exc_info=True)
        raise HTTPException(500, f"Failed to get status: {str(e)}")

# ================= HELPER FUNCTIONS =================

def find_example_template() -> Optional[str]:
    """Find an example letter template (for download reference only)."""
    app_dir = Path(__file__).resolve().parent
    project_dir = app_dir.parent
    
    possible_paths = [
        app_dir / "templates" / "letters" / "letter_template.docx",
        app_dir / "templates" / "letters" / "example_template.docx",
    ]
    
    logger.info(f"Searching for example template in {len(possible_paths)} locations...")
    
    for path in possible_paths:
        path_str = str(path)
        if os.path.exists(path_str):
            logger.info(f"✓ Found example template at: {path_str}")
            return path_str
        else:
            logger.debug(f"✗ Not found: {path_str}")
    
    logger.warning("No example template found - users must upload their own")
    return None

def cleanup_temp_files(file_paths: list):
    """
    Background task to clean up temporary files.
    
    Args:
        file_paths: List of file paths to delete
    """
    for file_path in file_paths:
        if file_path and os.path.exists(file_path):
            try:
                os.unlink(file_path)
                logger.info(f"Cleaned up temp file: {file_path}")
            except Exception as e:
                logger.warning(f"Failed to clean up temp file {file_path}: {str(e)}")

def cleanup_temp_files_sync(file_paths: list):
    """
    Synchronously clean up temporary files (for error handling).
    
    Args:
        file_paths: List of file paths to delete
    """
    for file_path in file_paths:
        if file_path and os.path.exists(file_path):
            try:
                os.unlink(file_path)
                logger.info(f"Cleaned up temp file: {file_path}")
            except Exception as e:
                logger.warning(f"Failed to clean up temp file {file_path}: {str(e)}")