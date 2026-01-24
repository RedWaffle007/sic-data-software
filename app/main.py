from fastapi import FastAPI
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pathlib import Path

app = FastAPI(
    docs_url=None,
    redoc_url=None
)

# --- PATHS ---
APP_DIR = Path(__file__).resolve().parent          # /app
PROJECT_DIR = APP_DIR.parent                      # project root
FRONTEND_DIR = PROJECT_DIR / "frontend"           # /frontend

# --- STATIC FILES (CSS, JS, manifest, icons) ---
app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")

# --- FRONTEND ENTRY POINT ---
@app.get("/", response_class=HTMLResponse)
async def serve_index():
    return FileResponse(FRONTEND_DIR / "index.html")

# --- API ROUTES ---
from app.routes import router as core_router
from app.routes_database import router as db_router

app.include_router(core_router)
app.include_router(db_router)
