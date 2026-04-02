"""FastAPI application entry point."""

from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from api.routes import router
from api.websocket import ws_router
from config.settings import settings

app = FastAPI(
    title="Healthcare AI Investigation Engine",
    description="Multi-agent orchestration system for healthcare fraud detection and analytics",
    version="1.0.0",
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Routes
app.include_router(router)
app.include_router(ws_router)

# Dashboard
DASHBOARD_DIR = Path(__file__).parent.parent / "dashboard"


@app.get("/")
async def serve_dashboard():
    """Serve the investigation dashboard."""
    index_path = DASHBOARD_DIR / "index.html"
    if index_path.exists():
        return FileResponse(str(index_path))
    return {"message": "Healthcare AI Investigation Engine API", "docs": "/docs"}


@app.on_event("startup")
async def startup():
    from utils.logging import get_logger
    logger = get_logger("startup")
    logger.info(
        "server_started",
        host=settings.api_host,
        port=settings.api_port,
        llm=settings.llm_provider,
        model=settings.llm_model,
    )


@app.on_event("shutdown")
async def shutdown():
    from utils.snowflake_client import snowflake_client
    snowflake_client.close()
