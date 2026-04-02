"""API route definitions."""

from __future__ import annotations

import asyncio
from typing import Any

from fastapi import APIRouter, HTTPException, BackgroundTasks

from api.schemas import (
    InvestigateRequest,
    InvestigateResponse,
    InvestigationDetailResponse,
    InvestigationListItem,
    HealthResponse,
)
from orchestrator.engine import engine
from orchestrator.dispatcher import AGENT_REGISTRY

router = APIRouter(prefix="/api")


@router.post("/investigate", response_model=InvestigateResponse)
async def start_investigation(
    request: InvestigateRequest,
    background_tasks: BackgroundTasks,
):
    """Submit a new investigation query. Runs in the background."""

    async def _run():
        await engine.investigate(request.query)

    # Start the investigation as a background task
    # First create a placeholder so we can return the ID
    status = await engine.investigate.__wrapped__(engine, request.query) if False else None  # noqa

    # Actually we run it inline but non-blocking via background task
    inv_id_holder: dict[str, str] = {}

    async def run_investigation():
        result = await engine.investigate(request.query)
        inv_id_holder["id"] = result.id

    # For the MVP, run synchronously to return results immediately
    # In production, use background tasks + WebSocket for streaming
    result = await engine.investigate(request.query)

    return InvestigateResponse(
        investigation_id=result.id,
        status=result.status,
        message=f"Investigation {'completed' if result.status == 'complete' else result.status}. "
                f"Found {sum(len(r.findings) for r in result.agent_results.values())} findings "
                f"in {result.elapsed():.1f}s.",
    )


@router.get("/investigations/{investigation_id}", response_model=InvestigationDetailResponse)
async def get_investigation(investigation_id: str):
    """Get the status and results of an investigation."""
    status = engine.get_investigation(investigation_id)
    if status is None:
        raise HTTPException(status_code=404, detail="Investigation not found")

    return InvestigationDetailResponse(**status.to_dict())


@router.get("/investigations", response_model=list[InvestigationListItem])
async def list_investigations(limit: int = 20):
    """List recent investigations."""
    items = engine.list_investigations(limit=limit)
    return [InvestigationListItem(**item) for item in items]


@router.get("/agents")
async def list_agents():
    """List available agents and their configuration."""
    return {
        agent_id: {
            "name": cls().agent_name,
            "domain": cls().domain,
            "wave": cls().wave,
        }
        for agent_id, cls in AGENT_REGISTRY.items()
    }


@router.get("/health", response_model=HealthResponse)
async def health_check():
    """API health check."""
    return HealthResponse(
        status="healthy",
        version="1.0.0",
        agents_available=len(AGENT_REGISTRY),
    )
