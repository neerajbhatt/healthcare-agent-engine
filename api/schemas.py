"""Pydantic models for API request/response validation."""

from __future__ import annotations

from pydantic import BaseModel, Field
from typing import Any


class InvestigateRequest(BaseModel):
    query: str = Field(..., min_length=5, max_length=2000, description="Natural language investigation query")

    model_config = {"json_schema_extra": {
        "examples": [
            {"query": "Investigate Dr. Smith (NPI: 1234567890) for unusually high billing in Q4 2025"},
            {"query": "Find suspicious billing patterns for orthopedic providers in Texas"},
            {"query": "Analyze claims cost trends for cardiology in Florida over the last 6 months"},
        ]
    }}


class InvestigateResponse(BaseModel):
    investigation_id: str
    status: str
    message: str


class AgentResultResponse(BaseModel):
    agent_id: str
    agent_name: str
    status: str
    confidence: float
    findings: list[dict[str, Any]]
    follow_ups: list[str]
    execution_time: float
    error: str | None = None


class InvestigationDetailResponse(BaseModel):
    id: str
    status: str
    query: str
    plan: dict[str, Any] | None = None
    agent_results: dict[str, AgentResultResponse] = {}
    report: dict[str, Any] = {}
    progress: list[dict[str, Any]] = []
    elapsed_seconds: float = 0.0
    error: str | None = None


class InvestigationListItem(BaseModel):
    id: str
    status: str
    query: str
    elapsed_seconds: float


class HealthResponse(BaseModel):
    status: str
    version: str
    agents_available: int
