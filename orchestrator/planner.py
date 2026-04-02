"""Planner: parses analyst queries, extracts entities, generates hypotheses."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from utils.llm_client import llm_complete
from utils.logging import get_logger

logger = get_logger(__name__)


@dataclass
class InvestigationPlan:
    """Structured plan produced by the planner."""

    original_query: str
    provider_npi: str | None = None
    member_id: str | None = None
    start_date: str | None = None
    end_date: str | None = None
    investigation_type: str = ""  # fraud | cost | utilization | general
    entities: dict[str, Any] = field(default_factory=dict)
    hypotheses: list[dict[str, Any]] = field(default_factory=list)
    agent_plan: list[dict[str, Any]] = field(default_factory=list)

    def to_context(self) -> dict[str, Any]:
        """Convert plan to agent execution context."""
        return {
            "original_query": self.original_query,
            "provider_npi": self.provider_npi or "",
            "member_id": self.member_id or "",
            "start_date": self.start_date or "",
            "end_date": self.end_date or "",
            "investigation_type": self.investigation_type,
            "entities": self.entities,
            "hypotheses": self.hypotheses,
        }


PLANNER_SYSTEM = """You are a healthcare investigation planner. Given an analyst's query,
you extract entities, timeframes, and generate ranked investigation hypotheses.

You always respond with valid JSON only. No markdown, no preamble."""


async def create_plan(query: str) -> InvestigationPlan:
    """Parse an analyst query and produce an investigation plan."""

    prompt = f"""Parse the following analyst query and produce an investigation plan.

Query: "{query}"

Extract:
1. Entities: provider NPIs, member IDs, facility names, specialties, geographies
2. Timeframe: start_date and end_date (use ISO format YYYY-MM-DD). If not specified, default to last 12 months.
3. Investigation type: fraud | cost | utilization | general
4. 3-5 ranked hypotheses to test
5. Which agents should run for each hypothesis

Respond with JSON:
{{
    "provider_npi": "extracted NPI or null",
    "member_id": "extracted member ID or null",
    "start_date": "YYYY-MM-DD",
    "end_date": "YYYY-MM-DD",
    "investigation_type": "fraud|cost|utilization|general",
    "entities": {{
        "providers": [],
        "members": [],
        "specialties": [],
        "geographies": []
    }},
    "hypotheses": [
        {{
            "id": "H1",
            "description": "...",
            "priority": 1,
            "agents_needed": ["claims", "provider"]
        }}
    ],
    "agent_plan": [
        {{
            "wave": 1,
            "agents": ["claims", "provider", "member", "eligibility"],
            "rationale": "Initial domain sweep"
        }},
        {{
            "wave": 2,
            "agents": ["temporal"],
            "rationale": "Time-based analysis"
        }},
        {{
            "wave": 3,
            "agents": ["fraud_synthesis", "network", "cost_impact"],
            "rationale": "Cross-domain correlation",
            "depends_on": [1, 2]
        }},
        {{
            "wave": 4,
            "agents": ["report"],
            "rationale": "Final report compilation",
            "depends_on": [3]
        }}
    ]
}}
"""

    response = await llm_complete(
        system_prompt=PLANNER_SYSTEM,
        user_message=prompt,
        response_format="json",
    )

    parsed = json.loads(response)

    plan = InvestigationPlan(
        original_query=query,
        provider_npi=parsed.get("provider_npi"),
        member_id=parsed.get("member_id"),
        start_date=parsed.get("start_date"),
        end_date=parsed.get("end_date"),
        investigation_type=parsed.get("investigation_type", "general"),
        entities=parsed.get("entities", {}),
        hypotheses=parsed.get("hypotheses", []),
        agent_plan=parsed.get("agent_plan", []),
    )

    logger.info(
        "plan_created",
        npi=plan.provider_npi,
        investigation_type=plan.investigation_type,
        hypotheses=len(plan.hypotheses),
        waves=len(plan.agent_plan),
    )

    return plan
