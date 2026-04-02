"""Cost Impact Agent — quantifies financial exposure from findings."""

from __future__ import annotations

import json
from typing import Any

from agents.base import AgentResult, BaseAgent, Finding, FindingType, Severity
from utils.llm_client import llm_complete
from utils.logging import get_logger

logger = get_logger(__name__)


class CostImpactAgent(BaseAgent):
    def __init__(self):
        super().__init__(
            agent_id="cost_impact",
            agent_name="Cost Impact Agent",
            domain="cross_domain",
            wave=3,
        )

    async def execute(self, context: dict[str, Any]) -> AgentResult:
        upstream_results = context.get("upstream_results", {})

        if not upstream_results:
            return AgentResult(
                agent_id=self.agent_id,
                agent_name=self.agent_name,
                status="failed",
                confidence=0.0,
                error="No upstream agent results provided",
            )

        upstream_summary = {}
        for agent_id, result in upstream_results.items():
            if isinstance(result, dict):
                upstream_summary[agent_id] = result
            elif hasattr(result, "to_dict"):
                upstream_summary[agent_id] = result.to_dict()

        analysis_prompt = f"""You are a healthcare financial analyst quantifying the cost impact of investigation findings.

Investigation target: Provider NPI {context.get('provider_npi', 'Unknown')}
Period: {context.get('start_date', '?')} to {context.get('end_date', '?')}

Upstream findings from domain agents:
{json.dumps(upstream_summary, indent=2, default=str)}

Your job:
1. ESTIMATE overpayment for each finding type:
   - Upcoding: difference between billed level and expected level × claim count
   - Duplicates: full allowed amount of duplicate claims
   - Coverage gaps: full allowed amount of claims without valid eligibility
   - Specialty mismatch: flag for recovery review
2. PROJECT annualized exposure (extrapolate from the investigation period)
3. RANK findings by recoverable amount and confidence level
4. Provide a total estimated financial impact range (low / mid / high)

Produce a JSON response:
{{
    "confidence": 0.0-1.0,
    "total_estimated_impact": {{
        "low": 0,
        "mid": 0,
        "high": 0,
        "currency": "USD"
    }},
    "annualized_exposure": 0,
    "findings": [
        {{
            "finding_type": "info",
            "severity": "critical|high|medium|low|info",
            "title": "...",
            "explanation": "...",
            "evidence": {{"source_agent": "...", "calculation": "..."}},
            "metrics": {{"estimated_overpayment": 0, "recoverable_amount": 0, "annualized": 0}}
        }}
    ],
    "follow_ups": ["..."]
}}
"""

        llm_response = await llm_complete(
            system_prompt=self._build_system_prompt(),
            user_message=analysis_prompt,
            response_format="json",
            max_tokens=4096,
        )

        parsed = json.loads(llm_response)
        findings = [
            Finding(
                finding_type=FindingType(f["finding_type"]),
                severity=Severity(f["severity"]),
                title=f["title"],
                explanation=f["explanation"],
                evidence=f.get("evidence", {}),
                metrics=f.get("metrics", {}),
            )
            for f in parsed.get("findings", [])
        ]

        return AgentResult(
            agent_id=self.agent_id,
            agent_name=self.agent_name,
            status="success",
            confidence=parsed.get("confidence", 0.5),
            findings=findings,
            follow_ups=parsed.get("follow_ups", []),
            raw_data={
                "total_estimated_impact": parsed.get("total_estimated_impact", {}),
                "annualized_exposure": parsed.get("annualized_exposure", 0),
            },
        )
