"""Fraud Pattern Synthesis Agent — correlates multi-domain signals into fraud risk."""

from __future__ import annotations

import json
from typing import Any

from agents.base import AgentResult, BaseAgent, Finding, FindingType, Severity
from utils.llm_client import llm_complete
from utils.logging import get_logger

logger = get_logger(__name__)


class FraudSynthesisAgent(BaseAgent):
    """Consumes results from domain agents and synthesizes fraud patterns."""

    def __init__(self):
        super().__init__(
            agent_id="fraud_synthesis",
            agent_name="Fraud Pattern Synthesis Agent",
            domain="cross_domain",
            wave=3,
        )

    async def execute(self, context: dict[str, Any]) -> AgentResult:
        # This agent reads upstream agent results, not raw data
        upstream_results: dict[str, Any] = context.get("upstream_results", {})

        if not upstream_results:
            return AgentResult(
                agent_id=self.agent_id,
                agent_name=self.agent_name,
                status="failed",
                confidence=0.0,
                error="No upstream agent results provided",
            )

        # Serialize upstream findings for LLM analysis
        upstream_summary = {}
        for agent_id, result in upstream_results.items():
            if isinstance(result, dict):
                upstream_summary[agent_id] = result
            elif hasattr(result, "to_dict"):
                upstream_summary[agent_id] = result.to_dict()

        analysis_prompt = f"""You are a healthcare fraud analyst synthesizing findings from multiple domain agents.

Investigation target: Provider NPI {context.get('provider_npi', 'Unknown')}
Period: {context.get('start_date', '?')} to {context.get('end_date', '?')}

Upstream agent findings:
{json.dumps(upstream_summary, indent=2, default=str)}

Your job:
1. CORRELATE signals across agents. Look for patterns no single agent could detect:
   - Provider billing anomalies + member utilization spikes = possible collusion
   - Eligibility retroactive adds + high-cost claims = possible coverage manipulation
   - Temporal spikes + new member enrollments = possible kickback scheme
2. Identify potential COORDINATED FRAUD schemes (provider + member + facility rings)
3. Calculate a COMPOSITE FRAUD RISK SCORE (0.0-1.0) based on multi-domain evidence
4. Flag PHANTOM BILLING (services without corresponding member activity)

Produce a JSON response:
{{
    "confidence": 0.0-1.0,
    "composite_risk_score": 0.0-1.0,
    "risk_category": "critical|high|medium|low",
    "findings": [
        {{
            "finding_type": "correlation",
            "severity": "critical|high|medium|low|info",
            "title": "...",
            "explanation": "...",
            "evidence": {{"correlated_agents": [...], "signals": [...]}},
            "metrics": {{}}
        }}
    ],
    "follow_ups": ["..."]
}}
"""

        llm_response = await llm_complete(
            system_prompt=self._build_system_prompt(),
            user_message=analysis_prompt,
            response_format="json",
            max_tokens=6000,
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
                "composite_risk_score": parsed.get("composite_risk_score", 0),
                "risk_category": parsed.get("risk_category", "unknown"),
            },
        )
