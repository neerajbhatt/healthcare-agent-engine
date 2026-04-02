"""Report Compiler Agent — synthesizes all findings into a structured investigation report."""

from __future__ import annotations

import json
from typing import Any

from agents.base import AgentResult, BaseAgent, Finding, FindingType, Severity
from utils.llm_client import llm_complete
from utils.logging import get_logger

logger = get_logger(__name__)


class ReportAgent(BaseAgent):
    def __init__(self):
        super().__init__(
            agent_id="report",
            agent_name="Report Compiler Agent",
            domain="utility",
            wave=4,
        )

    async def execute(self, context: dict[str, Any]) -> AgentResult:
        upstream_results = context.get("upstream_results", {})
        original_query = context.get("original_query", "")

        if not upstream_results:
            return AgentResult(
                agent_id=self.agent_id,
                agent_name=self.agent_name,
                status="failed",
                confidence=0.0,
                error="No upstream agent results to compile",
            )

        # Serialize all upstream results
        all_results = {}
        for agent_id, result in upstream_results.items():
            if isinstance(result, dict):
                all_results[agent_id] = result
            elif hasattr(result, "to_dict"):
                all_results[agent_id] = result.to_dict()

        report_prompt = f"""You are compiling a final investigation report from multiple agent findings.

Original analyst query: "{original_query}"
Investigation target: Provider NPI {context.get('provider_npi', 'Unknown')}
Period: {context.get('start_date', '?')} to {context.get('end_date', '?')}

All agent results:
{json.dumps(all_results, indent=2, default=str)}

Produce a comprehensive investigation report as JSON:
{{
    "confidence": 0.0-1.0,
    "report": {{
        "executive_summary": "2-3 paragraph summary of key findings for leadership",
        "risk_level": "critical|high|medium|low",
        "composite_risk_score": 0.0-1.0,
        "key_findings": [
            {{
                "rank": 1,
                "title": "...",
                "severity": "critical|high|medium|low",
                "description": "...",
                "supporting_agents": ["claims", "provider"],
                "estimated_impact_usd": 0
            }}
        ],
        "financial_summary": {{
            "total_estimated_overpayment": 0,
            "annualized_exposure": 0,
            "top_recovery_opportunities": ["..."]
        }},
        "recommended_actions": [
            {{
                "priority": 1,
                "action": "...",
                "rationale": "...",
                "assigned_to": "SIU|Medical Review|Compliance"
            }}
        ],
        "investigation_metadata": {{
            "agents_executed": 0,
            "agents_succeeded": 0,
            "total_findings": 0,
            "total_execution_time_seconds": 0
        }}
    }},
    "findings": [
        {{
            "finding_type": "info",
            "severity": "info",
            "title": "Investigation Report Compiled",
            "explanation": "Full report generated successfully",
            "evidence": {{}},
            "metrics": {{}}
        }}
    ],
    "follow_ups": []
}}
"""

        llm_response = await llm_complete(
            system_prompt=self._build_system_prompt(),
            user_message=report_prompt,
            response_format="json",
            max_tokens=8000,
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
            confidence=parsed.get("confidence", 0.8),
            findings=findings,
            follow_ups=parsed.get("follow_ups", []),
            raw_data={"report": parsed.get("report", {})},
        )
