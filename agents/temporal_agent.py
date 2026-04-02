"""Temporal Analysis Agent — detects time-based anomalies across domains."""

from __future__ import annotations

import json
from typing import Any

from agents.base import AgentResult, BaseAgent, Finding, FindingType, Severity
from semantic_layer.query_builder import query_builder
from semantic_layer.validator import result_validator
from utils.llm_client import llm_complete
from utils.snowflake_client import snowflake_client
from utils.logging import get_logger

logger = get_logger(__name__)

CONCEPTS = [
    "billing_spike_detection",
    "weekend_billing",
]


class TemporalAgent(BaseAgent):
    def __init__(self):
        super().__init__(
            agent_id="temporal",
            agent_name="Temporal Analysis Agent",
            domain="temporal",
            wave=2,
        )

    async def execute(self, context: dict[str, Any]) -> AgentResult:
        params = {
            "provider_npi": context.get("provider_npi", ""),
            "start_date": context.get("start_date", ""),
            "end_date": context.get("end_date", ""),
        }

        all_data: dict[str, Any] = {}
        errors: list[str] = []

        for concept_name in CONCEPTS:
            sql, err = query_builder.build(concept_name, params)
            if err:
                errors.append(f"{concept_name}: {err}")
                continue

            df, query_err = await snowflake_client.execute_query_safe(sql)
            if query_err:
                errors.append(f"{concept_name}: {query_err}")
                continue

            validation = result_validator.validate(df, concept_name)
            all_data[concept_name] = {
                "row_count": validation.row_count,
                "warnings": validation.warnings,
                "sample": (
                    df.head(30).to_dict(orient="records")
                    if df is not None and not df.empty
                    else []
                ),
            }

        if not all_data:
            return AgentResult(
                agent_id=self.agent_id,
                agent_name=self.agent_name,
                status="failed",
                confidence=0.0,
                error=f"All queries failed: {'; '.join(errors)}",
            )

        analysis_prompt = f"""Analyze temporal patterns for provider NPI {params['provider_npi']}
from {params['start_date']} to {params['end_date']}.

Data collected:
{json.dumps(all_data, indent=2, default=str)}

Errors: {errors if errors else 'None'}

Focus on:
1. Billing volume spikes (months with z-score > 2 vs rolling average)
2. Weekend/after-hours billing patterns (unusual for the specialty?)
3. Sudden ramp-ups that may indicate scheme initiation
4. Seasonal deviations from expected patterns

Produce a JSON response:
{{
    "confidence": 0.0-1.0,
    "findings": [
        {{
            "finding_type": "anomaly|pattern|outlier|correlation|info",
            "severity": "critical|high|medium|low|info",
            "title": "...",
            "explanation": "...",
            "evidence": {{}},
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
            status="success" if not errors else "partial",
            confidence=parsed.get("confidence", 0.5),
            findings=findings,
            follow_ups=parsed.get("follow_ups", []),
            raw_data=all_data,
        )
