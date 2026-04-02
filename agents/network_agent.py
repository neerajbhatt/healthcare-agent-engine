"""Network Analysis Agent — maps entity relationships, detects hidden connections."""

from __future__ import annotations

import json
from typing import Any

from agents.base import AgentResult, BaseAgent, Finding, FindingType, Severity
from utils.llm_client import llm_complete
from utils.snowflake_client import snowflake_client
from utils.logging import get_logger

logger = get_logger(__name__)


# Network-specific queries (not in the semantic layer because they span domains)
REFERRAL_NETWORK_SQL = """
SELECT c.referring_npi, c.provider_npi AS rendering_npi,
       COUNT(*) AS referral_count,
       SUM(c.allowed_amount) AS total_allowed,
       COUNT(DISTINCT c.member_id) AS unique_patients
FROM claims c
WHERE (c.provider_npi = %(provider_npi)s OR c.referring_npi = %(provider_npi)s)
  AND c.service_date BETWEEN %(start_date)s AND %(end_date)s
  AND c.referring_npi IS NOT NULL
GROUP BY c.referring_npi, c.provider_npi
ORDER BY referral_count DESC
LIMIT 50
"""

SHARED_PATIENTS_SQL = """
SELECT other_provider, COUNT(DISTINCT member_id) AS shared_patients,
       SUM(total_claims) AS combined_claims
FROM (
    SELECT c1.provider_npi AS target, c2.provider_npi AS other_provider,
           c1.member_id, COUNT(*) AS total_claims
    FROM claims c1
    JOIN claims c2 ON c1.member_id = c2.member_id
        AND c1.provider_npi != c2.provider_npi
        AND ABS(DATEDIFF('day', c1.service_date, c2.service_date)) <= 7
    WHERE c1.provider_npi = %(provider_npi)s
      AND c1.service_date BETWEEN %(start_date)s AND %(end_date)s
    GROUP BY c1.provider_npi, c2.provider_npi, c1.member_id
)
GROUP BY other_provider
HAVING COUNT(DISTINCT member_id) >= 3
ORDER BY shared_patients DESC
LIMIT 30
"""


class NetworkAgent(BaseAgent):
    def __init__(self):
        super().__init__(
            agent_id="network",
            agent_name="Network Analysis Agent",
            domain="cross_domain",
            wave=3,
        )

    async def execute(self, context: dict[str, Any]) -> AgentResult:
        params = {
            "provider_npi": context.get("provider_npi", ""),
            "start_date": context.get("start_date", ""),
            "end_date": context.get("end_date", ""),
        }

        all_data: dict[str, Any] = {}
        errors: list[str] = []

        # Referral network
        for name, sql_template in [
            ("referral_network", REFERRAL_NETWORK_SQL),
            ("shared_patients", SHARED_PATIENTS_SQL),
        ]:
            try:
                sql = sql_template % {
                    k: f"'{v}'" if isinstance(v, str) else v
                    for k, v in params.items()
                }
                df, err = await snowflake_client.execute_query_safe(sql)
                if err:
                    errors.append(f"{name}: {err}")
                else:
                    all_data[name] = {
                        "row_count": len(df) if df is not None else 0,
                        "sample": (
                            df.head(20).to_dict(orient="records")
                            if df is not None and not df.empty
                            else []
                        ),
                    }
            except Exception as e:
                errors.append(f"{name}: {e}")

        # Include upstream results for correlation
        upstream_results = context.get("upstream_results", {})

        analysis_prompt = f"""Analyze the network and relationships for provider NPI {params['provider_npi']}
from {params['start_date']} to {params['end_date']}.

Network data:
{json.dumps(all_data, indent=2, default=str)}

Upstream agent context:
{json.dumps({k: v.get('findings', []) if isinstance(v, dict) else [] for k, v in upstream_results.items()}, indent=2, default=str)}

Errors: {errors if errors else 'None'}

Focus on:
1. Unusual referral concentration (single referrer driving most volume)
2. Self-referral loops
3. Shared patient clusters with other providers (possible collusion ring)
4. Providers sharing addresses/TINs that might indicate shell entities

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
            status="success" if not errors else "partial",
            confidence=parsed.get("confidence", 0.5),
            findings=findings,
            follow_ups=parsed.get("follow_ups", []),
            raw_data=all_data,
        )
