"""Evaluator: reviews agent results and decides whether to adapt the plan."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from agents.base import AgentResult
from utils.llm_client import llm_complete
from utils.logging import get_logger

logger = get_logger(__name__)


@dataclass
class EvaluationOutcome:
    should_continue: bool
    escalate_to_cross_domain: bool
    additional_agents: list[str]
    pruned_hypotheses: list[str]
    reasoning: str


async def evaluate_wave_results(
    wave_results: dict[str, AgentResult],
    plan_context: dict[str, Any],
) -> EvaluationOutcome:
    """Evaluate results from a wave and decide next steps.

    This is the adaptive part of the plan-and-execute pattern.
    The orchestrator can:
    - Escalate strong signals to cross-domain agents
    - Prune hypotheses that were refuted
    - Spawn follow-up investigations
    """
    results_summary = {
        k: v.to_dict() for k, v in wave_results.items()
    }

    prompt = f"""Review the following agent results and decide next steps.

Investigation context:
- Hypotheses: {json.dumps(plan_context.get('hypotheses', []), default=str)}
- Investigation type: {plan_context.get('investigation_type', 'general')}

Wave results:
{json.dumps(results_summary, indent=2, default=str)}

Evaluate:
1. Which hypotheses are SUPPORTED (confidence > 0.6)?
2. Which hypotheses are REFUTED (confidence < 0.3)?
3. Should we escalate to cross-domain agents?
4. Are there unexpected findings that warrant additional investigation?

Respond with JSON:
{{
    "should_continue": true,
    "escalate_to_cross_domain": true,
    "additional_agents": [],
    "pruned_hypotheses": ["H3"],
    "reasoning": "H1 strongly supported by claims upcoding data..."
}}
"""

    response = await llm_complete(
        system_prompt="You are an investigation evaluator. Respond ONLY with valid JSON.",
        user_message=prompt,
        response_format="json",
    )

    parsed = json.loads(response)

    outcome = EvaluationOutcome(
        should_continue=parsed.get("should_continue", True),
        escalate_to_cross_domain=parsed.get("escalate_to_cross_domain", True),
        additional_agents=parsed.get("additional_agents", []),
        pruned_hypotheses=parsed.get("pruned_hypotheses", []),
        reasoning=parsed.get("reasoning", ""),
    )

    logger.info(
        "evaluation_complete",
        continue_=outcome.should_continue,
        escalate=outcome.escalate_to_cross_domain,
        pruned=len(outcome.pruned_hypotheses),
    )

    return outcome
