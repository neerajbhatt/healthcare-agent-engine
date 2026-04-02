"""Dispatcher: manages parallel agent execution by wave."""

from __future__ import annotations

import asyncio
from typing import Any

from agents.base import AgentResult, BaseAgent
from agents.claims_agent import ClaimsAgent
from agents.provider_agent import ProviderAgent
from agents.member_agent import MemberAgent
from agents.eligibility_agent import EligibilityAgent
from agents.temporal_agent import TemporalAgent
from agents.fraud_synthesis_agent import FraudSynthesisAgent
from agents.network_agent import NetworkAgent
from agents.cost_impact_agent import CostImpactAgent
from agents.report_agent import ReportAgent
from config.settings import settings
from utils.logging import get_logger

logger = get_logger(__name__)

# ── Agent Registry ──
AGENT_REGISTRY: dict[str, type[BaseAgent]] = {
    "claims": ClaimsAgent,
    "provider": ProviderAgent,
    "member": MemberAgent,
    "eligibility": EligibilityAgent,
    "temporal": TemporalAgent,
    "fraud_synthesis": FraudSynthesisAgent,
    "network": NetworkAgent,
    "cost_impact": CostImpactAgent,
    "report": ReportAgent,
}


class AgentDispatcher:
    """Dispatches agents in parallel within waves, sequentially across waves."""

    def __init__(self):
        self.semaphore = asyncio.Semaphore(settings.max_parallel_agents)

    async def _run_agent_with_semaphore(
        self, agent: BaseAgent, context: dict[str, Any]
    ) -> AgentResult:
        """Run a single agent with concurrency control."""
        async with self.semaphore:
            logger.info("agent_started", agent_id=agent.agent_id)
            result = await asyncio.wait_for(
                agent.run(context),
                timeout=settings.investigation_timeout,
            )
            logger.info(
                "agent_completed",
                agent_id=agent.agent_id,
                status=result.status,
                findings=len(result.findings),
                time=f"{result.execution_time:.1f}s",
            )
            return result

    async def dispatch_wave(
        self,
        agent_ids: list[str],
        context: dict[str, Any],
        callback=None,
    ) -> dict[str, AgentResult]:
        """Execute a wave of agents in parallel.

        Args:
            agent_ids: List of agent IDs to run in this wave.
            context: Shared investigation context.
            callback: Optional async function called with (agent_id, result) as each finishes.

        Returns:
            Dict mapping agent_id -> AgentResult.
        """
        tasks = {}
        for agent_id in agent_ids:
            agent_cls = AGENT_REGISTRY.get(agent_id)
            if agent_cls is None:
                logger.warning("unknown_agent", agent_id=agent_id)
                continue
            agent = agent_cls()
            tasks[agent_id] = self._run_agent_with_semaphore(agent, context)

        results: dict[str, AgentResult] = {}

        # Use as_completed for real-time progress
        agent_id_by_task = {}
        coros = []
        for aid, coro in tasks.items():
            task = asyncio.ensure_future(coro)
            agent_id_by_task[id(task)] = aid
            coros.append(task)

        for completed in asyncio.as_completed(coros):
            try:
                result = await completed
                # Find which agent this was
                for task in coros:
                    if task.done() and id(task) in agent_id_by_task:
                        aid = agent_id_by_task.pop(id(task))
                        results[aid] = result
                        if callback:
                            await callback(aid, result)
                        break
            except asyncio.TimeoutError:
                logger.error("agent_timeout")
            except Exception as e:
                logger.error("agent_dispatch_error", error=str(e))

        return results

    async def dispatch_plan(
        self,
        agent_plan: list[dict[str, Any]],
        context: dict[str, Any],
        callback=None,
    ) -> dict[str, AgentResult]:
        """Execute the full agent plan wave by wave.

        Args:
            agent_plan: List of wave definitions from the planner.
            context: Shared investigation context.
            callback: Optional progress callback.

        Returns:
            All agent results keyed by agent_id.
        """
        all_results: dict[str, AgentResult] = {}

        # Sort waves
        sorted_waves = sorted(agent_plan, key=lambda w: w.get("wave", 0))

        for wave_def in sorted_waves:
            wave_num = wave_def.get("wave", 0)
            agent_ids = wave_def.get("agents", [])

            logger.info("wave_started", wave=wave_num, agents=agent_ids)

            # Inject upstream results for cross-domain agents
            wave_context = {**context}
            if wave_num >= 3:
                wave_context["upstream_results"] = {
                    k: v.to_dict() for k, v in all_results.items()
                }

            wave_results = await self.dispatch_wave(
                agent_ids, wave_context, callback=callback
            )
            all_results.update(wave_results)

            logger.info(
                "wave_completed",
                wave=wave_num,
                results=len(wave_results),
                total=len(all_results),
            )

        return all_results


dispatcher = AgentDispatcher()
