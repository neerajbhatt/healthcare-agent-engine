"""Main orchestration engine: plan → dispatch → evaluate → report."""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Coroutine

from agents.base import AgentResult
from orchestrator.planner import InvestigationPlan, create_plan
from orchestrator.dispatcher import dispatcher
from orchestrator.evaluator import evaluate_wave_results
from utils.guardrails import validate_output
from utils.logging import get_logger

logger = get_logger(__name__)


@dataclass
class InvestigationStatus:
    id: str
    status: str  # planning | executing | evaluating | compiling | complete | failed
    query: str
    plan: InvestigationPlan | None = None
    agent_results: dict[str, AgentResult] = field(default_factory=dict)
    report: dict[str, Any] = field(default_factory=dict)
    progress: list[dict[str, Any]] = field(default_factory=list)
    started_at: float = 0.0
    completed_at: float = 0.0
    error: str | None = None

    def elapsed(self) -> float:
        end = self.completed_at or time.time()
        return round(end - self.started_at, 1)

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "status": self.status,
            "query": self.query,
            "plan": {
                "investigation_type": self.plan.investigation_type if self.plan else "",
                "hypotheses": self.plan.hypotheses if self.plan else [],
                "agent_plan": self.plan.agent_plan if self.plan else [],
            } if self.plan else None,
            "agent_results": {
                k: v.to_dict() for k, v in self.agent_results.items()
            },
            "report": self.report,
            "progress": self.progress,
            "elapsed_seconds": self.elapsed(),
            "error": self.error,
        }


# In-memory store (swap for Redis/DB in production)
_investigations: dict[str, InvestigationStatus] = {}


ProgressCallback = Callable[[str, str, dict[str, Any]], Coroutine[Any, Any, None]]


class OrchestrationEngine:
    """Main engine that coordinates the full investigation lifecycle."""

    async def investigate(
        self,
        query: str,
        on_progress: ProgressCallback | None = None,
    ) -> InvestigationStatus:
        """Run a complete investigation from query to report.

        Args:
            query: Natural language analyst query.
            on_progress: Optional async callback(investigation_id, event, data).

        Returns:
            Completed InvestigationStatus with report.
        """
        inv_id = str(uuid.uuid4())[:8]
        status = InvestigationStatus(
            id=inv_id,
            status="planning",
            query=query,
            started_at=time.time(),
        )
        _investigations[inv_id] = status

        async def _progress(event: str, data: dict[str, Any] | None = None):
            entry = {"event": event, "timestamp": time.time(), **(data or {})}
            status.progress.append(entry)
            if on_progress:
                await on_progress(inv_id, event, data or {})

        try:
            # ── Phase 1: Plan ──
            await _progress("planning_started")
            plan = await create_plan(query)
            status.plan = plan
            status.status = "executing"
            await _progress("plan_created", {
                "investigation_type": plan.investigation_type,
                "hypotheses": len(plan.hypotheses),
                "waves": len(plan.agent_plan),
            })

            context = plan.to_context()

            # ── Phase 2: Execute waves ──
            async def agent_callback(agent_id: str, result: AgentResult):
                status.agent_results[agent_id] = result
                await _progress("agent_completed", {
                    "agent_id": agent_id,
                    "agent_name": result.agent_name,
                    "status": result.status,
                    "findings": len(result.findings),
                    "confidence": result.confidence,
                    "execution_time": result.execution_time,
                })

            all_results = await dispatcher.dispatch_plan(
                plan.agent_plan,
                context,
                callback=agent_callback,
            )
            status.agent_results = all_results

            # ── Phase 3: Extract report ──
            status.status = "compiling"
            await _progress("compiling_report")

            report_result = all_results.get("report")
            if report_result and report_result.raw_data.get("report"):
                status.report = report_result.raw_data["report"]
                # Sanitize output
                if "executive_summary" in status.report:
                    status.report["executive_summary"] = validate_output(
                        status.report["executive_summary"]
                    )
            else:
                # Build a basic report from findings
                all_findings = []
                for r in all_results.values():
                    all_findings.extend([f.to_dict() for f in r.findings])
                status.report = {
                    "executive_summary": "Investigation completed. See individual agent findings.",
                    "key_findings": all_findings[:10],
                    "investigation_metadata": {
                        "agents_executed": len(all_results),
                        "agents_succeeded": sum(
                            1 for r in all_results.values() if r.status == "success"
                        ),
                        "total_findings": len(all_findings),
                    },
                }

            status.status = "complete"
            status.completed_at = time.time()
            await _progress("investigation_complete", {
                "total_findings": sum(
                    len(r.findings) for r in all_results.values()
                ),
                "elapsed_seconds": status.elapsed(),
            })

            logger.info(
                "investigation_complete",
                id=inv_id,
                elapsed=status.elapsed(),
                agents=len(all_results),
                findings=sum(len(r.findings) for r in all_results.values()),
            )

        except Exception as e:
            status.status = "failed"
            status.error = str(e)
            status.completed_at = time.time()
            await _progress("investigation_failed", {"error": str(e)})
            logger.error("investigation_failed", id=inv_id, error=str(e))

        return status

    def get_investigation(self, inv_id: str) -> InvestigationStatus | None:
        return _investigations.get(inv_id)

    def list_investigations(self, limit: int = 20) -> list[dict[str, Any]]:
        items = sorted(
            _investigations.values(),
            key=lambda x: x.started_at,
            reverse=True,
        )[:limit]
        return [
            {
                "id": i.id,
                "status": i.status,
                "query": i.query,
                "elapsed_seconds": i.elapsed(),
            }
            for i in items
        ]


engine = OrchestrationEngine()
