"""Abstract base class for all agents."""

from __future__ import annotations

import json
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class FindingType(str, Enum):
    ANOMALY = "anomaly"
    PATTERN = "pattern"
    OUTLIER = "outlier"
    CORRELATION = "correlation"
    INFO = "info"


class Severity(str, Enum):
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    INFO = "info"


@dataclass
class Finding:
    finding_type: FindingType
    severity: Severity
    title: str
    explanation: str
    evidence: dict[str, Any] = field(default_factory=dict)
    metrics: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "finding_type": self.finding_type.value,
            "severity": self.severity.value,
            "title": self.title,
            "explanation": self.explanation,
            "evidence": self.evidence,
            "metrics": self.metrics,
        }


@dataclass
class AgentResult:
    agent_id: str
    agent_name: str
    status: str  # success | partial | failed | timeout
    confidence: float  # 0.0 - 1.0
    findings: list[Finding] = field(default_factory=list)
    follow_ups: list[str] = field(default_factory=list)
    execution_time: float = 0.0
    error: str | None = None
    raw_data: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "agent_id": self.agent_id,
            "agent_name": self.agent_name,
            "status": self.status,
            "confidence": self.confidence,
            "findings": [f.to_dict() for f in self.findings],
            "follow_ups": self.follow_ups,
            "execution_time": self.execution_time,
            "error": self.error,
        }


class BaseAgent(ABC):
    """Abstract base for all agents in the system."""

    agent_id: str
    agent_name: str
    domain: str
    wave: int

    def __init__(self, agent_id: str, agent_name: str, domain: str, wave: int = 1):
        self.agent_id = agent_id
        self.agent_name = agent_name
        self.domain = domain
        self.wave = wave

    async def run(self, context: dict[str, Any]) -> AgentResult:
        """Execute the agent with timing and error handling."""
        start = time.time()
        try:
            result = await self.execute(context)
            result.execution_time = time.time() - start
            return result
        except Exception as e:
            return AgentResult(
                agent_id=self.agent_id,
                agent_name=self.agent_name,
                status="failed",
                confidence=0.0,
                error=str(e),
                execution_time=time.time() - start,
            )

    @abstractmethod
    async def execute(self, context: dict[str, Any]) -> AgentResult:
        """Implement the agent's investigation logic."""
        ...

    def _build_system_prompt(self) -> str:
        """Return the system prompt for this agent's LLM calls."""
        return f"""You are the {self.agent_name}, a specialized healthcare analytics agent.
Your domain is: {self.domain}.

You analyze data and produce structured findings. Each finding must include:
- finding_type: anomaly | pattern | outlier | correlation | info
- severity: critical | high | medium | low | info
- title: A concise title
- explanation: Plain-language explanation an analyst can understand
- evidence: Key data points supporting the finding
- metrics: Quantitative measures

Respond ONLY with valid JSON. No markdown fences, no preamble.
"""
