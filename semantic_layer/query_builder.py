"""Query builder: translates semantic concept + parameters into executable SQL."""

from __future__ import annotations

from typing import Any

from semantic_layer.definitions import CONCEPT_REGISTRY, SemanticConcept
from utils.guardrails import validate_sql, enforce_row_limit
from utils.logging import get_logger

logger = get_logger(__name__)


class QueryBuilder:
    """Builds safe, parameterized SQL from semantic concepts."""

    def build(
        self, concept_name: str, params: dict[str, Any]
    ) -> tuple[str | None, str | None]:
        """Build SQL from a concept name and parameters.

        Returns (sql, None) on success or (None, error_message) on failure.
        """
        concept = CONCEPT_REGISTRY.get(concept_name)
        if concept is None:
            return None, f"Unknown semantic concept: {concept_name}"

        # Validate required parameters
        missing = [p for p in concept.parameters if p not in params]
        if missing:
            return None, f"Missing parameters for {concept_name}: {missing}"

        # Build SQL
        try:
            sql = concept.sql_template % {
                k: f"'{v}'" if isinstance(v, str) else v
                for k, v in params.items()
            }
        except (KeyError, TypeError) as e:
            return None, f"Parameter interpolation failed: {e}"

        # Safety check
        is_safe, reason = validate_sql(sql)
        if not is_safe:
            return None, f"SQL safety check failed: {reason}"

        # Add row limit
        sql = enforce_row_limit(sql)

        logger.info(
            "query_built",
            concept=concept_name,
            params=list(params.keys()),
        )
        return sql, None

    def list_concepts(self, domain: str | None = None) -> list[dict[str, str]]:
        """List available semantic concepts, optionally filtered by domain."""
        concepts = CONCEPT_REGISTRY.values()
        if domain:
            concepts = [c for c in concepts if c.domain == domain]
        return [
            {
                "name": c.name,
                "description": c.description,
                "domain": c.domain,
                "parameters": c.parameters,
            }
            for c in concepts
        ]


query_builder = QueryBuilder()
