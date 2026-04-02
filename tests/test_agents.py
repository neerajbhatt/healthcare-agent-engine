"""Tests for the healthcare agent engine."""

import json
import pytest

# ── Semantic Layer Tests ──

class TestSemanticDefinitions:
    def test_concept_registry_populated(self):
        from semantic_layer.definitions import CONCEPT_REGISTRY
        assert len(CONCEPT_REGISTRY) >= 10

    def test_concept_has_required_fields(self):
        from semantic_layer.definitions import CONCEPT_REGISTRY
        for name, concept in CONCEPT_REGISTRY.items():
            assert concept.name == name
            assert concept.description
            assert concept.sql_template
            assert concept.domain

    def test_get_concepts_for_domain(self):
        from semantic_layer.definitions import get_concepts_for_domain
        claims = get_concepts_for_domain("claims")
        assert len(claims) >= 3
        assert all(c.domain == "claims" for c in claims)

    def test_get_concept(self):
        from semantic_layer.definitions import get_concept
        c = get_concept("high_cost_claims")
        assert c is not None
        assert c.domain == "claims"
        assert get_concept("nonexistent") is None


class TestQueryBuilder:
    def test_build_valid_query(self):
        from semantic_layer.query_builder import query_builder
        sql, err = query_builder.build("high_cost_claims", {
            "provider_npi": "1234567890",
            "start_date": "2025-01-01",
            "end_date": "2025-12-31",
        })
        assert err is None
        assert sql is not None
        assert "1234567890" in sql
        assert "LIMIT" in sql

    def test_build_missing_params(self):
        from semantic_layer.query_builder import query_builder
        sql, err = query_builder.build("high_cost_claims", {
            "provider_npi": "1234567890",
            # missing start_date, end_date
        })
        assert sql is None
        assert "Missing parameters" in err

    def test_build_unknown_concept(self):
        from semantic_layer.query_builder import query_builder
        sql, err = query_builder.build("nonexistent_concept", {})
        assert sql is None
        assert "Unknown" in err

    def test_list_concepts(self):
        from semantic_layer.query_builder import query_builder
        all_concepts = query_builder.list_concepts()
        assert len(all_concepts) >= 10

        claims_only = query_builder.list_concepts(domain="claims")
        assert all(c["domain"] == "claims" for c in claims_only)


# ── Guardrails Tests ──

class TestGuardrails:
    def test_block_dangerous_sql(self):
        from utils.guardrails import validate_sql
        is_safe, reason = validate_sql("DROP TABLE claims")
        assert not is_safe
        assert reason is not None

    def test_allow_select(self):
        from utils.guardrails import validate_sql
        is_safe, reason = validate_sql("SELECT * FROM claims WHERE provider_npi = '123'")
        assert is_safe
        assert reason is None

    def test_enforce_row_limit(self):
        from utils.guardrails import enforce_row_limit
        sql = "SELECT * FROM claims"
        result = enforce_row_limit(sql)
        assert "LIMIT" in result

    def test_no_double_limit(self):
        from utils.guardrails import enforce_row_limit
        sql = "SELECT * FROM claims LIMIT 100"
        result = enforce_row_limit(sql)
        assert result.count("LIMIT") == 1

    def test_mask_pii(self):
        from utils.guardrails import mask_pii_columns
        data = {"ssn": "123-45-6789", "claim_id": "CLM001", "first_name": "John"}
        masked = mask_pii_columns(data)
        assert masked["ssn"] == "***REDACTED***"
        assert masked["first_name"] == "***REDACTED***"
        assert masked["claim_id"] == "CLM001"

    def test_validate_output_masks_ssn(self):
        from utils.guardrails import validate_output
        text = "The member SSN is 123-45-6789 and they live at 123 Main St"
        result = validate_output(text)
        assert "123-45-6789" not in result
        assert "***-**-****" in result


# ── Agent Base Tests ──

class TestAgentBase:
    def test_finding_to_dict(self):
        from agents.base import Finding, FindingType, Severity
        f = Finding(
            finding_type=FindingType.ANOMALY,
            severity=Severity.HIGH,
            title="Test Finding",
            explanation="Test explanation",
            evidence={"key": "value"},
            metrics={"score": 0.9},
        )
        d = f.to_dict()
        assert d["finding_type"] == "anomaly"
        assert d["severity"] == "high"
        assert d["title"] == "Test Finding"

    def test_agent_result_to_dict(self):
        from agents.base import AgentResult, Finding, FindingType, Severity
        r = AgentResult(
            agent_id="test",
            agent_name="Test Agent",
            status="success",
            confidence=0.85,
            findings=[
                Finding(
                    finding_type=FindingType.PATTERN,
                    severity=Severity.MEDIUM,
                    title="Pattern Found",
                    explanation="A pattern was detected",
                )
            ],
            follow_ups=["Check more data"],
            execution_time=2.5,
        )
        d = r.to_dict()
        assert d["agent_id"] == "test"
        assert d["status"] == "success"
        assert d["confidence"] == 0.85
        assert len(d["findings"]) == 1


# ── API Schema Tests ──

class TestSchemas:
    def test_investigate_request_validation(self):
        from api.schemas import InvestigateRequest
        req = InvestigateRequest(query="Investigate NPI 1234567890 for fraud")
        assert req.query

    def test_investigate_request_too_short(self):
        from api.schemas import InvestigateRequest
        with pytest.raises(Exception):
            InvestigateRequest(query="Hi")


# ── Dispatcher Registry Tests ──

class TestDispatcher:
    def test_agent_registry_complete(self):
        from orchestrator.dispatcher import AGENT_REGISTRY
        expected = [
            "claims", "provider", "member", "eligibility",
            "temporal", "fraud_synthesis", "network",
            "cost_impact", "report",
        ]
        for agent_id in expected:
            assert agent_id in AGENT_REGISTRY, f"Missing agent: {agent_id}"

    def test_agent_instantiation(self):
        from orchestrator.dispatcher import AGENT_REGISTRY
        for agent_id, cls in AGENT_REGISTRY.items():
            agent = cls()
            assert agent.agent_id == agent_id
            assert agent.agent_name
            assert agent.domain


# ── Settings Tests ──

class TestSettings:
    def test_settings_defaults(self):
        from config.settings import Settings
        s = Settings()
        assert s.max_parallel_agents == 5
        assert s.investigation_timeout == 300
        assert s.api_port == 8000
