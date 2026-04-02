# Healthcare AI Multi-Agent Research Engine

An AI-powered investigation engine that orchestrates specialized agents across Claims, Provider, Member, and Eligibility data to automate healthcare fraud detection and analytics.

## Architecture

```
Analyst Query
    │
    ▼
┌─────────────────────────┐
│   FastAPI Gateway        │  ← REST API + WebSocket for streaming
└─────────┬───────────────┘
          │
          ▼
┌─────────────────────────┐
│   Orchestrator (LLM)    │  ← Adaptive Plan-and-Execute
│   - Parse & Classify    │
│   - Generate Hypotheses │
│   - Dispatch Agents     │
│   - Evaluate & Adapt    │
└─────────┬───────────────┘
          │
    ┌─────┼─────┬─────────┐
    ▼     ▼     ▼         ▼
┌──────┐┌──────┐┌──────┐┌──────┐   Domain Agents
│Claims││Provdr││Member││Eligib│   (Wave 1 - Parallel)
└──┬───┘└──┬───┘└──┬───┘└──┬───┘
   │       │       │       │
   ▼       ▼       ▼       ▼
┌─────────────────────────────┐
│   Semantic Abstraction Layer│  ← Business logic, no raw SQL in agents
└─────────────┬───────────────┘
              │
              ▼
┌─────────────────────────────┐
│   SQLite / Snowflake        │
└─────────────────────────────┘
```

## Quick Start

### 1. Prerequisites
- Python 3.11+
- Anthropic API key (Claude) or OpenAI API key
- No database needed — SQLite + synthetic data included!

### 2. Install
```bash
cd healthcare-agent-engine
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Configure
```bash
cp config/.env.example config/.env
# Edit config/.env — only LLM keys are required:
#   ANTHROPIC_API_KEY=sk-ant-...
#   LLM_PROVIDER=anthropic
#   LLM_MODEL=claude-sonnet-4-6
# Snowflake settings are OPTIONAL — SQLite is used by default
```

### 4. Run
```bash
# The first startup auto-creates the SQLite DB from included CSVs
uvicorn api.main:app --reload --port 8000

# Open dashboard at http://localhost:8000
```

### 5. Test with Known Anomalies
```bash
# Try these NPIs that have intentional fraud patterns baked in:
# NPI 1043321819 — upcoding (82% Level 4-5 E&M codes)
# NPI 1034131647 — billing spike (700 claims in Q4 2025 vs ~10/month normal)
# NPI 1767242388 — weekend billing anomaly (35% of claims on weekends)
curl -X POST http://localhost:8000/api/investigate \
  -H "Content-Type: application/json" \
  -d '{"query": "Investigate NPI 1043321819 for billing anomalies in 2025"}'
```

### 6. Run Tests
```bash
pytest tests/ -v
```

### Switching to Snowflake Later
```bash
# 1. Rename the SQLite client out of the way
mv utils/snowflake_client.py utils/sqlite_client.py
mv utils/snowflake_client.py.bak utils/snowflake_client.py

# 2. Uncomment snowflake-connector-python in requirements.txt
# 3. Run data/01_schema.sql and data/03_load.sql in Snowflake
# 4. Fill in Snowflake credentials in config/.env
```

## Project Structure

```
healthcare-agent-engine/
├── agents/                    # All agent implementations
│   ├── base.py               # Abstract base agent
│   ├── claims_agent.py       # Claims Intelligence Agent
│   ├── provider_agent.py     # Provider Behavior Agent
│   ├── member_agent.py       # Member Risk & Utilization Agent
│   ├── eligibility_agent.py  # Eligibility & Coverage Agent
│   ├── temporal_agent.py     # Temporal Analysis Agent
│   ├── fraud_synthesis_agent.py  # Cross-domain fraud correlation
│   ├── network_agent.py      # Network/relationship analysis
│   ├── cost_impact_agent.py  # Financial impact quantification
│   └── report_agent.py       # Report compilation
├── orchestrator/
│   ├── engine.py             # Main orchestration engine
│   ├── planner.py            # Hypothesis generation & planning
│   ├── dispatcher.py         # Parallel agent dispatch
│   └── evaluator.py          # Result evaluation & plan adaptation
├── semantic_layer/
│   ├── definitions.py        # Business concept definitions
│   ├── query_builder.py      # SQL generation from semantic intent
│   └── validator.py          # Query result validation
├── api/
│   ├── main.py               # FastAPI application
│   ├── routes.py             # API endpoints
│   ├── schemas.py            # Pydantic request/response models
│   └── websocket.py          # Real-time streaming
├── config/
│   ├── settings.py           # Application configuration
│   ├── .env.example          # Environment template
│   └── agents.yaml           # Agent configuration
├── utils/
│   ├── llm_client.py         # LLM provider abstraction
│   ├── snowflake_client.py   # Snowflake connection manager
│   ├── guardrails.py         # PHI/PII protection
│   └── logging.py            # Structured logging
├── dashboard/
│   └── index.html            # Investigation dashboard UI
├── tests/
│   ├── test_agents.py
│   ├── test_orchestrator.py
│   └── test_semantic_layer.py
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
└── README.md
```

## Configuration

### Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `ANTHROPIC_API_KEY` | Claude API key | Yes (if using Claude) |
| `OPENAI_API_KEY` | OpenAI API key | Yes (if using GPT) |
| `LLM_PROVIDER` | `anthropic` or `openai` | Yes |
| `LLM_MODEL` | Model name (e.g., `claude-sonnet-4-6`) | Yes |
| `SNOWFLAKE_ACCOUNT` | Snowflake account identifier | Yes |
| `SNOWFLAKE_USER` | Snowflake username | Yes |
| `SNOWFLAKE_PASSWORD` | Snowflake password | Yes |
| `SNOWFLAKE_WAREHOUSE` | Snowflake warehouse | Yes |
| `SNOWFLAKE_DATABASE` | Snowflake database | Yes |
| `SNOWFLAKE_SCHEMA` | Snowflake schema | Yes |
| `MAX_TOKENS_PER_AGENT` | Token budget per agent call | No (default: 4096) |
| `MAX_PARALLEL_AGENTS` | Max concurrent agent executions | No (default: 5) |
| `INVESTIGATION_TIMEOUT` | Max seconds per investigation | No (default: 300) |

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/investigate` | Submit a new investigation query |
| `GET` | `/api/investigations/{id}` | Get investigation status/results |
| `GET` | `/api/investigations` | List recent investigations |
| `WS` | `/ws/investigate/{id}` | Stream investigation progress |
| `GET` | `/api/agents` | List available agents and status |
| `GET` | `/api/health` | Health check |
