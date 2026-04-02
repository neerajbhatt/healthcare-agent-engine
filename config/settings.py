"""Application configuration loaded from environment variables."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Literal

from pydantic_settings import BaseSettings
from pydantic import Field


_ENV_FILE = Path(__file__).parent / ".env"


class Settings(BaseSettings):
    # ── LLM ──
    llm_provider: Literal["anthropic", "openai"] = "anthropic"
    llm_model: str = "claude-sonnet-4-6"
    anthropic_api_key: str = ""
    openai_api_key: str = ""

    # ── Snowflake ──
    snowflake_account: str = ""
    snowflake_user: str = ""
    snowflake_password: str = ""
    snowflake_warehouse: str = "COMPUTE_WH"
    snowflake_database: str = "HEALTHCARE_DB"
    snowflake_schema: str = "ANALYTICS"

    # ── Agent limits ──
    max_tokens_per_agent: int = 4096
    max_parallel_agents: int = 5
    investigation_timeout: int = 300  # seconds

    # ── API ──
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    cors_origins: list[str] = ["http://localhost:3000", "http://localhost:8000"]

    model_config = {
        "env_file": str(_ENV_FILE) if _ENV_FILE.exists() else None,
        "env_file_encoding": "utf-8",
        "extra": "ignore",
    }


settings = Settings()
