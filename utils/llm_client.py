"""Unified LLM client abstraction supporting Anthropic and OpenAI."""

from __future__ import annotations

import json
from typing import Any

from config.settings import settings


async def llm_complete(
    system_prompt: str,
    user_message: str,
    *,
    temperature: float = 0.2,
    max_tokens: int | None = None,
    response_format: str = "text",  # "text" | "json"
) -> str:
    """Send a completion request to the configured LLM provider."""
    max_tokens = max_tokens or settings.max_tokens_per_agent

    if settings.llm_provider == "anthropic":
        return await _anthropic_complete(
            system_prompt, user_message, temperature, max_tokens, response_format
        )
    elif settings.llm_provider == "openai":
        return await _openai_complete(
            system_prompt, user_message, temperature, max_tokens, response_format
        )
    else:
        raise ValueError(f"Unknown LLM provider: {settings.llm_provider}")


_anthropic_client = None


def _get_anthropic_client():
    global _anthropic_client
    if _anthropic_client is None:
        import anthropic
        _anthropic_client = anthropic.AsyncAnthropic(api_key=settings.anthropic_api_key)
    return _anthropic_client


async def _anthropic_complete(
    system: str, user: str, temperature: float, max_tokens: int, fmt: str
) -> str:
    client = _get_anthropic_client()

    message = await client.messages.create(
        model=settings.llm_model,
        max_tokens=max_tokens,
        temperature=temperature,
        system=system,
        messages=[{"role": "user", "content": user}],
    )

    text = message.content[0].text

    if fmt == "json":
        text = _extract_json(text)

    return text


_openai_client = None


def _get_openai_client():
    global _openai_client
    if _openai_client is None:
        from openai import AsyncOpenAI
        _openai_client = AsyncOpenAI(api_key=settings.openai_api_key)
    return _openai_client


async def _openai_complete(
    system: str, user: str, temperature: float, max_tokens: int, fmt: str
) -> str:
    client = _get_openai_client()

    kwargs: dict[str, Any] = {}
    if fmt == "json":
        kwargs["response_format"] = {"type": "json_object"}

    response = await client.chat.completions.create(
        model=settings.llm_model,
        temperature=temperature,
        max_tokens=max_tokens,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        **kwargs,
    )

    return response.choices[0].message.content or ""


def _extract_json(text: str) -> str:
    """Strip markdown fences and extract JSON from LLM output."""
    text = text.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        # Remove first and last lines (fences)
        lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        text = "\n".join(lines).strip()
    # Validate it's parseable JSON
    json.loads(text)
    return text
