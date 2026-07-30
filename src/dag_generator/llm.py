"""Provider-neutral LLM chat completions."""
from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any, Protocol

import yaml

from dag_generator.config import Settings


def load_default_models(path: Path) -> dict[str, str]:
    try:
        config = yaml.safe_load(path.read_text())
    except FileNotFoundError as exc:
        raise ValueError(f"Model config file not found: {path}") from exc

    if not isinstance(config, dict):
        raise ValueError(f"Model config must be a mapping: {path}")

    models = {
        provider: settings.get("default_model")
        for provider, settings in config.items()
        if isinstance(settings, dict)
    }
    if set(models) != {"openrouter", "anthropic"} or not all(
        isinstance(model, str) and model for model in models.values()
    ):
        raise ValueError(
            "Model config must define non-empty default_model values for openrouter and anthropic."
        )
    return models


@dataclass
class ToolCall:
    id: str
    name: str
    input: dict[str, Any]


@dataclass
class LLMResponse:
    text: str
    tool_calls: list[ToolCall]


class LLMProvider(Protocol):
    def complete(
        self,
        system: str,
        tools: list[dict[str, Any]],
        messages: list[dict[str, Any]],
    ) -> LLMResponse: ...


def create_llm_provider(
    provider: str,
    settings: Settings,
    model: str | None = None,
) -> LLMProvider:
    default_models = load_default_models(settings.model_config_path)
    if provider not in default_models:
        raise ValueError(f"Unsupported LLM provider: {provider}")
    model = model or default_models[provider]
    api_key = settings.api_key_for(provider)
    if not api_key:
        environment_name = (
            "OPENROUTER_API_KEY" if provider == "openrouter" else "ANTHROPIC_API_KEY"
        )
        raise ValueError(f"{environment_name} environment variable is not set.")

    if provider == "openrouter":
        return OpenRouterProvider(model, api_key)

    return AnthropicProvider(model, api_key)


class OpenRouterProvider:
    def __init__(self, model: str, api_key: str):
        from openai import OpenAI

        self.model = model
        self.client = OpenAI(base_url="https://openrouter.ai/api/v1", api_key=api_key)

    def complete(
        self,
        system: str,
        tools: list[dict[str, Any]],
        messages: list[dict[str, Any]],
    ) -> LLMResponse:
        response = self.client.chat.completions.create(
            model=self.model,
            max_tokens=8192,
            messages=_to_openrouter_messages(system, messages),
            tools=[_to_openrouter_tool(tool) for tool in tools],
        )
        message = response.choices[0].message
        tool_calls = [
            ToolCall(
                id=tool_call.id,
                name=tool_call.function.name,
                input=_parse_tool_arguments(tool_call.function.arguments),
            )
            for tool_call in message.tool_calls or []
        ]
        return LLMResponse(text=message.content or "", tool_calls=tool_calls)


class AnthropicProvider:
    def __init__(self, model: str, api_key: str):
        import anthropic

        self.model = model
        self.client = anthropic.Anthropic(api_key=api_key)

    def complete(
        self,
        system: str,
        tools: list[dict[str, Any]],
        messages: list[dict[str, Any]],
    ) -> LLMResponse:
        response = self.client.messages.create(
            model=self.model,
            max_tokens=8192,
            system=system,
            tools=tools,
            messages=_to_anthropic_messages(messages),
        )
        tool_calls = [
            ToolCall(id=block.id, name=block.name, input=block.input)
            for block in response.content
            if block.type == "tool_use"
        ]
        text = "\n".join(block.text for block in response.content if block.type == "text")
        return LLMResponse(text=text, tool_calls=tool_calls)


def _parse_tool_arguments(arguments: str) -> dict[str, Any]:
    try:
        return json.loads(arguments)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Model returned invalid tool arguments: {arguments}") from exc


def _to_openrouter_tool(tool: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": "function",
        "function": {
            "name": tool["name"],
            "description": tool["description"],
            "parameters": tool["input_schema"],
        },
    }


def _to_openrouter_messages(system: str, messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = [{"role": "system", "content": system}]
    for message in messages:
        if message["role"] == "tool":
            result.extend(
                {
                    "role": "tool",
                    "tool_call_id": tool_result["tool_call_id"],
                    "content": tool_result["content"],
                }
                for tool_result in message["tool_results"]
            )
        elif message["role"] == "assistant":
            result.append({
                "role": "assistant",
                "content": message["content"] or None,
                "tool_calls": [
                    {
                        "id": tool_call["id"],
                        "type": "function",
                        "function": {
                            "name": tool_call["name"],
                            "arguments": json.dumps(tool_call["input"]),
                        },
                    }
                    for tool_call in message["tool_calls"]
                ],
            })
        else:
            result.append(message)
    return result


def _to_anthropic_messages(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for message in messages:
        if message["role"] == "tool":
            result.append({
                "role": "user",
                "content": [
                    {
                        "type": "tool_result",
                        "tool_use_id": tool_result["tool_call_id"],
                        "content": tool_result["content"],
                    }
                    for tool_result in message["tool_results"]
                ],
            })
        elif message["role"] == "assistant":
            content: list[dict[str, Any]] = []
            if message["content"]:
                content.append({"type": "text", "text": message["content"]})
            content.extend(
                {
                    "type": "tool_use",
                    "id": tool_call["id"],
                    "name": tool_call["name"],
                    "input": tool_call["input"],
                }
                for tool_call in message["tool_calls"]
            )
            result.append({"role": "assistant", "content": content})
        else:
            result.append(message)
    return result
