"""Offline checks for provider message and tool conversion."""
from __future__ import annotations

from pathlib import Path
import tempfile

from dag_generator.config import MODEL_CONFIG_FILE
from dag_generator.llm import (
    load_default_models,
    _to_anthropic_messages,
    _to_openrouter_messages,
    _to_openrouter_tool,
)


TOOL = {
    "name": "get_lineage",
    "description": "Read dataset lineage.",
    "input_schema": {"type": "object", "properties": {"urn": {"type": "string"}}},
}
MESSAGES = [
    {"role": "user", "content": "Find the lineage."},
    {
        "role": "assistant",
        "content": "I will look it up.",
        "tool_calls": [{"id": "call_1", "name": "get_lineage", "input": {"urn": "urn:1"}}],
    },
    {"role": "tool", "tool_results": [{"tool_call_id": "call_1", "content": "{}"}]},
]


def test_openrouter_conversion():
    tool = _to_openrouter_tool(TOOL)
    messages = _to_openrouter_messages("System prompt", MESSAGES)
    assert tool["function"]["parameters"] == TOOL["input_schema"]
    assert messages[0] == {"role": "system", "content": "System prompt"}
    assert messages[2]["tool_calls"][0]["function"]["arguments"] == '{"urn": "urn:1"}'
    assert messages[3] == {"role": "tool", "tool_call_id": "call_1", "content": "{}"}


def test_anthropic_conversion():
    messages = _to_anthropic_messages(MESSAGES)
    assert messages[1]["content"][1] == {
        "type": "tool_use", "id": "call_1", "name": "get_lineage", "input": {"urn": "urn:1"},
    }
    assert messages[2] == {
        "role": "user",
        "content": [{"type": "tool_result", "tool_use_id": "call_1", "content": "{}"}],
    }


def test_default_models():
    models = load_default_models(MODEL_CONFIG_FILE)
    assert models["openrouter"] == "openai/gpt-5.2"
    assert models["anthropic"] == "claude-haiku-4-5-20251001"


def test_model_config_can_be_changed_for_tests():
    with tempfile.TemporaryDirectory() as directory:
        config_path = Path(directory) / "models.yaml"
        config_path.write_text(
            "openrouter:\n  default_model: deepseek/deepseek-v4-flash\n"
            "anthropic:\n  default_model: claude-test\n"
        )
        assert load_default_models(config_path) == {
            "openrouter": "deepseek/deepseek-v4-flash",
            "anthropic": "claude-test",
        }


if __name__ == "__main__":
    test_openrouter_conversion()
    test_anthropic_conversion()
    test_default_models()
    test_model_config_can_be_changed_for_tests()
    print("4 provider conversion tests passed")
