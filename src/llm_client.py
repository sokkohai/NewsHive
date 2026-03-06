"""Shared LLM client for NewsHive.

Supports providers:
  - "openai"    — OpenAI Chat Completions API (requires: pip install openai)
  - "anthropic" — Anthropic Messages API      (requires: pip install anthropic)
  - "custom"    — Custom HTTP endpoint (set LLM_API_URL)

Configure via environment variables:
  LLM_PROVIDER   openai | anthropic | custom
  LLM_MODEL      e.g. gpt-4o, claude-3-5-sonnet-20241022, your-model-name
  LLM_API_KEY    your API key / bearer token
  LLM_API_URL    required only for provider=custom
"""

import logging
from typing import Any

import requests as _requests

logger = logging.getLogger(__name__)

SUPPORTED_PROVIDERS = ("openai", "anthropic", "custom")


def call_llm(
    provider: str,
    model: str,
    api_key: str,
    prompt: str,
    system_message: str | None = None,
    api_url: str | None = None,
    max_tokens: int = 1024,
) -> str:
    """Call an LLM and return the response text.

    Args:
        provider:       "openai", "anthropic", or "custom"
        model:          Model identifier
        api_key:        API key / bearer token
        prompt:         User prompt text
        system_message: Optional system / context message
        api_url:        Required for provider="custom"
        max_tokens:     Maximum tokens in the response

    Returns:
        Response text from the LLM

    Raises:
        RuntimeError: If the API call fails or provider is unsupported
    """
    p = provider.lower()
    if p == "openai":
        return _call_openai(model, api_key, prompt, system_message, max_tokens)
    elif p == "anthropic":
        return _call_anthropic(model, api_key, prompt, system_message, max_tokens)
    elif p == "custom":
        if not api_url:
            raise RuntimeError(
                "provider='custom' requires LLM_API_URL to be set"
            )
        return _call_custom(model, api_key, api_url, prompt, system_message)
    else:
        raise RuntimeError(
            f"Unsupported LLM provider: '{provider}'. "
            f"Supported values: {', '.join(SUPPORTED_PROVIDERS)}"
        )


def _call_openai(
    model: str,
    api_key: str,
    prompt: str,
    system_message: str | None,
    max_tokens: int,
) -> str:
    try:
        from openai import OpenAI
    except ImportError:
        raise RuntimeError(
            "openai package not installed. Run: pip install openai"
        )
    client = OpenAI(api_key=api_key)
    messages: list[dict[str, str]] = []
    if system_message:
        messages.append({"role": "system", "content": system_message})
    messages.append({"role": "user", "content": prompt})
    response = client.chat.completions.create(
        model=model,
        messages=messages,
        max_tokens=max_tokens,
    )
    content = response.choices[0].message.content
    if content is None:
        raise RuntimeError("OpenAI returned empty content")
    return content


def _call_anthropic(
    model: str,
    api_key: str,
    prompt: str,
    system_message: str | None,
    max_tokens: int,
) -> str:
    try:
        import anthropic
    except ImportError:
        raise RuntimeError(
            "anthropic package not installed. Run: pip install anthropic"
        )
    client = anthropic.Anthropic(api_key=api_key)
    kwargs: dict[str, Any] = {
        "model": model,
        "max_tokens": max_tokens,
        "messages": [{"role": "user", "content": prompt}],
    }
    if system_message:
        kwargs["system"] = system_message
    message = client.messages.create(**kwargs)
    if not message.content:
        raise RuntimeError("Anthropic returned empty content")
    return message.content[0].text


def _call_custom(
    model: str,
    api_key: str,
    api_url: str,
    prompt: str,
    system_message: str | None,
) -> str:
    full_prompt = f"{system_message}\n\n{prompt}" if system_message else prompt
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload = {"input": full_prompt, "model": model}
    response = _requests.post(api_url, headers=headers, json=payload, timeout=120)
    response.raise_for_status()
    return _extract_custom_response(response.json())


def _extract_custom_response(result: dict[str, Any]) -> str:
    """Extract text from the custom endpoint response.

    Primary format:
        output (list) → type='message' → content (list) → type='output_text' → text

    Fallback (OpenAI-compatible):
        choices[0].message.content  or  choices[0].text
    """
    outputs = result.get("output")
    if isinstance(outputs, list):
        for output_item in outputs:
            if output_item.get("type") == "message":
                for content_item in output_item.get("content", []):
                    if content_item.get("type") == "output_text":
                        text = content_item.get("text")
                        if isinstance(text, str):
                            return text

    choices = result.get("choices")
    if isinstance(choices, list) and choices:
        choice = choices[0]
        if "message" in choice:
            content = choice["message"].get("content")
            if isinstance(content, str):
                return content
        if "text" in choice:
            return str(choice["text"])

    raise RuntimeError(
        f"Unexpected custom LLM response format. Keys found: {list(result.keys())}"
    )
