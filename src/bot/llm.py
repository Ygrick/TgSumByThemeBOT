from __future__ import annotations

import json
from json import JSONDecodeError
from typing import Any

from openai import AsyncOpenAI

from bot.config import Settings


class OpenRouterClient:
    def __init__(self, settings: Settings) -> None:
        self._model = settings.openrouter_model
        headers: dict[str, str] = {}
        if settings.openrouter_site_url:
            headers["HTTP-Referer"] = settings.openrouter_site_url
        if settings.openrouter_app_name:
            headers["X-Title"] = settings.openrouter_app_name

        self._client = AsyncOpenAI(
            api_key=settings.openrouter_api_key,
            base_url=settings.openai_base_url,
            default_headers=headers or None,
            timeout=60.0,
        )

    async def complete(self, system_prompt: str, user_prompt: str, temperature: float = 0.2) -> str:
        completion = await self._client.chat.completions.create(
            model=self._model,
            temperature=temperature,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        )

        if not completion.choices:
            raise RuntimeError("OpenRouter returned no choices.")

        content = completion.choices[0].message.content
        if content is None:
            raise RuntimeError("OpenRouter response has empty message content.")
        if isinstance(content, str):
            return content.strip()
        if isinstance(content, list):
            text_parts: list[str] = []
            for item in content:
                text = getattr(item, "text", None)
                if text:
                    text_parts.append(str(text))
            if text_parts:
                return "".join(text_parts).strip()

        raise RuntimeError("OpenRouter response has an unexpected message content format.")

    async def complete_json(
        self, system_prompt: str, user_prompt: str, temperature: float = 0.1
    ) -> Any:
        text = await self.complete(system_prompt=system_prompt, user_prompt=user_prompt, temperature=temperature)
        return extract_json_payload(text)


def extract_json_payload(text: str) -> Any:
    decoder = json.JSONDecoder()
    for index, char in enumerate(text):
        if char not in "[{":
            continue
        candidate = text[index:]
        try:
            value, _ = decoder.raw_decode(candidate)
            return value
        except JSONDecodeError:
            continue
    raise ValueError("Could not extract JSON payload from model response.")
