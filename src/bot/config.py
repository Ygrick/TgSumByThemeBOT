from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

from dotenv import load_dotenv


@dataclass(frozen=True, slots=True)
class Settings:
    telegram_bot_token: str
    openrouter_api_key: str
    openrouter_model: str
    openai_base_url: str
    openrouter_site_url: str | None
    openrouter_app_name: str | None
    database_path: Path
    max_messages_for_analysis: int


def load_settings(env_file: str | None = None) -> Settings:
    load_dotenv(env_file)

    telegram_bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    openrouter_api_key = os.getenv("OPENROUTER_API_KEY", "").strip()
    if not telegram_bot_token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is required.")
    if not openrouter_api_key:
        raise RuntimeError("OPENROUTER_API_KEY is required.")

    openrouter_model = os.getenv("OPENROUTER_MODEL", "openai/gpt-oss-120b").strip()
    openai_base_url = normalize_openai_base_url(
        os.getenv("OPENAI_BASE_URL", "").strip()
        or os.getenv("OPENROUTER_BASE_URL", "").strip()
        or "https://openrouter.ai/api/v1"
    )
    openrouter_site_url = os.getenv("OPENROUTER_SITE_URL", "").strip() or None
    openrouter_app_name = os.getenv("OPENROUTER_APP_NAME", "TgSumByThemeBOT").strip() or None

    database_path = Path(os.getenv("DATABASE_PATH", "bot.db")).expanduser()
    max_messages_for_analysis = int(os.getenv("MAX_MESSAGES_FOR_ANALYSIS", "500"))
    if max_messages_for_analysis < 50:
        max_messages_for_analysis = 50

    return Settings(
        telegram_bot_token=telegram_bot_token,
        openrouter_api_key=openrouter_api_key,
        openrouter_model=openrouter_model,
        openai_base_url=openai_base_url,
        openrouter_site_url=openrouter_site_url,
        openrouter_app_name=openrouter_app_name,
        database_path=database_path,
        max_messages_for_analysis=max_messages_for_analysis,
    )


def normalize_openai_base_url(raw_base_url: str) -> str:
    """
    OpenAI client expects base URL without `/chat/completions`.
    Users often paste full endpoint, so we normalize it.
    """
    default = "https://openrouter.ai/api/v1"
    value = (raw_base_url or "").strip()
    if not value:
        return default

    parsed = urlsplit(value)
    if not parsed.scheme or not parsed.netloc:
        return value.rstrip("/")

    path = parsed.path.rstrip("/")
    if path.lower().endswith("/chat/completions"):
        path = path[: -len("/chat/completions")]

    normalized = urlunsplit((parsed.scheme, parsed.netloc, path, "", ""))
    return normalized.rstrip("/")
