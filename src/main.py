from __future__ import annotations

import logging

from telegram import Update
from telegram.ext import Application, ApplicationBuilder

from bot.analytics import AnalyticsService
from bot.config import load_settings
from bot.db import Database
from bot.handlers import register_handlers, setup_bot_commands
from bot.llm import OpenRouterClient
from bot.repository import Repository


def configure_logging() -> None:
    logging.basicConfig(
        format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        level=logging.INFO,
    )


def main() -> None:
    configure_logging()
    settings = load_settings()

    db = Database(settings.database_path)
    db.initialize()
    repository = Repository(db)

    llm = OpenRouterClient(settings)
    analytics = AnalyticsService(repository=repository, llm=llm, max_messages=settings.max_messages_for_analysis)

    async def on_startup(application: Application) -> None:
        await setup_bot_commands(application)

    application = (
        ApplicationBuilder()
        .token(settings.telegram_bot_token)
        .post_init(on_startup)
        .build()
    )
    application.bot_data["repository"] = repository
    application.bot_data["analytics"] = analytics
    register_handlers(application)

    logging.getLogger(__name__).info("Bot started.")
    try:
        application.run_polling(allowed_updates=Update.ALL_TYPES)
    finally:
        db.close()


if __name__ == "__main__":
    main()
