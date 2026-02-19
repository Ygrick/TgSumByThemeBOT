from __future__ import annotations

import html
import logging
from datetime import datetime, timezone

from telegram import (
    BotCommand,
    BotCommandScopeAllGroupChats,
    BotCommandScopeAllPrivateChats,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Update,
)
from telegram.constants import ParseMode
from telegram.error import TelegramError
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from bot.analytics import AnalyticsService
from bot.repository import Repository

logger = logging.getLogger(__name__)

TOPIC_CALLBACK_PREFIX = "topic:"
BOT_COMMANDS = [
    BotCommand("help", "Подробная справка по командам"),
    BotCommand("examples", "Короткие примеры использования"),
    BotCommand("notify_all", "Оповестить всех участников"),
    BotCommand("analyze_topics", "Топ тем за 10 часов"),
    BotCommand("latest_topics", "Последние 10 тем"),
    BotCommand("topic", "Сводка темы по id"),
    BotCommand("open_questions", "Открытые вопросы за 24 часа"),
]


def register_handlers(application: Application) -> None:
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("examples", examples_command))
    application.add_handler(CommandHandler("notify_all", notify_all_command))
    application.add_handler(CommandHandler("analyze_topics", analyze_topics_command))
    application.add_handler(CommandHandler("latest_topics", latest_topics_command))
    application.add_handler(CommandHandler("topic", topic_command))
    application.add_handler(CommandHandler("open_questions", open_questions_command))

    application.add_handler(
        CallbackQueryHandler(topic_callback, pattern=rf"^{TOPIC_CALLBACK_PREFIX}\d+$")
    )
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, track_message_handler))

    application.add_error_handler(error_handler)


def get_repository(context: ContextTypes.DEFAULT_TYPE) -> Repository:
    return context.application.bot_data["repository"]


def get_analytics(context: ContextTypes.DEFAULT_TYPE) -> AnalyticsService:
    return context.application.bot_data["analytics"]


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await remember_sender(update, context)
    await help_command(update, context)


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    help_text = build_help_text()
    if update.message:
        await update.message.reply_text(help_text, parse_mode=ParseMode.HTML)


async def examples_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await remember_sender(update, context)
    if not update.message:
        return
    await update.message.reply_text(build_examples_text(), parse_mode=ParseMode.HTML)


async def notify_all_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.message:
        return
    await remember_sender(update, context)

    message_text = " ".join(context.args).strip()

    repository = get_repository(context)
    participants = repository.list_participants(update.effective_chat.id)
    if not participants:
        await update.message.reply_text("Пока нет участников в локальной базе.")
        return

    mentions = []
    for participant in participants:
        if participant.username:
            mention = f"@{participant.username}"
        else:
            display_name = html.escape(participant.display_name)
            mention = f'<a href="tg://user?id={participant.user_id}">{display_name}</a>'
        mentions.append(mention)

    prefix = f"{html.escape(message_text)}\n\n" if message_text else ""
    full_text = f"{prefix}{' '.join(mentions)}"
    for chunk in split_long_text(full_text):
        await update.message.reply_text(chunk, parse_mode=ParseMode.HTML, disable_web_page_preview=True)


async def analyze_topics_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.message:
        return
    await remember_sender(update, context)

    status = await update.message.reply_text("Анализирую сообщения за последние 10 часов...")
    analytics = get_analytics(context)
    topics = await analytics.analyze_topics(update.effective_chat.id)

    if not topics:
        await status.edit_text("Недостаточно сообщений для анализа тем.")
        return

    lines = ["<b>Топ тем за последние 10 часов</b>:"]
    keyboard = []
    for topic in topics:
        lines.append(
            f"<b>{topic.id}.</b> {html.escape(topic.title)} "
            f"<i>(сообщений:</i> <code>{topic.message_count}</code><i>)</i>"
        )
        keyboard.append(
            [InlineKeyboardButton(text=f"{topic.id}. {topic.title[:50]}", callback_data=f"{TOPIC_CALLBACK_PREFIX}{topic.id}")]
        )
    lines.append("")
    lines.append("Нажмите кнопку ниже или используйте <code>/topic &lt;id&gt;</code>.")
    await status.edit_text(
        "\n".join(lines),
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def latest_topics_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.message:
        return
    await remember_sender(update, context)

    repository = get_repository(context)
    topics = repository.list_recent_topics(update.effective_chat.id, limit=10)
    if not topics:
        await update.message.reply_text("В БД пока нет сохранённых тем.")
        return

    lines = ["<b>Последние 10 тем</b>:"]
    for topic in topics:
        lines.append(
            f"<b>{topic.id}.</b> {html.escape(topic.title)} "
            f"<code>[{format_timestamp(topic.created_at)}]</code>"
        )
    lines.append("Для сводки: <code>/topic &lt;id&gt;</code>.")
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)


async def topic_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.message:
        return
    await remember_sender(update, context)

    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("Использование: /topic <id>")
        return

    topic_id = int(context.args[0])
    await send_topic_summary(update.effective_chat.id, topic_id, update.message.reply_text, context)


async def topic_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.callback_query or not update.effective_chat:
        return
    await remember_sender(update, context)

    query = update.callback_query
    await query.answer()

    data = query.data or ""
    if not data.startswith(TOPIC_CALLBACK_PREFIX):
        return
    topic_id_raw = data.removeprefix(TOPIC_CALLBACK_PREFIX)
    if not topic_id_raw.isdigit():
        return

    topic_id = int(topic_id_raw)
    if query.message:
        await send_topic_summary(update.effective_chat.id, topic_id, query.message.reply_text, context)


async def open_questions_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.message:
        return
    await remember_sender(update, context)

    status = await update.message.reply_text("Анализирую открытые вопросы за последние 24 часа...")
    analytics = get_analytics(context)
    questions = await analytics.analyze_open_questions(update.effective_chat.id)

    if not questions:
        await status.edit_text("Открытых вопросов за последние 24 часа не найдено.")
        return

    lines = ["<b>Открытые вопросы за последние 24 часа</b>:"]
    for index, question in enumerate(questions, start=1):
        lines.append(f"<b>{index}.</b> {html.escape(question.question)}")
        lines.append(f"<i>Автор:</i> {html.escape(question.asked_by)}")
        if question.details:
            lines.append(f"<i>Комментарий:</i> {html.escape(question.details)}")
        lines.append("")
    await status.edit_text("\n".join(lines).strip(), parse_mode=ParseMode.HTML)


async def track_message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return
    text = (update.message.text or "").strip()
    if not text:
        return

    repository = get_repository(context)
    repository.upsert_participant(
        chat_id=update.effective_chat.id,
        user_id=update.effective_user.id,
        username=update.effective_user.username,
        display_name=build_display_name(update.effective_user.first_name, update.effective_user.last_name),
    )
    repository.save_message(
        chat_id=update.effective_chat.id,
        telegram_message_id=update.message.message_id,
        user_id=update.effective_user.id,
        username=update.effective_user.username,
        display_name=build_display_name(update.effective_user.first_name, update.effective_user.last_name),
        text=text,
        created_at=update.message.date.astimezone(timezone.utc),
    )


async def remember_sender(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user:
        return
    repository = get_repository(context)
    repository.upsert_participant(
        chat_id=update.effective_chat.id,
        user_id=update.effective_user.id,
        username=update.effective_user.username,
        display_name=build_display_name(update.effective_user.first_name, update.effective_user.last_name),
    )


async def send_topic_summary(
    chat_id: int,
    topic_id: int,
    sender,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    repository = get_repository(context)
    topic = repository.get_topic(chat_id=chat_id, topic_id=topic_id)
    if not topic:
        await sender("Тема с таким id не найдена.")
        return

    text = (
        f"<b>Тема #{topic.id}:</b> {html.escape(topic.title)}\n"
        f"<b>Сводка:</b> {html.escape(topic.summary)}\n"
        f"<b>Сообщений в теме:</b> <code>{topic.message_count}</code>\n"
        f"<b>Окно анализа:</b> "
        f"<code>{format_timestamp(topic.window_start)} - {format_timestamp(topic.window_end)}</code>"
    )
    await sender(text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)


def split_long_text(text: str, max_length: int = 3900) -> list[str]:
    if len(text) <= max_length:
        return [text]
    chunks: list[str] = []
    remaining = text.strip()
    while remaining:
        if len(remaining) <= max_length:
            chunks.append(remaining)
            break
        cut = remaining.rfind("\n", 0, max_length)
        if cut == -1:
            cut = remaining.rfind(" ", 0, max_length)
        if cut == -1:
            cut = max_length
        chunks.append(remaining[:cut].strip())
        remaining = remaining[cut:].strip()
    return chunks


def build_display_name(first_name: str | None, last_name: str | None) -> str:
    first = (first_name or "").strip()
    last = (last_name or "").strip()
    display = " ".join(part for part in [first, last] if part).strip()
    return display or "unknown"


def format_timestamp(value: str) -> str:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return value
    return parsed.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.exception("Unhandled error while processing update: %s", update, exc_info=context.error)


def build_help_text() -> str:
    return (
        "<b>Команды</b>:\n"
        "/notify_all [текст] - оповестить участников из БД (текст необязателен).\n"
        "/analyze_topics - найти и сохранить топ тем за 10 часов.\n"
        "/topic &lt;id&gt; - показать сводку темы из БД.\n"
        "/latest_topics - показать последние 10 тем.\n"
        "/open_questions - показать открытые вопросы за 24 часа.\n"
        "/examples - 2 готовых сценария использования.\n"
    )


def build_examples_text() -> str:
    return (
        "<b>Пример 1 (ежедневный обзор)</b>:\n"
        "1) <code>/analyze_topics</code>\n"
        "2) Нажмите кнопку нужной темы или <code>/topic &lt;id&gt;</code>\n"
        "3) <code>/open_questions</code>\n"
        "Результат: краткая картина обсуждений + список незакрытых вопросов.\n"
        "\n"
        "<b>Пример 2 (срочное оповещение)</b>:\n"
        "1) <code>/notify_all</code>\n"
        "2) <code>/notify_all Релиз откладываем на 30 минут, проверьте статусы задач.</code>\n"
        "Результат: бот отправит сообщение и упомянет известных участников."
    )


async def setup_bot_commands(application: Application) -> None:
    try:
        await application.bot.set_my_commands(BOT_COMMANDS)
        await application.bot.set_my_commands(
            BOT_COMMANDS, scope=BotCommandScopeAllPrivateChats()
        )
        await application.bot.set_my_commands(
            BOT_COMMANDS, scope=BotCommandScopeAllGroupChats()
        )
        logger.info("Telegram command menu is configured.")
    except TelegramError:
        logger.exception("Failed to configure Telegram bot commands.")
