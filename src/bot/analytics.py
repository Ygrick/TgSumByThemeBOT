from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from bot.llm import OpenRouterClient
from bot.models import Message, OpenQuestion, Topic, TopicDraft
from bot.repository import Repository

logger = logging.getLogger(__name__)


class AnalyticsService:
    def __init__(self, repository: Repository, llm: OpenRouterClient, max_messages: int = 500) -> None:
        self._repository = repository
        self._llm = llm
        self._max_messages = max_messages

    async def analyze_topics(self, chat_id: int) -> list[Topic]:
        window_end = datetime.now(timezone.utc)
        window_start = window_end - timedelta(hours=10)
        messages = self._repository.get_messages_in_window(
            chat_id=chat_id,
            window_start=window_start,
            window_end=window_end,
            limit=self._max_messages,
        )
        prepared_messages = self._prepare_messages(messages)
        if not prepared_messages:
            return []

        drafts: list[TopicDraft] = []
        try:
            response = await self._llm.complete_json(
                system_prompt=(
                    "Ты анализируешь групповые чаты и возвращаешь только строгий JSON. "
                    "Выделяй глобальные темы по смыслу и пиши только по-русски. "
                    "Все текстовые поля (title, summary) должны быть на русском языке. "
                    "Пиши дружелюбно и естественно для обычной беседы."
                ),
                user_prompt=self._build_topics_prompt(prepared_messages),
                temperature=0.1,
            )
            drafts = self._parse_topics_response(response, prepared_messages)
        except Exception:
            logger.exception("Topic analysis failed, fallback strategy is used.")

        if not drafts:
            drafts = self._fallback_topics(prepared_messages)

        return self._repository.create_topics(
            chat_id=chat_id,
            drafts=drafts[:3],
            window_start=window_start,
            window_end=window_end,
        )

    async def analyze_open_questions(self, chat_id: int) -> list[OpenQuestion]:
        window_end = datetime.now(timezone.utc)
        window_start = window_end - timedelta(hours=24)
        messages = self._repository.get_messages_in_window(
            chat_id=chat_id,
            window_start=window_start,
            window_end=window_end,
            limit=self._max_messages,
        )
        prepared_messages = self._prepare_messages(messages)
        if not prepared_messages:
            self._repository.save_open_question_report(chat_id, [], window_start, window_end)
            return []

        questions: list[OpenQuestion] = []
        try:
            response = await self._llm.complete_json(
                system_prompt=(
                    "Ты анализируешь обсуждение и находишь незакрытые вопросы. "
                    "Возвращай только строгий JSON. "
                    "Все текстовые поля должны быть на русском языке."
                ),
                user_prompt=self._build_open_questions_prompt(prepared_messages),
                temperature=0.1,
            )
            questions = self._parse_open_questions_response(response, prepared_messages)
        except Exception:
            logger.exception("Open questions analysis failed, fallback strategy is used.")

        if not questions:
            questions = self._fallback_open_questions(prepared_messages)

        self._repository.save_open_question_report(chat_id, questions, window_start, window_end)
        return questions

    def _build_topics_prompt(self, messages: list[Message]) -> str:
        lines = [self._messages_to_indexed_lines(messages)]
        lines.append("")
        lines.append("Задача:")
        lines.append("1) Найди топ-3 глобальные темы (или меньше, если данных мало).")
        lines.append("2) Названия тем должны быть короткими и только на русском языке.")
        lines.append("3) Для каждой темы дай сводку 2-4 предложения, только на русском языке.")
        lines.append(
            "4) По возможности упомяни: что обсуждали, к чему пришли, что осталось нерешенным. "
            "Если каких-то пунктов нет, не выдумывай их."
        )
        lines.append("5) Верни строгий JSON по схеме:")
        lines.append(
            """
{
  "topics": [
    {
      "title": "string",
      "summary": "string",
      "source_indexes": [1, 2, 3]
    }
  ]
}
            """.strip()
        )
        lines.append("source_indexes — индексы сообщений из списка выше (нумерация с 1).")
        lines.append("Не добавляй текст вне JSON.")
        return "\n".join(lines)

    def _build_open_questions_prompt(self, messages: list[Message]) -> str:
        lines = [self._messages_to_indexed_lines(messages)]
        lines.append("")
        lines.append("Задача:")
        lines.append("1) Найди открытые (незакрытые) вопросы в обсуждении.")
        lines.append("2) Включай только те вопросы, которые всё еще остаются без ответа.")
        lines.append("3) Все текстовые поля должны быть только на русском языке.")
        lines.append("4) Для каждого вопроса добавь поле context с кратким контекстом (до 160 символов).")
        lines.append("5) Верни строгий JSON:")
        lines.append(
            """
{
  "open_questions": [
    {
      "question": "string",
      "asked_by": "string",
      "context": "string",
      "details": "string",
      "source_indexes": [1, 2, 3]
    }
  ]
}
            """.strip()
        )
        lines.append("Максимум 10 элементов.")
        lines.append("source_indexes должны включать сам вопрос и соседние по смыслу реплики.")
        lines.append("Не добавляй текст вне JSON.")
        return "\n".join(lines)

    def _messages_to_indexed_lines(self, messages: list[Message]) -> str:
        lines = ["Сообщения:"]
        for index, message in enumerate(messages, start=1):
            username = f"@{message.username}" if message.username else message.display_name
            clean_text = self._normalize_message_text(message.text)
            if len(clean_text) > 280:
                clean_text = clean_text[:280].rstrip() + "…"
            lines.append(f"[{index}] {username}: {clean_text}")
        return "\n".join(lines)

    def _parse_topics_response(self, payload: Any, messages: list[Message]) -> list[TopicDraft]:
        topic_items: list[dict[str, Any]]
        if isinstance(payload, dict):
            topic_items = payload.get("topics", [])
        elif isinstance(payload, list):
            topic_items = payload
        else:
            return []

        result: list[TopicDraft] = []
        for item in topic_items:
            if not isinstance(item, dict):
                continue
            title = str(item.get("title", "")).strip()
            summary = str(item.get("summary", "")).strip()
            source_indexes = item.get("source_indexes", [])
            message_ids = self._map_source_indexes_to_ids(source_indexes, messages)
            if not title or not summary:
                continue
            result.append(
                TopicDraft(
                    title=title[:120],
                    summary=summary[:1200],
                    source_message_ids=message_ids,
                )
            )
            if len(result) == 3:
                break
        return result

    def _parse_open_questions_response(self, payload: Any, messages: list[Message]) -> list[OpenQuestion]:
        if not isinstance(payload, dict):
            return []
        items = payload.get("open_questions", [])
        if not isinstance(items, list):
            return []

        result: list[OpenQuestion] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            question = str(item.get("question", "")).strip()
            asked_by = str(item.get("asked_by", "")).strip()
            context_hint = str(item.get("context", "")).strip()
            details = str(item.get("details", "")).strip()
            source_indexes = item.get("source_indexes", [])
            message_ids = self._map_source_indexes_to_ids(source_indexes, messages)
            if not question:
                continue
            if not message_ids:
                continue
            if not asked_by and message_ids:
                asked_by = self._asked_by_from_message_id(message_ids[0], messages)
            local_context = self._build_context_from_source_ids(message_ids, messages)
            normalized_details = context_hint or details or local_context
            result.append(
                OpenQuestion(
                    question=question[:500],
                    asked_by=asked_by or "unknown",
                    details=normalized_details[:800],
                    source_message_ids=message_ids,
                )
            )
            if len(result) == 10:
                break
        return result

    def _fallback_topics(self, messages: list[Message]) -> list[TopicDraft]:
        if not messages:
            return []
        summary = (
            "Не удалось выполнить семантический анализ. "
            "Сохранен общий срез сообщений из окна анализа. "
            "Повторите команду позже для точной кластеризации."
        )
        return [
            TopicDraft(
                title="Общий срез обсуждения",
                summary=summary,
                source_message_ids=[message.id for message in messages[:20]],
            )
        ]

    def _fallback_open_questions(self, messages: list[Message]) -> list[OpenQuestion]:
        result: list[OpenQuestion] = []
        for index, message in enumerate(messages):
            text = message.text.strip()
            if "?" not in text:
                continue

            answered = False
            for next_message in messages[index + 1 : index + 9]:
                if next_message.user_id == message.user_id:
                    continue
                candidate_text = next_message.text.strip()
                if not candidate_text:
                    continue
                if "?" not in candidate_text and len(candidate_text) >= 8:
                    answered = True
                    break

            if answered:
                continue

            context_ids = [msg.id for msg in messages[max(0, index - 1) : min(len(messages), index + 2)]]
            context_text = self._build_context_from_source_ids(context_ids, messages)
            asked_by = f"@{message.username}" if message.username else message.display_name
            result.append(
                OpenQuestion(
                    question=text[:500],
                    asked_by=asked_by,
                    details=context_text
                    or "Определено эвристикой: в ближайших ответах не найдено явного закрытия вопроса.",
                    source_message_ids=context_ids or [message.id],
                )
            )
            if len(result) == 10:
                break
        return result

    @staticmethod
    def _map_source_indexes_to_ids(source_indexes: Any, messages: list[Message]) -> list[int]:
        if not isinstance(source_indexes, list):
            return []
        message_ids: list[int] = []
        for raw_index in source_indexes:
            if not isinstance(raw_index, int):
                continue
            if raw_index < 1 or raw_index > len(messages):
                continue
            message_ids.append(messages[raw_index - 1].id)
        return sorted(set(message_ids))

    @staticmethod
    def _asked_by_from_message_id(message_id: int, messages: list[Message]) -> str:
        for message in messages:
            if message.id != message_id:
                continue
            return f"@{message.username}" if message.username else message.display_name
        return "unknown"

    def _prepare_messages(self, messages: list[Message]) -> list[Message]:
        prepared: list[Message] = []
        previous_key: tuple[int, str] | None = None
        for message in messages:
            normalized = self._normalize_message_text(message.text)
            if not normalized:
                continue
            if len(normalized) < 3 and "?" not in normalized:
                continue
            key = (message.user_id, normalized.casefold())
            if key == previous_key:
                continue
            previous_key = key
            prepared.append(message)
        return prepared

    def _build_context_from_source_ids(self, message_ids: list[int], messages: list[Message]) -> str:
        if not message_ids:
            return ""
        by_id = {message.id: message for message in messages}
        parts: list[str] = []
        for message_id in message_ids[:3]:
            message = by_id.get(message_id)
            if not message:
                continue
            author = f"@{message.username}" if message.username else message.display_name
            text = self._normalize_message_text(message.text)
            if len(text) > 110:
                text = text[:110].rstrip() + "…"
            parts.append(f"{author}: {text}")
        return " | ".join(parts)

    @staticmethod
    def _normalize_message_text(text: str) -> str:
        return " ".join(text.split())
