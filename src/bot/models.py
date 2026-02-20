from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class Participant:
    chat_id: int
    user_id: int
    username: str | None
    display_name: str
    tags: str
    last_seen_at: str


@dataclass(slots=True)
class Message:
    id: int
    chat_id: int
    telegram_message_id: int
    reply_to_telegram_message_id: int | None
    user_id: int
    username: str | None
    display_name: str
    text: str
    created_at: str


@dataclass(slots=True)
class Topic:
    id: int
    chat_id: int
    title: str
    summary: str
    message_count: int
    window_start: str
    window_end: str
    created_at: str


@dataclass(slots=True)
class TopicDraft:
    title: str
    summary: str
    source_message_ids: list[int]


@dataclass(slots=True)
class OpenQuestion:
    question: str
    asked_by: str
    details: str
    source_message_ids: list[int]
