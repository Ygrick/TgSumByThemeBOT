from __future__ import annotations

import json
from datetime import datetime, timezone

from bot.db import Database
from bot.models import Message, OpenQuestion, Participant, Topic, TopicDraft


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class Repository:
    def __init__(self, db: Database) -> None:
        self._db = db

    def upsert_participant(
        self,
        chat_id: int,
        user_id: int,
        username: str | None,
        display_name: str,
        tags: str | None = None,
    ) -> None:
        now = utcnow_iso()
        with self._db.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO participants (
                    chat_id, user_id, username, display_name, tags, last_seen_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(chat_id, user_id) DO UPDATE SET
                    username = excluded.username,
                    display_name = excluded.display_name,
                    tags = COALESCE(?, participants.tags),
                    last_seen_at = excluded.last_seen_at
                """,
                (chat_id, user_id, username, display_name, tags or "", now, tags),
            )

    def set_participant_tags(self, chat_id: int, user_id: int, tags: str) -> None:
        with self._db.cursor() as cursor:
            cursor.execute(
                """
                UPDATE participants
                SET tags = ?, last_seen_at = ?
                WHERE chat_id = ? AND user_id = ?
                """,
                (tags, utcnow_iso(), chat_id, user_id),
            )

    def list_participants(self, chat_id: int) -> list[Participant]:
        with self._db.cursor() as cursor:
            rows = cursor.execute(
                """
                SELECT chat_id, user_id, username, display_name, tags, last_seen_at
                FROM participants
                WHERE chat_id = ?
                ORDER BY display_name COLLATE NOCASE ASC
                """,
                (chat_id,),
            ).fetchall()
        return [self._participant_from_row(row) for row in rows]

    def save_message(
        self,
        chat_id: int,
        telegram_message_id: int,
        user_id: int,
        username: str | None,
        display_name: str,
        text: str,
        created_at: datetime | None = None,
    ) -> int:
        created = (created_at or datetime.now(timezone.utc)).isoformat()
        with self._db.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO messages (
                    chat_id, telegram_message_id, user_id, username, display_name, text, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (chat_id, telegram_message_id, user_id, username, display_name, text, created),
            )
            return int(cursor.lastrowid)

    def get_messages_in_window(
        self, chat_id: int, window_start: datetime, window_end: datetime, limit: int
    ) -> list[Message]:
        with self._db.cursor() as cursor:
            rows = cursor.execute(
                """
                SELECT id, chat_id, telegram_message_id, user_id, username, display_name, text, created_at
                FROM messages
                WHERE chat_id = ?
                  AND created_at >= ?
                  AND created_at <= ?
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (chat_id, window_start.isoformat(), window_end.isoformat(), limit),
            ).fetchall()
        return [self._message_from_row(row) for row in reversed(rows)]

    def create_topics(
        self,
        chat_id: int,
        drafts: list[TopicDraft],
        window_start: datetime,
        window_end: datetime,
    ) -> list[Topic]:
        created = utcnow_iso()
        topics: list[Topic] = []
        with self._db.cursor() as cursor:
            for draft in drafts:
                unique_message_ids = sorted(set(draft.source_message_ids))
                cursor.execute(
                    """
                    INSERT INTO topics (
                        chat_id, title, summary, message_count, window_start, window_end, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        chat_id,
                        draft.title.strip(),
                        draft.summary.strip(),
                        len(unique_message_ids),
                        window_start.isoformat(),
                        window_end.isoformat(),
                        created,
                    ),
                )
                topic_id = int(cursor.lastrowid)
                for message_id in unique_message_ids:
                    cursor.execute(
                        """
                        INSERT OR IGNORE INTO topic_messages (topic_id, message_id)
                        VALUES (?, ?)
                        """,
                        (topic_id, message_id),
                    )
                topics.append(
                    Topic(
                        id=topic_id,
                        chat_id=chat_id,
                        title=draft.title.strip(),
                        summary=draft.summary.strip(),
                        message_count=len(unique_message_ids),
                        window_start=window_start.isoformat(),
                        window_end=window_end.isoformat(),
                        created_at=created,
                    )
                )
        return topics

    def get_topic(self, chat_id: int, topic_id: int) -> Topic | None:
        with self._db.cursor() as cursor:
            row = cursor.execute(
                """
                SELECT id, chat_id, title, summary, message_count, window_start, window_end, created_at
                FROM topics
                WHERE chat_id = ? AND id = ?
                """,
                (chat_id, topic_id),
            ).fetchone()
        return self._topic_from_row(row) if row else None

    def list_recent_topics(self, chat_id: int, limit: int = 10) -> list[Topic]:
        with self._db.cursor() as cursor:
            rows = cursor.execute(
                """
                SELECT id, chat_id, title, summary, message_count, window_start, window_end, created_at
                FROM topics
                WHERE chat_id = ?
                ORDER BY created_at DESC, id DESC
                LIMIT ?
                """,
                (chat_id, limit),
            ).fetchall()
        return [self._topic_from_row(row) for row in rows]

    def save_open_question_report(
        self,
        chat_id: int,
        questions: list[OpenQuestion],
        window_start: datetime,
        window_end: datetime,
    ) -> None:
        payload = [
            {
                "question": question.question,
                "asked_by": question.asked_by,
                "details": question.details,
                "source_message_ids": question.source_message_ids,
            }
            for question in questions
        ]
        with self._db.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO open_question_reports (
                    chat_id, questions_json, window_start, window_end, created_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    chat_id,
                    json.dumps(payload, ensure_ascii=False),
                    window_start.isoformat(),
                    window_end.isoformat(),
                    utcnow_iso(),
                ),
            )

    @staticmethod
    def _participant_from_row(row: object) -> Participant:
        return Participant(
            chat_id=row["chat_id"],
            user_id=row["user_id"],
            username=row["username"],
            display_name=row["display_name"],
            tags=row["tags"],
            last_seen_at=row["last_seen_at"],
        )

    @staticmethod
    def _message_from_row(row: object) -> Message:
        return Message(
            id=row["id"],
            chat_id=row["chat_id"],
            telegram_message_id=row["telegram_message_id"],
            user_id=row["user_id"],
            username=row["username"],
            display_name=row["display_name"],
            text=row["text"],
            created_at=row["created_at"],
        )

    @staticmethod
    def _topic_from_row(row: object) -> Topic:
        return Topic(
            id=row["id"],
            chat_id=row["chat_id"],
            title=row["title"],
            summary=row["summary"],
            message_count=row["message_count"],
            window_start=row["window_start"],
            window_end=row["window_end"],
            created_at=row["created_at"],
        )
