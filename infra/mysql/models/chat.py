"""用户、会话、消息与聊天任务的持久化结构。"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from infra.mysql.base import Base, IdMixin, TimestampMixin, utc_now
from infra.mysql.status import TaskStatus


class User(IdMixin, TimestampMixin, Base):
    __tablename__ = "users"
    __table_args__ = (UniqueConstraint("email", name="uq_users_email"),)

    email: Mapped[str] = mapped_column(String(254), nullable=False)
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)


class Conversation(IdMixin, TimestampMixin, Base):
    __tablename__ = "conversations"
    __table_args__ = (Index("ix_conversations_user_id", "user_id"),)

    user_id: Mapped[str] = mapped_column(String(36), ForeignKey("users.id"), nullable=False)
    active_request_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("chat_requests.id", use_alter=True, name="fk_conversations_active_request_id_chat_requests")
    )
    next_message_sequence: Mapped[int] = mapped_column(Integer(), default=1, nullable=False)


class Message(IdMixin, TimestampMixin, Base):
    __tablename__ = "messages"
    __table_args__ = (
        UniqueConstraint("conversation_id", "sequence", name="uq_messages_conversation_sequence"),
        UniqueConstraint("request_id", "role", name="uq_messages_request_role"),
        CheckConstraint("role IN ('user', 'assistant')", name="role_valid"),
        Index("ix_messages_conversation_id", "conversation_id"),
    )

    conversation_id: Mapped[str] = mapped_column(String(36), ForeignKey("conversations.id"), nullable=False)
    # request_id 在创建 ChatRequest 前即可预先生成；此处不设外键以打破插入循环。
    request_id: Mapped[str] = mapped_column(String(36), nullable=False)
    sequence: Mapped[int] = mapped_column(Integer(), nullable=False)
    role: Mapped[str] = mapped_column(String(16), nullable=False)
    content: Mapped[str] = mapped_column(Text(), nullable=False)


class ChatRequest(IdMixin, TimestampMixin, Base):
    __tablename__ = "chat_requests"
    __table_args__ = (
        UniqueConstraint("user_id", "idempotency_key", name="uq_chat_requests_user_idempotency"),
        CheckConstraint(
            "status IN ('pending', 'running', 'retry_wait', 'succeeded', 'failed')",
            name="status_valid",
        ),
        Index("ix_chat_requests_conversation_status", "conversation_id", "status"),
        Index("ix_chat_requests_status_lease", "status", "lease_expires_at"),
    )

    user_id: Mapped[str] = mapped_column(String(36), ForeignKey("users.id"), nullable=False)
    conversation_id: Mapped[str] = mapped_column(String(36), ForeignKey("conversations.id"), nullable=False)
    user_message_id: Mapped[str] = mapped_column(String(36), ForeignKey("messages.id"), nullable=False)
    result_message_id: Mapped[str | None] = mapped_column(String(36), ForeignKey("messages.id"))
    history_until_sequence: Mapped[int] = mapped_column(Integer(), nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(128), nullable=False)
    request_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(16), default=TaskStatus.PENDING.value, nullable=False)
    attempt: Mapped[int] = mapped_column(Integer(), default=0, nullable=False)
    available_at: Mapped[datetime] = mapped_column(DateTime(), default=utc_now, nullable=False)
    lease_owner: Mapped[str | None] = mapped_column(String(128))
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime())
    last_error: Mapped[str | None] = mapped_column(Text())
