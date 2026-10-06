"""跨组件发布前持久化的待发送事件。"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import CheckConstraint, DateTime, Index, Integer, JSON, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from infra.mysql.base import Base, IdMixin, TimestampMixin, utc_now
from infra.mysql.status import OutboxStatus


class OutboxEvent(IdMixin, TimestampMixin, Base):
    __tablename__ = "outbox_events"
    __table_args__ = (
        CheckConstraint("status IN ('pending', 'publishing', 'sent')", name="status_valid"),
        CheckConstraint("destination IN ('rabbitmq', 'redis_stream')", name="destination_valid"),
        Index("ix_outbox_events_due", "status", "available_at", "lease_expires_at"),
        Index("ix_outbox_events_aggregate", "aggregate_type", "aggregate_id"),
    )

    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    destination: Mapped[str] = mapped_column(String(16), nullable=False)
    routing_key: Mapped[str | None] = mapped_column(String(128))
    aggregate_type: Mapped[str] = mapped_column(String(32), nullable=False)
    aggregate_id: Mapped[str] = mapped_column(String(36), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON(), nullable=False)
    status: Mapped[str] = mapped_column(String(16), default=OutboxStatus.PENDING.value, nullable=False)
    available_at: Mapped[datetime] = mapped_column(DateTime(), default=utc_now, nullable=False)
    attempt_count: Mapped[int] = mapped_column(Integer(), default=0, nullable=False)
    lease_owner: Mapped[str | None] = mapped_column(String(128))
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime())
    last_error: Mapped[str | None] = mapped_column(Text())
    sent_at: Mapped[datetime | None] = mapped_column(DateTime())
