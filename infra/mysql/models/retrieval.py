"""一次聊天请求的检索输入、结果和内部 trace。"""

from __future__ import annotations

from typing import Any
from datetime import datetime

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, Index, Integer, JSON, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from infra.mysql.base import Base, IdMixin, TimestampMixin
from infra.mysql.status import RetrievalStatus


class RetrievalRun(IdMixin, TimestampMixin, Base):
    __tablename__ = "retrieval_runs"
    __table_args__ = (
        CheckConstraint("status IN ('pending', 'running', 'succeeded', 'failed')", name="status_valid"),
        Index("ix_retrieval_runs_status_lease", "status", "lease_expires_at"),
    )

    request_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("chat_requests.id"), unique=True, nullable=False
    )
    user_id: Mapped[str] = mapped_column(String(36), ForeignKey("users.id"), nullable=False)
    search_query: Mapped[str] = mapped_column(Text(), nullable=False)
    input_data: Mapped[dict[str, Any]] = mapped_column(JSON(), nullable=False)
    status: Mapped[str] = mapped_column(
        String(16), default=RetrievalStatus.PENDING.value, nullable=False
    )
    result_data: Mapped[dict[str, Any] | None] = mapped_column(JSON())
    retrieved_parent_ids: Mapped[list[str] | None] = mapped_column(JSON())
    source_version_ids: Mapped[list[str] | None] = mapped_column(JSON())
    trace_data: Mapped[dict[str, Any] | None] = mapped_column(JSON())
    last_error: Mapped[str | None] = mapped_column(Text())
    attempt: Mapped[int] = mapped_column(Integer(), default=0, nullable=False)
    lease_owner: Mapped[str | None] = mapped_column(String(36))
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime())
