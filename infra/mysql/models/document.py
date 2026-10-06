"""文档版本、父块正文与入库任务的持久化结构。"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, Index, Integer, JSON, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from infra.mysql.base import Base, IdMixin, TimestampMixin, utc_now
from infra.mysql.status import DocumentVersionStatus, TaskStatus


class Document(IdMixin, TimestampMixin, Base):
    __tablename__ = "documents"
    __table_args__ = (Index("ix_documents_user_id", "user_id"),)

    user_id: Mapped[str] = mapped_column(String(36), ForeignKey("users.id"), nullable=False)
    original_filename: Mapped[str] = mapped_column(String(255), nullable=False)
    storage_key: Mapped[str] = mapped_column(String(512), nullable=False)
    current_version_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("document_versions.id", use_alter=True, name="fk_documents_current_version_id_document_versions")
    )


class DocumentVersion(IdMixin, TimestampMixin, Base):
    __tablename__ = "document_versions"
    __table_args__ = (
        CheckConstraint("status IN ('processing', 'published', 'failed')", name="status_valid"),
        Index("ix_document_versions_document_status", "document_id", "status"),
    )

    document_id: Mapped[str] = mapped_column(String(36), ForeignKey("documents.id"), nullable=False)
    file_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    storage_key: Mapped[str] = mapped_column(String(512), nullable=False)
    processing_config: Mapped[dict[str, Any]] = mapped_column(JSON(), nullable=False)
    status: Mapped[str] = mapped_column(
        String(16), default=DocumentVersionStatus.PROCESSING.value, nullable=False
    )
    published_at: Mapped[datetime | None] = mapped_column(DateTime())


class ParentChunk(IdMixin, TimestampMixin, Base):
    __tablename__ = "parent_chunks"
    __table_args__ = (
        UniqueConstraint("version_id", "chunk_index", name="uq_parent_chunks_version_index"),
        Index("ix_parent_chunks_document_version", "document_id", "version_id"),
    )

    document_id: Mapped[str] = mapped_column(String(36), ForeignKey("documents.id"), nullable=False)
    version_id: Mapped[str] = mapped_column(String(36), ForeignKey("document_versions.id"), nullable=False)
    chunk_index: Mapped[int] = mapped_column(Integer(), nullable=False)
    content: Mapped[str] = mapped_column(Text(), nullable=False)
    source: Mapped[str | None] = mapped_column(String(512))


class IngestJob(IdMixin, TimestampMixin, Base):
    __tablename__ = "ingest_jobs"
    __table_args__ = (
        UniqueConstraint("user_id", "idempotency_key", name="uq_ingest_jobs_user_idempotency"),
        UniqueConstraint("version_id", name="uq_ingest_jobs_version_id"),
        CheckConstraint(
            "status IN ('pending', 'running', 'retry_wait', 'succeeded', 'failed')",
            name="status_valid",
        ),
        Index("ix_ingest_jobs_status_lease", "status", "lease_expires_at"),
        Index("ix_ingest_jobs_document_id", "document_id"),
    )

    user_id: Mapped[str] = mapped_column(String(36), ForeignKey("users.id"), nullable=False)
    document_id: Mapped[str] = mapped_column(String(36), ForeignKey("documents.id"), nullable=False)
    version_id: Mapped[str] = mapped_column(String(36), ForeignKey("document_versions.id"), nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(128), nullable=False)
    request_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(16), default=TaskStatus.PENDING.value, nullable=False)
    attempt: Mapped[int] = mapped_column(Integer(), default=0, nullable=False)
    available_at: Mapped[datetime] = mapped_column(DateTime(), default=utc_now, nullable=False)
    lease_owner: Mapped[str | None] = mapped_column(String(128))
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime())
    last_error: Mapped[str | None] = mapped_column(Text())
