"""创建聊天、文档、检索和 Outbox 的第一版业务表。"""

from alembic import op
import sqlalchemy as sa


revision = "0001_initial_business_schema"
down_revision = None
branch_labels = None
depends_on = None


# 作用：创建全部业务表、唯一约束、状态检查和查询索引。
def upgrade() -> None:
    op.create_table(
        "users",
        sa.Column("id", sa.String(36), nullable=False),
        sa.Column("email", sa.String(254), nullable=False),
        sa.Column("password_hash", sa.String(255), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_users"),
        sa.UniqueConstraint("email", name="uq_users_email"),
    )
    op.create_table(
        "conversations",
        sa.Column("id", sa.String(36), nullable=False),
        sa.Column("user_id", sa.String(36), nullable=False),
        sa.Column("active_request_id", sa.String(36)),
        sa.Column("next_message_sequence", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_conversations"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], name="fk_conversations_user_id_users"),
    )
    op.create_index("ix_conversations_user_id", "conversations", ["user_id"])
    op.create_table(
        "messages",
        sa.Column("id", sa.String(36), nullable=False),
        sa.Column("conversation_id", sa.String(36), nullable=False),
        sa.Column("request_id", sa.String(36), nullable=False),
        sa.Column("sequence", sa.Integer(), nullable=False),
        sa.Column("role", sa.String(16), nullable=False),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_messages"),
        sa.ForeignKeyConstraint(["conversation_id"], ["conversations.id"], name="fk_messages_conversation_id_conversations"),
        sa.UniqueConstraint("conversation_id", "sequence", name="uq_messages_conversation_sequence"),
        sa.UniqueConstraint("request_id", "role", name="uq_messages_request_role"),
        sa.CheckConstraint("role IN ('user', 'assistant')", name="ck_messages_role_valid"),
    )
    op.create_index("ix_messages_conversation_id", "messages", ["conversation_id"])
    op.create_table(
        "chat_requests",
        sa.Column("id", sa.String(36), nullable=False),
        sa.Column("user_id", sa.String(36), nullable=False),
        sa.Column("conversation_id", sa.String(36), nullable=False),
        sa.Column("user_message_id", sa.String(36), nullable=False),
        sa.Column("result_message_id", sa.String(36)),
        sa.Column("history_until_sequence", sa.Integer(), nullable=False),
        sa.Column("idempotency_key", sa.String(128), nullable=False),
        sa.Column("request_fingerprint", sa.String(64), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("attempt", sa.Integer(), nullable=False),
        sa.Column("available_at", sa.DateTime(), nullable=False),
        sa.Column("lease_owner", sa.String(128)),
        sa.Column("lease_expires_at", sa.DateTime()),
        sa.Column("last_error", sa.Text()),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_chat_requests"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], name="fk_chat_requests_user_id_users"),
        sa.ForeignKeyConstraint(["conversation_id"], ["conversations.id"], name="fk_chat_requests_conversation_id_conversations"),
        sa.ForeignKeyConstraint(["user_message_id"], ["messages.id"], name="fk_chat_requests_user_message_id_messages"),
        sa.ForeignKeyConstraint(["result_message_id"], ["messages.id"], name="fk_chat_requests_result_message_id_messages"),
        sa.UniqueConstraint("user_id", "idempotency_key", name="uq_chat_requests_user_idempotency"),
        sa.CheckConstraint(
            "status IN ('pending', 'running', 'retry_wait', 'succeeded', 'failed')",
            name="ck_chat_requests_status_valid",
        ),
    )
    op.create_index("ix_chat_requests_conversation_status", "chat_requests", ["conversation_id", "status"])
    op.create_index("ix_chat_requests_status_lease", "chat_requests", ["status", "lease_expires_at"])
    op.create_foreign_key(
        "fk_conversations_active_request_id_chat_requests",
        "conversations", "chat_requests", ["active_request_id"], ["id"],
    )

    op.create_table(
        "documents",
        sa.Column("id", sa.String(36), nullable=False),
        sa.Column("user_id", sa.String(36), nullable=False),
        sa.Column("original_filename", sa.String(255), nullable=False),
        sa.Column("storage_key", sa.String(512), nullable=False),
        sa.Column("current_version_id", sa.String(36)),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_documents"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], name="fk_documents_user_id_users"),
    )
    op.create_index("ix_documents_user_id", "documents", ["user_id"])
    op.create_table(
        "document_versions",
        sa.Column("id", sa.String(36), nullable=False),
        sa.Column("document_id", sa.String(36), nullable=False),
        sa.Column("file_sha256", sa.String(64), nullable=False),
        sa.Column("processing_config", sa.JSON(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("published_at", sa.DateTime()),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_document_versions"),
        sa.ForeignKeyConstraint(["document_id"], ["documents.id"], name="fk_document_versions_document_id_documents"),
        sa.CheckConstraint("status IN ('processing', 'published', 'failed')", name="ck_document_versions_status_valid"),
    )
    op.create_index("ix_document_versions_document_status", "document_versions", ["document_id", "status"])
    op.create_foreign_key(
        "fk_documents_current_version_id_document_versions",
        "documents", "document_versions", ["current_version_id"], ["id"],
    )
    op.create_table(
        "parent_chunks",
        sa.Column("id", sa.String(36), nullable=False),
        sa.Column("document_id", sa.String(36), nullable=False),
        sa.Column("version_id", sa.String(36), nullable=False),
        sa.Column("chunk_index", sa.Integer(), nullable=False),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("source", sa.String(512)),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_parent_chunks"),
        sa.ForeignKeyConstraint(["document_id"], ["documents.id"], name="fk_parent_chunks_document_id_documents"),
        sa.ForeignKeyConstraint(["version_id"], ["document_versions.id"], name="fk_parent_chunks_version_id_document_versions"),
        sa.UniqueConstraint("version_id", "chunk_index", name="uq_parent_chunks_version_index"),
    )
    op.create_index("ix_parent_chunks_document_version", "parent_chunks", ["document_id", "version_id"])
    op.create_table(
        "ingest_jobs",
        sa.Column("id", sa.String(36), nullable=False),
        sa.Column("user_id", sa.String(36), nullable=False),
        sa.Column("document_id", sa.String(36), nullable=False),
        sa.Column("version_id", sa.String(36), nullable=False),
        sa.Column("idempotency_key", sa.String(128), nullable=False),
        sa.Column("request_fingerprint", sa.String(64), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("attempt", sa.Integer(), nullable=False),
        sa.Column("available_at", sa.DateTime(), nullable=False),
        sa.Column("lease_owner", sa.String(128)),
        sa.Column("lease_expires_at", sa.DateTime()),
        sa.Column("last_error", sa.Text()),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_ingest_jobs"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], name="fk_ingest_jobs_user_id_users"),
        sa.ForeignKeyConstraint(["document_id"], ["documents.id"], name="fk_ingest_jobs_document_id_documents"),
        sa.ForeignKeyConstraint(["version_id"], ["document_versions.id"], name="fk_ingest_jobs_version_id_document_versions"),
        sa.UniqueConstraint("user_id", "idempotency_key", name="uq_ingest_jobs_user_idempotency"),
        sa.UniqueConstraint("version_id", name="uq_ingest_jobs_version_id"),
        sa.CheckConstraint(
            "status IN ('pending', 'running', 'retry_wait', 'succeeded', 'failed')",
            name="ck_ingest_jobs_status_valid",
        ),
    )
    op.create_index("ix_ingest_jobs_status_lease", "ingest_jobs", ["status", "lease_expires_at"])
    op.create_index("ix_ingest_jobs_document_id", "ingest_jobs", ["document_id"])

    op.create_table(
        "retrieval_runs",
        sa.Column("id", sa.String(36), nullable=False),
        sa.Column("request_id", sa.String(36), nullable=False),
        sa.Column("user_id", sa.String(36), nullable=False),
        sa.Column("search_query", sa.Text(), nullable=False),
        sa.Column("input_data", sa.JSON(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("result_data", sa.JSON()),
        sa.Column("retrieved_parent_ids", sa.JSON()),
        sa.Column("source_version_ids", sa.JSON()),
        sa.Column("trace_data", sa.JSON()),
        sa.Column("last_error", sa.Text()),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_retrieval_runs"),
        sa.ForeignKeyConstraint(["request_id"], ["chat_requests.id"], name="fk_retrieval_runs_request_id_chat_requests"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], name="fk_retrieval_runs_user_id_users"),
        sa.UniqueConstraint("request_id", name="uq_retrieval_runs_request_id"),
        sa.CheckConstraint("status IN ('pending', 'running', 'succeeded', 'failed')", name="ck_retrieval_runs_status_valid"),
    )
    op.create_table(
        "outbox_events",
        sa.Column("id", sa.String(36), nullable=False),
        sa.Column("event_type", sa.String(64), nullable=False),
        sa.Column("destination", sa.String(16), nullable=False),
        sa.Column("routing_key", sa.String(128)),
        sa.Column("aggregate_type", sa.String(32), nullable=False),
        sa.Column("aggregate_id", sa.String(36), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("available_at", sa.DateTime(), nullable=False),
        sa.Column("attempt_count", sa.Integer(), nullable=False),
        sa.Column("lease_owner", sa.String(128)),
        sa.Column("lease_expires_at", sa.DateTime()),
        sa.Column("last_error", sa.Text()),
        sa.Column("sent_at", sa.DateTime()),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_outbox_events"),
        sa.CheckConstraint("status IN ('pending', 'publishing', 'sent')", name="ck_outbox_events_status_valid"),
        sa.CheckConstraint("destination IN ('rabbitmq', 'redis_stream')", name="ck_outbox_events_destination_valid"),
    )
    op.create_index("ix_outbox_events_due", "outbox_events", ["status", "available_at", "lease_expires_at"])
    op.create_index("ix_outbox_events_aggregate", "outbox_events", ["aggregate_type", "aggregate_id"])


# 作用：按外键依赖的逆序撤销第一版业务表。
def downgrade() -> None:
    op.drop_table("outbox_events")
    op.drop_table("retrieval_runs")
    op.drop_table("ingest_jobs")
    op.drop_table("parent_chunks")
    op.drop_constraint("fk_documents_current_version_id_document_versions", "documents", type_="foreignkey")
    op.drop_table("document_versions")
    op.drop_table("documents")
    op.drop_constraint("fk_conversations_active_request_id_chat_requests", "conversations", type_="foreignkey")
    op.drop_table("chat_requests")
    op.drop_table("messages")
    op.drop_table("conversations")
    op.drop_table("users")
