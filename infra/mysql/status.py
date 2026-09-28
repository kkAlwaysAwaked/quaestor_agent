"""数据库状态值。应用层的状态转换必须遵守这些持久化值。"""

from enum import StrEnum


class TaskStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    RETRY_WAIT = "retry_wait"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class DocumentVersionStatus(StrEnum):
    PROCESSING = "processing"
    PUBLISHED = "published"
    FAILED = "failed"


class RetrievalStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class OutboxStatus(StrEnum):
    PENDING = "pending"
    PUBLISHING = "publishing"
    SENT = "sent"


class OutboxDestination(StrEnum):
    RABBITMQ = "rabbitmq"
    REDIS_STREAM = "redis_stream"
