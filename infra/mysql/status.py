"""数据库状态值。应用层的状态转换必须遵守这些持久化值。"""

from enum import StrEnum

# 接收一条聊天请求时，系统在同一个 MySQL 事务里写入任务记录和一条 Outbox 事件；
# 事务提交后，独立的发布程序再读取这条事件，把任务通知发给 RabbitMQ。
# 任务不会因发送失败而消失，因为 Outbox 事件还在，发布程序可以稍后重试。
# 任务状态和 Outbox 状态要分别保存，是因为它们回答不同的问题：
# 任务状态：回答 “这项工作执行得怎样？”
# Outbox 状态：回答 “这条通知发送得怎样？”

# 因为这些值会持久化到 MySQL 表中，并与表的状态约束对应。

# 任务状态
class TaskStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    RETRY_WAIT = "retry_wait"
    SUCCEEDED = "succeeded"
    FAILED = "failed"

# 一份文档版本的状态：正在切分，写入父块和向量；完整写入并通过校验；版本处理失败
class DocumentVersionStatus(StrEnum):
    PROCESSING = "processing"
    PUBLISHED = "published"
    FAILED = "failed"

# 一次检索任务的状态：记录已创建但是尚未开始；正在检索；
# 结果已保存，同一 request_id 可复用；检索未成功
class RetrievalStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"

# Outbox 状态
class OutboxStatus(StrEnum):
    PENDING = "pending"
    PUBLISHING = "publishing"
    SENT = "sent"


class OutboxDestination(StrEnum):
    RABBITMQ = "rabbitmq"
    REDIS_STREAM = "redis_stream"
