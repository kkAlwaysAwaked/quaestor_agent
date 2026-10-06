"""聊天队列通知、实时事件和 Agent 可预期错误；导入时不初始化服务。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, field_validator


class ChatJob(BaseModel):
    model_config = ConfigDict(extra="forbid")
    schema_version: Literal[1]
    request_id: UUID
    conversation_id: UUID
    user_id: UUID

    # 作用：拒绝布尔值等隐式转换，只接受明确的队列契约版本整数。
    @field_validator("schema_version", mode="before")
    @classmethod
    def validate_schema_version(cls, value):
        if type(value) is not int or value != 1:
            raise ValueError("不支持的 ChatJob schema_version")
        return value


class ChatStreamEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")
    event: Literal["status", "token", "done", "error"]
    data: dict[str, Any]

    # 作用：核对每条回流事件都有合法请求标识和正整数执行次数。
    @field_validator("data")
    @classmethod
    def validate_identity(cls, value):
        try:
            UUID(value.get("request_id", ""))
        except (ValueError, TypeError, AttributeError) as exc:
            raise ValueError("Stream 事件必须包含合法 request_id") from exc
        if type(value.get("attempt")) is not int or value["attempt"] < 1:
            raise ValueError("Stream 事件必须包含正整数 attempt")
        return value


@dataclass(frozen=True)
class AgentEvent:
    event: Literal["status", "token"]
    data: dict[str, Any]


class AgentError(Exception):
    # 作用：向任务编排报告稳定错误码与是否可重试，不把底层异常正文传给浏览器。
    def __init__(self, code: str, message: str, *, retryable: bool = True) -> None:
        super().__init__(message)
        self.code = code
        self.retryable = retryable


class LeaseLost(AgentError):
    # 作用：标记旧执行者已经失去任务写入权，交由现有持有者或恢复循环继续处理。
    def __init__(self) -> None:
        super().__init__("task_lease_lost", "任务执行权已变化", retryable=True)


# 作用：生成与既定 SSE 契约一致的 Redis Stream 键。
def chat_stream_key(request_id: str) -> str:
    return f"chat:{UUID(request_id)}"
