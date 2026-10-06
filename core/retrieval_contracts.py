"""检索接口和内部授权范围；只定义数据契约，不加载模型或连接中间件。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, model_validator


class DialogMessage(BaseModel):
    model_config = ConfigDict(extra="forbid")
    role: Literal["user", "assistant"]
    content: str = Field(min_length=1, max_length=12000)


class RetrieveRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    request_id: UUID
    messages: list[DialogMessage] = Field(min_length=1, max_length=100)
    search_query: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=2000)]

    # 作用：限制上下文总量并要求最后一条消息是本次用户提问。
    @model_validator(mode="after")
    def validate_context(self) -> RetrieveRequest:
        if self.messages[-1].role != "user":
            raise ValueError("检索上下文必须以本次 user 消息结尾")
        if sum(len(message.content) for message in self.messages) > 60000:
            raise ValueError("检索上下文总长度不能超过 60000 字符")
        return self


class RetrievedDocument(BaseModel):
    parent_id: str
    document_id: str
    version_id: str
    content: str
    source: str
    rrf_score: float
    rerank_score: float


class RetrieveResponse(BaseModel):
    request_id: str
    status: Literal["success", "empty"]
    doc_count: int
    retrieved_parent_ids: list[str]
    knowledge_content: str
    documents: list[RetrievedDocument]


@dataclass(frozen=True)
class RetrievalScope:
    user_id: str
    version_ids: tuple[str, ...]


class RetrievalError(Exception):
    # 作用：用稳定错误码向接口报告可预期失败，避免暴露底层连接或密钥信息。
    def __init__(self, status_code: int, code: str, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code


# 作用：把父块结果编码为兼容原 RAG 工具的 JSON 响应，并补充来源版本信息。
def build_retrieve_response(request_id: str, documents: list[dict]) -> RetrieveResponse:
    parsed = [RetrievedDocument.model_validate(document) for document in documents]
    content = "知识库中未检索到相关内容，请尝试更换搜索词或如实告诉用户。"
    if parsed:
        parts = ["以下是为你检索到的参考资料："]
        for index, document in enumerate(parsed, start=1):
            parts.append(f"【参考资料 {index}】(来源: {document.source})\n{document.content}\n---")
        content = "\n".join(parts)
    return RetrieveResponse(
        request_id=request_id, status="success" if parsed else "empty",
        doc_count=len(parsed), retrieved_parent_ids=[document.parent_id for document in parsed],
        knowledge_content=content, documents=parsed,
    )
