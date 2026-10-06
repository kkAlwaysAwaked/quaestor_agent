"""RAG 工具的 HTTP 适配；只依赖共享契约，不导入检索算法或本地模型。"""

from __future__ import annotations

import asyncio
import json

import httpx
from pydantic import ValidationError

from core.chat_contracts import AgentError
from core.config import AgentSettings, ServiceTokenSettings
from core.retrieval_contracts import RetrieveRequest, RetrieveResponse
from core.service_auth import issue_retrieval_token


class RetrievalClient:
    # 作用：复用服务启动时创建的异步 HTTP 客户端及专用内部凭据。
    def __init__(self, http: httpx.AsyncClient, *, settings: AgentSettings, credentials: ServiceTokenSettings) -> None:
        self.http = http
        self.settings = settings
        self.credentials = credentials

    # 作用：为当前任务签发短期凭据调用检索，区分暂时故障和不可恢复的契约冲突。
    async def retrieve(self, user_id: str, payload: RetrieveRequest) -> RetrieveResponse:
        token = issue_retrieval_token(self.credentials, user_id=user_id, request_id=str(payload.request_id))
        try:
            async with asyncio.timeout(self.settings.retrieval_timeout_seconds):
                response = await self.http.post(
                    f"{self.settings.retrieval_url}/v1/retrieve", json=payload.model_dump(mode="json"),
                    headers={"Authorization": f"Bearer {token}"}, timeout=self.settings.retrieval_timeout_seconds,
                )
        except (httpx.HTTPError, TimeoutError) as exc:
            raise AgentError("retrieval_unavailable", "参考资料暂不可用") from exc
        if response.status_code != 200:
            try:
                detail = response.json().get("detail", {})
                code = detail.get("code", "retrieval_failed") if isinstance(detail, dict) else "retrieval_failed"
            except (ValueError, AttributeError):
                code = "retrieval_failed"
            retryable = response.status_code in (408, 429, 500, 502, 503, 504) or code in (
                "retrieval_in_progress", "retrieval_lease_lost", "chat_task_not_running",
            )
            # 只透传稳定、短小的错误码，不透传远端错误正文或任意 response 内容。
            if not isinstance(code, str) or not code.replace("_", "").isalnum() or len(code) > 64:
                code = "retrieval_failed"
            raise AgentError(code, "参考资料调用未完成", retryable=retryable)
        try:
            result = RetrieveResponse.model_validate(response.json())
        except (ValueError, ValidationError) as exc:
            raise AgentError("invalid_retrieval_response", "参考资料响应不完整") from exc
        if result.request_id != str(payload.request_id):
            raise AgentError("retrieval_request_mismatch", "参考资料请求标识不一致", retryable=False)
        return result


class RetrievalContext:
    # 作用：为单个 request_id 保存不可变检索输入和完整成功资料，下一次提问创建新上下文。
    def __init__(self, client: RetrievalClient, *, request_id: str, user_id: str, messages: list[dict], saved_input: RetrieveRequest | None = None, previously_succeeded: bool = False) -> None:
        self.client = client
        self.request_id = request_id
        self.user_id = user_id
        self.messages = messages
        self.saved_input = saved_input
        self.previously_succeeded = previously_succeeded
        self.result = None

    # 作用：判断模型是否需要产生真实工具调用以恢复此前成功检索的完整 tool message。
    @property
    def restore_required(self) -> bool:
        return self.previously_succeeded and self.result is None

    # 作用：执行一次成功逻辑检索，失败重试复用首次保存输入，重复工具调用返还同一完整资料。
    async def search(self, query: str) -> str:
        if self.result is None:
            if self.saved_input is None:
                try:
                    self.saved_input = RetrieveRequest(request_id=self.request_id, messages=self.messages, search_query=query)
                except ValueError as exc:
                    raise AgentError("invalid_retrieval_input", "检索输入不符合契约", retryable=False) from exc
            self.result = await self.client.retrieve(self.user_id, self.saved_input)
        return json.dumps(self.result.model_dump(mode="json"), ensure_ascii=False)
