"""Agent 工具只通过 HTTP 获取参考资料，不导入检索算法或模型。"""

from __future__ import annotations

from typing import Annotated

from pydantic import StringConstraints

from core.chat_contracts import AgentError
from services.agent.retrieval_client import RetrievalContext
from services.agent.tools.tool_registry import register_tool


# 作用：使用 Worker 注入的任务身份查询资料，同一请求的重复调用复用完整成功结果。
@register_tool
async def RAG(
    query: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=2000)],
    agent_messages: list | None = None,
    retrieval_context: RetrievalContext | None = None,
) -> str:
    """
    【知识库检索工具】
    需要回答内部文档、业务规定、项目细节等事实性问题时调用。
    每个请求最多一次成功检索，返回完整参考资料与来源；下一次提问可重新检索。

    Args:
        query (str): 从用户提问与上下文提取独立完整的搜索关键词，消除指代不明。
    """
    if retrieval_context is None:
        raise AgentError("missing_tool_identity", "RAG 必须由已领取任务的 Worker 调用", retryable=False)
    return await retrieval_context.search(query)
