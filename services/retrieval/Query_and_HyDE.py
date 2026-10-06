"""保留关键词改写和 HyDE 流程，复用启动时创建的异步 HTTP 客户端。"""

from __future__ import annotations

import asyncio

import httpx

from core.config import ModelSettings


REWRITE_SYSTEM_PROMPT = """你是稀疏检索关键词提取器。
根据对话历史和当前检索意图，输出用于 SPLADE 稀疏检索的关键词。
只输出 5-12 个关键词或短语，用空格分隔；保留专有名词、编号、日期、人名。
不要回答问题，不要输出对话开场白。"""
HYDE_MAX_CHARS = 120
HYDE_SYSTEM_PROMPT = f"""你是企业内部知识库文档撰写助手。
根据用户问题写一段可能出现在内部周报、SOP、复盘报告中的客观陈述，用于语义检索。
只写一段连续正文，不要标题、列表、开场白。不要编造具体电话号码、金额、人名、日期。
只描述可能涉及的字段名或业务主题，严格控制在 {HYDE_MAX_CHARS} 字以内。"""


# 作用：只保留自然语言 user/assistant 轮次，排除工具和系统消息。
def extract_dialog_messages(messages: list) -> list[dict[str, str]]:
    dialog = []
    for message in messages:
        role = message.get("role")
        content = message.get("content") or ""
        if role in ("user", "assistant") and isinstance(content, str) and content.strip():
            dialog.append({"role": role, "content": content.strip()})
    return dialog


# 作用：提取当前检索意图、历史轮次和最后一个完整用户问题。
def resolve_query_inputs(messages: list, search_query: str | None = None) -> tuple[list, str, str]:
    dialog = extract_dialog_messages(messages)
    last_user_index = next((index for index in range(len(dialog) - 1, -1, -1) if dialog[index]["role"] == "user"), None)
    last_user = dialog[last_user_index]["content"] if last_user_index is not None else ""
    latest = search_query.strip() if search_query and search_query.strip() else last_user
    history = dialog if search_query else dialog[:last_user_index] if last_user_index is not None else dialog
    return history, latest, last_user or latest


# 作用：压缩 HyDE 文本并在最大长度内优先保留完整句尾。
def truncate_hyde_document(text: str, max_chars: int = HYDE_MAX_CHARS) -> str:
    text = " ".join(text.split())
    if len(text) <= max_chars:
        return text
    clipped = text[:max_chars]
    for separator in ("。", "；", "，", ".", ";", ","):
        position = clipped.rfind(separator)
        if position >= max_chars // 2:
            return clipped[:position + 1].strip()
    return clipped.strip()


class QueryTransformer:
    # 作用：接收服务生命周期内复用的 HTTP 客户端和显式查询模式。
    def __init__(self, client: httpx.AsyncClient, settings: ModelSettings | None, *, mode: str = "llm") -> None:
        self.client = client
        self.settings = settings
        self.mode = mode

    # 作用：调用生成模型；网络或格式失败返回空值，由上层按原算法降级。
    async def _generate(self, system: str, content: str, *, hyde: bool = False) -> str:
        if self.settings is None:
            raise RuntimeError("LLM 查询模式缺少生成模型配置")
        payload = {
            "model": "deepseek-chat", "temperature": 0.3 if hyde else 0.0,
            "messages": [{"role": "system", "content": system}, {"role": "user", "content": content}],
        }
        if hyde:
            payload["max_tokens"] = 160
        try:
            response = await self.client.post(
                f"{self.settings.base_url.rstrip('/')}/chat/completions",
                headers={"Authorization": f"Bearer {self.settings.api_key}"},
                json=payload, timeout=15 if hyde else 10,
            )
            response.raise_for_status()
            result = response.json()["choices"][0]["message"]["content"]
            return result.strip() if isinstance(result, str) else ""
        except (httpx.HTTPError, ValueError, KeyError, IndexError, TypeError):
            return ""

    # 作用：生成关键词、补充 Sparse 问题与 HyDE；固定模式隔离 LLM 改写波动。
    async def transform(self, messages: list, search_query: str) -> dict:
        history, latest, question = resolve_query_inputs(messages, search_query)
        rewritten = latest
        hyde = ""
        if self.mode == "llm":
            async with asyncio.TaskGroup() as tasks:
                hyde_task = tasks.create_task(self._generate(HYDE_SYSTEM_PROMPT, question, hyde=True))
                rewrite_task = None
                if len(search_query.strip()) < 6:
                    history_text = "\n".join(f"{message['role']}: {message['content']}" for message in history) or "（无）"
                    rewrite_task = tasks.create_task(self._generate(
                        REWRITE_SYSTEM_PROMPT, f"历史对话：\n{history_text}\n\n当前检索意图：{latest}"
                    ))
            hyde = hyde_task.result()
            if rewrite_task is not None:
                rewritten = rewrite_task.result() or latest
        return {
            "rewritten_query": rewritten,
            "sparse_fallback_query": question if question.strip() != rewritten.strip() else "",
            "hyde_document": truncate_hyde_document(hyde or question),
            "standalone_question": question,
            "query_mode": self.mode,
            "hyde_fallback": self.mode == "llm" and not bool(hyde),
        }
