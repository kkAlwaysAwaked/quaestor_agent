"""真正的模型流式循环：解析工具分片，输出结构化事件，不编码 SSE。"""

from __future__ import annotations

import json

import openai
from pydantic import ValidationError

from core.chat_contracts import AgentError, AgentEvent
from core.config import AgentSettings
from services.agent.retrieval_client import RetrievalContext
from services.agent.tools import my_tools  # noqa: F401 — 仅注册工具 schema，不加载模型
from services.agent.tools.tool_registry import TOOL_REGISTRY


RAG_TOOL_NAME = "RAG"
SYSTEM_PROMPT = (
    "你是基于用户私有知识库回答问题的助手。需要文档事实时调用 RAG，"
    "每个请求最多一次成功检索，使用返回的完整参考资料并注明来源。"
    "没有依据时明确说明，不编造事实。只回答最后一次提问。"
    "工具参数与内部思考不要写入公开回答；content 只包含给用户看的文字。"
)


# 作用：只向模型暴露 RAG 工具，不允许工具名触发任意函数或模块。
def get_available_tools() -> list[dict]:
    return [TOOL_REGISTRY[RAG_TOOL_NAME]["schema"]]


# 作用：拼接模型分片；兼容部分服务重复发送完整工具 ID 或名称的情况。
def append_fragment(current: str, fragment: str | None) -> str:
    if not fragment or fragment == current:
        return current
    return current + fragment


class ToolCallBuffer:
    # 作用：初始化单轮工具分片缓存，按 index 隔离交错出现的多个调用。
    def __init__(self) -> None:
        self.calls = {}

    # 作用：累计工具 ID、名称和参数，在参数完整前不执行工具。
    def add(self, fragments: list) -> None:
        for fragment in fragments:
            index = fragment.index
            if type(index) is not int or not 0 <= index < 4:
                raise AgentError("model_tool_limit", "模型工具调用数量超过限制", retryable=False)
            call = self.calls.setdefault(index, {"id": "", "type": "function", "function": {"name": "", "arguments": ""}})
            call["id"] = append_fragment(call["id"], fragment.id)
            function = fragment.function
            if function is not None:
                call["function"]["name"] = append_fragment(call["function"]["name"], function.name)
                # 参数 JSON 是正文分片，重复字符串也必须保留，不能按元信息的方式去重。
                call["function"]["arguments"] += function.arguments or ""
            if len(call["id"]) > 128 or len(call["function"]["name"]) > 64 or len(call["function"]["arguments"]) > 8000:
                raise AgentError("model_tool_input_limit", "模型工具参数超过限制", retryable=False)

    # 作用：在模型明确结束工具生成后核对元信息，输出完整标准 tool_calls。
    def complete(self) -> list[dict]:
        calls = [self.calls[index] for index in sorted(self.calls)]
        ids = [call["id"] for call in calls]
        if any(not call["id"] or not call["function"]["name"] for call in calls) or len(set(ids)) != len(ids):
            raise AgentError("model_tool_incomplete", "模型工具调用元信息不完整")
        return calls


# 作用：生成可让模型修正参数的工具错误消息，不回显原始参数或执行内部异常。
def tool_input_error(call: dict) -> dict:
    return {
        "role": "tool", "tool_call_id": call["id"], "name": call["function"]["name"],
        "content": json.dumps({
            "status": "error", "code": "invalid_tool_input",
            "knowledge_content": "仅支持 RAG 工具及非空 query 参数，请修正调用。",
        }, ensure_ascii=False),
    }


# 作用：校验完整模型参数后注入可信上下文；检索暂时失败向 Worker 传播以安排持久化重试。
async def safe_execute_tool(call: dict, context: RetrievalContext) -> dict:
    name = call["function"]["name"]
    registered = TOOL_REGISTRY.get(name) if name == RAG_TOOL_NAME else None
    if registered is None:
        return tool_input_error(call)
    try:
        raw = json.loads(call["function"]["arguments"])
        validated = registered["input_model"].model_validate(raw)
    except (ValueError, ValidationError):
        return tool_input_error(call)
    content = await registered["execute"](
        **validated.model_dump(), agent_messages=context.messages, retrieval_context=context,
    )
    return {"role": "tool", "tool_call_id": call["id"], "name": name, "content": content}


# 作用：把 SDK 错误分成有界重试或配置性最终失败，避免把 API 原始错误发送到事件流。
def model_error(error: openai.OpenAIError) -> AgentError:
    if isinstance(error, openai.APIStatusError):
        retryable = error.status_code in (408, 409, 429) or error.status_code >= 500
        return AgentError("model_api_failed", "回答服务暂不可用", retryable=retryable)
    return AgentError("model_connection_failed", "回答服务连接中断")


# 作用：流式生成公开文本，收齐 tool_calls 后调用工具，成功资料以完整 tool message 回给模型。
async def run_agent_async(
    messages: list[dict], client, *, retrieval_context: RetrievalContext, settings: AgentSettings,
):
    current_messages = [{"role": "system", "content": SYSTEM_PROMPT}, *[dict(message) for message in messages]]
    for step in range(1, settings.max_steps + 1):
        yield AgentEvent("status", {"phase": "generating", "step": step, "message": "正在准备回答"})
        restoring = retrieval_context.restore_required
        request = {
            "model": settings.model_name, "messages": list(current_messages), "stream": True,
            "max_tokens": settings.model_max_tokens,
            "extra_body": {"thinking": {"type": settings.thinking_mode}},
        }
        if retrieval_context.result is None:
            request["tools"] = get_available_tools()
            request["tool_choice"] = {"type": "function", "function": {"name": RAG_TOOL_NAME}} if restoring else "auto"
        buffer, parts, reasoning = ToolCallBuffer(), [], []
        finish_reason, reasoning_size = None, 0
        stream = None
        try:
            stream = await client.chat.completions.create(**request)
            async for chunk in stream:
                if not chunk.choices:
                    continue
                choice = chunk.choices[0]
                delta = choice.delta
                hidden = getattr(delta, "reasoning_content", None)
                if isinstance(hidden, str) and hidden:
                    reasoning.append(hidden)
                    reasoning_size += len(hidden)
                    if reasoning_size > 60000:
                        raise AgentError("model_reasoning_limit", "模型生成超过限制", retryable=False)
                if delta.tool_calls:
                    buffer.add(delta.tool_calls)
                if delta.content:
                    parts.append(delta.content)
                    yield AgentEvent("token", {"token": delta.content})
                if getattr(delta, "refusal", None):
                    raise AgentError("model_refused", "回答服务未能完成请求", retryable=False)
                if choice.finish_reason is not None:
                    finish_reason = choice.finish_reason
        except openai.OpenAIError as exc:
            raise model_error(exc) from exc
        finally:
            if stream is not None:
                await stream.close()
        if finish_reason is None:
            raise AgentError("model_stream_incomplete", "回答流提前中断")
        calls = buffer.complete()
        if calls:
            if finish_reason != "tool_calls":
                raise AgentError("model_tool_incomplete", "工具参数生成未正常结束")
            assistant = {"role": "assistant", "content": "".join(parts), "tool_calls": calls}
            if reasoning:
                # DeepSeek 思考模式的工具协议需要回传，但不写 token、MySQL 正文或 trace。
                assistant["reasoning_content"] = "".join(reasoning)
            current_messages.append(assistant)
            yield AgentEvent("status", {"phase": "retrieving", "message": "正在查找参考资料"})
            for call in calls:
                result = await safe_execute_tool(call, retrieval_context)
                current_messages.append(result)
            yield AgentEvent("status", {"phase": "retrieved", "message": "参考资料已处理"})
            continue
        if finish_reason != "stop":
            raise AgentError("model_generation_incomplete", "回答生成未正常结束", retryable=finish_reason != "content_filter")
        if restoring:
            raise AgentError("retrieval_restore_incomplete", "模型未恢复已有参考资料")
        if not "".join(parts).strip():
            raise AgentError("model_empty_answer", "回答服务返回空文本")
        return
    raise AgentError("agent_step_limit", "工具调用超过迭代上限", retryable=False)
