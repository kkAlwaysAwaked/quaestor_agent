import model_hub_setup  # noqa: F401 — 配置模型下载源

import json
import asyncio
import inspect
import sys

import httpx
from openai import AsyncOpenAI

from Tools_Registry.tool_registry import TOOL_REGISTRY
from Tools_Registry import my_tools  # noqa: F401 — 注册 RAG 工具
from config import DEEPSEEK_API_KEY, DEEPSEEK_BASE_URL

MODEL_NAME = "deepseek-v4-flash"
RAG_TOOL_NAME = "RAG"

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")


def get_available_tools() -> list:
    """仅暴露 RAG 工具给大模型。"""
    if RAG_TOOL_NAME not in TOOL_REGISTRY:
        raise RuntimeError(f"未找到 {RAG_TOOL_NAME} 工具，请检查 Tools_Registry/my_tools.py")
    return [TOOL_REGISTRY[RAG_TOOL_NAME]["schema"]]


async def safe_execute_tool(tool_call, current_messages: list) -> dict:
    """安全地执行单个工具，并返回标准化格式。"""
    function_name = tool_call.function.name

    try:
        function_args = json.loads(tool_call.function.arguments)
    except json.JSONDecodeError:
        function_args = {}

    print(f"[Async] 开始执行工具: {function_name}, 参数: {function_args}")

    if function_name in TOOL_REGISTRY:
        try:
            function_args["agent_messages"] = current_messages
            exec_result = TOOL_REGISTRY[function_name]["execute"](**function_args)

            if inspect.iscoroutine(exec_result):
                tool_result = await exec_result
            else:
                tool_result = exec_result
        except Exception as e:
            tool_result = f"工具执行时发生未捕获异常: {str(e)}"
    else:
        tool_result = f"Error: 找不到名为 {function_name} 的工具。"

    print(f"[Async] 工具 {function_name} 执行完毕")

    return {
        "role": "tool",
        "tool_call_id": tool_call.id,
        "name": function_name,
        "content": str(tool_result),
    }


def _skipped_rag_tool_message(tool_call, *, scope: str = "step") -> dict:
    """重复 RAG 调用的占位 tool 结果，满足 OpenAI 工具协议。"""
    if scope == "session":
        knowledge_content = (
            "本对话已执行过一次 RAG 检索，请勿再次调用。"
            "请基于已有检索结果作答；若信息不足，请如实说明。"
        )
    else:
        knowledge_content = (
            "本轮已执行过一次 RAG 检索，请勿重复调用。"
            "请基于第一次检索结果作答。"
        )
    return {
        "role": "tool",
        "tool_call_id": tool_call.id,
        "name": tool_call.function.name,
        "content": json.dumps({
            "status": "skipped",
            "retrieved_parent_ids": [],
            "knowledge_content": knowledge_content,
        }, ensure_ascii=False),
    }


async def execute_tool_calls(
    tool_calls: list,
    current_messages: list,
    *,
    rag_used_in_session: bool,
) -> tuple[list[dict], bool]:
    """执行 tool_calls；同轮内、整段对话内 RAG 均最多真正执行一次。

    Returns:
        (tool 结果列表, 本轮是否实际执行了 RAG)
    """
    results: list[dict] = []
    rag_used_this_step = False
    rag_executed_now = False

    for tool_call in tool_calls:
        if tool_call.function.name == RAG_TOOL_NAME:
            if rag_used_in_session:
                print(
                    f"[Async] 跳过对话级重复 RAG, 参数: {tool_call.function.arguments}"
                )
                results.append(_skipped_rag_tool_message(tool_call, scope="session"))
                continue
            if rag_used_this_step:
                print(f"[Async] 跳过本轮重复 RAG, 参数: {tool_call.function.arguments}")
                results.append(_skipped_rag_tool_message(tool_call, scope="step"))
                continue
            rag_used_this_step = True

        try:
            result = await safe_execute_tool(tool_call, current_messages)
            results.append(result)
            if tool_call.function.name == RAG_TOOL_NAME:
                rag_executed_now = True
        except Exception as exc:
            results.append({
                "role": "tool",
                "tool_call_id": tool_call.id,
                "name": tool_call.function.name,
                "content": f"系统级执行异常: {str(exc)}",
            })

    return results, rag_executed_now


async def run_agent_async(
    messages: list,
    http_client: httpx.AsyncClient,
    max_steps: int = 5,
    rag_trace: list | None = None,
):
    """
    运行 Agent 循环，直接使用传入的消息列表作为初始对话上下文。
    :param messages: 初始消息列表，应包含 system、user 等历史消息。
    :param http_client: HTTP 客户端
    :param max_steps: 最大迭代步数
    """
    client = AsyncOpenAI(
        api_key=DEEPSEEK_API_KEY,
        base_url=DEEPSEEK_BASE_URL,
        http_client=http_client,
    )
    available_tools = get_available_tools()
    current_messages = messages[:]
    rag_used_in_session = False

    step = 0
    while step < max_steps:
        step += 1
        yield f"data: [系统] Agent 开始第 {step} 轮思考...\n\n"

        request_kwargs: dict = {
            "model": MODEL_NAME,
            "messages": current_messages,
        }
        if not rag_used_in_session:
            request_kwargs["tools"] = available_tools
            request_kwargs["tool_choice"] = "auto"

        response = await client.chat.completions.create(**request_kwargs)

        response_message = response.choices[0].message
        assistant_msg = response_message.model_dump(exclude_none=True)

        if assistant_msg.get("tool_calls"):
            for tc in assistant_msg["tool_calls"]:
                tc["type"] = "function"

        if "content" not in assistant_msg or assistant_msg.get("content") is None:
            assistant_msg["content"] = ""

        current_messages.append(assistant_msg)

        if response_message.tool_calls:
            tool_calls = response_message.tool_calls
            tool_names = [tc.function.name for tc in tool_calls]
            rag_count = sum(1 for name in tool_names if name == RAG_TOOL_NAME)

            if rag_count > 1:
                yield "data: [系统] Agent 请求多次 RAG，本轮仅执行第一次检索...\n\n"
            elif rag_used_in_session and RAG_TOOL_NAME in tool_names:
                yield "data: [系统] 本对话已检索过，忽略重复 RAG 请求...\n\n"
            else:
                yield f"data: [系统] Agent 正在调用工具: {', '.join(tool_names)}...\n\n"

            results, rag_executed = await execute_tool_calls(
                tool_calls,
                current_messages=current_messages,
                rag_used_in_session=rag_used_in_session,
            )
            if rag_executed:
                rag_used_in_session = True

            for res in results:
                current_messages.append(res)
                if rag_trace is not None and res.get("name") == RAG_TOOL_NAME:
                    try:
                        payload = json.loads(res.get("content", "{}"))
                    except json.JSONDecodeError:
                        payload = {}
                    rag_trace.append({
                        "retrieved_parent_ids": payload.get("retrieved_parent_ids", []),
                        "status": payload.get("status", "unknown"),
                    })

            continue

        print("\n=== Agent 最终回答 ===")
        yield "data: ✅ [系统] 思考完毕，开始输出最终答案：\n<br><br>\n\n"

        final_text = response_message.content
        for char in final_text:
            if char == "\n":
                yield "data: <br>\n\n"
            else:
                yield f"data: {char}\n\n"
            await asyncio.sleep(0.01)

        yield "data: [DONE]\n\n"
        return

    yield f"data: ⚠️ [系统保护] 触发熔断：达到最大步数 ({max_steps})。\n\n"
    yield "data: [DONE]\n\n"
