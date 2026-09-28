# 注册并存放工具
import json
import traceback

from .tool_registry import register_tool
from RAG_for_FunctionCalling.Query_and_HyDE import extract_dialog_messages


def simple_context_trimmer(messages: list, max_chars: int = 6000) -> list:
    """从最新消息向前保留 user/assistant 对话，直到达到字符上限。"""
    total = 0
    trimmed = []
    for msg in reversed(messages):
        content = msg.get("content", "") or ""
        total += len(content)
        trimmed.insert(0, msg)
        if total >= max_chars:
            break
    return trimmed


@register_tool
async def RAG(query: str, agent_messages: list = None) -> str:
    """
    【知识库检索工具】
    当你需要回答关于内部文档、业务规定、项目细节等你不确定的事实性问题时，调用此工具。
    工具会基于混合检索引擎返回最相关的参考资料。

    Args:
        query (str): 根据用户提问和前文语境，提取出的独立、完整的搜索关键词。
                     请消除指代不明（例如将「它怎么用」改写为「XX 系统功能使用说明」）。
    """
    from RAG_for_FunctionCalling.Search_Internal_Docs import search_internal_docs

    print(f"\n[Tool Calling] 正在触发 RAG，核心检索词: '{query}'")

    raw_messages = agent_messages or [{"role": "user", "content": query}]
    dialog_messages = extract_dialog_messages(raw_messages)
    if not dialog_messages:
        dialog_messages = [{"role": "user", "content": query}]
    concise_messages = simple_context_trimmer(messages=dialog_messages, max_chars=6000)

    try:
        results = await search_internal_docs(
            messages=concise_messages,
            search_query=query,
        )

        if not results:
            return json.dumps({
                "status": "empty",
                "retrieved_parent_ids": [],
                "knowledge_content": "知识库中未检索到相关内容，请尝试更换搜索词或如实告诉用户。",
            }, ensure_ascii=False)

        parent_ids = [
            doc.get("parent_id") for doc in results if isinstance(doc, dict) and doc.get("parent_id")
        ]
        print(f"[Tool Calling] RAG 召回父文档 ID: {parent_ids}")

        formatted_result = "以下是为你检索到的参考资料：\n"
        for i, doc in enumerate(results):
            content = ""
            source = "未知来源"

            if isinstance(doc, str):
                content = doc.strip()
            elif isinstance(doc, dict):
                content = doc.get("content", doc.get("text", "")).strip()
                if "source" in doc:
                    source = doc["source"]
                elif "metadata" in doc:
                    if isinstance(doc["metadata"], dict):
                        source = doc["metadata"].get("source", "未知来源")
                    else:
                        source = str(doc["metadata"])

            if not content:
                continue

            formatted_result += f"【参考资料 {i + 1}】(来源: {source})\n{content}\n---\n"

        return json.dumps({
            "status": "success",
            "doc_count": len(results),
            "retrieved_parent_ids": [
                doc.get("parent_id") for doc in results if isinstance(doc, dict) and doc.get("parent_id")
            ],
            "knowledge_content": formatted_result,
        }, ensure_ascii=False)

    except Exception as e:
        print("\n" + "=" * 50)
        traceback.print_exc()
        print("=" * 50 + "\n")

        return json.dumps({
            "status": "error",
            "knowledge_content": f"检索工具内部发生异常: {str(e)}",
        }, ensure_ascii=False)
