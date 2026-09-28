# 实现了 Rewrite 和 HyDE 逻辑
import httpx

from core.config import load_model_settings

REWRITE_SYSTEM_PROMPT = """你是稀疏检索关键词提取器。
根据对话历史和当前检索意图，输出用于 SPLADE 稀疏检索的关键词。

要求：
- 只输出关键词或短语，用空格分隔；不要完整句子，不要标点结尾
- 保留专有名词、编号、日期、门店名、产品名、人名
- 不要回答问题，不要输出「我来查询」「根据资料」等口语
- 关键词数量控制在 5-12 个"""

HYDE_MAX_CHARS = 120

HYDE_SYSTEM_PROMPT = f"""你是企业内部知识库文档撰写助手。
请根据用户问题，写一段可能出现在内部周报/SOP/复盘报告中的客观陈述，用于语义向量检索。

要求：
- 只写一段连续正文，不要标题、不要 Markdown、不要列表编号
- 不要对话开场白，不要写「根据您的问题」
- 不要拒答或下结论；用「相关记录涉及…」「字段包括…」等文档式表述
- 不要编造具体电话号码、金额、人名、日期；只描述可能涉及的字段名或业务主题
- 严格控制在 {HYDE_MAX_CHARS} 字以内，越短越好"""


def extract_dialog_messages(messages: list) -> list[dict[str, str]]:
    """只保留 user/assistant 自然语言轮次，排除 system、tool 及空 assistant。"""
    dialog: list[dict[str, str]] = []
    for msg in messages:
        role = msg.get("role")
        if role not in ("user", "assistant"):
            continue
        content = (msg.get("content") or "").strip()
        if role == "assistant" and not content:
            continue
        dialog.append({"role": role, "content": content})
    return dialog


def resolve_query_inputs(
    messages: list,
    search_query: str | None = None,
) -> tuple[list[dict[str, str]], str, str]:
    """
    解析改写所需输入。

    Returns:
        chat_history: 用于指代消解的历史 user/assistant 轮次
        latest_query: 当前检索意图（优先 Agent 传入的 search_query）
        hyde_question: 用于 HyDE / 精排的完整用户问题（取最后一条 user）
    """
    dialog = extract_dialog_messages(messages)

    last_user = ""
    for msg in reversed(dialog):
        if msg["role"] == "user":
            last_user = msg["content"]
            break

    if search_query and search_query.strip():
        latest_query = search_query.strip()
        chat_history = dialog
    elif last_user:
        latest_query = last_user
        last_user_idx = next(
            i for i in range(len(dialog) - 1, -1, -1) if dialog[i]["role"] == "user"
        )
        chat_history = dialog[:last_user_idx]
    else:
        latest_query = ""
        chat_history = dialog

    hyde_question = last_user or latest_query
    return chat_history, latest_query, hyde_question


async def rewrite_query(chat_history: list, latest_query: str) -> str | None:
    """将检索意图改写为稀疏检索关键词（供 SPLADE 路使用）。"""
    model_settings = load_model_settings()
    history_str = "\n".join(
        f"{msg['role']}: {msg['content']}" for msg in chat_history
    ) or "（无）"
    user_content = f"历史对话：\n{history_str}\n\n当前检索意图：{latest_query}"

    headers = {
        "Authorization": f"Bearer {model_settings.api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": "deepseek-chat",
        "messages": [
            {"role": "system", "content": REWRITE_SYSTEM_PROMPT},
            {"role": "user", "content": user_content},
        ],
        "temperature": 0.0,
    }

    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(
                f"{model_settings.base_url.rstrip('/')}/chat/completions",
                headers=headers,
                json=payload,
                timeout=10.0,
            )
            response.raise_for_status()
            data = response.json()
            return data["choices"][0]["message"]["content"].strip()
        except httpx.HTTPError:
            print("[Query_and_HyDE] 关键词改写网络请求失败")
            return None


def truncate_hyde_document(text: str, max_chars: int = HYDE_MAX_CHARS) -> str:
    """硬截断 HyDE 文本，优先在句末标点处截断。"""
    text = " ".join(text.split())
    if len(text) <= max_chars:
        return text

    clipped = text[:max_chars]
    for sep in ("。", "；", "，", ".", ";", ","):
        pos = clipped.rfind(sep)
        if pos >= max_chars // 2:
            return clipped[: pos + 1].strip()
    return clipped.strip()


async def generate_hyde_document(question: str) -> str:
    """生成假设性文档段落（供 Dense 路使用）。"""
    model_settings = load_model_settings()
    headers = {
        "Authorization": f"Bearer {model_settings.api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": "deepseek-chat",
        "messages": [
            {"role": "system", "content": HYDE_SYSTEM_PROMPT},
            {"role": "user", "content": question},
        ],
        "temperature": 0.3,
        "max_tokens": 160,
    }

    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(
                f"{model_settings.base_url.rstrip('/')}/chat/completions",
                headers=headers,
                json=payload,
                timeout=15.0,
            )
            response.raise_for_status()
            data = response.json()
            raw = data["choices"][0]["message"]["content"].strip()
            return truncate_hyde_document(raw)
        except httpx.HTTPError as e:
            print(f"[Query_and_HyDE] HyDE 生成失败: {e}")
            return ""


async def generate_hyde_vector(
    messages: list,
    search_query: str | None = None,
) -> dict[str, str]:
    """
    从对话上下文提取检索输入，生成 Sparse 关键词与 Dense HyDE 文档。
    """
    if not messages:
        return {
            "rewritten_query": "",
            "sparse_fallback_query": "",
            "hyde_document": "",
            "standalone_question": "",
        }

    chat_history, latest_query, hyde_question = resolve_query_inputs(
        messages, search_query=search_query
    )

    print(
        f"[Query_and_HyDE] 提取完成 | latest_query={latest_query!r} | "
        f"hyde_question={hyde_question!r} | history轮次={len(chat_history)}"
    )

    # Agent 已传入结构化检索词时，直接用于 Sparse，避免二次 LLM 改写漂移
    if search_query and search_query.strip() and len(search_query.strip()) >= 6:
        rewritten_query = search_query.strip()
    else:
        rewritten_query = await rewrite_query(
            chat_history=chat_history, latest_query=latest_query
        )
        if not rewritten_query:
            rewritten_query = latest_query

    # 第二路 Sparse：用完整 user 问题补充（与 Agent 关键词形成互补）
    sparse_fallback_query = ""
    if hyde_question and hyde_question.strip() != rewritten_query.strip():
        sparse_fallback_query = hyde_question.strip()

    hyde_doc = await generate_hyde_document(question=hyde_question)
    if not hyde_doc:
        hyde_doc = truncate_hyde_document(hyde_question)

    print(f"[Query_and_HyDE] Sparse 关键词: {rewritten_query}")
    if sparse_fallback_query:
        print(f"[Query_and_HyDE] Sparse 补充关键词: {sparse_fallback_query}")
    print(f"[Query_and_HyDE] HyDE 文档（{len(hyde_doc)} 字）: {hyde_doc}")

    return {
        "rewritten_query": rewritten_query,
        "sparse_fallback_query": sparse_fallback_query,
        "hyde_document": hyde_doc,
        "standalone_question": hyde_question,
    }
