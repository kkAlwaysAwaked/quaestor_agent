import asyncio
from typing import List, Dict, Any
from .Query_and_HyDE import generate_hyde_vector
from .Qdrant_Search_Dense import qdrant_search_dense
from .Qdrant_Search_Sparse import qdrant_search_sparse
from .map_to_parent_and_rrf import map_to_parent_and_rrf
from .Docs_for_Reranker import fetch_parent_docs_by_ids
from .Reranker_Model import rerank_documents

# 检索与精排参数
RECALL_TOP_K = 10       # Dense / Sparse 每路召回子文档数
RERANK_CANDIDATE_K = 15 # 送入 Cross-Encoder 的父文档候选数（RRF 排序后截取）
RERANK_TOP_K = 5        # Cross-Encoder 精排后返回的父文档数

# Config 数据库来源

async def search_internal_docs(
    messages: List[Dict[str, str]],
    search_query: str | None = None,
) -> List[Dict[str, Any]]:
    """
    在内部知识库中搜索技术文档、API参考和代码示例。
    当用户询问内部框架、具体技术实现细节或需要事实性文档时，请调用此工具。

    Args:
        messages: The full conversation history. A list of message dictionaries,
                  where each dict has 'role' (e.g., 'user', 'assistant') and 'content' keys.

    Returns:
        A list containing the most relevant document chunks.
    """
    print(f"[Tool Calling] 接收到上下文，正在解析意图并搜索内部文档...")

    # 1. Query Transformation (Rewrite & HyDE)
    # 传入完整的对话上下文，交由内部模块去理解多轮对话、补全指代
    optimized_query_vector = await generate_hyde_vector(
        messages=messages,
        search_query=search_query,
    )
    rewritten_query = optimized_query_vector["rewritten_query"]
    sparse_fallback_query = optimized_query_vector.get("sparse_fallback_query", "")
    hyde_doc = optimized_query_vector["hyde_document"]
    standalone_question = optimized_query_vector["standalone_question"]

    print(f"[RAG_for_FunctionCalling] Sparse 关键词: {rewritten_query}")
    if sparse_fallback_query:
        print(f"[RAG_for_FunctionCalling] Sparse 补充: {sparse_fallback_query}")
    print(f"[RAG_for_FunctionCalling] Dense HyDE（{len(hyde_doc)} 字）: {hyde_doc[:100]}{'...' if len(hyde_doc) > 100 else ''}")

    # 2. 双路查询 (Dense + Sparse) - 并发执行；Sparse 可叠加 Agent query 与补充 query
    print("[RAG_for_FunctionCalling] 正在并发执行 Dense 和 Sparse 双路检索...")
    sparse_tasks = [qdrant_search_sparse(rewritten_query, limit=RECALL_TOP_K)]
    extra_sparse: list = []
    if sparse_fallback_query:
        sparse_tasks.append(qdrant_search_sparse(sparse_fallback_query, limit=RECALL_TOP_K))

    dense_child_results = await qdrant_search_dense(hyde_doc, limit=RECALL_TOP_K)
    sparse_results = await asyncio.gather(*sparse_tasks)
    sparse_child_results = sparse_results[0]
    if len(sparse_results) > 1:
        extra_sparse = [sparse_results[1]]
    # return formatted_results = [
    #     {
    #         "id": point.id,  # 子文档的 ID（用于 RRF 打分）
    #         "parent_id": point.payload["parent_id"],
    #     }
    #     for point in raw_results
    # ]

    # 3. Small-to-Big Mapping & RRF Fusion
    print("[RAG_for_FunctionCalling] 正在执行父文档映射与 RRF 融合...")
    fused_parents = map_to_parent_and_rrf(
        dense_child_results,
        sparse_child_results,
        k=60,
        extra_sparse_results=extra_sparse or None,
    )
    # 传回来了 parent_id 和 得分
    # return fused_parents =
    # [
    #     ("parent_doc_id_001", 0.03278),  # (父文档的 ID, 融合后的 RRF 得分)
    #     ("parent_doc_id_042", 0.03154),
    #     ("parent_doc_id_017", 0.01639),
    #     ...
    # ]

    # 4. 去数据库里查找完整父文档，传给Reranker
    # docs_for_reranker.append({
    #     "id": pid,
    #     "text": doc_info.get("content", ""),  # 提取真实文本供 Reranker 评估
    #     "metadata": doc_info.get("source", ""),
    #     "rrf_score": rrf_score  # 保留初筛分数（可选）
    # })
    final_docs = fetch_parent_docs_by_ids(fused_parents[:RERANK_CANDIDATE_K])

    # 5. Cross-Encoder Reranking
    print("[RAG_for_FunctionCalling] 正在执行 Cross-Encoder 重排序...")
    top_k_docs = rerank_documents(standalone_question, final_docs, top_k=RERANK_TOP_K)
    recalled_ids = [doc["parent_id"] for doc in top_k_docs]
    print(f"[RAG_for_FunctionCalling] 精排 Top-{RERANK_TOP_K} 父文档 ID: {recalled_ids}")
    print(f"[RAG_for_FunctionCalling] 搜索完成，返回 {len(top_k_docs)} 条高相关性父文档（精排 Top-{RERANK_TOP_K}）。")
    return top_k_docs
