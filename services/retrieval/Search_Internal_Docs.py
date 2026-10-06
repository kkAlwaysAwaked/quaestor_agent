"""保留改写、双路召回、父块映射、RRF 和重排的检索算法编排。"""

from __future__ import annotations

import asyncio
from time import perf_counter

from core.index_config import DENSE_MODEL, RERANKER_MODEL, SPARSE_MODEL
from core.retrieval_contracts import RetrievalScope
from services.retrieval.Docs_for_Reranker import fetch_parent_docs_by_ids
from services.retrieval.Qdrant_Search_Dense import qdrant_search_dense
from services.retrieval.Qdrant_Search_Sparse import qdrant_search_sparse
from services.retrieval.Reranker_Model import rerank_documents
from services.retrieval.map_to_parent_and_rrf import map_to_parent_and_rrf


RECALL_TOP_K = 10
RERANK_CANDIDATE_K = 15
RERANK_TOP_K = 5


class RetrievalPipeline:
    # 作用：注入客户端、模型和会话工厂，并限制同时运行的检索流水线。
    def __init__(self, *, sessions, client, models_runtime, compute, transformer, concurrency: int = 2) -> None:
        self.sessions = sessions
        self.client = client
        self.models = models_runtime
        self.compute = compute
        self.transformer = transformer
        self.slots = asyncio.Semaphore(concurrency)

    # 作用：在同一授权范围内并发调度召回，经过父块复核和受限重排后返回结果及 trace。
    async def search(self, messages: list, search_query: str, scope: RetrievalScope) -> tuple[list[dict], dict]:
        started = perf_counter()
        trace = {"models": {"dense": DENSE_MODEL, "sparse": SPARSE_MODEL, "reranker": RERANKER_MODEL}, "version_ids": list(scope.version_ids)}
        async with self.slots:
            if not scope.version_ids:
                return [], {**trace, "empty_scope": True, "total_ms": round((perf_counter() - started) * 1000)}
            inputs = await self.transformer.transform(messages, search_query)
            trace["query_transformation"] = inputs
            stage = perf_counter()
            queries = [inputs["rewritten_query"]]
            if inputs["sparse_fallback_query"]:
                queries.append(inputs["sparse_fallback_query"])
            async with asyncio.TaskGroup() as tasks:
                dense_task = tasks.create_task(qdrant_search_dense(
                    inputs["hyde_document"], client=self.client, models_runtime=self.models,
                    compute=self.compute, scope=scope, limit=RECALL_TOP_K,
                ))
                sparse_tasks = [tasks.create_task(qdrant_search_sparse(
                    query, client=self.client, models_runtime=self.models,
                    compute=self.compute, scope=scope, limit=RECALL_TOP_K,
                )) for query in queries]
            dense = dense_task.result()
            sparse_routes = [task.result() for task in sparse_tasks]
            trace["recall_ms"] = round((perf_counter() - stage) * 1000)
            fused = map_to_parent_and_rrf(dense, sparse_routes[0], extra_sparse_results=sparse_routes[1:])
            candidates = await fetch_parent_docs_by_ids(fused[:RERANK_CANDIDATE_K], sessions=self.sessions, scope=scope)
            candidate_ids = {document["id"] for document in candidates}
            trace["dense_hits"] = [hit for hit in dense if hit["parent_id"] in candidate_ids]
            trace["sparse_hits"] = [[hit for hit in route if hit["parent_id"] in candidate_ids] for route in sparse_routes]
            stage = perf_counter()
            documents = await self.compute.run(
                rerank_documents, inputs["standalone_question"], candidates,
                model=self.models.reranker, top_k=RERANK_TOP_K,
            )
            trace["rerank_ms"] = round((perf_counter() - stage) * 1000)
            # 重排期间可能发布新版本，返回前再次丢弃已失去当前版本身份的父块。
            current = await fetch_parent_docs_by_ids(
                [(document["parent_id"], document["rrf_score"]) for document in documents],
                sessions=self.sessions, scope=scope,
            )
            valid_ids = {document["id"] for document in current}
            documents = [document for document in documents if document["parent_id"] in valid_ids]
            trace["candidate_parent_ids"] = [document["id"] for document in candidates]
            trace["retrieved_parent_ids"] = [document["parent_id"] for document in documents]
            trace["total_ms"] = round((perf_counter() - started) * 1000)
            return documents, trace


# 作用：保留原检索函数名作为显式依赖入口，调用者必须提供已授权的运行时和范围。
async def search_internal_docs(messages: list, search_query: str, *, pipeline: RetrievalPipeline, scope: RetrievalScope) -> list[dict]:
    documents, _ = await pipeline.search(messages, search_query, scope)
    return documents
